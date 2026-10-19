'use strict';
/**
 * Jádro klienta lokálního spojení wattery pro Node-RED na Victron GX.
 *
 * Port `wattery-ha/custom_components/wattery/edge.py` (EdgeRunner, protokol 2) pro přenos mqtt — držet stejně:
 *   POST /api/edge/v1/pair  {code, kind, sn, clientVer, label, caps}  → {token, readPlan, nextPollS, profile?}
 *   POST /api/edge/v1/sync  (Bearer) {client: {ver, backend, ctl, caps}, reads, results, leaseReport?} →
 *                           {readPlan, nextPollS, commands, control, accepted, profile?, leases, leaseAck?, serverTime}
 * Co a jak často číst, určuje server (`readPlan`, položky `type: 'mqtt'`).
 *
 * Řízení (od 0.2): příkaz `mqtt` {id, dev, op: 'read', paths} provede uzel vždy; {op: 'write', values: [[cesta,
 * hodnota], …]} jen se zapnutým přepínačem „Řízení z wattery“ (výchozí vypnuto), celou dávku v profilu zápisu, který
 * určí server (`profile`) — jen vestavěný a jen když identita GX (portal ID) sedí na spárovanou —, v limitech profilu
 * (za minutu / hodinu, trvalá nastavení za den, odstup WRITE_GAP_S). Výsledek {id, ok, echo: {cesta: hodnota} | err}
 * (ozvěnu N/ po zápisu posoudí server) jde serveru hned dalším sync. Bez profilu (null) = jen čtení.
 *
 * Pojistky (`leases`, caps.features 'lease'): úplný seznam platných pojistek od serveru. Do platnosti uzel opakuje
 * `refresh.cmds` á `everyS` (i bez spojení se serverem; trvalá nastavení se neobnovují); když platnost uplyne a server
 * se neozývá (síť, 5xx — ne odmítnutí 4xx), provede jednou `onExpire` a po obnovení spojení pošle `leaseReport`.
 * Pojistku, která v seznamu chybí, zahodí bez onExpire. Stav pojistek se ukládá (`onPersist`, přežije restart).
 */
const P = require('./profile');

const PAIR_PATH = '/api/edge/v1/pair';
const SYNC_PATH = '/api/edge/v1/sync';
const RETRY_FAILED_READ_S = 30;
const HEARTBEAT_S = 30;
const POLL_MIN_S = 5;
const POLL_MAX_S = 300;
const BACKOFF_S = 300;
const FAST_POLL_S = 1.5;
const FAST_WINDOW_S = 20;
const WRITE_GAP_S = 1.0;
const RATE_PER_MIN = 15;
const RATE_PER_H = 60;
const MAX_COMMANDS = 8;
const MAX_RESULTS = 16;
const KEEP_RESULTS = 32;
const SEEN_IDS = 64;
const FLUSH_ROUNDS = 3;
const PROTO = 2;
const FEATURES = ['lease'];
const MAX_READS = 16;
const READS_BUDGET = 48 * 1024;
const MQTT_MAX_PATHS = 1000;
const LEASE_MAX = 4;
const LEASE_MAX_CMDS = 6;
const REFRESH_MIN_S = 30;
const EXPIRE_TTL_S = 120;              // příkazy pojistky jdou po sobě s odstupem WRITE_GAP_S — žádný „neprošlý“
const EXPIRE_TRIES = 3;
const EXPIRE_RETRY_S = 300;
const SAVE_S = 300;                    // platnost se posouvá každým sync; ukládat ji nejvýš jednou za 5 min

const STARTING = 'starting';
const ONLINE = 'online';
const ERROR = 'error';
const REVOKED = 'revoked';

const PROFILES = P.loadAll();

class Revoked extends Error {}

const shortStr = (v, n) => typeof v === 'string' && v.length > 0 && v.length <= n;
const isNum = (v) => typeof v === 'number' && Number.isFinite(v);

/** `caps` pro pair i každý sync: přenos mqtt, vestavěné profily zápisu, pojistky. */
function clientCaps() {
  return { proto: PROTO, transports: ['mqtt'], profiles: Object.keys(PROFILES).sort(), features: [...FEATURES] };
}

/** Z plánu serveru jen položky mqtt, které umíme (stejně jako edge.py clean_items). */
function cleanItems(plan) {
  const out = [];
  for (const b of Array.isArray(plan) ? plan : []) {
    if (!b || typeof b !== 'object' || b.type !== 'mqtt') continue;
    const every = Math.max(5, parseInt(b.everyS === undefined ? 60 : b.everyS, 10));
    if (!Number.isFinite(every) || !shortStr(b.name, 40)) continue;
    const paths = b.paths;
    if (!Array.isArray(paths) || paths.length < 1 || paths.length > MQTT_MAX_PATHS
        || !paths.every((p) => shortStr(p, 200))) continue;
    const it = { type: 'mqtt', name: b.name, everyS: every, paths: [...paths] };
    for (const k of ['dev', 'base']) if (shortStr(b[k], 120)) it[k] = b[k];
    out.push(it);
  }
  return out;
}

/** Čtení → dávky pod strop těla sync (nejvýš maxN položek); položka větší než strop se zahodí. */
function splitReads(reads, budget = READS_BUDGET, maxN = MAX_READS, log = () => {}) {
  const out = [];
  let cur = [];
  let size = 0;
  for (const rd of reads) {
    const n = JSON.stringify(rd).length;
    if (n > budget) { log(`čtení ${rd.name} má ${n} B (víc než ${budget}) — neposílám`); continue; }
    if (cur.length && (size + n > budget || cur.length >= maxN)) { out.push(cur); cur = []; size = 0; }
    cur.push(rd);
    size += n;
  }
  if (cur.length) out.push(cur);
  return out;
}

const clampPoll = (v) => {
  const x = Number(v);
  return Number.isFinite(x) ? Math.min(POLL_MAX_S, Math.max(POLL_MIN_S, x)) : 10;
};

const ttlOf = (cmd) => {
  const x = Number(cmd.expiresInS || 10);
  return Number.isFinite(x) ? x : 10;
};

class EdgeClient {
  /**
   * @param post      async (path, body, token|null) → [httpCode, json]
   * @param transport GxMqtt (readItem, setFilters, portal, write)
   * @param sn        spárované portal ID (identita pro profil zápisu); bez něj portal z párování
   * @param state     uložený stav (profil, pojistky, hlášení) z `onPersist`
   */
  constructor({ post, transport, version, backend = 'nodered', clock = () => Date.now() / 1000,
                log = () => {}, onStatus = () => {}, sn = null, state = null, onPersist = null,
                sleep = (s) => new Promise((r) => setTimeout(r, s * 1000)) }) {
    this._post = post;
    this._tr = transport;
    this._version = version;
    this._backend = backend;
    this._clock = clock;
    this._log = log;
    this._onStatus = onStatus;
    this._onPersist = onPersist;
    this._sleep = sleep;
    this._sn = sn ? String(sn) : null;
    this.caps = clientCaps();
    this.token = null;
    this.items = [];
    this.nextPollS = 10;
    this._filtersDirty = false;
    this._lastRead = new Map();
    this._lastSync = 0;
    this._backoffUntil = 0;
    this._fastUntil = 0;
    this._pending = [];
    this._done = new Map();
    this.status = STARTING;
    this.lastError = null;
    this.lastSyncAt = null;
    this.lastAccepted = null;
    this.readErrors = 0;
    this.commandsDone = 0;
    this.lastCommand = null;
    this.serverControl = null;
    // řízení
    this.controlEnabled = false;       // přepínač „Řízení z wattery“ (uzel, výchozí vypnuto)
    this.profileId = null;             // profil zápisu od serveru (`profile`); null = jen čtení
    this._verified = {};               // profil → identita GX sedí (true / false)
    this._writes = [];                 // časy zápisů (limit za minutu / hodinu)
    this._persistWrites = [];          // časy zápisů trvalých nastavení (limits.persistPerDay)
    this._lastWrite = 0;
    this._syncFailed = false;          // poslední sync nedošel (síť / 5xx) — jen tehdy onExpire
    // pojistky: id → {id, source, onExpire, refresh, deadline, appliedAt, tries, retryAt, nextRefresh}
    this.leases = {};
    this._leaseReports = [];
    this._leaseSaved = {};
    this.refreshes = 0;
    this._loadState(state || {});
  }

  // ── stav (přežije restart) ──
  _loadState(st) {
    if (typeof st.profile === 'string' && PROFILES[st.profile]) this.profileId = st.profile;
    for (const x of Array.isArray(st.leases) ? st.leases : []) {
      const lease = this._cleanLease(x, false);
      if (!lease || !isNum(x.deadline)) continue;
      Object.assign(lease, { deadline: x.deadline, appliedAt: isNum(x.appliedAt) ? x.appliedAt : null, tries: 0,
                             retryAt: 0, nextRefresh: 0 });
      this.leases[lease.id] = lease;
    }
    this._leaseReports = (Array.isArray(st.leaseReports) ? st.leaseReports : [])
      .filter((r) => r && typeof r.id === 'string').slice(0, LEASE_MAX);
    this._leaseSaved = Object.fromEntries(Object.values(this.leases).map((x) => [x.id, x.deadline]));
  }

  state() {
    return {
      profile: this.profileId,
      leaseReports: [...this._leaseReports],
      leases: Object.values(this.leases).map((x) => ({ id: x.id, source: x.source, onExpire: x.onExpire,
                                                       refresh: x.refresh, deadline: x.deadline, appliedAt: x.appliedAt })),
    };
  }

  _persist() {
    this._leaseSaved = Object.fromEntries(Object.values(this.leases).map((x) => [x.id, x.deadline]));
    if (!this._onPersist) return;
    try { this._onPersist(this.state()); } catch (e) { this._log(`uložení stavu pojistek selhalo: ${e.message}`); }
  }

  /** Párování kódem z aplikace wattery: identita = portal ID z GX (transport už musí být připojený). */
  async pair(code, { kind = 'nodered', label = 'Node-RED na GX' } = {}) {
    const sn = this._tr.portal;
    if (!sn) throw new Error('portal ID GX zatím neznám — je GX připojené?');
    const [st, res] = await this._post(PAIR_PATH, { code, kind, sn, clientVer: this._version, label, caps: this.caps }, null);
    if (st !== 200 || !res || !res.token) {
      throw new Error((res && (res.message || res.error)) || `párování selhalo (HTTP ${st})`);
    }
    this._sn = sn;
    this.start(res.token, res.readPlan, res.nextPollS);
    this._acceptProfile(res);
    return res;
  }

  /** Start s uloženým tokenem (po restartu) nebo hned po párování. */
  start(token, readPlan, nextPollS) {
    this.token = token;
    this.items = cleanItems(readPlan);
    this._filtersDirty = this.items.length > 0;
    this.nextPollS = clampPoll(nextPollS === undefined ? 10 : nextPollS);
    this._lastSync = 0;
    if (!this._sn && this._tr.portal) this._sn = String(this._tr.portal);
  }

  /** Po znovupřipojení GX nový přenos: stav klienta zůstává, odběr se přihlásí znovu, identita se ověří znovu. */
  setTransport(transport) {
    this._tr = transport;
    this._filtersDirty = this.items.length > 0;
    this._verified = {};
  }

  /** Přepínač „Řízení z wattery“. Změnu nahlásit serveru hned v příštím kole (směruje podle ní příkazy). */
  setControl(enabled) {
    if (!!enabled !== this.controlEnabled) {
      this.controlEnabled = !!enabled;
      this._lastSync = 0;
    }
  }

  _set(status, error = null) {
    this.status = status;
    this.lastError = error;
    try { this._onStatus(this); } catch (e) { this._log(`chyba při aktualizaci stavu: ${e.message}`); }
  }

  pollS() {
    return this._clock() < this._fastUntil ? FAST_POLL_S : this.nextPollS;
  }

  // ── profil zápisu ──
  _acceptProfile(res) {
    if (!res || !('profile' in res)) return;
    const pid = res.profile;
    const next = typeof pid === 'string' && PROFILES[pid] ? pid : null;
    if (typeof pid === 'string' && !PROFILES[pid]) this._log(`profil zápisu ${pid} v uzlu není — jen čtení`);
    if (next !== this.profileId) {
      this.profileId = next;
      this._persist();
    }
  }

  /** Profil od serveru platí, jen když portal ID GX sedí na spárované. */
  _ensureProfile() {
    const pid = this.profileId;
    if (!pid || this._verified[pid] !== undefined) return;
    const portal = this._tr && this._tr.connected !== false ? String(this._tr.portal || '') : '';
    if (!portal) return;                                // GX nepřipojené — ověří se v dalším kole
    const ok = !!(this._sn && portal === this._sn);
    this._verified[pid] = ok;
    if (!ok) this._log(`profil zápisu ${pid} nesedí na GX ${portal} (spárováno ${this._sn}) — zápisy odmítám`);
  }

  /** [zkompilovaný profil, důvod když není]. */
  _comp() {
    const pid = this.profileId;
    if (!pid) return [null, 'zařízení jen ke čtení (server neurčil profil zápisu)'];
    const comp = PROFILES[pid];
    if (!comp) return [null, `profil ${pid} v uzlu není`];
    if (!this._verified[pid]) return [null, `profil ${pid}: identita GX neověřená`];
    return [comp, null];
  }

  _writeCheck(cmd) {
    const [comp, why] = this._comp();
    return comp ? P.batchProblem(comp, cmd.values) : why;
  }

  _rateOk(comp, now) {
    const lim = (key, def) => {
      const v = parseInt((comp.limits || {})[key], 10);
      return Number.isFinite(v) && v > 0 ? Math.min(def, v) : def;
    };
    this._writes = this._writes.filter((t) => now - t < 3600);
    return this._writes.filter((t) => now - t < 60).length < lim('perMin', RATE_PER_MIN)
      && this._writes.length < lim('perHour', RATE_PER_H);
  }

  // ── pojistky ──
  /** Zápis z pojistky → příkaz bez id / expiresInS, nebo null (jiný tvar, jiné zařízení, mimo profil). */
  _cleanWrite(c, check = true) {
    if (!c || typeof c !== 'object' || c.type !== 'mqtt' || c.op !== 'write' || !Array.isArray(c.values)
        || !shortStr(c.dev, 120)) return null;
    if (this._sn && c.dev !== this._sn) return null;
    const out = { type: 'mqtt', dev: c.dev, op: 'write', values: c.values.map((x) => (Array.isArray(x) ? [...x] : x)) };
    return !check || this._writeCheck(out) === null ? out : null;
  }

  /** Pojistka z `leases` → {id, source, onExpire, refresh} nebo null. Projde jen celá; obnova bez trvalých nastavení. */
  _cleanLease(raw, check = true) {
    if (!raw || typeof raw !== 'object' || !shortStr(raw.id, 40)) return null;
    const cmds = (lst) => {
      if (!Array.isArray(lst) || lst.length < 1 || lst.length > LEASE_MAX_CMDS) return null;
      const out = lst.map((c) => this._cleanWrite(c, check));
      return out.some((c) => c === null) ? null : out;
    };
    const onExpire = cmds(raw.onExpire);
    if (!onExpire) return null;
    let refresh = null;
    if (raw.refresh !== undefined && raw.refresh !== null) {
      const r = raw.refresh;
      const rc = r && typeof r === 'object' ? cmds(r.cmds) : null;
      if (!rc || !isNum(r.everyS)) return null;
      const comp = PROFILES[this.profileId];
      if (comp && rc.some((c) => c.values.some(([p]) => P.persistent(comp, p)))) return null;
      refresh = { cmds: rc, everyS: Math.max(REFRESH_MIN_S, r.everyS) };
    }
    return { id: raw.id, source: String(raw.source || 'command').slice(0, 20), onExpire, refresh };
  }

  /** Blok `leases` = úplný seznam platných pojistek; co v něm chybí, uzel zahodí bez onExpire. */
  _acceptLeases(res, now) {
    if (!('leases' in res)) return;
    const st = res.serverTime;
    const offset = isNum(st) ? now - st : 0;
    const keep = {};
    for (const x of (Array.isArray(res.leases) ? res.leases : []).slice(0, LEASE_MAX)) {
      if (!x || typeof x !== 'object') continue;
      let lease = this._cleanLease(x);
      let deadline = null;
      if (lease && isNum(x.validUntil)) {
        deadline = x.validUntil + offset;
        if (x.hardUntil !== undefined && x.hardUntil !== null) deadline = isNum(x.hardUntil) ? Math.min(deadline, x.hardUntil + offset) : null;
      }
      if (deadline === null) lease = null;
      if (!lease) { this._log(`pojistka ${String(x.id).slice(0, 40)} ze serveru neprošla kontrolou — neukládám`); continue; }
      const old = this.leases[lease.id] || {};
      const first = lease.refresh ? now + lease.refresh.everyS : 0;
      Object.assign(lease, { deadline, appliedAt: old.appliedAt === undefined ? null : old.appliedAt, tries: old.tries || 0,
                             retryAt: old.retryAt || 0, nextRefresh: old.nextRefresh === undefined ? first : old.nextRefresh });
      keep[lease.id] = lease;
    }
    const ids = (o) => Object.keys(o).sort().join('\u0000');
    const changed = ids(keep) !== ids(this.leases)
      || Object.values(keep).some((x) => Math.abs(x.deadline - (this._leaseSaved[x.id] || 0)) >= SAVE_S);
    this.leases = keep;
    if (changed) this._persist();
  }

  /** Obnova příkazu: do platnosti pojistky opakovat zápis á everyS — i bez spojení se serverem (mimo limity). */
  async _maybeRefresh(now) {
    for (const lease of Object.values(this.leases)) {
      const ref = lease.refresh;
      if (!ref || now >= lease.deadline || now < lease.nextRefresh || !this.controlEnabled) continue;
      lease.nextRefresh = now + ref.everyS;
      const outs = [];
      for (const c of ref.cmds) outs.push(await this._execute({ ...c, expiresInS: EXPIRE_TTL_S }, this._clock(), true));
      this.refreshes += 1;
      const bad = outs.filter((o) => !o.ok).map((o) => String(o.err));
      if (bad.length) {
        this.lastCommand = `obnova ${lease.id}: ${bad.join('; ')}`.slice(0, 170);
        this._log(this.lastCommand);
      }
    }
  }

  /** Bez spojení se serverem po platnosti pojistky → jednou onExpire, po obnovení spojení `leaseReport`. */
  async _maybeLeases(now) {
    for (const lease of Object.values(this.leases)) {
      if (!this._syncFailed || now < lease.deadline || !this.controlEnabled
          || (lease.appliedAt !== null && lease.deadline <= lease.appliedAt) || now < lease.retryAt) continue;
      this._log(`bez spojení se serverem po platnosti pojistky ${lease.id} — provádím ukončení`);
      const steps = [];
      for (const c of lease.onExpire) {
        const out = await this._execute({ ...c, expiresInS: EXPIRE_TTL_S }, this._clock());
        const step = { type: c.type, dev: c.dev, values: c.values };
        for (const k of ['ok', 'err', 'echo']) if (k in out) step[k] = out[k];
        steps.push(step);
      }
      lease.tries += 1;
      const retry = steps.some((s) => !s.ok && /^(exec|rate-limit)/.test(String(s.err || '')));
      if (retry && lease.tries < EXPIRE_TRIES) { lease.retryAt = now + EXPIRE_RETRY_S; continue; }
      const ok = steps.every((s) => s.ok);
      Object.assign(lease, { tries: 0, retryAt: 0, appliedAt: this._clock() });
      this._leaseReports.push({ id: lease.id, source: lease.source, at: Math.floor(lease.appliedAt), ok, steps });
      this._leaseReports.splice(0, Math.max(0, this._leaseReports.length - LEASE_MAX));
      this.lastCommand = `pojistka ${lease.id}: ${ok ? 'ukončení provedeno' : 'provedeno jen částečně'}`;
      this._persist();
      this._set(this.status, this.lastError);
    }
  }

  // ── čtení a synchronizace ──
  /** Nové položky plánu → odběr u GX. true = přibyl odběr (GX pošle přehled až za chvíli — v tomto kole nečíst). */
  _applyFilters() {
    if (!this._filtersDirty) return false;
    this._filtersDirty = false;
    try {
      return this._tr.setFilters(this.items.flatMap((it) => it.paths.map((p) => String(it.base || '') + p))) > 0;
    } catch (e) {
      this._filtersDirty = true;
      this._log(`odběr mqtt selhal: ${e.message}`);
      return false;
    }
  }

  _readItem(it) {
    const rd = this._tr.readItem(it);
    if (!rd || typeof rd.values !== 'object' || !Object.keys(rd.values).length) return null;
    const tsBy = {};
    for (const [k, v] of Object.entries(rd.tsBy || {})) if (typeof v === 'number') tsBy[k] = Math.floor(v);
    const out = { type: 'mqtt', name: it.name, values: rd.values, ts: Math.floor(this._clock()), tsBy };
    if (rd.dev || it.dev) out.dev = String(rd.dev || it.dev);
    return out;
  }

  /** Jedno kolo: položky na řadě → poslat (+ výsledky příkazů) → provést příkazy z odpovědi. Revoked = token neplatí. */
  async tick() {
    if (!this.token) return;
    const now = this._clock();
    if (now < this._backoffUntil) return;
    const subscribed = this._applyFilters();
    const reads = [];
    for (const it of subscribed ? [] : this.items) {
      const key = `${it.type}\u0000${it.name}`;
      if (now - (this._lastRead.get(key) || 0) < it.everyS) continue;
      let rd = null;
      try {
        rd = this._readItem(it);
      } catch (e) {
        this.lastError = `čtení ${it.name}: ${e.message}`;
        this.readErrors += 1;
      }
      if (!rd) { this._lastRead.set(key, now - it.everyS + RETRY_FAILED_READ_S); continue; }
      this._lastRead.set(key, now);
      reads.push(rd);
    }
    const idleS = this.serverControl ? this.nextPollS - 0.5 : HEARTBEAT_S;
    if (!reads.length && !this._pending.length && !this._leaseReports.length && now >= this._fastUntil
        && now - this._lastSync < idleS) return;
    const chunks = splitReads(reads, READS_BUDGET, MAX_READS, this._log);
    if (!chunks.length) chunks.push([]);
    await this._sync(now, chunks[0]);
    for (const more of chunks.slice(1)) {
      if (this.status !== ONLINE) break;
      await this._sync(this._clock(), more);
    }
    for (let i = 0; i < FLUSH_ROUNDS && this._pending.length && this.status === ONLINE; i++) {
      await this._sync(this._clock(), []);
    }
    await this._offlineDuties(this._clock());
  }

  async _offlineDuties(now) {
    this._ensureProfile();
    await this._maybeRefresh(now);
    await this._maybeLeases(now);
  }

  async _sync(now, reads) {
    const body = { client: { ver: this._version, backend: this._backend, ctl: this.controlEnabled ? 'on' : 'off',
                             caps: this.caps }, reads };
    const sent = this._pending.slice(0, MAX_RESULTS);
    if (sent.length) body.results = sent;
    const lrep = this._leaseReports.length ? this._leaseReports[0] : null;
    if (lrep) body.leaseReport = lrep;
    let code;
    let res;
    try {
      [code, res] = await this._post(SYNC_PATH, body, this.token);
    } catch (e) {
      this._lastSync = now;
      this._syncFailed = true;
      this._set(ERROR, `server nedostupný: ${e.message}`);
      return;
    }
    this._lastSync = now;
    this._syncFailed = code >= 500;          // jen výpadek serveru je „bez spojení“ (ne odmítnutí 4xx)
    res = res && typeof res === 'object' ? res : {};
    if (lrep && (code === 200 || code === 400)) {
      if (res.leaseAck !== lrep.id) this._log(`server hlášení pojistky ${lrep.id} nepotvrdil (HTTP ${code})`);
      if (this._leaseReports[0] === lrep) this._leaseReports.shift();
      this._persist();
    }
    if (code === 200) {
      this._acceptProfile(res);
      this._ensureProfile();
      this._acceptLeases(res, now);
      this._pending.splice(0, sent.length);
      const items = cleanItems(res.readPlan);
      if (items.length) {
        this._filtersDirty = this._filtersDirty || JSON.stringify(items) !== JSON.stringify(this.items);
        this.items = items;
      }
      if ('nextPollS' in res) this.nextPollS = clampPoll(res.nextPollS);
      this.serverControl = ['test', 'on'].includes(res.control) ? res.control : null;
      this.lastSyncAt = now;
      this.lastAccepted = res.accepted === undefined ? null : res.accepted;
      this._set(ONLINE, reads.length ? null : this.lastError);
      const cmds = res.commands;
      if (Array.isArray(cmds) && cmds.length) {
        const received = this._clock();
        this._fastUntil = received + FAST_WINDOW_S;
        for (const cmd of cmds.slice(0, MAX_COMMANDS)) {
          const r = await this._command(cmd, received);
          if (r) this._pending.push(r);
        }
        this._pending.splice(0, Math.max(0, this._pending.length - KEEP_RESULTS));
      }
    } else if (code === 401) {
      this._set(REVOKED, 'spojení bylo ve wattery odpojeno — spárujte znovu');
      throw new Revoked();
    } else if (code === 429) {
      this._backoffUntil = now + 60;
      this._set(ERROR, 'příliš častá komunikace — zpomaluji');
    } else {
      if (code === 409 || code === 403) this._backoffUntil = now + BACKOFF_S;
      if (code === 400) this._pending.length = 0;
      this._set(ERROR, res.message || res.error || `HTTP ${code}`);
    }
  }

  // ── příkazy ──
  /** Jeden příkaz serveru → výsledek pro `results` (null = bez id). Provedený příkaz se neprovede podruhé. */
  async _command(cmd, received) {
    const cid = cmd && typeof cmd === 'object' ? cmd.id : null;
    if (typeof cid !== 'string' || !cid || cid.length > 40) { this._log('příkaz bez platného id — ignoruji'); return null; }
    if (this._done.has(cid)) return this._done.get(cid);
    let out;
    try {
      out = await this._execute(cmd, received);
    } catch (e) {
      out = { ok: false, err: `exec: ${e.message}`.slice(0, 200) };
    }
    out = { id: cid, ...out, dur: Math.round((this._clock() - received) * 100) / 100 };
    this._done.set(cid, out);
    while (this._done.size > SEEN_IDS) this._done.delete(this._done.keys().next().value);
    this.commandsDone += 1;
    const n = (cmd.values || cmd.paths || []).length;
    this.lastCommand = `MQTT ${cmd.op} (${n} cest): ${out.ok ? 'OK' : out.err}`;
    if (cmd.op === 'write' || !out.ok) this._log(`příkaz ${this.lastCommand}`);
    return out;
  }

  async _execute(cmd, received, refresh = false) {
    if (cmd.type !== 'mqtt') return { ok: false, err: 'bad-command: klient umí jen mqtt' };
    const tr = this._tr;
    const dev = String(tr.portal || this._sn || '');
    if (dev && String(cmd.dev || '') !== dev) return { ok: false, err: 'foreign-sn' };
    if (cmd.op === 'read') {
      const paths = cmd.paths;
      if (!Array.isArray(paths) || paths.length < 1 || paths.length > MQTT_MAX_PATHS || !paths.every((p) => shortStr(p, 200))) {
        return { ok: false, err: 'bad-command: paths' };
      }
      if (this._clock() - received > ttlOf(cmd)) return { ok: false, err: 'expired' };
      const rd = tr.readItem({ type: 'mqtt', name: 'cmd', paths: [...paths] });
      return { ok: true, values: rd && typeof rd.values === 'object' ? rd.values : {} };
    }
    if (cmd.op !== 'write') return { ok: false, err: 'bad-command: op' };
    if (!this.controlEnabled) return { ok: false, err: 'local-off' };
    this._ensureProfile();
    const prob = this._writeCheck(cmd);
    if (prob) return { ok: false, err: `not-allowed: ${prob}` };
    if (typeof tr.write !== 'function') return { ok: false, err: 'exec: přenos mqtt neumí zápis' };
    const comp = this._comp()[0];
    const now = this._clock();
    if (!refresh && !this._rateOk(comp, now)) return { ok: false, err: 'rate-limit' };
    const pairs = cmd.values.map(([p, v]) => [p, v]);
    if (pairs.some(([p]) => P.persistent(comp, p))) {     // trvalá nastavení (flash): denní limit profilu
      this._persistWrites = this._persistWrites.filter((t) => now - t < 86400);
      const perDay = (comp.limits || {}).persistPerDay;
      if (Number.isInteger(perDay) && this._persistWrites.length >= perDay) {
        return { ok: false, err: 'rate-limit: trvalá nastavení za den' };
      }
      this._persistWrites.push(now);
    }
    const gap = this._lastWrite + WRITE_GAP_S - now;
    if (gap > 0) await this._sleep(gap);
    if (this._clock() - received > ttlOf(cmd)) return { ok: false, err: 'expired' };
    if (!refresh) this._writes.push(this._clock());
    let echo;
    try {
      echo = await tr.write(pairs);
    } catch (e) {
      return { ok: false, err: `exec: ${e.message}`.slice(0, 200) };
    } finally {
      this._lastWrite = this._clock();
    }
    return { ok: true, echo: echo && typeof echo === 'object' ? echo : {} };
  }
}

module.exports = {
  EdgeClient, Revoked, cleanItems, splitReads, clientCaps, clampPoll, PROFILES,
  PAIR_PATH, SYNC_PATH, STARTING, ONLINE, ERROR, REVOKED, WRITE_GAP_S, RATE_PER_MIN, REFRESH_MIN_S,
};
