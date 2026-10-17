'use strict';
/**
 * Jádro klienta lokálního spojení wattery pro Node-RED na Victron GX — JEN ČTENÍ (NR1).
 *
 * Port čtecí části `wattery-ha/custom_components/wattery/edge.py` (EdgeRunner, protokol 2) — držet stejně:
 *   POST /api/edge/v1/pair  {code, kind, sn, clientVer, label, caps}  → {token, readPlan, nextPollS, profile?}
 *   POST /api/edge/v1/sync  (Bearer) {client: {ver, backend, ctl, caps}, reads, results} →
 *                           {readPlan, nextPollS, commands, control, accepted, …}
 * Co a jak často číst, určuje server (`readPlan`, položky `type: 'mqtt'`). Příkazy: `mqtt` read se provede vždy,
 * zápis klient odmítne (`local-off`) — přepínač řízení v NR1 není a `caps.profiles` je prázdné, takže server zápisy
 * posílá cloudem. Pojistky (`leases`, `fallback`) klient bez řízení nepotřebuje a ignoruje je.
 */

const PAIR_PATH = '/api/edge/v1/pair';
const SYNC_PATH = '/api/edge/v1/sync';
const RETRY_FAILED_READ_S = 30;
const HEARTBEAT_S = 30;
const POLL_MIN_S = 5;
const POLL_MAX_S = 300;
const BACKOFF_S = 300;
const FAST_POLL_S = 1.5;
const FAST_WINDOW_S = 20;
const MAX_COMMANDS = 8;
const MAX_RESULTS = 16;
const KEEP_RESULTS = 32;
const SEEN_IDS = 64;
const FLUSH_ROUNDS = 3;
const PROTO = 2;
const MAX_READS = 16;
const READS_BUDGET = 48 * 1024;
const MQTT_MAX_PATHS = 1000;

const STARTING = 'starting';
const ONLINE = 'online';
const ERROR = 'error';
const REVOKED = 'revoked';

class Revoked extends Error {}

const shortStr = (v, n) => typeof v === 'string' && v.length > 0 && v.length <= n;

/** `caps` pro pair i každý sync: jen přenos mqtt, žádný profil zápisu (jen čtení). */
function clientCaps() {
  return { proto: PROTO, transports: ['mqtt'], profiles: [], features: [] };
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
   * @param transport GxMqtt (readItem, setFilters, portal)
   */
  constructor({ post, transport, version, backend = 'nodered', clock = () => Date.now() / 1000,
                log = () => {}, onStatus = () => {} }) {
    this._post = post;
    this._tr = transport;
    this._version = version;
    this._backend = backend;
    this._clock = clock;
    this._log = log;
    this._onStatus = onStatus;
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
  }

  /** Párování kódem z aplikace wattery: identita = portal ID z GX (transport už musí být připojený). */
  async pair(code, { kind = 'nodered', label = 'Node-RED na GX' } = {}) {
    const sn = this._tr.portal;
    if (!sn) throw new Error('portal ID GX zatím neznám — je GX připojené?');
    const [st, res] = await this._post(PAIR_PATH, { code, kind, sn, clientVer: this._version, label, caps: this.caps }, null);
    if (st !== 200 || !res || !res.token) {
      throw new Error((res && (res.message || res.error)) || `párování selhalo (HTTP ${st})`);
    }
    this.start(res.token, res.readPlan, res.nextPollS);
    return res;
  }

  /** Start s uloženým tokenem (po restartu) nebo hned po párování. */
  start(token, readPlan, nextPollS) {
    this.token = token;
    this.items = cleanItems(readPlan);
    this._filtersDirty = this.items.length > 0;
    this.nextPollS = clampPoll(nextPollS === undefined ? 10 : nextPollS);
    this._lastSync = 0;
  }

  /** Po znovupřipojení GX nový přenos: stav klienta zůstává, odběr se přihlásí znovu. */
  setTransport(transport) {
    this._tr = transport;
    this._filtersDirty = this.items.length > 0;
  }

  _set(status, error = null) {
    this.status = status;
    this.lastError = error;
    try { this._onStatus(this); } catch (e) { this._log(`chyba při aktualizaci stavu: ${e.message}`); }
  }

  pollS() {
    return this._clock() < this._fastUntil ? FAST_POLL_S : this.nextPollS;
  }

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
    if (!reads.length && !this._pending.length && now >= this._fastUntil && now - this._lastSync < idleS) return;
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
  }

  async _sync(now, reads) {
    const body = { client: { ver: this._version, backend: this._backend, ctl: 'off', caps: this.caps }, reads };
    const sent = this._pending.slice(0, MAX_RESULTS);
    if (sent.length) body.results = sent;
    let code;
    let res;
    try {
      [code, res] = await this._post(SYNC_PATH, body, this.token);
    } catch (e) {
      this._lastSync = now;
      this._set(ERROR, `server nedostupný: ${e.message}`);
      return;
    }
    this._lastSync = now;
    res = res && typeof res === 'object' ? res : {};
    if (code === 200) {
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
          const r = this._command(cmd, received);
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

  /** Jeden příkaz serveru → výsledek pro `results` (null = bez id). Provedený příkaz se neprovede podruhé. */
  _command(cmd, received) {
    const cid = cmd && typeof cmd === 'object' ? cmd.id : null;
    if (typeof cid !== 'string' || !cid || cid.length > 40) { this._log('příkaz bez platného id — ignoruji'); return null; }
    if (this._done.has(cid)) return this._done.get(cid);
    let out;
    try {
      out = this._execute(cmd, received);
    } catch (e) {
      out = { ok: false, err: `exec: ${e.message}`.slice(0, 200) };
    }
    out = { id: cid, ...out, dur: Math.round((this._clock() - received) * 100) / 100 };
    this._done.set(cid, out);
    while (this._done.size > SEEN_IDS) this._done.delete(this._done.keys().next().value);
    this.commandsDone += 1;
    const n = (cmd.values || cmd.paths || []).length;
    this.lastCommand = `MQTT ${cmd.op} (${n} cest): ${out.ok ? 'OK' : out.err}`;
    return out;
  }

  _execute(cmd, received) {
    if (cmd.type !== 'mqtt') return { ok: false, err: 'bad-command: klient umí jen mqtt' };
    const dev = String(this._tr.portal || '');
    if (dev && String(cmd.dev || '') !== dev) return { ok: false, err: 'foreign-sn' };
    if (cmd.op === 'read') {
      const paths = cmd.paths;
      if (!Array.isArray(paths) || paths.length < 1 || paths.length > MQTT_MAX_PATHS || !paths.every((p) => shortStr(p, 200))) {
        return { ok: false, err: 'bad-command: paths' };
      }
      if (this._clock() - received > ttlOf(cmd)) return { ok: false, err: 'expired' };
      const rd = this._tr.readItem({ type: 'mqtt', name: 'cmd', paths: [...paths] });
      return { ok: true, values: rd && typeof rd.values === 'object' ? rd.values : {} };
    }
    if (cmd.op !== 'write') return { ok: false, err: 'bad-command: op' };
    return { ok: false, err: 'local-off' };          // NR1: jen čtení — zápis do GX klient neprovede nikdy
  }
}

module.exports = {
  EdgeClient, Revoked, cleanItems, splitReads, clientCaps, clampPoll,
  PAIR_PATH, SYNC_PATH, STARTING, ONLINE, ERROR, REVOKED,
};
