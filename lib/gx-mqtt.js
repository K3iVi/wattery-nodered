'use strict';
/**
 * Klient MQTT 3.1.1 na broker Victron GX (dbus-flashmq) — JEN ČTENÍ, bez závislostí (modul `net`).
 *
 * Port `wattery-ha/custom_components/wattery/transport_mqtt_gx.py` (držet stejně):
 * - Na Cerbu z Node-RED: `localhost:1883` bez hesla — funguje i v profilu Secured a s vypnutým „MQTT Access“
 *   (ověřeno NR0 17. 10. 2026). Heslo jde zadat pro testy proti simulátoru.
 * - Portal ID (identita GX) z retained `N/+/system/0/Serial`.
 * - Odběr `N/<portal>/<filtr>` podle plánu čtení serveru; `R/<portal>/keepalive` s prázdným tělem = plný přehled,
 *   pak á 30 s `{"keepalive-options":["suppress-republish"]}` (bez keepalive GX do 60 s přestane posílat).
 *
 * ŽÁDNÝ ZÁPIS: `_publish` pustí jen `R/<portal>/keepalive`. `W/…` klient neumí poslat.
 */
const net = require('net');
const { EventEmitter } = require('events');

const MQTT_KEEPALIVE_S = 60;
const GX_KEEPALIVE_S = 30;
const DISCOVER_S = 10;
const CONNECT_TIMEOUT_S = 10;
const MAX_PACKET = 256 * 1024;
const SERIAL_TOPIC = 'system/0/Serial';
const SUPPRESS = JSON.stringify({ 'keepalive-options': ['suppress-republish'] });
const PORTAL_RE = /^[0-9a-f]{12}$/;
const LEVEL_RE = /^[A-Za-z0-9_+\-.]+$/;

class GxError extends Error {}

function vlen(n) {
  const out = [];
  do {
    let b = n % 128;
    n = Math.floor(n / 128);
    if (n) b |= 0x80;
    out.push(b);
  } while (n);
  return Buffer.from(out);
}

function str(v) {
  const b = Buffer.from(v, 'utf8');
  const h = Buffer.alloc(2);
  h.writeUInt16BE(b.length);
  return Buffer.concat([h, b]);
}

/** MQTT filtr ('+' jedna úroveň, '#' zbytek) × cesta — stejně jako edge_victron.match_filter. */
function matchFilter(flt, path) {
  const f = flt.split('/');
  const p = path.split('/');
  for (let k = 0; k < f.length; k++) {
    if (f[k] === '#') return true;
    if (k >= p.length || (f[k] !== '+' && f[k] !== p[k])) return false;
  }
  return f.length === p.length;
}

/** Filtr z plánu serveru: relativní cesta, '+' jen celá úroveň, '#' jen na konci, žádné '..'. */
function validFilter(flt) {
  if (typeof flt !== 'string' || !flt || flt.length > 200) return false;
  const parts = flt.split('/');
  for (let k = 0; k < parts.length; k++) {
    const p = parts[k];
    if (p === '#') return k > 0 && k === parts.length - 1;
    if (p === '.' || p === '..' || (p.includes('+') && p !== '+') || !LEVEL_RE.test(p)) return false;
  }
  return true;
}

class GxMqtt extends EventEmitter {
  constructor({ host = '127.0.0.1', port = 1883, username = '', password = '', portal = null,
                clock = () => Date.now() / 1000, gxKeepaliveS = GX_KEEPALIVE_S, connect = net.connect } = {}) {
    super();
    this.host = host;
    this.port = port;
    this.username = username || '';
    this.password = password || '';
    this.portal = portal ? String(portal).trim().toLowerCase() : null;
    this._clock = clock;
    this._gxKeepaliveS = gxKeepaliveS;
    this._connect = connect;
    this._sock = null;
    this._buf = Buffer.alloc(0);
    this._pid = 0;
    this._waiters = [];
    this._timer = null;
    this.filters = [];
    this.values = {};
    this.ts = {};
    this.connected = false;
    this.published = [];              // témata, která klient poslal (test: nikdy W/)
    this._serials = new Set();
  }

  // ── spojení ──
  async start() {
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => { sock.destroy(); reject(new GxError(`GX ${this.host}:${this.port} nedostupné: timeout`)); },
                           CONNECT_TIMEOUT_S * 1000);
      const sock = this._connect({ host: this.host, port: this.port }, () => { clearTimeout(t); resolve(); });
      sock.on('error', (e) => {
        clearTimeout(t);
        if (!this.connected) reject(new GxError(`GX ${this.host}:${this.port} nedostupné: ${e.code || e.message}`));
        this._lost(e);
      });
      sock.on('data', (chunk) => this._data(chunk));
      sock.on('close', () => this._lost(new GxError('GX ukončil spojení')));
      this._sock = sock;
    });
    let flags = 0x02;                                   // clean session
    let payload = str(`wattery-nr-${Math.random().toString(16).slice(2, 12)}`);
    if (this.username || this.password) { flags |= 0x80; payload = Buffer.concat([payload, str(this.username)]); }
    if (this.password) { flags |= 0x40; payload = Buffer.concat([payload, str(this.password)]); }
    const ka = Buffer.alloc(2);
    ka.writeUInt16BE(MQTT_KEEPALIVE_S);
    this._send(0x10, Buffer.concat([str('MQTT'), Buffer.from([4, flags]), ka, payload]));
    const [t, body] = await this._packet(0x20, CONNECT_TIMEOUT_S);
    if (t !== 0x20 || body.length < 2) throw new GxError('GX neodpověděl na přihlášení MQTT');
    if (body[1] !== 0) {
      this.close();
      throw new GxError({ 4: 'GX odmítl heslo', 5: 'GX: přístup nepovolen' }[body[1]] || `GX odmítl spojení (${body[1]})`);
    }
    this.connected = true;
    if (!this.portal) await this._discover();
    this._subscribe([`N/${this.portal}/${SERIAL_TOPIC}`, ...this.filters.map((f) => `N/${this.portal}/${f}`)]);
    this._publish(`R/${this.portal}/keepalive`, '');   // plný přehled hodnot
    this._lastKeepalive = this._lastPing = this._clock();
    this._timer = setInterval(() => this._beat(), 1000);
    if (this._timer.unref) this._timer.unref();
  }

  async _discover() {
    this._subscribe([`N/+/${SERIAL_TOPIC}`]);
    const end = this._clock() + DISCOVER_S;
    while (!this._serials.size && this._clock() < end) await new Promise((r) => setTimeout(r, 50));
    if (this._serials.size !== 1) {
      this.close();
      throw new GxError(this._serials.size ? 'na brokeru je víc GX — zadejte portal ID' : 'portal ID GX se nepodařilo zjistit');
    }
    this.portal = [...this._serials][0];
  }

  close() {
    const was = this.connected;
    this.connected = false;
    if (this._timer) clearInterval(this._timer);
    this._timer = null;
    if (this._sock) {
      try { if (was) this._sock.write(Buffer.from([0xE0, 0])); } catch (e) { /* už zavřené */ }
      this._sock.destroy();
    }
    this._sock = null;
  }

  _lost(err) {
    if (!this._sock) return;
    const was = this.connected;
    this.close();
    for (const w of this._waiters.splice(0)) w.reject(err);
    if (was) this.emit('lost', err);
  }

  // ── odběr a stav ──
  /** Filtry z plánu čtení (relativní k N/<portal>/). Nové se hned přihlásí a GX pošle plný přehled. */
  setFilters(filters) {
    const fresh = [...new Set(filters)].filter((f) => validFilter(f) && !this.filters.includes(f));
    this.filters.push(...fresh);
    if (fresh.length && this.connected) {
      this._subscribe(fresh.map((f) => `N/${this.portal}/${f}`));
      this._publish(`R/${this.portal}/keepalive`, '');
    }
    return fresh.length;
  }

  snapshot(filters) {
    // Po ztrátě spojení jsou hodnoty staré — neposílat je jako čerstvé (čte se až po znovupřipojení).
    if (!this.connected) throw new GxError('GX není připojené');
    const values = {};
    const ts = {};
    for (const [p, v] of Object.entries(this.values)) {
      if (filters.some((f) => matchFilter(f, p))) { values[p] = v; ts[p] = this.ts[p]; }
    }
    return { values, ts };
  }

  /** Položka plánu `{type:'mqtt', name, paths, base?}` → čtení pro sync (cesty bez `base`, hub ho doplní). */
  readItem(item) {
    const base = String(item.base || '');
    const snap = this.snapshot((item.paths || []).map((f) => base + f));
    const values = {};
    const tsBy = {};
    for (const [p, v] of Object.entries(snap.values)) { values[p.slice(base.length)] = v; tsBy[p.slice(base.length)] = snap.ts[p]; }
    return { type: 'mqtt', name: item.name, dev: this.portal, values, tsBy, ts: Math.floor(this._clock()) };
  }

  static planFilters(plan) {
    return (plan || []).filter((it) => it && it.type === 'mqtt')
      .flatMap((it) => (it.paths || []).map((f) => String(it.base || '') + f));
  }

  // ── smyčka ──
  _beat() {
    if (!this.connected) return;
    const now = this._clock();
    try {
      if (now - this._lastKeepalive >= this._gxKeepaliveS) {
        this._publish(`R/${this.portal}/keepalive`, SUPPRESS);
        this._lastKeepalive = now;
      }
      if (now - this._lastPing >= MQTT_KEEPALIVE_S / 2) {
        this._send(0xC0, Buffer.alloc(0));
        this._lastPing = now;
      }
    } catch (e) {
      this._lost(e);
    }
  }

  _handle(t, body) {
    if (t !== 0x30 || body.length < 2) return;          // SUBACK, PINGRESP …
    const tl = body.readUInt16BE(0);
    const topic = body.subarray(2, 2 + tl).toString('utf8');
    const raw = body.subarray(2 + tl);
    const i1 = topic.indexOf('/');
    const i2 = topic.indexOf('/', i1 + 1);
    if (i1 < 0 || i2 < 0 || topic.slice(0, i1) !== 'N') return;
    const portal = topic.slice(i1 + 1, i2);
    const path = topic.slice(i2 + 1);
    if (path === SERIAL_TOPIC && PORTAL_RE.test(portal)) this._serials.add(portal);
    if (!this.portal || portal !== this.portal) return;
    let v = null;                                       // prázdné tělo = hodnota zmizela (dbus invalid)
    if (raw.length) {
      try {
        v = JSON.parse(raw.toString('utf8'));
        if (v && typeof v === 'object' && !Array.isArray(v)) v = v.value === undefined ? null : v.value;
      } catch (e) {
        return;
      }
    }
    this.values[path] = v;
    this.ts[path] = Math.floor(this._clock());
  }

  // ── drát ──
  _send(head, body) {
    if (!this._sock) throw new GxError('spojení s GX není otevřené');
    this._sock.write(Buffer.concat([Buffer.from([head]), vlen(body.length), body]));
  }

  _publish(topic, payload) {
    // Jediné, co klient smí poslat: požadavek na hodnoty (keepalive). Žádné W/ — do Victronu se nezapisuje.
    if (!this.portal || topic !== `R/${this.portal}/keepalive`) {
      throw new GxError(`publikování ${JSON.stringify(topic)} není povolené (klient GX jen čte)`);
    }
    this.published.push(topic);
    this._send(0x30, Buffer.concat([str(topic), Buffer.from(payload, 'utf8')]));
  }

  _subscribe(topics) {
    for (let i = 0; i < topics.length; i += 32) {
      this._pid = (this._pid % 65535) + 1;
      const pid = Buffer.alloc(2);
      pid.writeUInt16BE(this._pid);
      this._send(0x82, Buffer.concat([pid, ...topics.slice(i, i + 32).map((t) => Buffer.concat([str(t), Buffer.from([0])]))]));
    }
  }

  _data(chunk) {
    this._buf = Buffer.concat([this._buf, chunk]);
    for (;;) {
      let pkt;
      try {
        pkt = this._take();
      } catch (e) {
        this._lost(e);
        return;
      }
      if (!pkt) return;
      const w = this._waiters.findIndex((x) => x.type === pkt[0]);
      if (w >= 0) this._waiters.splice(w, 1)[0].resolve(pkt);
      else this._handle(pkt[0], pkt[1]);
    }
  }

  _take() {
    const b = this._buf;
    let mult = 1;
    let n = 0;
    let i = 1;
    for (;;) {
      if (i >= b.length) return null;
      const x = b[i];
      n += (x & 0x7F) * mult;
      mult *= 128;
      i += 1;
      if (!(x & 0x80)) break;
      if (i > 4) throw new GxError('vadný paket MQTT');
    }
    if (n > MAX_PACKET) throw new GxError('příliš velký paket MQTT');
    if (b.length < i + n) return null;
    this._buf = b.subarray(i + n);
    return [b[0] & 0xF0, b.subarray(i, i + n)];
  }

  _packet(type, timeoutS) {
    return new Promise((resolve, reject) => {
      const w = { type, resolve: (p) => { clearTimeout(t); resolve(p); }, reject: (e) => { clearTimeout(t); reject(e); } };
      const t = setTimeout(() => {
        const k = this._waiters.indexOf(w);
        if (k >= 0) this._waiters.splice(k, 1);
        reject(new GxError('GX neodpověděl včas'));
      }, timeoutS * 1000);
      this._waiters.push(w);
    });
  }
}

module.exports = { GxMqtt, GxError, matchFilter, validFilter, SERIAL_TOPIC };
