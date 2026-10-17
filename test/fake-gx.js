'use strict';
/** Falešný broker GX pro testy (jako wattery-ha/tests/gx_sim.py): retained Serial, keepalive = přehled, heslo. */
const net = require('net');
const { matchFilter } = require('../lib/gx-mqtt');

const PORTAL = 'c0ffee000001';

function defaultTree() {
  return {
    'system/0/Serial': PORTAL,
    'platform/0/Firmware/Installed/Version': 'v3.81',
    'system/0/Dc/Battery/Soc': 57.5,
    'system/0/Dc/Battery/Power': -1200,
    'system/0/Ac/Grid/L1/Power': 300,
    'system/0/Ac/Grid/L2/Power': null,
    'settings/0/Settings/DynamicEss/Mode': 4,
    'settings/0/Settings/DynamicEss/Schedule/0/Soc': 80,
    'settings/0/Settings/DynamicEss/Schedule/1/Soc': 60,
  };
}

const vlen = (n) => { const o = []; do { let b = n % 128; n = Math.floor(n / 128); if (n) b |= 0x80; o.push(b); } while (n); return Buffer.from(o); };
const str = (s) => { const b = Buffer.from(s); const h = Buffer.alloc(2); h.writeUInt16BE(b.length); return Buffer.concat([h, b]); };
const pkt = (head, body) => Buffer.concat([Buffer.from([head]), vlen(body.length), body]);

class FakeGx {
  constructor({ password = null, portal = PORTAL, tree = null } = {}) {
    this.password = password;
    this.portal = portal;
    this.tree = tree || defaultTree();
    this.clients = new Set();
    this.clientTopics = [];
    this.keepalives = [];
  }

  start() {
    return new Promise((resolve) => {
      this.server = net.createServer((s) => this._client(s));
      this.server.listen(0, '127.0.0.1', () => { this.port = this.server.address().port; resolve(this); });
    });
  }

  stop() {
    for (const c of this.clients) c.sock.destroy();
    return new Promise((r) => this.server.close(() => r()));
  }

  set(path, value) {
    this.tree[path] = value;
    for (const c of this.clients) this._send(c, path);
  }

  _send(c, path) {
    const topic = `N/${this.portal}/${path}`;
    if (!c.subs.some((f) => matchFilter(f, topic))) return;
    const v = this.tree[path];
    const payload = v === null ? Buffer.alloc(0) : Buffer.from(JSON.stringify({ value: v }));
    c.sock.write(pkt(0x30, Buffer.concat([str(topic), payload])));
  }

  _client(sock) {
    const c = { sock, subs: [], buf: Buffer.alloc(0) };
    this.clients.add(c);
    sock.on('close', () => this.clients.delete(c));
    sock.on('error', () => {});
    sock.on('data', (d) => {
      c.buf = Buffer.concat([c.buf, d]);
      for (;;) {
        let n = 0; let mult = 1; let i = 1;
        for (;;) { if (i >= c.buf.length) return; const x = c.buf[i++]; n += (x & 0x7f) * mult; mult *= 128; if (!(x & 0x80)) break; }
        if (c.buf.length < i + n) return;
        const t = c.buf[0] & 0xf0; const body = c.buf.subarray(i, i + n);
        c.buf = c.buf.subarray(i + n);
        this._packet(c, t, body);
      }
    });
  }

  _packet(c, t, body) {
    if (t === 0x10) {
      const flags = body[7];
      let p = 10;
      const rd = () => { const l = body.readUInt16BE(p); const s = body.subarray(p + 2, p + 2 + l).toString(); p += 2 + l; return s; };
      rd();                                            // client id
      if (flags & 0x80) rd();
      const pw = flags & 0x40 ? rd() : null;
      const ok = this.password === null || pw === this.password;
      c.sock.write(pkt(0x20, Buffer.from([0, ok ? 0 : 5])));
      if (!ok) c.sock.end();
    } else if (t === 0x80) {                           // SUBSCRIBE (0x82 & 0xF0)
      let p = 2;
      const added = [];
      while (p < body.length) { const l = body.readUInt16BE(p); added.push(body.subarray(p + 2, p + 2 + l).toString()); p += 3 + l; }
      c.subs.push(...added);
      c.sock.write(pkt(0x90, Buffer.concat([body.subarray(0, 2), Buffer.from(added.map(() => 0))])));
      if (added.some((f) => matchFilter(f, `N/${this.portal}/system/0/Serial`))) this._send(c, 'system/0/Serial');  // retained
    } else if (t === 0x30) {
      const l = body.readUInt16BE(0);
      const topic = body.subarray(2, 2 + l).toString();
      const payload = body.subarray(2 + l).toString();
      this.clientTopics.push(topic);
      if (topic === `R/${this.portal}/keepalive`) {
        this.keepalives.push(payload);
        if (!payload.includes('suppress-republish')) for (const path of Object.keys(this.tree)) this._send(c, path);
      }
    } else if (t === 0xc0) {
      c.sock.write(pkt(0xd0, Buffer.alloc(0)));
    }
  }
}

module.exports = { FakeGx, PORTAL };
