'use strict';
/** Obal uzlu (nodes/wattery.js) s falešným RED: párování → token v souboru → po restartu bez kódu; odpojení. */
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');
const { FakeGx, PORTAL } = require('./fake-gx');

const until = async (fn, ms = 5000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (fn()) return true; await new Promise((r) => setTimeout(r, 50)); }
  return false;
};

function fakeHub() {
  const hub = { pairs: [], syncs: [], revoke: false, delayMs: 0, onSync: null };
  hub.server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', async () => {
      if (req.url !== '/api/edge/v1/pair' && hub.onSync) hub.onSync();
      if (hub.delayMs) await new Promise((r) => setTimeout(r, hub.delayMs));
      const b = JSON.parse(body || '{}');
      let code = 200;
      let out;
      if (req.url === '/api/edge/v1/pair') {
        hub.pairs.push(b);
        out = { ok: true, token: 'TOK', clientId: 1, stationId: 9, nextPollS: 5,
                readPlan: [{ type: 'mqtt', name: 'live', everyS: 5, dev: PORTAL, paths: ['system/0/Dc/Battery/Soc'] }] };
      } else {
        hub.syncs.push({ auth: req.headers.authorization, body: b });
        if (hub.revoke) { code = 401; out = { ok: false }; } else out = { ok: true, accepted: b.reads.length, ...(hub.extra ? hub.extra(b) : {}) };
      }
      res.writeHead(code, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(out));
    });
  });
  return new Promise((r) => hub.server.listen(0, '127.0.0.1', () => { hub.url = `http://127.0.0.1:${hub.server.address().port}`; r(hub); }));
}

function fakeRED(userDir) {
  const RED = { settings: { userDir }, nodes: {}, global: {} };
  RED.nodes.createNode = (node, config) => {
    node.context = () => ({ global: { set: (k, v) => { RED.global[k] = v; } } });
    node.statuses = [];
    node.logs = [];
    node.handlers = {};
    node.status = (s) => node.statuses.push(s);
    node.log = (m) => node.logs.push(m);
    node.warn = (m) => node.logs.push('WARN ' + m);
    node.error = (m) => node.logs.push('ERR ' + m);
    node.on = (ev, fn) => { node.handlers[ev] = fn; };
    node.credentials = config.__credentials || {};
  };
  RED.nodes.registerType = (name, ctor, opts) => { RED.type = { name, ctor, opts }; };
  require('../nodes/wattery')(RED);
  return RED;
}

const close = (node) => new Promise((r) => node.handlers.close(r));

test('uzel: párování kódem, čtení, token v userDir, restart bez kódu, odpojení', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wattery-nr-'));
  const gx = await new FakeGx().start();
  const hub = await fakeHub();
  const RED = fakeRED(dir);
  assert.strictEqual(RED.type.name, 'wattery');
  assert.deepStrictEqual(Object.keys(RED.type.opts.credentials), ['code']);
  const cfg = { server: hub.url, host: '127.0.0.1', port: String(gx.port), kind: 'sim', __credentials: { code: 'ABCD-EFGH' } };
  try {
    const n1 = {};
    RED.type.ctor.call(n1, cfg);
    assert.ok(await until(() => hub.syncs.some((s) => s.body.reads.length), 12000), JSON.stringify(n1.statuses));
    assert.deepStrictEqual([hub.pairs[0].kind, hub.pairs[0].sn, hub.pairs[0].code], ['sim', PORTAL, 'ABCD-EFGH']);
    assert.strictEqual(hub.syncs[0].auth, 'Bearer TOK');
    const st = JSON.parse(fs.readFileSync(path.join(dir, 'wattery-edge.json'), 'utf8'));
    assert.strictEqual(st[`${hub.url}|${PORTAL}`].token, 'TOK');
    assert.ok(n1.statuses.some((s) => s.fill === 'green'));
    await close(n1);

    const n2 = {};
    RED.type.ctor.call(n2, { ...cfg, __credentials: {} });
    const before = hub.syncs.length;
    assert.ok(await until(() => hub.syncs.length > before, 12000));
    assert.strictEqual(hub.pairs.length, 1, 'po restartu bez nového párování');
    hub.revoke = true;
    assert.ok(await until(() => n2.statuses.some((s) => s.fill === 'red'), 12000), JSON.stringify(n2.statuses));
    assert.ok(await until(() => !fs.readFileSync(path.join(dir, 'wattery-edge.json'), 'utf8').includes('TOK')));
    await close(n2);
    assert.ok(gx.clientTopics.every((t) => t === `R/${PORTAL}/keepalive`));
  } finally {
    await gx.stop();
    hub.server.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('uzel: ztráta GX během běžící synchronizace → znovupřipojení, žádná stará data', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wattery-nr-'));
  const gx = await new FakeGx().start();
  const hub = await fakeHub();
  const RED = fakeRED(dir);
  const cfg = { server: hub.url, host: '127.0.0.1', port: String(gx.port), kind: 'sim', __credentials: { code: 'ABCD-EFGH' } };
  try {
    const n = {};
    RED.type.ctor.call(n, cfg);
    assert.ok(await until(() => hub.syncs.some((s) => s.body.reads.length), 12000), JSON.stringify(n.statuses));
    // Další sync visí na serveru a mezitím spadne broker GX (dřív smyčka po návratu zrušila znovupřipojení).
    hub.delayMs = 1500;
    let kicked = false;
    hub.onSync = () => { if (!kicked) { kicked = true; setTimeout(() => gx.kick(), 200); } };
    assert.ok(await until(() => RED.global.wattery_diag && RED.global.wattery_diag.gxLost >= 1, 12000));
    hub.delayMs = 0;
    hub.onSync = null;
    const lostAt = hub.syncs.length;
    assert.ok(await until(() => RED.global.wattery_diag.gxConnects >= 2, 15000), JSON.stringify(RED.global.wattery_diag));
    gx.set('system/0/Dc/Battery/Soc', 61);
    const fresh = (s) => s.body.reads.some((r) => r.values['system/0/Dc/Battery/Soc'] === 61);
    assert.ok(await until(() => hub.syncs.slice(lostAt).some(fresh), 15000), 'po znovupřipojení čte nové hodnoty');
    assert.strictEqual(RED.global.wattery_diag.status, 'online');
    assert.ok(!JSON.stringify(RED.global.wattery_diag).includes('TOK'), 'diagnostika bez tokenu');
    await close(n);
    assert.ok(gx.clientTopics.every((t) => t === `R/${PORTAL}/keepalive`));
  } finally {
    await gx.stop();
    hub.server.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('uzel: řízení zapnuté → ctl on, zápis W/ do GX s ozvěnou, pojistka uložená v userDir; vypnuté → nic', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wattery-nr-'));
  const gx = await new FakeGx().start();
  const hub = await fakeHub();
  const MODE = 'settings/0/Settings/DynamicEss/Mode';
  let sent = false;
  hub.extra = (b) => {
    const out = { profile: 'victron-gx@1', serverTime: Math.floor(Date.now() / 1000), control: 'on',
                  leases: [{ id: 'L1', validUntil: Math.floor(Date.now() / 1000) + 600,
                             onExpire: [{ type: 'mqtt', dev: PORTAL, op: 'write', values: [[MODE, 0]] }] }] };
    if (!sent && b.client.ctl === 'on') { sent = true; out.commands = [{ id: 'c-1', type: 'mqtt', dev: PORTAL, op: 'write', values: [[MODE, 0]], expiresInS: 30 }]; }
    return out;
  };
  const RED = fakeRED(dir);
  const cfg = { server: hub.url, host: '127.0.0.1', port: String(gx.port), kind: 'nodered', __credentials: { code: 'ABCD-EFGH' } };
  try {
    const off = {};
    RED.type.ctor.call(off, cfg);                        // výchozí: řízení vypnuté
    assert.ok(await until(() => hub.syncs.length >= 2, 12000));
    assert.ok(hub.syncs.every((x) => x.body.client.ctl === 'off'));
    assert.ok(gx.clientTopics.every((t) => t === `R/${PORTAL}/keepalive`), 'bez řízení jen keepalive');
    await close(off);

    const on = {};
    RED.type.ctor.call(on, { ...cfg, control: true, __credentials: {} });
    assert.ok(await until(() => hub.syncs.some((x) => (x.body.results || []).some((r) => r.id === 'c-1')), 12000),
              JSON.stringify(on.logs));
    const r = hub.syncs.flatMap((x) => x.body.results || []).find((x) => x.id === 'c-1');
    assert.deepStrictEqual([r.ok, r.echo], [true, { [MODE]: 0 }]);
    assert.strictEqual(gx.tree[MODE], 0);
    const st = JSON.parse(fs.readFileSync(path.join(dir, 'wattery-edge.json'), 'utf8'))[`${hub.url}|${PORTAL}`];
    assert.deepStrictEqual([st.token, st.edge.profile, st.edge.leases[0].id], ['TOK', 'victron-gx@1', 'L1']);
    assert.ok(on.statuses.some((x) => x.fill === 'green' && /řízení/.test(x.text)), JSON.stringify(on.statuses));
    await close(on);
  } finally {
    await gx.stop();
    await new Promise((res) => hub.server.close(() => res()));
  }
});
