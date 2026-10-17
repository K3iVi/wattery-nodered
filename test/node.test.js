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
  const hub = { pairs: [], syncs: [], revoke: false };
  hub.server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      const b = JSON.parse(body || '{}');
      let code = 200;
      let out;
      if (req.url === '/api/edge/v1/pair') {
        hub.pairs.push(b);
        out = { ok: true, token: 'TOK', clientId: 1, stationId: 9, nextPollS: 5,
                readPlan: [{ type: 'mqtt', name: 'live', everyS: 5, dev: PORTAL, paths: ['system/0/Dc/Battery/Soc'] }] };
      } else {
        hub.syncs.push({ auth: req.headers.authorization, body: b });
        if (hub.revoke) { code = 401; out = { ok: false }; } else out = { ok: true, accepted: b.reads.length };
      }
      res.writeHead(code, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(out));
    });
  });
  return new Promise((r) => hub.server.listen(0, '127.0.0.1', () => { hub.url = `http://127.0.0.1:${hub.server.address().port}`; r(hub); }));
}

function fakeRED(userDir) {
  const RED = { settings: { userDir }, nodes: {} };
  RED.nodes.createNode = (node, config) => {
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
