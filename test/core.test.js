'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { EdgeClient, Revoked, cleanItems, splitReads, clientCaps, ONLINE, REVOKED } = require('../lib/core');

const PORTAL = 'c0ffee000001';
const PLAN = [
  { type: 'mqtt', name: 'live', everyS: 30, dev: PORTAL, paths: ['system/0/Dc/Battery/Soc'] },
  { type: 'mqtt', name: 'dess', everyS: 300, dev: PORTAL, base: 'settings/0/Settings/DynamicEss/', paths: ['Schedule/+/Soc'] },
  { type: 'modbus', fc: 3, addr: 588, count: 1 },
];

function fakeTransport() {
  return {
    portal: PORTAL,
    filters: [],
    setFilters(f) { const n = f.filter((x) => !this.filters.includes(x)).length; this.filters = f; return n; },
    readItem(it) {
      const v = it.name === 'dess' ? { 'Schedule/0/Soc': 80 } : it.name === 'cmd' ? { [it.paths[0]]: 1 } : { 'system/0/Dc/Battery/Soc': 57 };
      return { type: 'mqtt', name: it.name, dev: PORTAL, values: v, tsBy: Object.fromEntries(Object.keys(v).map((k) => [k, 100])) };
    },
  };
}

function fakeServer(replies) {
  const calls = [];
  const post = async (path, body, token) => {
    calls.push({ path, body, token });
    const r = replies.shift();
    if (r instanceof Error) throw r;
    return r || [200, { ok: true, accepted: (body.reads || []).length }];
  };
  return { post, calls };
}

test('caps: přenos mqtt, vestavěný profil victron-gx@1, pojistky', () => {
  assert.deepStrictEqual(clientCaps(), { proto: 2, transports: ['mqtt'], profiles: ['victron-gx@1'], features: ['lease'] });
});

test('plán: jen platné položky mqtt (modbus a nesmysly pryč)', () => {
  const it = cleanItems([...PLAN, { type: 'mqtt', name: 'x', paths: [] }, { type: 'mqtt', paths: ['a'] }, null, 'x']);
  assert.deepStrictEqual(it.map((i) => i.name), ['live', 'dess']);
  assert.strictEqual(it[1].base, 'settings/0/Settings/DynamicEss/');
});

test('splitReads: strop těla a počtu položek', () => {
  const big = { name: 'big', values: { a: 'x'.repeat(100) } };
  const n = JSON.stringify(big).length;
  assert.deepStrictEqual(splitReads([big, big, big], 2 * n + 10).map((c) => c.length), [2, 1]);
  assert.deepStrictEqual(splitReads([big], 50), []);
  assert.strictEqual(splitReads(Array(20).fill({ n: 1 })).length, 2);
});

test('párování: kód + portal + caps → token, plán, odběr s base', async () => {
  const tr = fakeTransport();
  const srv = fakeServer([[200, { ok: true, token: 'T', clientId: 5, readPlan: PLAN, nextPollS: 10, profile: 'victron-gx@1' }]]);
  const c = new EdgeClient({ post: srv.post, transport: tr, version: 'nodered/test', clock: () => 1000 });
  await c.pair('ABCD-EFGH', { kind: 'sim' });
  const b = srv.calls[0].body;
  assert.deepStrictEqual([b.code, b.kind, b.sn, b.caps.transports], ['ABCD-EFGH', 'sim', PORTAL, ['mqtt']]);
  assert.strictEqual(c.token, 'T');
  await c.tick();
  assert.deepStrictEqual(tr.filters, ['system/0/Dc/Battery/Soc', 'settings/0/Settings/DynamicEss/Schedule/+/Soc']);
});

test('párování: odmítnutí serveru → chyba se zprávou', async () => {
  const srv = fakeServer([[400, { ok: false, error: 'bad-code', message: 'Neplatný nebo prošlý párovací kód.' }]]);
  const c = new EdgeClient({ post: srv.post, transport: fakeTransport(), version: 'v' });
  await assert.rejects(c.pair('XXXX-XXXX'), /Neplatný/);
});

test('sync: čtení podle everyS, ctl off, Bearer, heartbeat', async () => {
  let t = 1000;
  const srv = fakeServer([]);
  const c = new EdgeClient({ post: srv.post, transport: fakeTransport(), version: 'v', clock: () => t });
  c.start('T', PLAN, 10);
  await c.tick();
  assert.deepStrictEqual(srv.calls[0].body.reads, [], 'kolo s novým odběrem jen ohlášení (GX ještě neposlal přehled)');
  t += 1; await c.tick();
  const s = srv.calls[1];
  assert.strictEqual(s.token, 'T');
  assert.strictEqual(s.body.client.ctl, 'off');
  assert.deepStrictEqual(s.body.reads.map((r) => [r.name, r.dev]), [['live', PORTAL], ['dess', PORTAL]]);
  assert.deepStrictEqual(s.body.reads[1].values, { 'Schedule/0/Soc': 80 });
  assert.strictEqual(c.status, ONLINE);
  t += 10; await c.tick();
  assert.strictEqual(srv.calls.length, 2, 'nic na řadě a bez heartbeatu');
  t += 25; await c.tick();
  assert.deepStrictEqual(srv.calls[2].body.reads.map((r) => r.name), ['live']);
});

test('příkazy: mqtt read provede, zápis odmítne (local-off), cizí GX foreign-sn, opakované id jen výsledek', async () => {
  let t = 1000;
  const cmds = [
    { id: 'c-1', type: 'mqtt', dev: PORTAL, op: 'read', paths: ['settings/0/Settings/DynamicEss/Mode'], expiresInS: 10 },
    { id: 'c-2', type: 'mqtt', dev: PORTAL, op: 'write', values: [['settings/0/Settings/DynamicEss/Mode', 0]] },
    { id: 'c-3', type: 'mqtt', dev: 'deadbeef0001', op: 'read', paths: ['x'] },
    { id: 'c-4', type: 'modbus', fc: 16, addr: 1, values: [1] },
    { type: 'mqtt', op: 'read', paths: ['x'] },
  ];
  const srv = fakeServer([[200, { ok: true, commands: cmds }], [200, { ok: true }], [200, { ok: true, commands: [cmds[0]] }]]);
  const c = new EdgeClient({ post: srv.post, transport: fakeTransport(), version: 'v', clock: () => t });
  c.start('T', PLAN, 10);
  await c.tick();
  const res = srv.calls[1].body.results;
  assert.deepStrictEqual(res.map((r) => [r.id, r.ok, r.err || null]), [
    ['c-1', true, null], ['c-2', false, 'local-off'], ['c-3', false, 'foreign-sn'],
    ['c-4', false, 'bad-command: klient umí jen mqtt']]);
  assert.deepStrictEqual(res[0].values, { 'settings/0/Settings/DynamicEss/Mode': 1 });
  t += 60; await c.tick();
  assert.strictEqual(c.commandsDone, 4, 'c-1 podruhé neprovedeno');
});

test('chyby: 401 → Revoked, 429 → zpomalit, 409 → backoff, síť → error a znovu', async () => {
  let t = 1000;
  const srv = fakeServer([new Error('ECONNREFUSED'), [429, {}], [409, { error: 'unsupported' }], [401, {}]]);
  const c = new EdgeClient({ post: srv.post, transport: fakeTransport(), version: 'v', clock: () => t });
  c.start('T', PLAN, 10);
  await c.tick();
  assert.match(c.lastError, /server nedostupný/);
  t += 31; await c.tick();
  assert.match(c.lastError, /zpomaluji/);
  t += 30; await c.tick();
  assert.strictEqual(srv.calls.length, 2, 'po 429 minutu nic');
  t += 31; await c.tick();
  assert.strictEqual(c.lastError, 'unsupported');
  t += 299; await c.tick();
  assert.strictEqual(srv.calls.length, 3, 'po 409 pět minut nic');
  t += 2;
  await assert.rejects(c.tick(), Revoked);
  assert.strictEqual(c.status, REVOKED);
});
