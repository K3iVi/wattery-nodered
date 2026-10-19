'use strict';
/** Řízení (0.2): zápis W/ přes profil, limity, přepínač, identita GX, pojistky (leases) a zlatý scénář se simulátorem. */
const test = require('node:test');
const assert = require('node:assert');
const { EdgeClient } = require('../lib/core');
const { GxMqtt } = require('../lib/gx-mqtt');
const { FakeGx } = require('./fake-gx');

const PORTAL = 'c0ffee000001';
const D = 'settings/0/Settings/DynamicEss/';
const MODE = `${D}Mode`;
const PLAN = [{ type: 'mqtt', name: 'dess', everyS: 30, dev: PORTAL, paths: [MODE] }];
const write = (id, values, extra = {}) => ({ id, type: 'mqtt', dev: PORTAL, op: 'write', values, expiresInS: 30, ...extra });

/** Přenos bez sítě: zápis změní hodnotu a vrátí ozvěnu. */
function fakeTransport(values = { [MODE]: 4 }) {
  return {
    portal: PORTAL,
    connected: true,
    values,
    writes: [],
    setFilters() { return 0; },
    readItem(it) { return { values: Object.fromEntries(it.paths.map((p) => [p, this.values[p]])), tsBy: {} }; },
    async write(pairs) {
      this.writes.push(pairs);
      for (const [p, v] of pairs) this.values[p] = v;
      return Object.fromEntries(pairs);
    },
  };
}

/** Server: handler(body, n) → [kód, json] | Error. Pamatuje si těla. */
function fakeServer(handler) {
  const calls = [];
  const post = async (path, body) => {
    calls.push(body);
    const r = handler(body, calls.length);
    if (r instanceof Error) throw r;
    return r;
  };
  return { post, calls };
}

function client({ srv, tr = fakeTransport(), clock, control = true, sn = PORTAL, state = null, onPersist = null,
                  log = () => {} }) {
  const sleeps = [];
  const c = new EdgeClient({ post: srv.post, transport: tr, version: 'nodered/test', clock, sn, state, onPersist, log,
                             sleep: async (s) => { sleeps.push(s); } });
  c.start('T', PLAN, 10);
  c.setControl(control);
  c.sleeps = sleeps;
  return c;
}

const ok = (extra = {}) => [200, { ok: true, serverTime: 1000, profile: 'victron-gx@1', ...extra }];

test('zápis: přepínač vypnutý → local-off; ctl hlášen serveru', async () => {
  let t = 1000;
  const srv = fakeServer((b, n) => (n === 1 ? ok({ commands: [write('c-1', [[MODE, 0]])] }) : ok()));
  const tr = fakeTransport();
  const c = client({ srv, tr, clock: () => t, control: false });
  await c.tick();
  assert.strictEqual(srv.calls[0].client.ctl, 'off');
  assert.deepStrictEqual(srv.calls[1].results.map((r) => r.err), ['local-off']);
  assert.deepStrictEqual(tr.writes, []);
  c.setControl(true);
  t += 1; await c.tick();
  assert.strictEqual(srv.calls[2].client.ctl, 'on', 'změna přepínače hned v dalším kole');
});

test('zápis: v profilu → W/ a ozvěna; mimo profil / Mode 1 / bez profilu / cizí GX → nic do GX', async () => {
  const t = 1000;
  const cmds = [write('c-1', [[`${D}Schedule/0/Soc`, 80], [MODE, 4]]), write('c-2', [[MODE, 1]]),
                write('c-3', [[`${D}Hack`, 1]]), write('c-4', [[MODE, 0]], { dev: 'deadbeef0001' })];
  const srv = fakeServer((b, n) => (n === 1 ? ok({ commands: cmds }) : ok()));
  const tr = fakeTransport();
  const c = client({ srv, tr, clock: () => t });
  await c.tick();
  const res = srv.calls[1].results;
  assert.deepStrictEqual(res[0].echo, { [`${D}Schedule/0/Soc`]: 80, [MODE]: 4 });
  assert.deepStrictEqual(res.map((r) => [r.id, r.ok]), [['c-1', true], ['c-2', false], ['c-3', false], ['c-4', false]]);
  assert.match(res[1].err, /^not-allowed: .*není povolená/);
  assert.match(res[2].err, /^not-allowed: .*mimo profil/);
  assert.strictEqual(res[3].err, 'foreign-sn');
  assert.deepStrictEqual(tr.writes, [[[`${D}Schedule/0/Soc`, 80], [MODE, 4]]]);

  const srv2 = fakeServer((b, n) => (n === 1 ? ok({ profile: null, commands: [write('c-9', [[MODE, 0]])] }) : ok({ profile: null })));
  const c2 = client({ srv: srv2, clock: () => t });
  await c2.tick();
  assert.match(srv2.calls[1].results[0].err, /jen ke čtení/);

  const srv3 = fakeServer((b, n) => (n === 1 ? ok({ commands: [write('c-8', [[MODE, 0]])] }) : ok()));
  const c3 = client({ srv: srv3, clock: () => t, sn: 'abcdef000001' });   // token spárovaný s jiným GX
  await c3.tick();
  assert.match(srv3.calls[1].results[0].err, /identita GX neověřená/);
});

test('nesoulad profilu: server pošle profil, který uzel nemá (victron-gx@2) → jen čtení, hláška v logu, nic do GX', async () => {
  let t = 1000;
  const logs = [];
  const srv = fakeServer((b, n) => {
    if (n === 1) return ok({ profile: 'victron-gx@2', commands: [write('c-1', [[MODE, 0]])] });
    if (n === 2) return ok({ profile: 'victron-gx@2' });
    if (n === 3) return ok({ commands: [write('c-2', [[MODE, 0]])] });     // server zpět na @1
    return ok();
  });
  const tr = fakeTransport();
  const c = client({ srv, tr, clock: () => t, log: (m) => logs.push(String(m)) });
  assert.deepStrictEqual(c.caps.profiles, ['victron-gx@1'], 'caps nabízí jen vestavěné profily');
  await c.tick();
  assert.strictEqual(c.profileId, null);
  assert.ok(logs.some((m) => /victron-gx@2 v uzlu není/.test(m)), logs);
  assert.match(srv.calls[1].results[0].err, /jen ke čtení/);
  assert.deepStrictEqual(tr.writes, [], 'nic do GX');
  t += 1; await c.tick();
  assert.strictEqual(c.profileId, 'victron-gx@1', 'server zpět na známý profil → řízení zase jde');
  const res2 = srv.calls.flatMap((b) => b.results || []).filter((r) => r.id === 'c-2');
  assert.deepStrictEqual(res2.map((r) => r.ok), [true]);
  assert.deepStrictEqual(tr.writes, [[[MODE, 0]]]);
  const old = new EdgeClient({ post: srv.post, transport: fakeTransport(), version: 'x', clock: () => t, sn: PORTAL,
                               state: { profile: 'victron-gx@2' } });
  assert.strictEqual(old.profileId, null, 'uložený neznámý profil se po restartu nenačte');
});

test('zápis: limity profilu (6/min, trvalá nastavení 12/den), odstup 1 s, prošlý příkaz', async () => {
  let t = 1000;
  const queue = [];
  const srv = fakeServer(() => ok({ commands: queue.splice(0) }));
  const tr = fakeTransport();
  const c = client({ srv, tr, clock: () => t });
  for (let i = 0; i < 7; i++) queue.push(write(`s-${i}`, [[`${D}Schedule/${i}/Soc`, 50]]));
  await c.tick();
  const res = srv.calls[1].results;
  assert.deepStrictEqual(res.map((r) => r.ok), [true, true, true, true, true, true, false]);
  assert.strictEqual(res[6].err, 'rate-limit');
  assert.ok(c.sleeps.length >= 5 && c.sleeps.every((s) => s > 0 && s <= 1), JSON.stringify(c.sleeps));
  // trvalá nastavení: 12 za den (hodinový limit 60 nevyčerpat → á 2 min)
  const persist = [];
  for (let i = 0; i < 13; i++) {
    t += 120;
    queue.push(write(`p-${i}`, [[MODE, i % 2 ? 0 : 4]]));
    await c.tick();
    persist.push(srv.calls[srv.calls.length - 1].results.find((r) => r.id === `p-${i}`));
  }
  assert.deepStrictEqual(persist.slice(0, 12).map((r) => r.ok), Array(12).fill(true));
  assert.strictEqual(persist[12].err, 'rate-limit: trvalá nastavení za den');
  // prošlý: příkaz s platností 0 s a uplynulým časem
  const late = new EdgeClient({ post: fakeServer(() => ok()).post, transport: fakeTransport(), version: 'v', sn: PORTAL,
                                clock: () => t, sleep: async () => { t += 5; } });
  late.setControl(true);
  late.profileId = 'victron-gx@1';
  late._ensureProfile();
  late._lastWrite = t;                                 // odstup → sleep posune čas za platnost
  const out = await late._execute(write('x', [[MODE, 4]], { expiresInS: 1 }), t);
  assert.strictEqual(out.err, 'expired');
});

test('pojistky: obnova á everyS (bez trvalých), chybějící se zahodí bez onExpire, 4xx onExpire nespustí', async () => {
  let t = 1000;
  const lease = { id: 'L1', source: 'command', validUntil: 1600, refresh: { cmds: [write(null, [[`${D}Schedule/0/Soc`, 70]])], everyS: 5 },
                  onExpire: [write(null, [[MODE, 0]])] };
  delete lease.refresh.cmds[0].id;
  delete lease.onExpire[0].id;
  const bad = { id: 'L2', validUntil: 1600, refresh: { cmds: [write(null, [[MODE, 4]])], everyS: 30 }, onExpire: [write(null, [[MODE, 0]])] };
  let leases = [lease, bad];
  let code = 200;
  const srv = fakeServer(() => [code, { ok: true, serverTime: t, profile: 'victron-gx@1', leases }]);
  const tr = fakeTransport();
  const saved = [];
  const c = client({ srv, tr, clock: () => t, onPersist: (s) => saved.push(s) });
  await c.tick();
  assert.deepStrictEqual(Object.keys(c.leases), ['L1'], 'obnova trvalého nastavení (Mode) neprojde');
  assert.strictEqual(c.leases.L1.refresh.everyS, 30, 'obnova nejčastěji á 30 s');
  t += 31; await c.tick();
  assert.strictEqual(c.refreshes, 1);
  assert.deepStrictEqual(tr.writes.at(-1), [[`${D}Schedule/0/Soc`, 70]]);
  assert.ok(saved.at(-1).leases.some((x) => x.id === 'L1'), 'pojistka uložena');
  code = 403;                                          // odmítnutí, ne výpadek → onExpire ne
  t = 1700; await c.tick();
  assert.ok(!tr.writes.some((w) => w[0][0] === MODE), 'po 4xx se onExpire neprovede');
  code = 200;
  leases = [];
  t = 2100; await c.tick();
  assert.deepStrictEqual(c.leases, {}, 'co v seznamu chybí, zahodit');
  assert.ok(!tr.writes.some((w) => w[0][0] === MODE), 'bez onExpire');
});

test('pojistky: stav přežije restart (uložená pojistka se po výpadku provede)', async () => {
  let t = 1000;
  const lease = { id: 'L1', validUntil: 1100, onExpire: [{ type: 'mqtt', dev: PORTAL, op: 'write', values: [[MODE, 0]] }] };
  const srv1 = fakeServer(() => [200, { ok: true, serverTime: t, profile: 'victron-gx@1', leases: [lease] }]);
  let st = null;
  const c1 = client({ srv: srv1, clock: () => t, onPersist: (s) => { st = JSON.parse(JSON.stringify(s)); } });
  await c1.tick();
  assert.strictEqual(st.profile, 'victron-gx@1');
  assert.strictEqual(st.leases[0].deadline, 1100);
  const tr = fakeTransport();
  const srv2 = fakeServer(() => new Error('ECONNREFUSED'));
  t = 1200;
  const c2 = client({ srv: srv2, tr, clock: () => t, state: st });
  await c2.tick();
  assert.deepStrictEqual(tr.writes, [[[MODE, 0]]]);
});

test('ZLATÝ: zápis → ozvěna z GX → server zmizí → pojistka vyprší → návrat (onExpire) → hlášení po obnovení', async () => {
  const gx = await new FakeGx({ lagMs: 100 }).start();
  const tr = new GxMqtt({ port: gx.port, writable: true });
  let t = 10000;
  let down = false;
  const reports = [];
  const onExpire = [{ type: 'mqtt', dev: PORTAL, op: 'write', values: [[MODE, 0]] }];
  const srv = fakeServer((b, n) => {
    if (down) return new Error('ECONNREFUSED');
    if (b.leaseReport) reports.push(b.leaseReport);
    const res = { ok: true, serverTime: t, profile: 'victron-gx@1', control: 'on', readPlan: PLAN,
                  leases: [{ id: 'dess-1', source: 'command', validUntil: 10060, onExpire }] };
    if (n === 1) res.commands = [write('c-1', [[`${D}Schedule/0/Soc`, 95], [MODE, 4]])];
    if (b.leaseReport) { res.leaseAck = b.leaseReport.id; res.leases = []; }
    return [200, res];
  });
  try {
    await tr.start();
    gx.tree[MODE] = 0;
    const c = client({ srv, tr, clock: () => t });
    c.sleeps = null;
    await c.tick();
    const r1 = srv.calls[1].results[0];
    assert.deepStrictEqual([r1.id, r1.ok, r1.echo], ['c-1', true, { [`${D}Schedule/0/Soc`]: 95, [MODE]: 4 }]);
    assert.strictEqual(gx.tree[MODE], 4, 'GX: rozvrh wattery zapnutý');
    assert.deepStrictEqual(Object.keys(c.leases), ['dess-1']);
    down = true;                                       // server nedostupný
    t = 10030; await c.tick();
    assert.strictEqual(gx.tree[MODE], 4, 'před platností se nic neděje');
    t = 10061; c._lastSync = 0; await c.tick();
    assert.strictEqual(gx.tree[MODE], 0, 'po platnosti bez serveru: návrat (onExpire)');
    const writesAfter = gx.writes.length;
    t = 10100; c._lastSync = 0; await c.tick();
    assert.strictEqual(gx.writes.length, writesAfter, 'onExpire jen jednou');
    down = false;
    t = 10200; c._lastSync = 0; await c.tick();
    assert.strictEqual(reports.length, 1);
    assert.deepStrictEqual([reports[0].id, reports[0].ok, reports[0].steps[0].echo], ['dess-1', true, { [MODE]: 0 }]);
    assert.deepStrictEqual(c.leases, {});
    assert.deepStrictEqual(gx.writes.map(([p, v]) => `${p.split('/').pop()}=${v}`), ['Soc=95', 'Mode=4', 'Mode=0']);
    assert.ok(gx.clientTopics.every((x) => /^(R|W)\//.test(x)));
  } finally {
    tr.close();
    await gx.stop();
  }
});
