'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { GxMqtt, matchFilter, validFilter } = require('../lib/gx-mqtt');
const { FakeGx, PORTAL } = require('./fake-gx');

const settle = (ms = 150) => new Promise((r) => setTimeout(r, ms));

test('filtry: shoda a platnost jako v Pythonu', () => {
  assert.ok(matchFilter('system/0/Ac/Grid/+/Power', 'system/0/Ac/Grid/L1/Power'));
  assert.ok(!matchFilter('system/0/Ac/Grid/+/Power', 'system/0/Ac/Grid/L1/Current'));
  assert.ok(matchFilter('settings/#', 'settings/0/Settings/DynamicEss/Mode'));
  assert.ok(validFilter('solarcharger/+/Pv/+/P') && validFilter('a/#'));
  for (const bad of ['../W/x', 'a/b+/c', 'x/#/y', '', '#', 'a b']) assert.ok(!validFilter(bad), bad);
});

test('GX: zjištění portal ID, odběr, hodnoty, keepalive — a nikdy W/', async () => {
  const b = await new FakeGx().start();
  const c = new GxMqtt({ host: '127.0.0.1', port: b.port, gxKeepaliveS: 0.3 });
  c.setFilters(['system/0/Dc/Battery/Soc', 'system/0/Ac/Grid/+/Power', '../W/x']);
  try {
    await c.start();
    await settle();
    assert.strictEqual(c.portal, PORTAL);
    const rd = c.readItem({ type: 'mqtt', name: 'live', paths: ['system/0/Dc/Battery/Soc', 'system/0/Ac/Grid/+/Power'] });
    assert.deepStrictEqual(rd.values, { 'system/0/Dc/Battery/Soc': 57.5, 'system/0/Ac/Grid/L1/Power': 300,
                                        'system/0/Ac/Grid/L2/Power': null });
    assert.strictEqual(rd.dev, PORTAL);
    b.set('system/0/Dc/Battery/Soc', 58);
    await settle();
    assert.strictEqual(c.values['system/0/Dc/Battery/Soc'], 58);
    c.setFilters(['settings/0/Settings/DynamicEss/Schedule/+/Soc']);
    await settle();
    const dess = c.readItem({ type: 'mqtt', name: 'dess', base: 'settings/0/Settings/DynamicEss/', paths: ['Schedule/+/Soc'] });
    assert.deepStrictEqual(dess.values, { 'Schedule/0/Soc': 80, 'Schedule/1/Soc': 60 });
    await settle(1300);
    assert.ok(b.keepalives.some((p) => p.includes('suppress-republish')), 'keepalive suppress');
    assert.throws(() => c._publish(`W/${PORTAL}/settings/0/Settings/DynamicEss/Mode`, '{"value":0}'));
    assert.ok(b.clientTopics.every((t) => t === `R/${PORTAL}/keepalive`), b.clientTopics.join(','));
    assert.ok(!c.filters.includes('../W/x'));
    b.kick();                                           // restart brokeru → staré hodnoty se nesmí číst jako čerstvé
    await settle();
    assert.strictEqual(c.connected, false);
    assert.throws(() => c.readItem({ type: 'mqtt', name: 'live', paths: ['system/0/Dc/Battery/Soc'] }), /není připojené/);
  } finally {
    c.close();
    await b.stop();
  }
});

test('GX: heslo — špatné odmítnuto, správné projde; nedostupný GX = chyba', async () => {
  const b = await new FakeGx({ password: 'tajne' }).start();
  try {
    await assert.rejects(new GxMqtt({ port: b.port, password: 'spatne' }).start(), /přístup nepovolen/);
    const ok = new GxMqtt({ port: b.port, password: 'tajne' });
    await ok.start();
    assert.strictEqual(ok.portal, PORTAL);
    ok.close();
  } finally {
    await b.stop();
  }
  await assert.rejects(new GxMqtt({ port: 1 }).start(), /nedostupné/);
});

test('GX: ztráta spojení → událost lost', async () => {
  const b = await new FakeGx().start();
  const c = new GxMqtt({ port: b.port });
  await c.start();
  const lost = new Promise((r) => c.once('lost', r));
  await b.stop();
  const err = await lost;
  assert.ok(err && !c.connected);
});

const D = 'settings/0/Settings/DynamicEss/';

test('GX zápis: bez writable nic (W/ ani R/<cesta>), dávka neprojde', async () => {
  const b = await new FakeGx().start();
  const c = new GxMqtt({ port: b.port });
  try {
    await c.start();
    await assert.rejects(c.write([[`${D}Mode`, 0]]), /vypnutý/);
    assert.throws(() => c._publish(`W/${PORTAL}/${D}Mode`, '{"value":0}', true), /není povolené/);
    assert.throws(() => c._publish(`R/${PORTAL}/${D}Mode`, '', true), /není povolené/);
    await settle();
    assert.deepStrictEqual(b.writes, []);
    assert.ok(b.clientTopics.every((t) => t === `R/${PORTAL}/keepalive`), b.clientTopics.join(','));
  } finally {
    c.close();
    await b.stop();
  }
});

test('GX zápis: W/ v pořadí, R/<cesta>, ozvěna N/ po zápisu (i se zpožděním dbus)', async () => {
  const b = await new FakeGx({ lagMs: 300 }).start();
  const c = new GxMqtt({ port: b.port, writable: true });
  try {
    await c.start();
    const t0 = Date.now();
    const echo = await c.write([[`${D}Schedule/0/Soc`, 90], [`${D}Mode`, 0]]);
    assert.deepStrictEqual(echo, { [`${D}Schedule/0/Soc`]: 90, [`${D}Mode`]: 0 });
    assert.ok(Date.now() - t0 >= 250 && Date.now() - t0 < 3000, `${Date.now() - t0} ms`);
    assert.deepStrictEqual(b.writes, [[`${D}Schedule/0/Soc`, 90], [`${D}Mode`, 0]]);
    assert.deepStrictEqual(b.tree[`${D}Mode`], 0);
    const pub = b.clientTopics.filter((t) => t !== `R/${PORTAL}/keepalive`);
    assert.deepStrictEqual(pub, [`W/${PORTAL}/${D}Schedule/0/Soc`, `W/${PORTAL}/${D}Mode`,
                                 `R/${PORTAL}/${D}Schedule/0/Soc`, `R/${PORTAL}/${D}Mode`]);
    // stejná hodnota znovu: GX změnu nepublikuje, ozvěnu dá R/<cesta>
    assert.deepStrictEqual(await c.write([[`${D}Mode`, 0]]), { [`${D}Mode`]: 0 });
  } finally {
    c.close();
    await b.stop();
  }
});

test('GX zápis: GX hodnotu nepřijme → ozvěna se starou hodnotou po timeoutu; neznámá cesta chybí', async () => {
  const b = await new FakeGx({ reject: [`${D}Mode`] }).start();
  const c = new GxMqtt({ port: b.port, writable: true, echoTimeoutS: 0.5 });
  try {
    await c.start();
    const echo = await c.write([[`${D}Mode`, 0], [`${D}Neni`, 1]]);
    assert.deepStrictEqual(echo, { [`${D}Mode`]: 4 });
    await assert.rejects(c.write([[`${D}Schedule/+/Soc`, 1]]), /neplatná položka/);
    await assert.rejects(c.write([[`${D}Mode`, 0], [`${D}Mode`, 4]]), /neplatná položka/);
    await assert.rejects(c.write([[`${D}Mode`, '0']]), /neplatná položka/);
  } finally {
    c.close();
    await b.stop();
  }
});
