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
