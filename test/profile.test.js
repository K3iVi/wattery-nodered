'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const P = require('../lib/profile');

const PID = 'victron-gx@1';
const V = JSON.parse(fs.readFileSync(path.join(__dirname, 'vectors', `${PID}.json`), 'utf8'));
const comp = P.compileProfile(P.load(PID));

test('zlaté vektory z Pythonu: každá cesta × hodnota rozhodnuta stejně (povoleno / zakázáno, trvalé)', () => {
  const off = [];
  for (const c of V.paths) {
    const v = c.special ? Number(c.value) : c.value;           // NaN / Infinity JSON neumí
    const ok = P.pathProblem(comp, c.path, v) === null;
    if (ok !== c.ok || P.persistent(comp, c.path) !== c.persist) off.push(`${c.path} = ${JSON.stringify(c.value)}`);
  }
  assert.deepStrictEqual(off.slice(0, 10), [], `${off.length} rozdílů`);
  assert.ok(V.paths.length > 2000 && V.paths.some((c) => c.ok) && V.paths.some((c) => !c.ok));
});

test('zlaté vektory z Pythonu: dávky (pořadí, duplicity, maxPaths)', () => {
  for (const b of V.batches) {
    assert.strictEqual(P.batchProblem(comp, b.values) === null, b.ok, JSON.stringify(b.values).slice(0, 120));
  }
});

test('profil je bajtově stejný jako v HA klientu a na serveru (bez ohledu na konce řádků)', (t) => {
  const mine = fs.readFileSync(path.join(P.PROFILES_DIR, `${PID}.json`), 'utf8').replace(/\r\n/g, '\n');
  const fve = path.join(__dirname, '..', '..');
  const others = [path.join(fve, 'wattery-ha', 'custom_components', 'wattery', 'profiles', `${PID}.json`),
                  path.join(fve, 'deye-monitor', 'deye', 'edge_profiles', `${PID}.json`)].filter((f) => fs.existsSync(f));
  if (!others.length) { t.skip('sourozenecká repa nejsou vedle'); return; }
  for (const f of others) assert.strictEqual(fs.readFileSync(f, 'utf8').replace(/\r\n/g, '\n'), mine, f);
});

test('neplatný profil → chyba (uzel pak nic nezapíše)', () => {
  const base = { id: 'x@1', transport: 'mqtt', limits: { maxPaths: 5 } };
  const bad = [
    { ...base, write: [{ path: 'a/b', check: 'mask', mask: 1 }] },
    { ...base, write: [{ path: 'a/{0..x}', min: 0, max: 1 }] },
    { ...base, write: [{ path: '/a', min: 0, max: 1 }] },
    { ...base, write: [{ path: 'a', min: 2, max: 1 }] },
    { ...base, write: [{ path: 'a', min: 0, max: 1 }, { path: 'a', min: 0, max: 1 }] },
    { ...base, write: [{ addr: 1, min: 0, max: 1 }] },
    { id: 'x@1', transport: 'mqtt', write: [] },
    { id: 'x@1', transport: 'modbus', write: [] },
  ];
  for (const p of bad) assert.throws(() => P.compileProfile(p), Error, JSON.stringify(p));
});

test('vestavěné profily: victron-gx@1 se načte, Mode jen 0/4, MinimumSocLimit celé 5–80', () => {
  const all = P.loadAll();
  assert.deepStrictEqual(Object.keys(all), [PID]);
  const D = 'settings/0/Settings/DynamicEss/';
  assert.strictEqual(P.pathProblem(comp, `${D}Mode`, 4), null);
  assert.match(P.pathProblem(comp, `${D}Mode`, 1), /není povolená/);
  assert.match(P.pathProblem(comp, 'settings/0/Settings/CGwacs/BatteryLife/MinimumSocLimit', 10.5), /celé/);
  assert.match(P.pathProblem(comp, 'settings/0/Settings/CGwacs/Hub4Mode', 1), /mimo profil/);
  assert.ok(P.persistent(comp, `${D}Mode`) && !P.persistent(comp, `${D}Schedule/3/Soc`));
});

test('balíček npm obsahuje profily (bez nich by uzel nemohl řídit)', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
  assert.ok(pkg.files.includes('profiles/') && pkg.files.includes('lib/'), pkg.files);
  assert.strictEqual(path.relative(path.join(__dirname, '..'), P.PROFILES_DIR), 'profiles');
});
