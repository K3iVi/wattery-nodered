'use strict';
/**
 * Profil zápisu wattery pro přenos MQTT (Victron GX) — co smí uzel do GX zapsat. Port části „Profil MQTT“
 * z `wattery-ha/custom_components/wattery/write_profile.py` (server: deye-monitor `deye/edge_profile.py`) — držet
 * stejně; shodu rozhodnutí hlídají zlaté vektory z Python enginu (tools/gen_vectors.py → test/vectors).
 *
 * Položky {path, check: range|enum, min/max | values, int?, persist?}; cesta je relativní k `W/<portal>/` a smí mít
 * vzory `{0..47}` (číslo z rozsahu) a `{n}` (libovolná instance). Hodnoty jsou čísla (i desetinná), `int: true` = jen
 * celá. Platí první položka, jejíž vzor cestu pokryje. `limits.maxPaths` (povinné) = nejvíc cest v jedné dávce,
 * `limits.persistPerDay` = denní limit trvalých nastavení (flash).
 * Neznámá kontrola, chybná cesta nebo vzor = profil neplatný (Error) → uzel nic nezapíše.
 */
const fs = require('fs');
const path = require('path');

const ID_RE = /^[a-z0-9][a-z0-9-]{0,40}@[0-9]{1,4}$/;
const PATH_TOKEN = /(\{[^{}]*\})/;
const PATH_RANGE = /^\{([0-9]{1,4})\.\.([0-9]{1,4})\}$/;
const PATH_CHARS = /^[A-Za-z0-9_./{}-]+$/;
const MAX_PATH_LEN = 200;
const PROFILES_DIR = path.join(__dirname, '..', 'profiles');

const isNum = (v) => typeof v === 'number' && Number.isFinite(v);

function num(v, what) {
  if (!isNum(v)) throw new Error(`${what}: čekám číslo, je ${JSON.stringify(v)}`);
  return v;
}

function int(v, what) {
  if (!Number.isInteger(v)) throw new Error(`${what}: čekám celé číslo, je ${JSON.stringify(v)}`);
  return v;
}

const escapeRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** Vzor cesty MQTT → regulární výraz (celá cesta). */
function pathRe(pat, what) {
  if (typeof pat !== 'string' || !(pat.length > 0 && pat.length <= MAX_PATH_LEN) || !PATH_CHARS.test(pat)
      || pat.startsWith('/') || pat.endsWith('/') || pat.includes('//')) {
    throw new Error(`${what}: neplatná cesta ${JSON.stringify(pat)}`);
  }
  let out = '';
  for (const tok of pat.split(PATH_TOKEN)) {
    if (tok === '{n}') {
      out += '[0-9]+';
    } else if (tok.startsWith('{')) {
      const m = PATH_RANGE.exec(tok);
      const lo = m ? parseInt(m[1], 10) : 0;
      const hi = m ? parseInt(m[2], 10) : -1;
      if (!m || !(hi - lo >= 0 && hi - lo <= 1000)) throw new Error(`${what}: neplatný vzor ${tok}`);
      const alts = [];
      for (let k = lo; k <= hi; k++) alts.push(String(k));
      out += `(?:${alts.join('|')})`;
    } else if (tok.includes('{') || tok.includes('}') || tok.includes('..')) {
      throw new Error(`${what}: neplatný vzor v cestě ${JSON.stringify(pat)}`);
    } else {
      out += escapeRe(tok);
    }
  }
  return new RegExp(`^(?:${out})$`);
}

/** Kontrola hodnoty jedné cesty → funkce (cesta, hodnota) → důvod | null. */
function pathCheck(spec, what) {
  const name = spec.check === undefined ? 'range' : spec.check;
  const asInt = spec.int === undefined ? false : spec.int;
  if (typeof asInt !== 'boolean') throw new Error(`${what}: int musí být true/false`);
  if (name === 'range') {
    if (!('min' in spec) || !('max' in spec)) throw new Error(`${what}: range potřebuje min i max`);
    const lo = num(spec.min, what);
    const hi = num(spec.max, what);
    if (lo > hi) throw new Error(`${what}: min > max`);
    return (p, v) => {
      if (asInt && !Number.isInteger(v)) return `${p}: hodnota ${v} není celé číslo`;
      if (!(lo <= v && v <= hi)) return `${p}: hodnota ${v} mimo ${lo}–${hi}`;
      return null;
    };
  }
  if (name === 'enum') {
    if (!Array.isArray(spec.values)) throw new Error(`${what}: enum potřebuje values`);
    const vals = new Set(spec.values.map((x) => int(x, what)));
    return (p, v) => (Number.isInteger(v) && vals.has(v) ? null : `${p}: hodnota ${v} není povolená`);
  }
  throw new Error(`${what}: neznámá kontrola cesty ${JSON.stringify(name)}`);
}

/** Profil (JSON) → {id, transport, limits, maxPaths, paths: [{re, problem, persist}]}. Jen přenos mqtt. */
function compileProfile(prof) {
  if (!prof || typeof prof !== 'object' || !Array.isArray(prof.write)) throw new Error('profil bez seznamu write');
  const pid = prof.id;
  const transport = prof.transport === undefined ? 'modbus' : prof.transport;
  if (transport !== 'mqtt') throw new Error(`${pid}: uzel Node-RED umí jen profil mqtt (je ${transport})`);
  const limits = { ...(prof.limits || {}) };
  if (!('maxPaths' in limits)) throw new Error(`${pid}: profil mqtt potřebuje limits.maxPaths`);
  const maxPaths = int(limits.maxPaths, `${pid} maxPaths`);
  if (maxPaths < 1) throw new Error(`${pid}: maxPaths < 1`);
  const paths = [];
  const seen = new Set();
  prof.write.forEach((e, i) => {
    const what = `${pid} write[${i}]`;
    if (!e || typeof e !== 'object' || Array.isArray(e)) throw new Error(`${what}: položka není objekt`);
    if (!('path' in e) || 'addr' in e) throw new Error(`${what}: profil mqtt má jen položky s cestou (path)`);
    if (e.enabled === false) return;
    if (seen.has(e.path)) throw new Error(`${what}: cesta ${e.path} je v profilu dvakrát`);
    seen.add(e.path);
    const persist = e.persist === undefined ? false : e.persist;
    if (typeof persist !== 'boolean') throw new Error(`${what}: persist musí být true/false`);
    paths.push({ re: pathRe(e.path, what), problem: pathCheck(e, what), persist });
  });
  return { id: pid, transport, limits, maxPaths, paths };
}

/** Vestavěný profil `profiles/<id>.json`. */
function load(profileId, dir = PROFILES_DIR) {
  if (typeof profileId !== 'string' || !ID_RE.test(profileId)) throw new Error(`neplatné id profilu ${JSON.stringify(profileId)}`);
  const prof = JSON.parse(fs.readFileSync(path.join(dir, `${profileId}.json`), 'utf8'));
  if (prof.id !== profileId) throw new Error(`profil ${profileId}: id v souboru ${JSON.stringify(prof.id)}`);
  return prof;
}

/** Všechny vestavěné profily → {id: zkompilovaný}. Neplatný se vynechá (log). */
function loadAll(dir = PROFILES_DIR, log = () => {}) {
  const out = {};
  for (const fn of fs.existsSync(dir) ? fs.readdirSync(dir).sort() : []) {
    if (!fn.endsWith('.json')) continue;
    const pid = fn.slice(0, -5);
    try {
      out[pid] = compileProfile(load(pid, dir));
    } catch (e) {
      log(`profil zápisu ${pid} je neplatný — nepoužiji ho: ${e.message}`);
    }
  }
  return out;
}

function entry(comp, p) {
  if (typeof p !== 'string') return null;
  return comp.paths.find((x) => x.re.test(p)) || null;
}

/** null = cestu smí uzel zapsat touto hodnotou; jinak důvod. */
function pathProblem(comp, p, value) {
  const ent = entry(comp, p);
  if (!ent) return `cesta ${p} mimo profil`;
  if (!isNum(value)) return `${p}: hodnota ${JSON.stringify(value)} není číslo`;
  return ent.problem(p, value);
}

/** Dávka [[cesta, hodnota], …] (pořadí závazné) → null = celá smí; jinak první důvod. Cesta nejvýš jednou. */
function batchProblem(comp, values) {
  if (!Array.isArray(values) || values.length < 1 || values.length > comp.maxPaths) return 'prázdná nebo příliš dlouhá dávka';
  const seen = new Set();
  for (const item of values) {
    if (!Array.isArray(item) || item.length !== 2 || typeof item[0] !== 'string') return 'neplatná položka dávky';
    if (seen.has(item[0])) return `cesta ${item[0]} je v dávce dvakrát`;
    seen.add(item[0]);
    const bad = pathProblem(comp, item[0], item[1]);
    if (bad) return bad;
  }
  return null;
}

/** Cesta je trvalé nastavení (flash) → počítá se do `limits.persistPerDay`. */
function persistent(comp, p) {
  const ent = entry(comp, p);
  return !!(ent && ent.persist);
}

module.exports = { ID_RE, PROFILES_DIR, compileProfile, load, loadAll, pathProblem, batchProblem, persistent };
