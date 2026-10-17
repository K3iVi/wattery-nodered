'use strict';
/**
 * Stejné jádro jako uzel, ale bez Node-RED — pro E2E (tools/e2e.py) a ladění:
 *   node tools/headless.js --url http://127.0.0.1:8099 --code ABCD-EFGH --host 127.0.0.1 --port 1883
 *                          [--password x] [--kind sim] [--seconds 20]
 * Na konci vypíše jeden řádek JSON se stavem (status, accepted, commands, published témata).
 */
const { GxMqtt } = require('../lib/gx-mqtt');
const { EdgeClient, Revoked } = require('../lib/core');
const { makePost } = require('../lib/http');

function args() {
  const a = {};
  const v = process.argv.slice(2);
  for (let i = 0; i < v.length; i += 2) a[v[i].replace(/^--/, '')] = v[i + 1];
  return a;
}

async function main() {
  const a = args();
  const gx = new GxMqtt({ host: a.host || '127.0.0.1', port: parseInt(a.port || '1883', 10), password: a.password || '' });
  await gx.start();
  const log = (m) => process.stderr.write(`[wattery] ${m}\n`);
  const client = new EdgeClient({ post: makePost(a.url), transport: gx, version: 'nodered/headless', log,
                                  onStatus: (c) => log(`stav ${c.status}${c.lastError ? ' — ' + c.lastError : ''}`) });
  const res = await client.pair(a.code, { kind: a.kind || 'nodered', label: 'headless' });
  log(`spárováno: klient ${res.clientId}, plán ${client.items.map((i) => i.name).join(', ')}`);
  const end = Date.now() + parseFloat(a.seconds || '20') * 1000;
  let revoked = false;
  while (Date.now() < end) {
    try {
      await client.tick();
    } catch (e) {
      if (e instanceof Revoked) { revoked = true; break; }
      throw e;
    }
    await new Promise((r) => setTimeout(r, Math.min(client.pollS(), 1) * 1000));
  }
  gx.close();
  process.stdout.write(JSON.stringify({
    portal: gx.portal, status: client.status, revoked, accepted: client.lastAccepted, items: client.items.length,
    commandsDone: client.commandsDone, lastCommand: client.lastCommand, published: [...new Set(gx.published)],
    clientId: res.clientId,
  }) + '\n');
}

main().catch((e) => { process.stderr.write(`chyba: ${e.stack || e}\n`); process.exit(1); });
