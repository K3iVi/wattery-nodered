'use strict';
/**
 * Uzel Node-RED „wattery“ — lokální spojení Victron GX ↔ wattery (NR1: jen čtení).
 * Čte broker GX na localhost (bez hesla), párování kódem z aplikace wattery, token v souboru v userDir
 * (kontext Node-RED je na Venus OS jen v paměti — NR0). Do GX nic nezapisuje.
 */
const fs = require('fs');
const path = require('path');
const { GxMqtt } = require('../lib/gx-mqtt');
const { EdgeClient, Revoked, ONLINE, REVOKED } = require('../lib/core');
const { makePost } = require('../lib/http');
const VERSION = `nodered/${require('../package.json').version}`;

const DEFAULT_SERVER = 'https://monitoring.wattery.cz';
const RECONNECT_S = 10;
const STATE_FILE = 'wattery-edge.json';

function stateFile(RED) {
  return path.join((RED.settings && RED.settings.userDir) || process.cwd(), STATE_FILE);
}

function loadState(RED) {
  try { return JSON.parse(fs.readFileSync(stateFile(RED), 'utf8')); } catch (e) { return {}; }
}

function saveState(RED, st) {
  const f = stateFile(RED);
  fs.writeFileSync(`${f}.tmp`, JSON.stringify(st), { mode: 0o600 });
  fs.renameSync(`${f}.tmp`, f);
}

const hhmm = (t) => new Date(t * 1000).toTimeString().slice(0, 5);

module.exports = function register(RED) {
  function WatteryNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const server = String(config.server || DEFAULT_SERVER).replace(/\/+$/, '');
    const host = config.host || '127.0.0.1';
    const port = parseInt(config.port, 10) || 1883;
    const kind = config.kind || 'nodered';
    let gx = null;
    let client = null;
    let timer = null;
    let closing = false;

    const status = (fill, text) => node.status({ fill, shape: 'dot', text: String(text).slice(0, 60) });
    const later = (fn, s) => { clearTimeout(timer); if (!closing) timer = setTimeout(fn, s * 1000); };

    function onStatus(c) {
      if (c.status === ONLINE) status('green', `čte · ${hhmm(c.lastSyncAt)}${c.lastError ? ' · ' + c.lastError : ''}`);
      else if (c.status === REVOKED) status('red', 'odpojeno ve wattery — spárujte znovu');
      else if (c.lastError) status('yellow', c.lastError);
    }

    async function loop() {
      try {
        await client.tick();
      } catch (e) {
        if (e instanceof Revoked) {
          const st = loadState(RED);
          delete st[`${server}|${gx.portal}`];
          saveState(RED, st);
          node.warn('wattery: token neplatí (odpojeno ve wattery) — zadejte nový párovací kód a nasaďte');
          return;
        }
        node.error(`wattery: chyba ve smyčce: ${e.message}`);
      }
      later(loop, client.pollS());
    }

    async function connect() {
      status('grey', 'připojuji se ke GX…');
      gx = new GxMqtt({ host, port });
      gx.on('lost', (err) => {
        status('yellow', `GX: ${err.message} — zkusím znovu`);
        clearTimeout(timer);
        later(connect, RECONNECT_S);
      });
      try {
        await gx.start();
      } catch (e) {
        status('red', `GX: ${e.message}`);
        later(connect, RECONNECT_S);
        return;
      }
      const key = `${server}|${gx.portal}`;
      const saved = loadState(RED)[key];
      client = client || new EdgeClient({ post: makePost(server, { userAgent: VERSION }), transport: gx, version: VERSION,
                                          log: (m) => node.log(m), onStatus });
      client.setTransport(gx);                          // po znovupřipojení GX nový přenos, stejný stav klienta
      if (client.token) {
        // už běží (jen nové spojení s GX)
      } else if (saved && saved.token) {
        client.start(saved.token, saved.readPlan, saved.nextPollS);
      } else {
        const code = node.credentials && node.credentials.code;
        if (!code) { status('yellow', `GX ${gx.portal} — zadejte párovací kód z aplikace wattery`); return; }
        status('grey', 'páruji…');
        try {
          const res = await client.pair(code, { kind, label: config.name || 'Node-RED na GX' });
          const st = loadState(RED);
          st[key] = { token: res.token, readPlan: res.readPlan, nextPollS: res.nextPollS, pairedAt: Math.floor(Date.now() / 1000) };
          saveState(RED, st);
          node.log(`wattery: spárováno (GX ${gx.portal}, elektrárna ${res.stationId})`);
        } catch (e) {
          status('red', `párování: ${e.message}`);
          return;
        }
      }
      status('green', `GX ${gx.portal} — čtu`);
      later(loop, 1);
    }

    connect();

    node.on('close', (done) => {
      closing = true;
      clearTimeout(timer);
      if (gx) gx.close();
      done();
    });
  }

  RED.nodes.registerType('wattery', WatteryNode, { credentials: { code: { type: 'password' } } });
};
