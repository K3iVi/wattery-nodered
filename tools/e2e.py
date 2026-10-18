"""E2E: klient Node-RED (tools/headless.js = stejné jádro jako uzel) × skutečný hub deye-monitor × simulátor GX.

Hub běží jako `server.Handler` z deye-monitor na 127.0.0.1 nad čerstvou testovací DB (nikdy produkční), GX je
simulátor brokeru z wattery-ha (`tests/gx_sim.py`) se stavem `edge_victron.GxTree`. Ověří: párování kódem,
plán mqtt, čtení → stínový snímek (identita ze Serial, dataList), že klient do GX nic nepublikuje kromě keepalive
a že server zápis klientovi bez profilu nepošle (jde cloudem).

Spuštění (z kořene wattery-nodered; sourozenecká repa deye-monitor a wattery-ha vedle):
  python tools/e2e.py [--node C:\\cesta\\node.exe] [--seconds 25]
Do produkce nic nejde: DB v dočasné složce, HTTPS_PROXY na neexistující proxy (cloud nedostupný).
"""
import argparse
import asyncio
import json
import os
import shutil
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FVE = os.path.dirname(ROOT)
DM = os.path.join(FVE, 'deye-monitor')
HA_TESTS = os.path.join(FVE, 'wattery-ha', 'tests')

TMP = tempfile.mkdtemp(prefix='wattery-nr-e2e-')
os.environ.update({'DEYE_DB_PATH': os.path.join(TMP, 'e2e.db'), 'DEYE_EDGE': '1', 'DEYE_EDGE_BRANDS': 'victron',
                   'DEYE_EDGE_CONTROL': '1', 'HTTPS_PROXY': 'http://127.0.0.1:9', 'HTTP_PROXY': 'http://127.0.0.1:9',
                   'DEYE_SECRET_KEY': 'jen-e2e', 'PYTHONUTF8': '1'})
sys.path[:0] = [DM, HA_TESTS]

from http.server import ThreadingHTTPServer  # noqa: E402

from deye import edge, edge_channel, edge_victron as ev, store  # noqa: E402
import gx_sim  # noqa: E402

FAILS = []


def check(name, ok, detail=''):
    print(('[OK ] ' if ok else '[FAIL] ') + name + ('' if ok else f'  → {detail}'))
    if not ok:
        FAILS.append(name)


def seed(t):
    assert store.DB_PATH == os.environ['DEYE_DB_PATH'], store.DB_PATH
    store.init()
    owner, sid = 7001, 990001
    g = ev.GxTree(float(t))
    store.upsert_station({'id': sid, 'name': 'GX e2e'}, user_id=owner)
    store.grant_station_access(sid, owner)
    vsn = ev.SN_PREFIX + g.portal
    with store._conn() as c:
        c.execute("INSERT INTO devices (device_sn, station_id, model, brand, device_type) VALUES (?,?,?,?,?)",
                  (vsn, sid, 'Cerbo GX', 'victron', 'GX'))
    code = edge.create_pair_code(sid, owner)
    assert code.get('ok'), code
    return g, vsn, code['code']


def snapshot(vsn):
    with store._conn() as c:
        r = c.execute("SELECT raw_json, data_json FROM edge_snapshots WHERE device_sn=?", (vsn,)).fetchone()
        cl = c.execute("SELECT id, kind, client_ver, caps_json FROM edge_clients WHERE device_sn=? AND revoked_at IS NULL",
                       (vsn,)).fetchall()
    return (json.loads(r['raw_json']), json.loads(r['data_json'])) if r else ({}, {}), [dict(x) for x in cl]


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--node', default=shutil.which('node') or 'node')
    ap.add_argument('--seconds', type=float, default=25)
    a = ap.parse_args()
    t = int(time.time())
    g, vsn, code = seed(t)
    import server
    httpd = ThreadingHTTPServer(('127.0.0.1', 0), server.Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{httpd.server_address[1]}'
    broker = await gx_sim.GxBroker(password=None, portal=g.portal, tree=dict(g.values)).start()
    print(f'hub {url}, GX simulátor :{broker.port}, portal {g.portal}, DB {store.DB_PATH}')
    proc = await asyncio.create_subprocess_exec(
        a.node, os.path.join(HERE, 'headless.js'), '--url', url, '--code', code, '--host', '127.0.0.1',
        '--port', str(broker.port), '--kind', 'sim', '--seconds', str(a.seconds),
        stdout=asyncio.subprocess.PIPE, cwd=ROOT)
    await asyncio.sleep(min(10.0, a.seconds / 2))
    write = edge_channel.send(vsn, {'kind': 'victron.write', 'values': {ev.vmq.DESS_PREFIX + 'Mode': 0}})
    read = edge_channel.send(vsn, {'kind': 'victron.read', 'paths': ['settings/0/Settings/DynamicEss/Mode']})
    out, _ = await proc.communicate()
    await broker.stop()
    httpd.shutdown()
    res = json.loads(out.decode('utf-8').strip().splitlines()[-1]) if out else {}
    (raw, snap), clients = snapshot(vsn)
    dl = {it['key']: it['value'] for it in snap.get('dataList') or []}
    print('klient:', res)
    check('párování kódem: klient na serveru (kind sim, caps jen mqtt)', len(clients) == 1
          and json.loads(clients[0]['caps_json'] or '{}').get('transports') == ['mqtt'], clients)
    check('klient online, plán mqtt přijat', res.get('status') == 'online' and res.get('items', 0) >= 5, res)
    check('stínový snímek: identita ze system/0/Serial ověřená, dataList s SOC a výkony',
          snap.get('snVerified') is True and 'SOC' in dl and 'TotalGridPower' in dl, (snap.get('snVerified'), sorted(dl)[:8]))
    check('snímek: SOC = hodnota ze simulátoru GX', abs(float(dl.get('SOC', -1)) - float(g.values['system/0/Dc/Battery/Soc'])) < 1e-6,
          (dl.get('SOC'), g.values.get('system/0/Dc/Battery/Soc')))
    check('všechny položky plánu dorazily (raw.mqtt)', len(raw.get('mqtt') or {}) == res.get('items'),
          (sorted(raw.get('mqtt') or {}), res.get('items')))
    check('zápis Victron → klientovi bez profilu se nepošle (cloud)', write.cid is None, write)
    print('victron.read přes kanál:', read, '→', read.result() if read.cid else read.why)
    topics = set(broker.client_topics)
    check('klient do GX publikoval jen R/<portal>/keepalive (žádné W/)', topics == {f'R/{g.portal}/keepalive'}, topics)
    print('VŠE OK' if not FAILS else f'CHYBY: {len(FAILS)}')
    shutil.rmtree(TMP, ignore_errors=True)
    return 1 if FAILS else 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
