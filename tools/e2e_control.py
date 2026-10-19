"""E2E řízení: uzel Node-RED (tools/headless.js --control 1) × skutečný hub deye-monitor × simulátor GX (zápis W/).

Do produkce ani do žádného Cerba nic nejde: DB v dočasné složce, cloud za neexistující proxy, GX = simulátor
z wattery-ha (`tests/gx_sim.py`). Ověří celou cestu, kterou bude platforma řídit Victron přes Cerbo:
  1. párování kind 'nodered', admin zapne řízení klienta, uzel hlásí přepínač (ctl on), identita GX ověřená,
  2. operace `victron.write` (rozvrh DESS: slot + Mode 4) → edge_channel → příkaz mqtt → W/ do GX → ozvěna N/ →
     server ji ověří (edge_victron.verify) → výsledek ok, GX má nové hodnoty,
  3. zápis mimo profil (Mode 1 = VRM) server do klienta vůbec nepošle (jde cloudem),
  4. pojistka příkazu: server pošle lease s onExpire (Mode 0) → výpadek serveru (503, zkouška edge_cmd) → po platnosti
     uzel sám vrátí Mode 0 → po obnovení pošle leaseReport → server ho zapíše do auditu a pojistku smaže.

Spuštění (z kořene wattery-nodered; sourozenecká repa deye-monitor a wattery-ha vedle):
  python tools/e2e_control.py [--node C:\\cesta\\node.exe]
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

TMP = tempfile.mkdtemp(prefix='wattery-nr-e2ec-')
os.environ.update({'DEYE_DB_PATH': os.path.join(TMP, 'e2e.db'), 'DEYE_EDGE': '1', 'DEYE_EDGE_BRANDS': 'victron',
                   'DEYE_EDGE_CONTROL': '1', 'HTTPS_PROXY': 'http://127.0.0.1:9', 'HTTP_PROXY': 'http://127.0.0.1:9',
                   'DEYE_SECRET_KEY': 'jen-e2e', 'PYTHONUTF8': '1'})
sys.path[:0] = [DM, HA_TESTS]

from http.server import ThreadingHTTPServer  # noqa: E402

from deye import edge, edge_adapters, edge_channel, edge_cmd, edge_victron as ev, store  # noqa: E402
import gx_sim  # noqa: E402

FAILS = []
D = 'settings/0/Settings/DynamicEss/'


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


def client_row(vsn):
    with store._conn() as c:
        r = c.execute("SELECT * FROM edge_clients WHERE device_sn=? AND revoked_at IS NULL", (vsn,)).fetchone()
        s = c.execute("SELECT data_json FROM edge_snapshots WHERE device_sn=?", (vsn,)).fetchone()
    info = json.loads(r['info_json'] or '{}') if r else {}
    snap = json.loads(s['data_json'] or '{}') if s else {}
    return (dict(r) if r else None), info, snap


async def until(fn, s=20.0):
    end = time.time() + s
    while time.time() < end:
        if fn():
            return True
        await asyncio.sleep(0.2)
    return False


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--node', default=shutil.which('node') or 'node')
    a = ap.parse_args()
    assert 'nodered' in edge_cmd.CONTROL_KINDS, 'deye-monitor bez řízení nodered (potřeba 16bcd80+)'
    t = int(time.time())
    g, vsn, code = seed(t)
    tree = dict(g.values)
    tree[D + 'Mode'] = 0
    import server
    httpd = ThreadingHTTPServer(('127.0.0.1', 0), server.Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{httpd.server_address[1]}'
    broker = await gx_sim.GxBroker(password=None, portal=g.portal, tree=tree).start()
    print(f'hub {url}, GX simulátor :{broker.port}, portal {g.portal}, DB {store.DB_PATH}')
    proc = await asyncio.create_subprocess_exec(
        a.node, os.path.join(HERE, 'headless.js'), '--url', url, '--code', code, '--host', '127.0.0.1',
        '--port', str(broker.port), '--kind', 'nodered', '--control', '1', '--seconds', '75',
        stdout=asyncio.subprocess.PIPE, cwd=ROOT)
    try:
        # 1) párování, řízení, identita
        ok = await until(lambda: client_row(vsn)[0] is not None and client_row(vsn)[2].get('snVerified') is True)
        check('párování nodered + stínový snímek s ověřenou identitou', ok, client_row(vsn)[2].get('snVerified'))
        cl = client_row(vsn)[0]
        edge_cmd.set_control(cl['id'], 'on')
        t_on = int(time.time())
        # O zapnutém řízení se uzel dozví v příštím sync (bez řízení á 30 s); pak se ptá á 5 s.
        ok = await until(lambda: (client_row(vsn)[0]['last_seen_at'] or 0) > t_on and client_row(vsn)[1].get('ctl') == 'on', 40)
        check('uzel po zapnutí řízení: sync s přepínačem zapnutým (client.ctl on)', ok, client_row(vsn)[1])
        caps = edge_adapters.client_caps(client_row(vsn)[0])
        check('caps: mqtt, profil victron-gx@1, lease', caps.get('profiles') == ['victron-gx@1']
              and 'lease' in (caps.get('features') or []), caps)
        check('route: klient je cesta pro zápis', edge_cmd.route(vsn) is not None, edge_cmd._route(vsn))

        # 2) zápis rozvrhu přes uzel → W/ → ozvěna → ověření serverem
        op = ev.dess_op([('Schedule/0/Soc', 95), ('Schedule/0/Duration', 3600), ('Mode', 4)])
        p = edge_channel.send(vsn, op, source='e2e')
        res = await asyncio.to_thread(p.result)
        check('victron.write přes Node-RED: výsledek ověřený ozvěnou (via edge)', bool(res and res.get('ok') and res.get('via') == 'edge'),
              (p, res, edge_cmd.recent(cl['id'], 3)))
        check('GX simulátor má zapsané hodnoty (Mode 4, Soc 95)', broker.tree.get(D + 'Mode') == 4
              and broker.tree.get(D + 'Schedule/0/Soc') == 95, {k: broker.tree.get(D + k) for k in ('Mode', 'Schedule/0/Soc')})
        check('W/ přišly v pořadí (slot, Mode až nakonec)', [w[0] for w in broker.writes] ==
              [D + 'Schedule/0/Soc', D + 'Schedule/0/Duration', D + 'Mode'], broker.writes)

        # 3) mimo profil → do klienta nic
        n = len(broker.writes)
        p2 = edge_channel.send(vsn, ev.dess_op([('Mode', 1)]), source='e2e')
        res2 = await asyncio.to_thread(p2.result)
        check('Mode 1 (VRM) mimo profil: server klientovi nepošle (cloud)', res2 is None and len(broker.writes) == n, (p2, res2))

        # 4) pojistka: onExpire Mode 0 po výpadku serveru
        lease = {'id': 'e2e-dess', 'onExpire': [{'type': 'mqtt', 'dev': g.portal, 'op': 'write', 'values': [[D + 'Mode', 0]]}]}
        with store._conn() as c:
            c.execute("INSERT INTO edge_leases (client_id, lease_id, device_sn, op_kind, lease_json, valid_for, hard_until, "
                      "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                      (cl['id'], lease['id'], vsn, 'victron.write', json.dumps(lease), 6, None, int(time.time())))
        t_ins = int(time.time())
        await until(lambda: (client_row(vsn)[0]['last_seen_at'] or 0) > t_ins + 1, 15)   # uzel pojistku převezme
        check('před výpadkem je v GX rozvrh wattery (Mode 4)', broker.tree.get(D + 'Mode') == 4, broker.tree.get(D + 'Mode'))
        edge_cmd.set_fallback_test(cl['id'], hold_s=60, outage_in_s=0, outage_s=20)
        ok = await until(lambda: broker.tree.get(D + 'Mode') == 0, 30)
        check('výpadek serveru (503) po platnosti pojistky: uzel sám vrátil Mode 0', ok, broker.writes[-3:])
        with store._conn() as c:
            gone = lambda: c.execute("SELECT 1 FROM edge_leases WHERE lease_id=?", (lease['id'],)).fetchone() is None  # noqa: E731
            ok = await until(gone, 40)
            audit = c.execute("SELECT * FROM edge_commands WHERE source='lease' ORDER BY id DESC LIMIT 1").fetchone()
        check('po obnovení spojení leaseReport: server pojistku smazal a zapsal audit (ok, ozvěna Mode 0)',
              ok and audit is not None and audit['ok'] == 1 and D + 'Mode' in (audit['result_json'] or ''),
              dict(audit) if audit else None)
        check('do GX šly jen W/ z příkazů a pojistky (žádný jiný zápis)', [w for w in broker.writes] ==
              [(D + 'Schedule/0/Soc', 95), (D + 'Schedule/0/Duration', 3600), (D + 'Mode', 4), (D + 'Mode', 0)], broker.writes)
    finally:
        edge_cmd.set_fallback_test(client_row(vsn)[0]['id'], None) if client_row(vsn)[0] else None
        out, _ = await proc.communicate()
        await broker.stop()
        httpd.shutdown()
    res = json.loads(out.decode('utf-8').strip().splitlines()[-1]) if out else {}
    print('uzel:', res)
    check('uzel na konci online, profil victron-gx@1, bez pojistek', res.get('status') == 'online'
          and res.get('profile') == 'victron-gx@1' and res.get('leases') == [], res)
    print('VŠE OK' if not FAILS else f'CHYBY: {len(FAILS)}')
    shutil.rmtree(TMP, ignore_errors=True)
    return 1 if FAILS else 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
