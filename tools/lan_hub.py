"""Lokální hub deye-monitor v síti pro zkoušku uzlu na skutečném GX (bez produkce).

Testovací DB (výchozí čerstvá v dočasné složce, `--db` = trvalá → token klienta přežije restart hubu), elektrárna
s jedním Victron GX (portal z argumentu), nový párovací kód na výstup, `server.Handler` na 0.0.0.0:<port>.
Každých `--every` s vypíše stav (klienti, stínový snímek). Cloud nedostupný (proxy).
  python tools/lan_hub.py --portal <12 hex> [--port 8099] [--minutes 30] [--db <cesta>] [--every 30]
"""
import argparse
import json
import os
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
FVE = os.path.dirname(os.path.dirname(HERE))


def parse():
    ap = argparse.ArgumentParser()
    ap.add_argument('--portal', required=True)
    ap.add_argument('--port', type=int, default=8099)
    ap.add_argument('--minutes', type=float, default=30)
    ap.add_argument('--db', help='trvalá testovací DB (nikdy produkční)')
    ap.add_argument('--every', type=float, default=30)
    return ap.parse_args()


A = parse()
DB = os.path.abspath(A.db) if A.db else os.path.join(tempfile.mkdtemp(prefix='wattery-nr-hub-'), 'hub.db')
assert not DB.replace('\\', '/').startswith('/srv/'), DB
os.environ.update({'DEYE_DB_PATH': DB, 'DEYE_EDGE': '1', 'DEYE_EDGE_BRANDS': 'victron',
                   'HTTPS_PROXY': 'http://127.0.0.1:9', 'HTTP_PROXY': 'http://127.0.0.1:9',
                   'DEYE_SECRET_KEY': 'jen-test', 'PYTHONUTF8': '1'})
sys.path.insert(0, os.path.join(FVE, 'deye-monitor'))

from http.server import ThreadingHTTPServer  # noqa: E402

from deye import edge, edge_victron as ev, store  # noqa: E402


def seed(owner, sid, vsn):
    with store._conn() as c:
        if c.execute("SELECT 1 FROM devices WHERE device_sn=?", (vsn,)).fetchone():
            return False
    store.upsert_station({'id': sid, 'name': 'GX test'}, user_id=owner)
    store.grant_station_access(sid, owner)
    with store._conn() as c:
        c.execute("INSERT INTO devices (device_sn, station_id, model, brand, device_type) VALUES (?,?,?,?,?)",
                  (vsn, sid, 'Cerbo GX', 'victron', 'GX'))
    return True


def main():
    portal = ev.normalize_portal(A.portal)
    assert portal, 'portal = 12 hex znaků'
    assert store.DB_PATH == os.environ['DEYE_DB_PATH'], store.DB_PATH
    store.init()
    owner, sid, vsn = 7001, 990001, ev.SN_PREFIX + portal
    fresh = seed(owner, sid, vsn)
    code = edge.create_pair_code(sid, owner, now=int(time.time()))['code']
    import server
    httpd = ThreadingHTTPServer(('0.0.0.0', A.port), server.Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    print(f'HUB port {A.port}, DB {store.DB_PATH} ({"nová" if fresh else "trvalá, klienti zachováni"}), '
          f'GX {vsn}, PÁROVACÍ KÓD {code} (15 min)', flush=True)
    end = time.time() + A.minutes * 60
    while time.time() < end:
        time.sleep(A.every)
        with store._conn() as c:
            cl = [dict(x) for x in c.execute("SELECT id, kind, client_ver, last_seen_at FROM edge_clients")]
            r = c.execute("SELECT raw_json, data_json FROM edge_snapshots WHERE device_sn=?", (vsn,)).fetchone()
        snap = json.loads(r['data_json']) if r else {}
        raw = json.loads(r['raw_json']) if r else {}
        dl = {it['key']: it['value'] for it in snap.get('dataList') or []}
        print(json.dumps({'t': time.strftime('%H:%M:%S'), 'clients': cl, 'snVerified': snap.get('snVerified'),
                          'items': sorted(raw.get('mqtt') or {}),
                          'paths': sum(len(v.get('values') or {}) for v in (raw.get('mqtt') or {}).values()
                                       if isinstance(v, dict)),
                          'fw': (snap.get('extra') or {}).get('fw') or snap.get('fw'), 'dataList': sorted(dl)[:12]},
                         ensure_ascii=False), flush=True)
    httpd.shutdown()


if __name__ == '__main__':
    main()
