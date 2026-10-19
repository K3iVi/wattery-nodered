"""Zlaté testovací vektory profilu zápisu z Python enginu (wattery-ha write_profile.py) → test/vectors/<id>.json.

JS engine (lib/profile.js) je třetí implementace vedle Pythonu (HA klient, server) a C++ (ESP); test/profile.test.js
ověří, že na těchto vektorech rozhoduje stejně (povoleno / zakázáno). Spuštění z kořene wattery-nodered
(sourozenecké repo wattery-ha vedle):  python tools/gen_vectors.py
"""
import importlib.util
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
WP = os.path.join(os.path.dirname(ROOT), 'wattery-ha', 'custom_components', 'wattery', 'write_profile.py')
PID = 'victron-gx@1'

spec = importlib.util.spec_from_file_location('write_profile', WP)
wp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wp)

with open(os.path.join(ROOT, 'profiles', PID + '.json'), encoding='utf-8') as f:
    raw = json.load(f)
comp = wp.compile_profile(raw)

D = 'settings/0/Settings/DynamicEss/'
S = D + 'Schedule/'
paths = [D + 'Mode', D + 'BatteryCapacity', D + 'Unknown', D + 'mode', 'W/' + D + 'Mode', '/' + D + 'Mode',
         'settings/0/Settings/CGwacs/BatteryLife/MinimumSocLimit', 'settings/0/Settings/CGwacs/Hub4Mode',
         'evcharger/40/StartStop', 'evcharger/x/StartStop', 'evcharger//StartStop', 'evcharger/40/Mode',
         'evcharger/40/SetCurrent', 'evcharger/40/MaxCurrent', '']
for i in (0, 1, 23, 47, 48, 100, -1):
    for fld in ('Start', 'Duration', 'TargetSoc', 'Soc', 'Strategy', 'Restrictions', 'AllowGridFeedIn', 'Flags', 'Nope'):
        paths.append(f'{S}{i}/{fld}')
paths += [S + '01/Soc', S + '0/Soc/x', S + '+/Soc', S + '#']
values = [0, 1, 2, 3, 4, 5, 6, 10, 32, 79, 80, 80.5, 81, 100, 100.5, 101, -1, 0.5, 1.0, 4.0, 4.5, 300, 300.01,
          86400, 86401, 4102444800, 4102444801, 1e300, True, False, None, '4', [4]]

cases = []
for p in paths:
    for v in values:
        cases.append({'path': p, 'value': v, 'ok': wp.path_problem(comp, p, v) is None,
                      'persist': wp.persistent(comp, p)})
for bad in (float('nan'), float('inf')):
    cases.append({'path': D + 'Mode', 'value': 'NaN' if bad != bad else 'Infinity', 'ok': wp.path_problem(comp, D + 'Mode', bad) is None,
                  'persist': True, 'special': True})

batches = [
    [[D + 'Mode', 4]],
    [[S + '0/Start', 1760000000], [S + '0/Duration', 3600], [S + '0/Soc', 80], [D + 'Mode', 4]],
    [[D + 'Mode', 4], [D + 'Mode', 0]],
    [[D + 'Mode', 4], [D + 'Hack', 1]],
    [[D + 'Mode', 1]],
    [],
    [[D + 'Mode']],
    [['x', 1, 2]],
    [[4, D + 'Mode']],
    'nope',
    [[f'{S}{i // 8}/{("Start", "Duration", "TargetSoc", "Soc", "Strategy", "Restrictions", "AllowGridFeedIn", "Flags")[i % 8]}', 0]
     for i in range(384)] + [[D + 'Mode', 4]],
    [[f'{S}{i // 8}/{("Start", "Duration", "TargetSoc", "Soc", "Strategy", "Restrictions", "AllowGridFeedIn", "Flags")[i % 8]}', 0]
     for i in range(384)] + [[D + 'Mode', 4], [D + 'BatteryCapacity', 30]],
]
bcases = [{'values': b, 'ok': wp.batch_problem(comp, b) is None} for b in batches]

out = os.path.join(ROOT, 'test', 'vectors', PID + '.json')
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, 'w', encoding='utf-8', newline='\n') as f:
    json.dump({'profile': PID, 'generator': 'wattery-ha write_profile.py', 'paths': cases, 'batches': bcases}, f,
              ensure_ascii=False, separators=(',', ':'))
    f.write('\n')
print(f'{out}: {len(cases)} cest, {len(bcases)} dávek; povoleno {sum(c["ok"] for c in cases)}')
