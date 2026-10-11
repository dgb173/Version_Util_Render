import json
import re
import sys
import unicodedata
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent

def norm(s):
    s = unicodedata.normalize('NFKD', str(s or ''))
    s = ''.join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r'\b(fc|cf|sc|afc|club|deportivo|futbol|football)\b', ' ', s)
    s = re.sub(r'\b(w|women|woman|f|femenino|femenina|ladies)\b', ' ', s)
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', s).split())

import sys
sys.path.insert(0, str(ROOT))
from src.modules import sql_store

matches = sql_store.fetch_matches('data_precacheo.json')
aliases = json.loads((ROOT / 'data' / 'sofascore_league_aliases.json').read_text(encoding='utf-8'))

leagues = sorted(set(m.get('league_name') for m in matches if m.get('league_name')))

suspicious = []
valid = []
missing = []

for l in leagues:
    n = norm(l)
    target = aliases.get(n)
    if not target:
        missing.append(l)
        continue
    tname = target.get('name', '')
    if 'friendly' in tname.lower() and 'friendly' not in l.lower():
        suspicious.append((l, target['id'], tname, "Mapeado a amistosos"))
    elif 'cup' in tname.lower() and 'cup' not in l.lower() and 'league' in l.lower():
        suspicious.append((l, target['id'], tname, "Mapeado a copa siendo liga"))
    else:
        valid.append((l, target['id'], tname))

print(f"Total ligas precacheo: {len(leagues)}")
print(f"Ligas con mapeo valido: {len(valid)}")
print(f"Ligas sospechosas: {len(suspicious)}")
print(f"Ligas sin mapear: {len(missing)}")
print("\n--- DETALLE SOSPECHOSAS ---")
for orig, tid, tname, reason in suspicious:
    print(f"  [X] '{orig}' -> ID {tid}: '{tname}' ({reason})")

print("\n--- DETALLE SIN MAPEAR ---")
for m in missing:
    print(f"  [?] '{m}'")
