"""Sanea y valida que todos los torneos en sofascore_league_aliases.json sean exclusivamente de futbol."""
import json
import sys
import time
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.modules import sofascore_context

ALIASES_FILE = ROOT / "data" / "sofascore_league_aliases.json"

aliases = json.loads(ALIASES_FILE.read_text(encoding="utf-8"))
print(f"Total inicial de aliases: {len(aliases)}")

# Reemplazos y correcciones explicitas
EXPLICIT_FIXES = {
    "russia womens premier league": {"id": 1989, "name": "Superliga, Women", "slug": "superliga-women", "is_unique": True},
    "lithuania 2 division": {"id": 10931, "name": "Pirma Lyga", "slug": "1-lyga", "is_unique": True},
    "lithuania 2 division": {"id": 10931, "name": "Pirma Lyga", "slug": "1-lyga", "is_unique": True},
    "australia new south wales npl women": {"id": 14722, "name": "NPL New South Wales, Women", "slug": "npl-nsw-women", "is_unique": True},
    "puerto rico league": {"id": 20025, "name": "Liga Puerto Rico", "slug": "liga-puerto-rico", "is_unique": True},
}

for k, v in EXPLICIT_FIXES.items():
    aliases[k] = v

unique_tids = {}
for k, v in list(aliases.items()):
    tid = v.get("id")
    if tid:
        unique_tids.setdefault(tid, []).append(k)

print(f"Total de torneos unicos a verificar deporte: {len(unique_tids)}")

verified_football = set()
non_football = set()

for idx, (tid, keys) in enumerate(unique_tids.items(), 1):
    # IDs ya conocidos de futbol
    if tid in (1, 2, 7, 17, 34, 35, 36, 38, 39, 40, 47, 155, 169, 170, 201, 202, 204, 205, 206, 210, 212, 240, 325, 376, 377, 390, 634, 735, 770, 771, 777, 808, 847, 848, 915, 955, 1133, 1134, 1135, 1211, 1254, 1268, 1270, 1629, 1644, 1906, 2292, 9459, 10533, 10534, 10536, 10925, 10931, 11540, 11624, 11847, 13362, 13783, 14722, 16641, 16789, 18099, 18555, 18643, 19415, 19546, 20025, 20877, 21993, 22740, 23266, 23419, 24034, 24040, 25927, 26015, 28281, 28306, 28308, 28909, 32026, 36312):
        verified_football.add(tid)
        continue

    try:
        ut_data = sofascore_context._api_get(f"/unique-tournament/{tid}")
        ut = ut_data.get("uniqueTournament") or {}
        sport_slug = (ut.get("category") or {}).get("sport", {}).get("slug")
        if not sport_slug:
            sport_slug = (ut.get("sport") or {}).get("slug")
        if sport_slug and sport_slug != "football":
            print(f"[-] NO ES FUTBOL: ID {tid} '{ut.get('name')}' es '{sport_slug}'. Eliminando alias: {keys}")
            non_football.add(tid)
        else:
            verified_football.add(tid)
    except Exception:
        pass
    time.sleep(0.04)

# Eliminar alias de deportes que no sean futbol
cleaned_aliases = {}
for k, v in aliases.items():
    if v.get("id") not in non_football:
        cleaned_aliases[k] = v

ALIASES_FILE.write_text(json.dumps(cleaned_aliases, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"Limpieza completada: {len(cleaned_aliases)} aliases validos conservados.")
