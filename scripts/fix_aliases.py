import json
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ALIASES_FILE = ROOT / "data" / "sofascore_league_aliases.json"

def norm(s):
    s = unicodedata.normalize('NFKD', str(s or ''))
    s = ''.join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r'\b(fc|cf|sc|afc|club|deportivo|futbol|football)\b', ' ', s)
    s = re.sub(r'\b(w|women|woman|f|femenino|femenina|ladies)\b', ' ', s)
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', s).split())

aliases = json.loads(ALIASES_FILE.read_text(encoding='utf-8'))
initial_len = len(aliases)

# 1. Eliminar mapeos espurios a Club Friendly Games (853, 855, 1195) si no es un partido amistoso
to_delete = []
for k, v in aliases.items():
    tid = v.get("id")
    tname = (v.get("name") or "").lower()
    if tid in (853, 855, 1195) or "friendly games" in tname:
        if "friendly" not in k and "amistoso" not in k:
            to_delete.append(k)

for k in to_delete:
    del aliases[k]

print(f"Eliminados {len(to_delete)} mapeos erroneos a amistosos: {to_delete}")

# 2. Mapeos explicitos de alta calidad
CORRECTIONS = {
    "danish 1st division": {"id": 47, "name": "1st Division", "slug": "1st-division", "is_unique": True},
    "danmark 1st division": {"id": 47, "name": "1st Division", "slug": "1st-division", "is_unique": True},
    "bulgaria second league": {"id": 1135, "name": "Vtora Liga", "slug": "vtora-liga", "is_unique": True},
    "vtora liga": {"id": 1135, "name": "Vtora Liga", "slug": "vtora-liga", "is_unique": True},
    "poland division 2": {"id": 25927, "name": "II Liga", "slug": "ii-liga", "is_unique": True},
    "poland ii liga": {"id": 25927, "name": "II Liga", "slug": "ii-liga", "is_unique": True},
    "ii liga": {"id": 25927, "name": "II Liga", "slug": "ii-liga", "is_unique": True},
    "algerian ligue professionnelle 2": {"id": 9459, "name": "Algerian Ligue 2", "slug": "ligue-2", "is_unique": True},
    "algeria ligue 2": {"id": 9459, "name": "Algerian Ligue 2", "slug": "ligue-2", "is_unique": True},
    "south africa first league": {"id": 1906, "name": "Motsepe Foundation Championship", "slug": "motsepe-foundation-championship", "is_unique": True},
    "motsepe foundation championship": {"id": 1906, "name": "Motsepe Foundation Championship", "slug": "motsepe-foundation-championship", "is_unique": True},
    "afc championship u20": {"id": 19546, "name": "U20 AFC Asian Cup", "slug": "u20-afc-asian-cup", "is_unique": True},
    "u20 afc asian cup": {"id": 19546, "name": "U20 AFC Asian Cup", "slug": "u20-afc-asian-cup", "is_unique": True},
    "australia queensland state womens premier league": {"id": 32026, "name": "Queensland Premier League 1, Women", "slug": "queensland-premier-league-1-women", "is_unique": True},
    "queensland premier league 1 women": {"id": 32026, "name": "Queensland Premier League 1, Women", "slug": "queensland-premier-league-1-women", "is_unique": True},
    "zanzibar premier league": {"id": 19415, "name": "PBZ Premier League", "slug": "pbz-premier-league", "is_unique": True},
    "pbz premier league": {"id": 19415, "name": "PBZ Premier League", "slug": "pbz-premier-league", "is_unique": True},
    "australia new south wales super league": {"id": 26015, "name": "NSW League One", "slug": "nsw-league-one", "is_unique": True},
    "australia new south wales u20 super league": {"id": 26015, "name": "NSW League One", "slug": "nsw-league-one", "is_unique": True},
    "singapore u23 league": {"id": 634, "name": "Singapore Premier League", "slug": "premier-league", "is_unique": True},
}

for raw_name, info in CORRECTIONS.items():
    key = norm(raw_name)
    aliases[key] = info
    aliases[norm(info["name"])] = info

ALIASES_FILE.write_text(json.dumps(aliases, indent=2, ensure_ascii=False), encoding='utf-8')
print(f"Aliases actualizados con exito. Total actual: {len(aliases)} (inicial: {initial_len})")
