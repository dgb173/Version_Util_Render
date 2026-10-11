import json
import sys
from pathlib import Path
from urllib.parse import quote_plus

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.modules import sofascore_context

queries = [
    "Danish 1st Division",
    "NordicBet Liga",
    "Bulgaria Second League",
    "Vtora Liga",
    "Poland Division 2",
    "II Liga",
    "Algerian Ligue Professionnelle 2",
    "Ligue 2 Algeria",
    "South Africa First",
    "Motsepe Foundation Championship",
    "AFC U20 Asian Cup",
    "AFC Championship U20",
    "Australia New South Wales Super League",
    "NSW League One",
    "Danish U17",
    "Singapore Premier League",
    "Zanzibar Premier League",
    "Queensland Premier League Women",
]

for q in queries:
    try:
        res = sofascore_context._api_get(f"/search/all?q={quote_plus(q)}")
        ut_list = [item.get("entity") for item in res.get("results") or [] if item.get("type") == "uniqueTournament"]
        print(f"QUERY: '{q}'")
        for ut in ut_list[:4]:
            print(f"   -> ID {ut.get('id')}: {ut.get('name')} (slug: {ut.get('slug')})")
    except Exception as e:
        print(f"QUERY: '{q}' -> Error: {e}")
