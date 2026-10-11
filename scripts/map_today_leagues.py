"""Mapeador automatico y manual de ligas de Precacheo a SofaScore."""
from __future__ import annotations

import json
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.modules import sql_store, sofascore_context

ALIASES_FILE = ROOT / "data" / "sofascore_league_aliases.json"

KNOWN_CORRECTIONS: Dict[str, Dict[str, Any]] = {
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
    "nsw league one": {"id": 26015, "name": "NSW League One", "slug": "nsw-league-one", "is_unique": True},
    "singapore u23 league": {"id": 634, "name": "Singapore Premier League", "slug": "premier-league", "is_unique": True},
    "singapore premier league": {"id": 634, "name": "Singapore Premier League", "slug": "premier-league", "is_unique": True},
    "argentina reserve league": {"id": 20877, "name": "Copa de la Liga Reserves", "slug": "copa-de-la-liga-reserves", "is_unique": True},
    "k league 2": {"id": 777, "name": "K League 2", "slug": "k-league-2", "is_unique": True},
    "egypt division 2": {"id": 10536, "name": "Egyptian Second Division A", "slug": "egyptian-second-division-a", "is_unique": True},
    "egyptian second division a": {"id": 10536, "name": "Egyptian Second Division A", "slug": "egyptian-second-division-a", "is_unique": True},
    "india sikkim s league": {"id": 36312, "name": "SFA \"A\" Division S-League", "is_unique": True},
}


def normalize_key(text: Any) -> str:
    s = unicodedata.normalize("NFKD", str(text or ""))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"\b(fc|cf|sc|afc|club|deportivo|futbol|football)\b", " ", s)
    s = re.sub(r"\b(w|women|woman|f|femenino|femenina|ladies)\b", " ", s)
    return " ".join(re.sub(r"[^a-z0-9]+", " ", s).split())


def clean_team_name(team: str) -> str:
    s = re.sub(r"\s*\([^\)]*\)", "", team).strip()
    return s


def is_valid_tournament(ut: Dict[str, Any], league_name: str) -> bool:
    if not ut or not ut.get("id"):
        return False
    tid = ut.get("id")
    # Nunca mapear a amistosos salvo que la liga sea expresamente amistosa
    if tid in (853, 855, 1195):
        return "friendly" in league_name.lower() or "amistoso" in league_name.lower()
    tname = (ut.get("name") or "").lower()
    tslug = (ut.get("slug") or "").lower()
    if ("friendly" in tname or "friendly" in tslug) and ("friendly" not in league_name.lower() and "amistoso" not in league_name.lower()):
        return False
    return True


def find_tournament_for_league(league_name: str, sample_matches: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    norm = normalize_key(league_name)
    if norm in KNOWN_CORRECTIONS:
        return KNOWN_CORRECTIONS[norm]

    clean_league = re.sub(r"\s*-\s*", " ", league_name).strip()
    try:
        data = sofascore_context._api_get(f"/search/all?q={quote_plus(clean_league)}")
        for item in data.get("results") or []:
            if item.get("type") == "uniqueTournament":
                entity = item.get("entity") or {}
                if is_valid_tournament(entity, league_name):
                    return {
                        "id": entity.get("id"),
                        "name": entity.get("name"),
                        "slug": entity.get("slug"),
                        "is_unique": True,
                    }
    except Exception:
        pass

    for m in sample_matches[:3]:
        home = clean_team_name(m.get("home_name") or "")
        away = clean_team_name(m.get("away_name") or "")

        for team_name in (home, away):
            if not team_name or len(team_name) < 4:
                continue
            try:
                tdata = sofascore_context._api_get(f"/search/all?q={quote_plus(team_name)}")
                for item in tdata.get("results") or []:
                    if item.get("type") == "event":
                        ev_id = (item.get("entity") or {}).get("id")
                        if ev_id:
                            try:
                                ev_data = sofascore_context._api_get(f"/event/{ev_id}")
                                ut = (ev_data.get("event") or {}).get("tournament", {}).get("uniqueTournament")
                                if is_valid_tournament(ut, league_name):
                                    return {
                                        "id": ut.get("id"),
                                        "name": ut.get("name"),
                                        "slug": ut.get("slug"),
                                        "is_unique": True,
                                    }
                            except Exception:
                                pass

                team_entity = next(
                    (it.get("entity") for it in tdata.get("results") or [] if it.get("type") == "team"),
                    None
                )
                if team_entity and team_entity.get("id"):
                    tid = team_entity["id"]
                    # Priorizar "next" (proximo partido suele ser de la liga actual), luego "last"
                    for direction in ("next", "last"):
                        try:
                            ev_list = sofascore_context._api_get(f"/team/{tid}/events/{direction}/0")
                            for ev in ev_list.get("events") or []:
                                ut = (ev.get("tournament") or {}).get("uniqueTournament")
                                if is_valid_tournament(ut, league_name):
                                    return {
                                        "id": ut.get("id"),
                                        "name": ut.get("name"),
                                        "slug": ut.get("slug"),
                                        "is_unique": True,
                                    }
                        except Exception:
                            pass
            except Exception:
                pass
            time.sleep(0.05)

    return None


def run():
    print("=" * 70)
    print("MAPEO DE LIGAS DE PRECACHEO A SOFASCORE (CON FILTRO ESTRICTO)")
    print("=" * 70)

    aliases: Dict[str, Any] = {}
    if ALIASES_FILE.exists():
        try:
            aliases = json.loads(ALIASES_FILE.read_text(encoding="utf-8"))
        except Exception:
            aliases = {}

    # Sembrar correcciones manuales primero
    for raw_name, info in KNOWN_CORRECTIONS.items():
        key = normalize_key(raw_name)
        aliases[key] = info
        aliases[normalize_key(info["name"])] = info

    # Limpiar cualquier amistoso espurio
    cleaned = {}
    for k, v in aliases.items():
        if is_valid_tournament(v, k):
            cleaned[k] = v
    aliases = cleaned

    print(f"[*] Alias base verificados: {len(aliases)}")

    matches = sql_store.fetch_matches("data_precacheo.json")
    by_league: Dict[str, List[Dict[str, Any]]] = {}
    for m in matches:
        l = (m.get("league_name") or "").strip()
        if l:
            by_league.setdefault(l, []).append(m)

    sorted_leagues = sorted(by_league.items(), key=lambda x: len(x[1]), reverse=True)
    print(f"[*] Total ligas en precacheo: {len(sorted_leagues)}")

    mapped_count = 0
    already_count = 0
    failed = []

    for idx, (league_name, match_list) in enumerate(sorted_leagues, 1):
        norm_key = normalize_key(league_name)
        if norm_key in aliases and aliases[norm_key].get("id") and is_valid_tournament(aliases[norm_key], league_name):
            already_count += 1
            continue

        print(f"[{idx}/{len(sorted_leagues)}] {league_name}...", end=" ", flush=True)
        res = find_tournament_for_league(league_name, match_list)

        if res and res.get("id"):
            aliases[norm_key] = res
            aliases[normalize_key(res["name"])] = res
            mapped_count += 1
            print(f"-> ID {res['id']} ({res['name']})")
            ALIASES_FILE.write_text(json.dumps(aliases, indent=2, ensure_ascii=False), encoding="utf-8")
        else:
            print("-> NO ENCONTRADO")
            failed.append(league_name)

        time.sleep(0.06)

    ALIASES_FILE.write_text(json.dumps(aliases, indent=2, ensure_ascii=False), encoding="utf-8")
    print("=" * 70)
    print(f"Completado: {mapped_count} nuevas ligas mapeadas. Existentes: {already_count}. Fallidas: {len(failed)}")
    print(f"Total en catalogo final: {len(aliases)}")


if __name__ == "__main__":
    run()
