"""Mapeo exhaustivo de todas las ligas de data/data_precacheo.json a SofaScore."""
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
PRECACHEO_FILE = ROOT / "data" / "data_precacheo.json"


def normalize_key(text: Any) -> str:
    s = unicodedata.normalize("NFKD", str(text or ""))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"\b(fc|cf|sc|afc|club|deportivo|futbol|football)\b", " ", s)
    s = re.sub(r"\b(w|women|woman|f|femenino|femenina|ladies)\b", " ", s)
    return " ".join(re.sub(r"[^a-z0-9]+", " ", s).split())


def clean_team_name(team: str) -> str:
    s = re.sub(r"\s*\([^\)]*\)", "", str(team or "")).strip()
    return s


EXACT_LEAGUES: Dict[str, Dict[str, Any]] = {
    "danish 1st division": {"id": 47, "name": "1st Division", "slug": "1st-division", "is_unique": True},
    "danmark 1st division": {"id": 47, "name": "1st Division", "slug": "1st-division", "is_unique": True},
    "bulgaria second league": {"id": 1135, "name": "Vtora Liga", "slug": "vtora-liga", "is_unique": True},
    "bulgaria first league": {"id": 1134, "name": "Parva Liga", "slug": "parva-liga", "is_unique": True},
    "vtora liga": {"id": 1135, "name": "Vtora Liga", "slug": "vtora-liga", "is_unique": True},
    "poland division 2": {"id": 25927, "name": "II Liga", "slug": "ii-liga", "is_unique": True},
    "poland ii liga": {"id": 25927, "name": "II Liga", "slug": "ii-liga", "is_unique": True},
    "ii liga": {"id": 25927, "name": "II Liga", "slug": "ii-liga", "is_unique": True},
    "poland liga 1": {"id": 201, "name": "Betclic 1. Liga", "slug": "i-liga", "is_unique": True},
    "poland i liga": {"id": 201, "name": "Betclic 1. Liga", "slug": "i-liga", "is_unique": True},
    "poland ekstraklasa": {"id": 202, "name": "Ekstraklasa", "slug": "ekstraklasa", "is_unique": True},
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
    "singapore premier league": {"id": 634, "name": "Singapore Premier League", "slug": "premier-league", "is_unique": True},
    "argentina reserve league": {"id": 20877, "name": "Copa de la Liga Reserves", "slug": "copa-de-la-liga-reserves", "is_unique": True},
    "k league 2": {"id": 777, "name": "K League 2", "slug": "k-league-2", "is_unique": True},
    "egypt division 2": {"id": 10536, "name": "Egyptian Second Division A", "slug": "egyptian-second-division-a", "is_unique": True},
    "egyptian second division a": {"id": 10536, "name": "Egyptian Second Division A", "slug": "egyptian-second-division-a", "is_unique": True},
    "india sikkim s league": {"id": 36312, "name": "SFA \"A\" Division S-League", "is_unique": True},
    "ukrainian premier league": {"id": 204, "name": "Premier League", "slug": "premier-league", "is_unique": True},
    "ukraine division 2": {"id": 205, "name": "Persha Liga", "slug": "persha-liga", "is_unique": True},
    "ukraine division 3": {"id": 206, "name": "Druha Liga", "slug": "druha-liga", "is_unique": True},
    "kenya premier league": {"id": 1644, "name": "FKF Kenyan Premier League", "slug": "premier-league", "is_unique": True},
    "kuwaiti premier league": {"id": 848, "name": "Zain Premier League", "slug": "premier-league", "is_unique": True},
    "liga super malaysia": {"id": 770, "name": "Super League", "slug": "super-league", "is_unique": True},
    "liga de expansion mx": {"id": 1629, "name": "Liga de Expansión MX", "slug": "liga-de-expansion-mx", "is_unique": True},
    "liga pro ecuador serie b": {"id": 11847, "name": "LigaPro Serie B", "slug": "ligapro-serie-b", "is_unique": True},
    "liga pro ecuador serie a": {"id": 240, "name": "LigaPro Serie A", "slug": "serie-a", "is_unique": True},
    "thai league 1": {"id": 771, "name": "Thai League 1", "slug": "thai-league-1", "is_unique": True},
    "thai league 2": {"id": 1133, "name": "Thai League 2", "slug": "thai-league-2", "is_unique": True},
    "uae pro league": {"id": 847, "name": "UAE Pro League", "slug": "arabian-gulf-league", "is_unique": True},
    "uae division 1 group a": {"id": 1211, "name": "Division 1", "slug": "division-1", "is_unique": True},
    "v league 1": {"id": 773, "name": "V-League 1", "slug": "v-league-1", "is_unique": True},
    "slovakia 2 liga": {"id": 210, "name": "2. Liga", "slug": "2-liga", "is_unique": True},
    "slovenia 2 liga": {"id": 212, "name": "2. SNL", "slug": "2-snl", "is_unique": True},
    "czech republic 2 liga": {"id": 194, "name": "FNL", "slug": "fnl", "is_unique": True},
    "kosovo superliga": {"id": 10925, "name": "AlbiMall Superliga", "slug": "superliga", "is_unique": True},
    "rwanda national league": {"id": 11624, "name": "Rwanda Premier League", "slug": "premier-league", "is_unique": True},
    "tanzania ligue 1": {"id": 16641, "name": "Premier League", "slug": "premier-league", "is_unique": True},
    "belarus pershaya liga": {"id": 170, "name": "Pervaya Liga", "slug": "pervaya-liga", "is_unique": True},
    "belarusian premier league": {"id": 169, "name": "Vysshaya Liga", "slug": "vysshaya-liga", "is_unique": True},
    "azerbaijan premier league": {"id": 735, "name": "Misli Premier League", "slug": "misli-premier-league", "is_unique": True},
    "categoria primera a": {"id": 384, "name": "Primera A", "slug": "primera-a", "is_unique": True},
    "brazil serie d": {"id": 377, "name": "Brasileirão Série D", "slug": "brasileirao-serie-d", "is_unique": True},
    "brazil serie a": {"id": 325, "name": "Brasileirão Betano", "slug": "brasileirao-betano", "is_unique": True},
    "brazil serie b": {"id": 390, "name": "Brasileirão Betnacional Série B", "slug": "brasileirao-serie-b", "is_unique": True},
    "brazil serie c": {"id": 376, "name": "Brasileirão Série C", "slug": "brasileirao-serie-c", "is_unique": True},
    "croatia 3 division": {"id": 23419, "name": "2. NL", "slug": "druga-nl", "is_unique": True},
    "finland ykkonen": {"id": 23275, "name": "Ykkösliiga", "slug": "ykkosliiga", "is_unique": True},
    "tajikistan vysshaya liga": {"id": 12717, "name": "Vysshaya Liga", "slug": "vysshaya-liga", "is_unique": True},
    "indonesia super league": {"id": 769, "name": "Liga 1", "slug": "liga-1", "is_unique": True},
    "liga nacional de honduras": {"id": 10534, "name": "Liga Betcris de Honduras", "slug": "liga-betcris-de-honduras", "is_unique": True},
    "romania liga 2 seria": {"id": 153, "name": "Liga II", "slug": "liga-2", "is_unique": True},
    "romania liga 3 seria": {"id": 1251, "name": "Liga III", "slug": "liga-iii", "is_unique": True},
    "burundi league": {"id": 16643, "name": "Primus League", "slug": "primus-league", "is_unique": True},
    "zimbabwe premier": {"id": 16644, "name": "Castle Lager Premier Soccer League", "slug": "premier-soccer-league", "is_unique": True},
    "myanmar professional league": {"id": 11370, "name": "National League", "slug": "national-league", "is_unique": True},
}


def is_acceptable_tournament(ut: Dict[str, Any], league_name: str) -> bool:
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
    # No mapear ligas a copas de eliminacion salvo que la liga sea una copa
    is_cup_query = any(c in league_name.lower() for c in ("cup", "copa", "trophy", "pokal", "taca", "taça", "championship", "coppa"))
    is_cup_result = any(c in tname for c in ("cup", "copa", "pokal", "taca", "taça", "coppa"))
    if not is_cup_query and is_cup_result:
        return False
    return True


def find_tournament(league_name: str, sample_matches: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    norm = normalize_key(league_name)
    if norm in EXACT_LEAGUES:
        return EXACT_LEAGUES[norm]

    clean_league = re.sub(r"\s*-\s*", " ", league_name).strip()
    try:
        data = sofascore_context._api_get(f"/search/all?q={quote_plus(clean_league)}")
        for item in data.get("results") or []:
            if item.get("type") == "uniqueTournament":
                entity = item.get("entity") or {}
                if is_acceptable_tournament(entity, league_name):
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
                team_entity = next(
                    (it.get("entity") for it in tdata.get("results") or [] if it.get("type") == "team"),
                    None
                )
                if team_entity and team_entity.get("id"):
                    tid = team_entity["id"]
                    for direction in ("next", "last"):
                        try:
                            ev_list = sofascore_context._api_get(f"/team/{tid}/events/{direction}/0")
                            for ev in ev_list.get("events") or []:
                                ut = (ev.get("tournament") or {}).get("uniqueTournament")
                                if is_acceptable_tournament(ut, league_name):
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
            time.sleep(0.04)

    return None


def run():
    print("=" * 70)
    print("MAPEADOR EXHAUSTIVO DE LIGAS (DE PRECACHEO A SOFASCORE)")
    print("=" * 70)

    aliases: Dict[str, Any] = {}
    if ALIASES_FILE.exists():
        try:
            aliases = json.loads(ALIASES_FILE.read_text(encoding="utf-8"))
        except Exception:
            aliases = {}

    # Aplicar EXACT_LEAGUES
    for raw_name, info in EXACT_LEAGUES.items():
        key = normalize_key(raw_name)
        aliases[key] = info
        aliases[normalize_key(info["name"])] = info

    # Cargar partidos de las dos fuentes posibles
    all_matches = []
    if PRECACHEO_FILE.exists():
        try:
            all_matches.extend(json.loads(PRECACHEO_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass

    try:
        db_matches = sql_store.fetch_matches("data_precacheo.json")
        all_matches.extend(db_matches)
    except Exception:
        pass

    by_league: Dict[str, List[Dict[str, Any]]] = {}
    for m in all_matches:
        l = (m.get("league_name") or "").strip()
        if l:
            by_league.setdefault(l, []).append(m)

    sorted_leagues = sorted(by_league.items(), key=lambda x: len(x[1]), reverse=True)
    print(f"Total ligas unicas a procesar: {len(sorted_leagues)}")

    mapped_count = 0
    already_count = 0
    failed = []

    for idx, (league_name, match_list) in enumerate(sorted_leagues, 1):
        norm_key = normalize_key(league_name)
        if norm_key in aliases and aliases[norm_key].get("id") and is_acceptable_tournament(aliases[norm_key], league_name):
            already_count += 1
            continue

        print(f"[{idx}/{len(sorted_leagues)}] {league_name}...", end=" ", flush=True)
        res = find_tournament(league_name, match_list)

        if res and res.get("id"):
            aliases[norm_key] = res
            aliases[normalize_key(res["name"])] = res
            mapped_count += 1
            print(f"-> ID {res['id']} ({res['name']})")
            ALIASES_FILE.write_text(json.dumps(aliases, indent=2, ensure_ascii=False), encoding="utf-8")
        else:
            print("-> NO ENCONTRADO")
            failed.append(league_name)

        time.sleep(0.05)

    ALIASES_FILE.write_text(json.dumps(aliases, indent=2, ensure_ascii=False), encoding="utf-8")
    print("=" * 70)
    print(f"Completado: {mapped_count} nuevas ligas mapeadas. Existentes: {already_count}. Fallidas: {len(failed)}")
    print(f"Total en catalogo final: {len(aliases)}")
    if failed:
        print(f"Ligas no encontradas ({len(failed)}): {failed}")


if __name__ == "__main__":
    run()
