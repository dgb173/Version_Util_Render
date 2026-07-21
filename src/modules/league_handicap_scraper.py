"""Extraccion de partidos de liga por la linea AH visible en NowGoal."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
import urllib3

from . import data_manager, sql_store
from .estudio_scraper import analizar_partido_completo


BASE_URL = "https://football.nowgoal26.com"
DEFAULT_COMPANY_ID = 8
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    ),
    "Referer": f"{BASE_URL}/",
}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ODDS_RE = re.compile(r'oddsData\["L_(\d+)"\]\s*=\s*(\[\[.*?\]\]);')


def _get_text(session: requests.Session, url: str) -> str:
    response = session.get(url, headers=HEADERS, timeout=30, verify=False)
    response.raise_for_status()
    return response.text.lstrip("\ufeff")


def parse_league_reference(raw_value: str, explicit_season: str = "") -> Tuple[str, str]:
    """Devuelve ``(league_id, season)`` desde un ID o una URL de liga."""
    raw = str(raw_value or "").strip()
    season = str(explicit_season or "").strip()
    if raw.isdigit():
        return raw, season

    match = re.search(r"/league(?:/([^/?#]+))?/(\d+)(?:[/?#]|$)", raw)
    if not match:
        raise ValueError("Introduce un ID de liga o una URL de NowGoal valida")
    url_season, league_id = match.groups()
    if not season and url_season and url_season != league_id:
        season = url_season
    return league_id, season


def _discover_league(
    session: requests.Session,
    league_id: str,
    requested_season: str,
) -> Tuple[str, str]:
    suffix = f"/{requested_season}/{league_id}" if requested_season else f"/{league_id}"
    html = _get_text(session, f"{BASE_URL}/league{suffix}")
    season_match = re.search(r'const\s+_season\s*=\s*"([^"]+)"', html)
    path_match = re.search(r'const\s+_dataPath\s*=\s*"([^"]+)"', html)
    if not season_match or not path_match:
        raise RuntimeError("NowGoal no expuso la temporada o el calendario de la liga")

    season = season_match.group(1)
    if requested_season and requested_season != season:
        raise ValueError(f"La pagina devolvio la temporada {season}, no {requested_season}")
    return season, urljoin(BASE_URL, path_match.group(1))


def _round_sort_key(value: Tuple[str, str]) -> Tuple[str, int, str]:
    sub_id, round_value = value
    if str(round_value).isdigit():
        return sub_id, int(round_value), ""
    return sub_id, 10**9, str(round_value)


def _flatten_schedule(data: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], List[Tuple[str, str]]]:
    teams = {str(row[0]): row[1] for row in data.get("TeamInfo", []) if len(row) > 1}
    sub_names = {
        str(row[0]): str(row[1])
        for row in data.get("SubLeagueInfo", [])
        if isinstance(row, list) and len(row) > 1
    }
    matches: Dict[str, Dict[str, Any]] = {}
    rounds: List[Tuple[str, str]] = []

    for schedule_key, round_map in (data.get("ScheduleList") or {}).items():
        if not isinstance(round_map, dict):
            continue
        sub_id = schedule_key.removeprefix("sub_") if schedule_key.startswith("sub_") else "0"
        for round_key, rows in round_map.items():
            round_match = re.fullmatch(r"R_(.+)", str(round_key))
            if not round_match or not isinstance(rows, list):
                continue
            round_value = round_match.group(1)
            rounds.append((sub_id, round_value))
            for row in rows:
                if not isinstance(row, list) or len(row) < 8:
                    continue
                match_id = str(row[0])
                matches[match_id] = {
                    "id": match_id,
                    "sub_id": str(sub_id),
                    "sub_name": sub_names.get(str(sub_id), ""),
                    "round": round_value,
                    "date": row[3],
                    "home": teams.get(str(row[4]), str(row[4])),
                    "away": teams.get(str(row[5]), str(row[5])),
                    "score": row[6] or "-",
                    "source_state": row[2],
                }
    return matches, sorted(set(rounds), key=_round_sort_key)


def parse_round_odds(text: str, company_id: int = DEFAULT_COMPANY_ID) -> Dict[str, Dict[str, float]]:
    """Extrae la linea AH usada por la casa seleccionada en la tabla de liga."""
    output: Dict[str, Dict[str, float]] = {}
    for match_id, raw_rows in ODDS_RE.findall(text or ""):
        try:
            rows = json.loads(raw_rows)
        except json.JSONDecodeError:
            continue
        selected = next(
            (row for row in rows if len(row) >= 4 and int(row[0]) == int(company_id)),
            None,
        )
        if not selected:
            continue
        output[match_id] = {
            "home_odds_hk": float(selected[1]),
            "visible_ah": float(selected[2]),
            "away_odds_hk": float(selected[3]),
        }
    return output


def _is_finished(match: Dict[str, Any]) -> bool:
    return int(match.get("source_state", 0)) == -1 and str(match.get("score", "-")) not in {"", "-"}


def preview_league_handicap(
    league_reference: str,
    target_ah: Optional[float] = None,
    season: str = "",
    company_id: int = DEFAULT_COMPANY_ID,
    match_status: str = "all",
) -> Dict[str, Any]:
    league_id, requested_season = parse_league_reference(league_reference, season)
    session = requests.Session()
    discovered_season, data_url = _discover_league(session, league_id, requested_season)
    league_data = json.loads(_get_text(session, data_url))
    match_map, rounds = _flatten_schedule(league_data)

    all_odds: Dict[str, Dict[str, float]] = {}
    for sub_id, round_value in rounds:
        odds_url = (
            f"{BASE_URL}/ajax/LeagueOddsAjax?sclassId={league_id}"
            f"&subSclassId={sub_id}&matchSeason={discovered_season}&round={round_value}"
        )
        for match_id, odds in parse_round_odds(_get_text(session, odds_url), company_id).items():
            all_odds[match_id] = odds

    if target_ah is None:
        selected_ids = list(match_map)
    else:
        selected_ids = [
            match_id
            for match_id, odds in all_odds.items()
            if abs(odds["visible_ah"] - float(target_ah)) < 1e-9
        ]

    matches: List[Dict[str, Any]] = []
    for match_id in selected_ids:
        odds = all_odds.get(match_id) or {}
        match = dict(match_map.get(match_id) or {"id": match_id})
        finished = _is_finished(match)
        if match_status == "finished" and not finished:
            continue
        if match_status == "upcoming" and finished:
            continue

        existing = sql_store.get_match(match_id)
        match.update(
            {
                "visible_ah": odds.get("visible_ah"),
                "company_id": int(company_id),
                "home_odds_decimal": (
                    odds["home_odds_hk"] + 1 if odds.get("home_odds_hk") is not None else None
                ),
                "away_odds_decimal": (
                    odds["away_odds_hk"] + 1 if odds.get("away_odds_hk") is not None else None
                ),
                "finished": finished,
                "already_in_sql": existing is not None,
                "sql_bucket": sql_store.get_match_bucket(match_id) if existing else None,
                "stored_initial_ah": (
                    existing.get("handicap")
                    if existing and existing.get("handicap") is not None
                    else (existing.get("main_match_odds") or {}).get("ah_linea") if existing else None
                ),
            }
        )
        matches.append(match)

    matches.sort(key=lambda row: (str(row.get("date", "")), str(row["id"])))
    league_info = league_data.get("LeagueInfo") or []
    return {
        "league_id": league_id,
        "league_name": league_info[1] if len(league_info) > 1 else f"Liga {league_id}",
        "season": discovered_season,
        "target_ah": float(target_ah) if target_ah is not None else None,
        "company_id": int(company_id),
        "match_status": match_status,
        "matches": matches,
    }


def scrape_match_to_sql(match: Dict[str, Any], league_id: str, force: bool = False) -> Dict[str, Any]:
    """Analiza un ID con el flujo normal de la web y lo persiste en SQL."""
    match_id = "".join(filter(str.isdigit, str(match.get("id") or match.get("match_id") or "")))
    if not match_id:
        return {"id": "", "status": "error", "error": "ID no valido"}

    existing = sql_store.get_match(match_id)
    if existing and not force:
        return {
            "id": match_id,
            "status": "exists",
            "bucket": sql_store.get_match_bucket(match_id),
        }

    try:
        result = analizar_partido_completo(match_id, force_refresh=force, check_odds_early=False)
        if isinstance(result, tuple):
            result = result[0]
        if not isinstance(result, dict) or result.get("error"):
            error = result.get("error", "sin datos") if isinstance(result, dict) else "sin datos"
            return {"id": match_id, "status": "error", "error": error}

        result["match_id"] = match_id
        result.setdefault("league_id", str(league_id))
        result["league_page_visible_ah"] = match.get("visible_ah")
        result["league_page_company_id"] = match.get("company_id", DEFAULT_COMPANY_ID)
        result["league_page_scraped_at"] = datetime.utcnow().replace(microsecond=0).isoformat()
        if not data_manager.save_match(result):
            return {
                "id": match_id,
                "status": "filtered",
                "error": "No supera los filtros de historial/AH de data_manager",
            }
        return {
            "id": match_id,
            "status": "saved",
            "bucket": sql_store.get_match_bucket(match_id),
            "stored_initial_ah": result.get("handicap")
            if result.get("handicap") is not None
            else (result.get("main_match_odds") or {}).get("ah_linea"),
        }
    except Exception as exc:
        return {"id": match_id, "status": "error", "error": str(exc)}


def sanitize_selected_matches(matches: Iterable[Dict[str, Any]], company_id: int) -> List[Dict[str, Any]]:
    """Normaliza el payload procedente de la tabla de previsualizacion."""
    output: List[Dict[str, Any]] = []
    seen = set()
    for raw in matches or []:
        if not isinstance(raw, dict):
            continue
        match_id = "".join(filter(str.isdigit, str(raw.get("id") or raw.get("match_id") or "")))
        if not match_id or match_id in seen:
            continue
        seen.add(match_id)
        raw_visible_ah = raw.get("visible_ah")
        try:
            visible_ah = None if raw_visible_ah in (None, "") else float(raw_visible_ah)
        except (TypeError, ValueError):
            continue
        output.append(
            {
                "id": match_id,
                "visible_ah": visible_ah,
                "company_id": int(company_id),
                "home": str(raw.get("home") or ""),
                "away": str(raw.get("away") or ""),
                "date": str(raw.get("date") or ""),
                "round": str(raw.get("round") or ""),
                "sub_id": str(raw.get("sub_id") or "0"),
                "sub_name": str(raw.get("sub_name") or ""),
            }
        )
    return output
