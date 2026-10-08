"""Extraccion de partidos de liga por la linea AH visible en NowGoal."""

from __future__ import annotations

import concurrent.futures
import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
import urllib3

from . import data_manager, sql_store
from .estudio_scraper import analizar_partido_completo
from .nowgoal_fetcher import parse_goal_line_numeric, parse_handicap_numeric


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

    match = re.search(r"/(?:sub)?league(?:/([^/?#]+))?/(\d+)(?:[/?#]|$)", raw, re.IGNORECASE)
    if match:
        url_season, league_id = match.groups()
        if not season and url_season and url_season != league_id:
            season = url_season
        return league_id, season

    # Si contiene dígitos, extraer el ID numérico
    digits = "".join(filter(str.isdigit, raw))
    if digits:
        return digits, season

    raise ValueError("Introduce un ID de liga válido (ej. 36) o una URL de NowGoal")


def _parse_schedule_payload(text: str) -> Dict[str, Any]:
    text = text.lstrip("\ufeff").strip()
    if text.startswith("{") and text.endswith("}"):
        return json.loads(text)
    match = re.search(r'(?:var\s+\w+\s*=\s*|^\s*)({.*?});?$', text, re.DOTALL)
    if match:
        return json.loads(match.group(1))
    return json.loads(text)


def _discover_league(
    session: requests.Session,
    league_id: str,
    requested_season: str,
) -> Tuple[str, Dict[str, Any]]:
    """Obtiene la temporada y los datos de calendario de la liga probando HTML y endpoints directos."""
    now_year = datetime.utcnow().year
    candidate_seasons: List[str] = []
    if requested_season:
        candidate_seasons.append(requested_season)
    else:
        candidate_seasons.extend([
            f"{now_year-1}-{now_year}",
            f"{now_year}-{now_year+1}",
            str(now_year),
            str(now_year-1),
            f"{now_year-2}-{now_year-1}",
            str(now_year+1),
        ])

    # 1. Intentar descubrir temporada oficial en la página HTML
    if not requested_season:
        for prefix in ("/league/", "/subleague/"):
            try:
                html = _get_text(session, f"{BASE_URL}{prefix}{league_id}")
                season_match = re.search(r'const\s+_season\s*=\s*"([^"]+)"', html)
                path_match = re.search(r'const\s+_dataPath\s*=\s*"([^"]+)"', html)
                if season_match and path_match:
                    season = season_match.group(1)
                    raw_data = _get_text(session, urljoin(BASE_URL, path_match.group(1)))
                    parsed = _parse_schedule_payload(raw_data)
                    if parsed and (parsed.get("ScheduleList") or parsed.get("TeamInfo")):
                        return season, parsed
            except Exception:
                pass

    # 2. Fallback / Intentos directos a los endpoints JSON y JS
    for season in candidate_seasons:
        for url in (
            f"{BASE_URL}/jsData/matchResult/json/{season}/s{league_id}_en.json",
            f"{BASE_URL}/jsData/matchResult/{season}/s{league_id}_en.js",
            f"{BASE_URL}/jsData/matchResult/json/{season}/s{league_id}.json",
            f"{BASE_URL}/jsData/matchResult/{season}/s{league_id}.js",
        ):
            try:
                resp = session.get(url, headers=HEADERS, timeout=10, verify=False)
                if resp.status_code == 200 and resp.text:
                    parsed = _parse_schedule_payload(resp.text)
                    if parsed and (parsed.get("ScheduleList") or parsed.get("TeamInfo")):
                        return season, parsed
            except Exception:
                continue

    # 3. Si se pidió una temporada específica, intentar HTML con esa temporada
    if requested_season:
        for prefix in ("/league/", "/subleague/"):
            try:
                html = _get_text(session, f"{BASE_URL}{prefix}{requested_season}/{league_id}")
                path_match = re.search(r'const\s+_dataPath\s*=\s*"([^"]+)"', html)
                if path_match:
                    raw_data = _get_text(session, urljoin(BASE_URL, path_match.group(1)))
                    parsed = _parse_schedule_payload(raw_data)
                    if parsed:
                        return requested_season, parsed
            except Exception:
                pass

    raise RuntimeError(
        f"No se pudo cargar el calendario para la liga {league_id}. "
        "Verifica que el ID sea correcto en NowGoal (ej. 36 para Premier League, 273 para A-League)."
    )


def _calculate_favorite_coverage(score_str: str, ah_val: Optional[float]) -> str:
    """Calcula si el favorito cubrió la línea de AH."""
    if ah_val is None:
        return "unknown"
    score_clean = str(score_str or "").strip()
    parts = re.split(r"[-:]", score_clean)
    if len(parts) != 2:
        return "unknown"
    try:
        h_goals = int(parts[0].strip())
        a_goals = int(parts[1].strip())
    except ValueError:
        return "unknown"

    h_diff = h_goals - a_goals
    abs_ah = abs(ah_val)

    if abs_ah < 1e-6:
        # Partido nivelado (AH 0: sin favorito de mercado)
        # Se describe el resultado respecto al equipo local:
        if h_diff > 0:
            return "home_win"
        elif h_diff < 0:
            return "away_win"
        return "push"

    # En NowGoal: AH > 0 = Local Favorito; AH < 0 = Visitante Favorito
    fav_is_local = ah_val > 0
    diff_from_fav = h_diff if fav_is_local else -h_diff
    final_diff = diff_from_fav - abs_ah

    if abs(final_diff) < 1e-6:
        return "push"
    return "covered" if final_diff > 0 else "no_covered"


def _round_sort_key(value: Tuple[str, str]) -> Tuple[str, int, str]:
    sub_id, round_value = value
    if str(round_value).isdigit():
        return sub_id, int(round_value), ""
    return sub_id, 10**9, str(round_value)


def _walk_schedule_matches(node: Any, sub_id: str = "0", round_name: str = "") -> Iterable[Tuple[str, str, list]]:
    if isinstance(node, dict):
        for key, val in node.items():
            key_str = str(key)
            new_sub_id = sub_id
            new_round = round_name
            if key_str.startswith("sub_"):
                new_sub_id = key_str.removeprefix("sub_")
            elif key_str.startswith("R_"):
                new_round = key_str.removeprefix("R_")
            elif key_str.startswith("G"):
                new_round = key_str
            yield from _walk_schedule_matches(val, new_sub_id, new_round)
    elif isinstance(node, list):
        if len(node) >= 8 and str(node[0]).isdigit() and (isinstance(node[1], int) or str(node[1]).isdigit()):
            yield sub_id, round_name, node
        else:
            for item in node:
                yield from _walk_schedule_matches(item, sub_id, round_name)


def _flatten_schedule(data: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, Any]], List[Tuple[str, str]]]:
    teams = {str(row[0]): row[1] for row in data.get("TeamInfo", []) if isinstance(row, list) and len(row) > 1}
    sub_names = {
        str(row[0]): str(row[1])
        for row in data.get("SubLeagueInfo", [])
        if isinstance(row, list) and len(row) > 1
    }
    cup_names = {
        str(row[0]): str(row[2])
        for row in data.get("CupKindList", [])
        if isinstance(row, list) and len(row) > 2
    }
    matches: Dict[str, Dict[str, Any]] = {}
    rounds: List[Tuple[str, str]] = []

    for sub_id, round_value, row in _walk_schedule_matches(data.get("ScheduleList") or {}):
        if not isinstance(row, list) or len(row) < 8:
            continue
        match_id = str(row[0])

        row_ah = None
        row_ou = None
        # En NowGoal ScheduleList: row[8]=Puesto Local, row[9]=Puesto Visitante, row[10]=Hándicap Asiático, row[12]=Over/Under
        if len(row) > 10 and row[10] is not None and str(row[10]).strip() not in ("", "-"):
            row_ah = parse_handicap_numeric(row[10])
        if len(row) > 12 and row[12] is not None and str(row[12]).strip() not in ("", "-"):
            row_ou = parse_goal_line_numeric(row[12])

        if round_value:
            rounds.append((sub_id, round_value))

        stage_name = sub_names.get(str(sub_id), "")
        if round_value.startswith("G"):
            stage_digits = "".join(filter(str.isdigit, round_value))
            if stage_digits in cup_names:
                stage_name = cup_names[stage_digits]

        matches[match_id] = {
            "id": match_id,
            "sub_id": str(sub_id),
            "sub_name": stage_name,
            "round": round_value,
            "date": str(row[3]) if len(row) > 3 else "",
            "home": teams.get(str(row[4]), str(row[4])) if len(row) > 4 else str(row[4]),
            "away": teams.get(str(row[5]), str(row[5])) if len(row) > 5 else str(row[5]),
            "score": row[6] or "-" if len(row) > 6 else "-",
            "source_state": row[2] if len(row) > 2 else 0,
            "row_ah": row_ah,
            "row_ou": row_ou,
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
    discovered_season, league_data = _discover_league(session, league_id, requested_season)
    match_map, rounds = _flatten_schedule(league_data)

    all_odds: Dict[str, Dict[str, float]] = {}
    valid_rounds = [
        (sub_id, round_value)
        for sub_id, round_value in rounds
        if round_value and not str(round_value).startswith("G")
    ]

    def fetch_round_odds(sub_id: str, round_val: str) -> Dict[str, Dict[str, float]]:
        url = (
            f"{BASE_URL}/ajax/LeagueOddsAjax?sclassId={league_id}"
            f"&subSclassId={sub_id}&matchSeason={discovered_season}&round={round_val}"
        )
        try:
            resp = session.get(url, headers=HEADERS, timeout=4, verify=False)
            if resp.status_code == 200 and resp.text:
                return parse_round_odds(resp.text.lstrip("\ufeff"), company_id)
        except Exception:
            pass
        return {}

    if valid_rounds:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_to_round = {
                executor.submit(fetch_round_odds, sub_id, round_val): (sub_id, round_val)
                for sub_id, round_val in valid_rounds
            }
            for future in concurrent.futures.as_completed(future_to_round):
                try:
                    res = future.result()
                    if res:
                        all_odds.update(res)
                except Exception:
                    pass

    for match_id, m in match_map.items():
        if match_id not in all_odds and m.get("row_ah") is not None:
            all_odds[match_id] = {
                "home_odds_hk": None,
                "visible_ah": m["row_ah"],
                "away_odds_hk": None,
            }

    if target_ah is None:
        selected_ids = list(match_map)
    else:
        selected_ids = [
            match_id
            for match_id, odds in all_odds.items()
            if odds.get("visible_ah") is not None and abs(odds["visible_ah"] - float(target_ah)) < 1e-9
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

        vis_ah = odds.get("visible_ah")
        score_str = str(match.get("score") or "-").strip()
        fav_role = (
            "home_fav" if vis_ah is not None and vis_ah > 0 else
            "away_fav" if vis_ah is not None and vis_ah < 0 else
            "even" if vis_ah is not None and vis_ah == 0 else
            "none"
        )
        fav_coverage = (
            _calculate_favorite_coverage(score_str, vis_ah)
            if finished else "pending"
        )

        existing = sql_store.get_match(match_id)
        match.update(
            {
                "visible_ah": vis_ah,
                "company_id": int(company_id),
                "home_odds_decimal": (
                    odds["home_odds_hk"] + 1 if odds.get("home_odds_hk") is not None else None
                ),
                "away_odds_decimal": (
                    odds["away_odds_hk"] + 1 if odds.get("away_odds_hk") is not None else None
                ),
                "finished": finished,
                "favorite_role": fav_role,
                "favorite_coverage": fav_coverage,
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

    total_matches = len(matches)
    total_finished = sum(1 for m in matches if m.get("finished"))
    total_pending = total_matches - total_finished
    total_in_sql = sum(1 for m in matches if m.get("already_in_sql"))
    total_new = total_matches - total_in_sql
    covered_count = sum(1 for m in matches if m.get("favorite_coverage") == "covered")
    no_covered_count = sum(1 for m in matches if m.get("favorite_coverage") == "no_covered")
    push_count = sum(1 for m in matches if m.get("favorite_coverage") == "push")

    return {
        "league_id": league_id,
        "league_name": league_info[1] if len(league_info) > 1 else f"Liga {league_id}",
        "season": discovered_season,
        "target_ah": float(target_ah) if target_ah is not None else None,
        "company_id": int(company_id),
        "match_status": match_status,
        "stats": {
            "total": total_matches,
            "finished": total_finished,
            "pending": total_pending,
            "in_sql": total_in_sql,
            "new": total_new,
            "covered": covered_count,
            "no_covered": no_covered_count,
            "push": push_count,
        },
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


def prefilter_matches_by_ah(
    league_id: Union[int, str],
    season: Optional[str] = None,
    target_ah: float = 0.0,
    tolerance: float = 0.0,
    only_finished: bool = True,
) -> Dict[str, Any]:
    """
    Prefiltrado liviano de partidos candidatos exclusivamente por línea AH del calendario liviano NowGoal (ScheduleList row[10]).

    Alcance formal autorizado por Dirección Técnica:
    - Consulta exclusivamente el calendario liviano en memoria (ScheduleList row[10]).
    - Cero scraping profundo HTTP de análisis / H2H.
    - Cero escritura o alteración de base de datos SQLite.
    """
    normalized_league_id = str(league_id or "").strip()
    if not re.fullmatch(r"[1-9]\d{0,11}", normalized_league_id):
        raise ValueError("El ID debe ser un sclassId NowGoal numérico y positivo.")

    session = requests.Session()
    actual_season, schedule_data = _discover_league(session, normalized_league_id, str(season or "").strip())
    # Fail closed if NowGoal does not echo the requested league identity. A valid
    # numeric ID alone cannot prove that the caller supplied the correct provider's
    # ID, so never return a calendar whose LeagueInfo disagrees with that ID.
    league_info = schedule_data.get("LeagueInfo")
    if not isinstance(league_info, list) or len(league_info) < 2:
        raise RuntimeError("NowGoal no devolvió LeagueInfo; no se puede verificar la identidad de la liga.")
    returned_league_id = str(league_info[0] or "").strip()
    if returned_league_id != normalized_league_id:
        raise RuntimeError(
            f"NowGoal devolvió la liga {returned_league_id or 'sin ID'}, no la solicitada {normalized_league_id}."
        )
    canonical_league_name = str(league_info[1] or "").strip()
    if not canonical_league_name:
        raise RuntimeError("NowGoal no devolvió el nombre canónico de la liga.")
    canonical_season = str(league_info[2] or "").strip() if len(league_info) > 2 else ""
    if canonical_season:
        actual_season = canonical_season

    matches_dict, _ = _flatten_schedule(schedule_data)
    finished_with_score = sum(1 for match in matches_dict.values() if _is_finished(match))
    matches_with_calendar_ah = sum(1 for match in matches_dict.values() if match.get("row_ah") is not None)

    candidates: List[Dict[str, Any]] = []
    for match_id, m in matches_dict.items():
        if only_finished and not _is_finished(m):
            continue
        row_ah = m.get("row_ah")
        if row_ah is None:
            continue

        diff = abs(row_ah - target_ah)
        if diff <= tolerance + 1e-6:
            candidates.append(
                {
                    "match_id": match_id,
                    "league_id": str(league_id),
                    "season": actual_season,
                    "home": m.get("home"),
                    "away": m.get("away"),
                    "score": m.get("score"),
                    "calendar_ah": row_ah,
                    "date": m.get("date"),
                    "round": m.get("round"),
                    "exact_match": diff < 1e-6,
                }
            )

    return {
        "league_id": normalized_league_id,
        "league_name": canonical_league_name,
        "league_identity_verified": True,
        "season": actual_season,
        "target_ah": target_ah,
        "tolerance": tolerance,
        "total_calendar_matches": len(matches_dict),
        "finished_with_score": finished_with_score,
        "matches_with_calendar_ah": matches_with_calendar_ah,
        "total_candidates": len(candidates),
        "candidates": candidates,
    }
