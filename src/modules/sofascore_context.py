"""Clasificaciones de SofaScore cargadas bajo demanda.

La API web de SofaScore no es una API publica versionada para terceros. Este
modulo la mantiene aislada del scraper principal, aplica resolucion conservadora
de equipos/partidos y cachea solo respuestas que contienen una tabla valida.
"""

from __future__ import annotations

import hashlib
import os
import re
import threading
import time
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from . import sql_store
except Exception:  # pragma: no cover - permite usar el modulo de forma aislada
    sql_store = None


API_BASE = "https://www.sofascore.com/api/v1"
CACHE_VERSION = "v2"
CACHE_TTL_SECONDS = max(300, int(os.getenv("SOFASCORE_TABLE_CACHE_SECONDS", "21600")))
REQUEST_TIMEOUT_SECONDS = max(3, int(os.getenv("SOFASCORE_TIMEOUT_SECONDS", "8")))
VERIFY_SSL = os.getenv("SOFASCORE_VERIFY_SSL", "false").strip().lower() not in {
    "0", "false", "no", "off"
}
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_session: Optional[requests.Session] = None
_session_lock = threading.Lock()
_memory_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = threading.Lock()


def _http_session() -> requests.Session:
    global _session
    with _session_lock:
        if _session is None:
            session = requests.Session()
            retry = Retry(
                total=1,
                connect=1,
                read=1,
                backoff_factor=0.25,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset({"GET"}),
            )
            session.mount("https://", HTTPAdapter(max_retries=retry))
            session.headers.update({
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mozilla/5.0 (compatible; LeagueTableContext/1.0)",
            })
            _session = session
    return _session


def _api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response = _http_session().get(
        f"{API_BASE}{path}",
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
        verify=VERIFY_SSL,
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {}


def _normalize_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char)).lower()
    text = re.sub(r"\b(fc|cf|sc|afc|club|deportivo|futbol|football)\b", " ", text)
    text = re.sub(r"\b(w|women|woman|f|femenino|femenina|ladies)\b", " ", text)
    # Alias observado en el feed de Nowgoal para Launceston United.
    text = re.sub(r"\blan\s+thurston\b", " launceston united ", text)
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def _team_search_queries(team_name: str) -> List[str]:
    raw = str(team_name or "").strip()
    normalized = _normalize_name(raw)
    # Algunos feeds añaden el color de la plantilla aunque SofaScore catalogue
    # el equipo sin ese sufijo (p. ej. "Robina City Blue" -> "Robina City").
    without_color = re.sub(r"\b(?:blue|red)\b$", "", normalized).strip()
    return list(dict.fromkeys(q for q in (raw, normalized, without_color) if q))


def _similarity(left: Any, right: Any) -> float:
    a, b = _normalize_name(left), _normalize_name(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a in b or b in a:
        shortest = min(len(a), len(b))
        longest = max(len(a), len(b))
        return 0.88 + (0.1 * shortest / longest)
    return SequenceMatcher(None, a, b).ratio()


def _parse_date(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    # Los snapshots de Precacheo guardan match_date como M/D/YYYY.
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw[:10], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _select_team_result(payload: Dict[str, Any], requested_name: str) -> Optional[Dict[str, Any]]:
    candidates: List[Tuple[float, Dict[str, Any]]] = []
    requested_text = str(requested_name or "").lower()
    requested_is_women = bool(re.search(
        r"(?:\(\s*[wf]\s*\)|\b(?:women|woman|femenin[oa]|ladies)\b)",
        requested_text,
    ))
    for result in payload.get("results") or []:
        if not isinstance(result, dict) or result.get("type") != "team":
            continue
        entity = result.get("entity") or {}
        sport = entity.get("sport") or {}
        if sport.get("slug") != "football" or not entity.get("id"):
            continue
        score = max(
            _similarity(requested_name, entity.get("name")),
            _similarity(requested_name, entity.get("shortName")),
        )
        candidates.append((score, entity))

    if requested_is_women:
        women_candidates = [item for item in candidates if item[1].get("gender") == "F"]
        if women_candidates:
            candidates = women_candidates

    candidates.sort(key=lambda item: item[0], reverse=True)
    if not candidates or candidates[0][0] < 0.76:
        return None
    # El buscador ordena por relevancia. No descartamos empates de nombre
    # (primer equipo/equipo femenino), porque la comprobacion posterior exige
    # que tambien coincidan rival, fecha y competicion del evento.
    return candidates[0][1]


def _resolve_team(requested_name: str) -> Optional[Dict[str, Any]]:
    for query in _team_search_queries(requested_name):
        search = _api_get("/search/all", params={"q": query})
        team = _select_team_result(search, requested_name)
        if team:
            return team
    return None


def _event_timestamp(event: Dict[str, Any]) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(int(event.get("startTimestamp")), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _event_team_score(event: Dict[str, Any], home_name: str, away_name: str) -> float:
    event_home = (event.get("homeTeam") or {}).get("name")
    event_away = (event.get("awayTeam") or {}).get("name")
    direct = (_similarity(home_name, event_home) + _similarity(away_name, event_away)) / 2
    reverse = (_similarity(home_name, event_away) + _similarity(away_name, event_home)) / 2
    return max(direct, reverse)


def _select_event(
    events: Iterable[Dict[str, Any]],
    home_name: str,
    away_name: str,
    league_name: str = "",
    match_date: Any = None,
) -> Optional[Dict[str, Any]]:
    target_date = _parse_date(match_date)
    ranked: List[Tuple[float, Dict[str, Any]]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        tournament = event.get("tournament") or {}
        unique = tournament.get("uniqueTournament") or {}
        season = event.get("season") or {}
        if not unique.get("id") or not season.get("id"):
            continue

        team_score = _event_team_score(event, home_name, away_name)
        if team_score < 0.75:
            continue

        league_score = _similarity(league_name, unique.get("name") or tournament.get("name")) if league_name else 0.7
        event_date = _event_timestamp(event)
        date_score = 0.7
        if target_date and event_date:
            days = abs((event_date.date() - target_date.date()).days)
            date_score = max(0.0, 1.0 - days / 21.0)

        # El nombre de los dos equipos manda; fecha y liga desempatan.
        score = team_score * 0.72 + date_score * 0.18 + league_score * 0.10
        ranked.append((score, event))

    ranked.sort(key=lambda item: item[0], reverse=True)
    if not ranked or ranked[0][0] < 0.72:
        return None
    return ranked[0][1]


def _flatten_standings(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for table in payload.get("standings") or []:
        if not isinstance(table, dict):
            continue
        group_name = table.get("name") or ""
        for raw in table.get("rows") or []:
            if not isinstance(raw, dict):
                continue
            team = raw.get("team") or {}
            if not team.get("name"):
                continue
            scores_for = raw.get("scoresFor")
            scores_against = raw.get("scoresAgainst")
            try:
                goal_difference = int(scores_for) - int(scores_against)
            except (TypeError, ValueError):
                goal_difference = raw.get("scoreDiffFormatted") or "-"
            promotion = raw.get("promotion") or {}
            rows.append({
                "group": group_name,
                "position": raw.get("position"),
                "team_id": team.get("id"),
                "team": team.get("name"),
                "short_name": team.get("shortName") or team.get("name"),
                "matches": raw.get("matches", 0),
                "wins": raw.get("wins", 0),
                "draws": raw.get("draws", 0),
                "losses": raw.get("losses", 0),
                "scores_for": scores_for if scores_for is not None else 0,
                "scores_against": scores_against if scores_against is not None else 0,
                "goal_difference": goal_difference,
                "points": raw.get("points", 0),
                "promotion": promotion.get("text") or "",
            })
    return rows


def _event_score(event: Dict[str, Any], side: str) -> Optional[int]:
    score = event.get(f"{side}Score") or {}
    for key in ("normaltime", "current", "display"):
        try:
            return int(score.get(key))
        except (TypeError, ValueError):
            continue
    return None


def _load_season_events(unique_id: Any, season_id: Any, max_pages: int = 8) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for page in range(max_pages):
        payload = _api_get(
            f"/unique-tournament/{unique_id}/season/{season_id}/events/last/{page}"
        )
        for event in payload.get("events") or []:
            event_id = str(event.get("id") or "")
            if not event_id or event_id in seen:
                continue
            seen.add(event_id)
            events.append(event)
        if not payload.get("hasNextPage"):
            break
    return events


def _build_ou_views(events: Iterable[Dict[str, Any]], line: float) -> Dict[str, List[Dict[str, Any]]]:
    counters: Dict[str, Dict[str, Dict[str, Any]]] = {
        "total": {}, "home": {}, "away": {}
    }

    def add(view: str, team: Dict[str, Any], goals: int) -> None:
        team_id = str(team.get("id") or "")
        if not team_id:
            return
        row = counters[view].setdefault(team_id, {
            "team_id": team.get("id"),
            "team": team.get("name") or "-",
            "matches": 0,
            "over": 0,
            "under": 0,
            "push": 0,
            "total_goals": 0,
        })
        row["matches"] += 1
        row["total_goals"] += goals
        if goals > line:
            row["over"] += 1
        elif goals < line:
            row["under"] += 1
        else:
            row["push"] += 1

    for event in events:
        if (event.get("status") or {}).get("type") != "finished":
            continue
        home_score = _event_score(event, "home")
        away_score = _event_score(event, "away")
        if home_score is None or away_score is None:
            continue
        home_team = event.get("homeTeam") or {}
        away_team = event.get("awayTeam") or {}
        goals = home_score + away_score
        add("total", home_team, goals)
        add("total", away_team, goals)
        add("home", home_team, goals)
        add("away", away_team, goals)

    output: Dict[str, List[Dict[str, Any]]] = {}
    for view, teams in counters.items():
        rows = []
        for row in teams.values():
            matches = int(row.pop("matches", 0))
            total_goals = int(row.pop("total_goals", 0))
            row["matches"] = matches
            row["over_pct"] = round((row["over"] / matches) * 100, 1) if matches else 0.0
            row["avg_goals"] = round(total_goals / matches, 2) if matches else 0.0
            rows.append(row)
        rows.sort(key=lambda item: (-item["over_pct"], -item["matches"], item["team"]))
        for position, row in enumerate(rows, start=1):
            row["position"] = position
        if rows:
            output[view] = rows
    return output


def _ou_signal(
    ou_views: Dict[str, List[Dict[str, Any]]],
    home_team_id: Any,
    away_team_id: Any,
) -> Dict[str, Any]:
    def find(view: str, team_id: Any) -> Optional[Dict[str, Any]]:
        return next(
            (row for row in ou_views.get(view, []) if str(row.get("team_id")) == str(team_id)),
            None,
        )

    home = find("home", home_team_id) or find("total", home_team_id)
    away = find("away", away_team_id) or find("total", away_team_id)
    samples = [row for row in (home, away) if row and row.get("matches")]
    if not samples:
        return {"label": "SIN DATOS", "tone": "neutral", "over_pct": None, "sample": 0}
    weighted_matches = sum(int(row["matches"]) for row in samples)
    weighted_over = sum(float(row["over_pct"]) * int(row["matches"]) for row in samples)
    percentage = round(weighted_over / weighted_matches, 1) if weighted_matches else 0.0
    if percentage >= 62:
        label, tone = "TENDENCIA OVER", "over"
    elif percentage <= 38:
        label, tone = "TENDENCIA UNDER", "under"
    else:
        label, tone = "TENDENCIA NEUTRA", "neutral"
    return {"label": label, "tone": tone, "over_pct": percentage, "sample": weighted_matches}


def _cache_key(home_name: str, away_name: str, league_name: str, match_date: Any, goal_line: Any) -> str:
    raw = "|".join([
        CACHE_VERSION,
        _normalize_name(home_name),
        _normalize_name(away_name),
        _normalize_name(league_name),
        str(match_date or "")[:10],
        str(goal_line or ""),
    ])
    return "sofascore_table_context_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _read_cache(key: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    with _cache_lock:
        cached = _memory_cache.get(key)
    if isinstance(cached, dict) and now - float(cached.get("cached_at_epoch", 0)) < CACHE_TTL_SECONDS:
        return cached.get("payload")

    if sql_store is None:
        return None
    try:
        cached = sql_store.get_json_state(key, default=None)
        if isinstance(cached, dict) and now - float(cached.get("cached_at_epoch", 0)) < CACHE_TTL_SECONDS:
            with _cache_lock:
                _memory_cache[key] = cached
            return cached.get("payload")
    except Exception:
        return None
    return None


def _write_cache(key: str, payload: Dict[str, Any]) -> None:
    wrapped = {"cached_at_epoch": time.time(), "payload": payload}
    with _cache_lock:
        _memory_cache[key] = wrapped
    if sql_store is not None:
        try:
            sql_store.set_json_state(key, wrapped)
        except Exception:
            pass


def _unavailable(reason: str) -> Dict[str, Any]:
    return {"available": False, "reason": reason, "views": {}}


def get_league_table_context(
    home_name: str,
    away_name: str,
    league_name: str = "",
    match_date: Any = None,
    goal_line: Any = None,
) -> Dict[str, Any]:
    """Resuelve el partido y devuelve tablas total/local/visitante normalizadas."""
    if not str(home_name or "").strip() or not str(away_name or "").strip():
        return _unavailable("missing_teams")

    try:
        ou_line = float(goal_line)
    except (TypeError, ValueError):
        ou_line = 2.5
    ou_line = min(8.5, max(0.5, ou_line))

    key = _cache_key(home_name, away_name, league_name, match_date, ou_line)
    stable_key = _cache_key(home_name, away_name, league_name, None, ou_line)
    cached = _read_cache(key) or (stable_key != key and _read_cache(stable_key))
    if cached:
        result = dict(cached)
        result["cached"] = True
        return result

    try:
        anchor_team = _resolve_team(home_name)
        if not anchor_team:
            anchor_team = _resolve_team(away_name)
        if not anchor_team:
            return _unavailable("teams_not_resolved")

        events: List[Dict[str, Any]] = []
        for direction in ("next", "last"):
            try:
                payload = _api_get(f"/team/{anchor_team['id']}/events/{direction}/0")
                events.extend(payload.get("events") or [])
            except requests.RequestException:
                continue

        event = _select_event(events, home_name, away_name, league_name, match_date)
        if not event:
            return _unavailable("match_not_resolved")

        tournament = event.get("tournament") or {}
        unique = tournament.get("uniqueTournament") or {}
        season = event.get("season") or {}
        unique_id, season_id = unique.get("id"), season.get("id")
        if not unique_id or not season_id:
            return _unavailable("competition_not_resolved")

        views: Dict[str, List[Dict[str, Any]]] = {}
        for view in ("total", "home", "away"):
            try:
                table_payload = _api_get(
                    f"/unique-tournament/{unique_id}/season/{season_id}/standings/{view}"
                )
            except requests.RequestException:
                # Algunas competiciones solo publican la clasificación general.
                continue
            normalized = _flatten_standings(table_payload)
            if normalized:
                views[view] = normalized

        if not views.get("total"):
            return _unavailable("standings_not_available")

        event_home = event.get("homeTeam") or {}
        event_away = event.get("awayTeam") or {}
        # Mantener la orientacion solicitada aunque SofaScore invierta un cruce historico.
        requested_home_id = event_home.get("id")
        requested_away_id = event_away.get("id")
        if _similarity(home_name, event_away.get("name")) > _similarity(home_name, event_home.get("name")):
            requested_home_id, requested_away_id = requested_away_id, requested_home_id

        try:
            season_events = _load_season_events(unique_id, season_id)
            ou_lines = sorted({1.5, 2.5, 3.5, 4.5, ou_line})
            ou_tables = {}
            for line in ou_lines:
                line_views = _build_ou_views(season_events, line)
                ou_tables[f"{line:g}"] = {
                    "line": line,
                    "views": line_views,
                    "signal": _ou_signal(line_views, requested_home_id, requested_away_id),
                }
            selected_ou = ou_tables.get(f"{ou_line:g}", {})
            ou_views = selected_ou.get("views", {})
        except requests.RequestException:
            season_events = []
            ou_tables = {}
            ou_views = {}

        external_links: Dict[str, str] = {}
        if int(unique_id) == 34563:
            flashscore_base = "https://www.flashscore.es/futbol/china/league-one-women/"
            external_links = {
                "flashscore": flashscore_base,
                "flashscore_ou": (
                    f"{flashscore_base}#/baT3Pnwf/mas-de_menos-de/general/{ou_line:g}/"
                ),
            }

        result = {
            "available": True,
            "cached": False,
            "source": "SofaScore",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "tournament": unique.get("name") or tournament.get("name") or league_name,
            "season": season.get("name") or season.get("year") or "",
            "tournament_id": unique_id,
            "season_id": season_id,
            "home_team_id": requested_home_id,
            "away_team_id": requested_away_id,
            "home_name": home_name,
            "away_name": away_name,
            "views": views,
            "ou": {
                "line": ou_line,
                "views": ou_views,
                "matches_analyzed": len(season_events),
                "signal": selected_ou.get("signal", {}) if season_events else {},
                "tables": ou_tables,
            },
            "external_links": external_links,
        }
        _write_cache(key, result)
        if stable_key != key:
            _write_cache(stable_key, result)
        return result
    except (requests.RequestException, ValueError, TypeError, KeyError):
        return _unavailable("provider_unavailable")


__all__ = ["get_league_table_context"]
