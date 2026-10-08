"""Clasificaciones de SofaScore cargadas bajo demanda.

Módulo optimizado para soportar ligas mundiales (principales y regionales/amateurs),
múltiples endpoints de SofaScore (unique-tournament y tournament), selección de temporadas
y extracción completa de rachas (Last 5) y zonas de ascenso/descenso.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from . import sql_store
except Exception:  # pragma: no cover - permite usar el modulo de forma aislada
    sql_store = None


API_HOSTS = [
    "https://api.sofascore.com/api/v1",
    "https://www.sofascore.com/api/v1",
]
CACHE_VERSION = "v8_verified_teams"
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
                total=2,
                connect=2,
                read=2,
                backoff_factor=0.25,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset({"GET"}),
            )
            session.mount("https://", HTTPAdapter(max_retries=retry))
            session.headers.update({
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
                "Referer": "https://www.sofascore.com/",
                "Origin": "https://www.sofascore.com",
                "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            })
            _session = session
    return _session


def _api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    last_err = None
    for base in API_HOSTS:
        try:
            response = _http_session().get(
                f"{base}{path}",
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
                verify=VERIFY_SSL,
            )
            if response.status_code == 200:
                payload = response.json()
                return payload if isinstance(payload, dict) else {}
            elif response.status_code in (404, 400):
                continue
            response.raise_for_status()
        except Exception as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    return {}


def _normalize_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char)).lower()
    text = re.sub(r"\b(fc|cf|sc|afc|club|deportivo|futbol|football)\b", " ", text)
    text = re.sub(r"\b(w|women|woman|f|femenino|femenina|ladies)\b", " ", text)
    text = re.sub(r"\blan\s+thurston\b", " launceston united ", text)
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def _team_search_queries(team_name: str) -> List[str]:
    raw = str(team_name or "").strip()
    normalized = _normalize_name(raw)
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


def _table_team_similarity(requested: Any, candidate: Any) -> float:
    """Keep reserve sides distinct from the first team in a league table."""
    requested_norm = _normalize_name(requested)
    candidate_norm = _normalize_name(candidate)
    reserve = re.fullmatch(r"(.+) (?:b|ii|2|reserves?)", requested_norm)
    if reserve:
        candidate_reserve = re.fullmatch(r"(.+) (?:b|ii|2|reserves?)", candidate_norm)
        if candidate_reserve and candidate_reserve.group(1) == reserve.group(1):
            return 1.0
        if not candidate_reserve:
            return min(_similarity(requested, candidate), 0.55)
    return _similarity(requested, candidate)


def _parse_date(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
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
    return candidates[0][1]


def _resolve_team(requested_name: str) -> Optional[Dict[str, Any]]:
    for query in _team_search_queries(requested_name):
        try:
            search = _api_get("/search/all", params={"q": query})
            team = _select_team_result(search, requested_name)
            if team:
                return team
        except Exception:
            continue
    return None


def _load_league_aliases() -> Dict[str, Dict[str, Any]]:
    base_aliases: Dict[str, Dict[str, Any]] = {
        _normalize_name("India Sikkim S-League"): {"id": 36312, "is_unique": True, "name": 'SFA "A" Division S-League'},
        _normalize_name("SFA A Division S-League"): {"id": 36312, "is_unique": True, "name": 'SFA "A" Division S-League'},
        _normalize_name("Sikkim S-League"): {"id": 36312, "is_unique": True, "name": 'SFA "A" Division S-League'},
        _normalize_name("Sikkim Premier Division"): {"id": 36312, "is_unique": True, "name": 'SFA "A" Division S-League'},
    }
    alias_file = Path(__file__).resolve().parent.parent.parent / "data" / "sofascore_league_aliases.json"
    if alias_file.exists():
        try:
            loaded = json.loads(alias_file.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                for k, v in loaded.items():
                    norm_k = _normalize_name(k)
                    if isinstance(v, dict) and "id" in v:
                        base_aliases[norm_k] = v
                    elif isinstance(v, (int, str)) and str(v).isdigit():
                        # Legacy scalar entries do not encode whether the ID belongs
                        # to a tournament or uniqueTournament, so leave them
                        # untyped and let resolution fail safely.
                        base_aliases[norm_k] = {"id": int(v)}
        except Exception:
            pass

    master_file = Path(__file__).resolve().parent.parent.parent / "data" / "sofascore_master_registry.json"
    if master_file.exists():
        try:
            master = json.loads(master_file.read_text(encoding="utf-8"))
            if isinstance(master, dict):
                for k, v in master.items():
                    if isinstance(v, dict) and "id" in v:
                        base_aliases[_normalize_name(k)] = v
        except Exception:
            pass

    return base_aliases


def _lookup_league_alias(league_name: str) -> Optional[Dict[str, Any]]:
    if not league_name:
        return None
    raw = str(league_name).strip()
    is_women = bool(re.search(r"(?:\(\s*[wf]\s*\)|\b(?:women|woman|femenin[oa]|ladies)\b)", raw, re.I))
    norm = _normalize_name(raw)
    aliases = _load_league_aliases()
    entry = aliases.get(norm)
    if entry:
        # IDs are provider-specific. Accept only a positive numeric SofaScore ID
        # and an explicit entity kind; never silently reinterpret a tournament ID
        # as a uniqueTournament ID.
        try:
            tournament_id = int(entry.get("id"))
        except (TypeError, ValueError):
            return None
        if tournament_id <= 0 or not isinstance(entry.get("is_unique"), bool):
            return None
        alias_name = str(entry.get("name") or norm)
        alias_is_women = bool(re.search(r"\b(?:women|woman|femenin[oa]|ladies)\b", alias_name, re.I))
        if is_women != alias_is_women:
            return None
        return {**entry, "id": tournament_id}
    return None


def _save_league_alias(league_name: str, tournament_id: int, is_unique: bool = True, tournament_name: str = "") -> None:
    norm = _normalize_name(league_name)
    if not norm:
        return
    try:
        tournament_id = int(tournament_id)
    except (TypeError, ValueError):
        return
    if tournament_id <= 0 or not isinstance(is_unique, bool):
        return
    alias_file = Path(__file__).resolve().parent.parent.parent / "data" / "sofascore_league_aliases.json"
    try:
        alias_file.parent.mkdir(parents=True, exist_ok=True)
        data = {}
        if alias_file.exists():
            try:
                data = json.loads(alias_file.read_text(encoding="utf-8"))
            except Exception:
                data = {}
        data[norm] = {
            "id": tournament_id,
            "is_unique": is_unique,
            "name": tournament_name or league_name,
        }
        alias_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _tournament_search_queries(league_name: str) -> List[str]:
    raw = str(league_name or "").strip()
    if not raw:
        return []
    normalized = _normalize_name(raw)
    queries = [raw, normalized]
    for country in ("india", "spain", "england", "italy", "germany", "france", "colombia", "argentina", "brazil", "mexico", "australia", "china", "japan", "usa"):
        if normalized.startswith(country + " "):
            sub = normalized[len(country):].strip()
            if sub:
                queries.append(sub)
        if raw.lower().startswith(country + " "):
            sub_raw = raw[len(country):].strip()
            if sub_raw:
                queries.append(sub_raw)
    for word in ("league", "division", "cup", "amateur", "premier", "copa", "s league", "s-league"):
        cleaned = re.sub(rf"\b{word}\b", " ", normalized).strip()
        cleaned = " ".join(cleaned.split())
        if cleaned and len(cleaned) >= 3:
            queries.append(cleaned)
    return list(dict.fromkeys(q for q in queries if len(q) >= 3))


def _resolve_tournament(league_name: str) -> Optional[Dict[str, Any]]:
    norm = _normalize_name(league_name)
    info = _lookup_league_alias(league_name)
    if info:
        t_id = info["id"]
        is_u = info.get("is_unique")
        if not isinstance(is_u, bool):
            return {"status": "unresolved", "reason": "invalid_alias_entity_type"}
        return {
            "id": t_id,
            "name": info.get("name", league_name),
            "is_unique": is_u,
            "status": "resolved",
            "source": "alias",
        }

    candidates: Dict[Tuple[int, bool], Dict[str, Any]] = {}
    search_succeeded = False
    search_errors = 0
    for query in _tournament_search_queries(league_name):
        try:
            search = _api_get("/search/all", params={"q": query})
            if isinstance(search, dict) and isinstance(search.get("results"), list):
                search_succeeded = True
            for result in search.get("results") or []:
                if not isinstance(result, dict):
                    continue
                rtype = result.get("type")
                if rtype == "uniqueTournament":
                    entity = result.get("entity") or {}
                    sport = entity.get("sport") or {}
                    if sport.get("slug") and sport.get("slug") != "football":
                        continue
                    t_name = entity.get("name") or ""
                    sim = _similarity(league_name, t_name)
                    entity_id = entity.get("id")
                    if entity_id and sim >= 0.72:
                        key = (int(entity_id), True)
                        candidate = candidates.setdefault(key, {
                            "id": int(entity_id), "name": t_name, "is_unique": True,
                            "entity": entity, "score": sim,
                        })
                        candidate["score"] = max(candidate["score"], sim)
                elif rtype == "tournament":
                    entity = result.get("entity") or {}
                    sport = entity.get("sport") or {}
                    if sport.get("slug") and sport.get("slug") != "football":
                        continue
                    t_name = entity.get("name") or ""
                    unique_t = entity.get("uniqueTournament")
                    if unique_t and unique_t.get("id"):
                        u_id = unique_t["id"]
                        u_name = unique_t.get("name") or t_name
                        sim = _similarity(league_name, u_name)
                        if sim >= 0.72:
                            key = (int(u_id), True)
                            candidate = candidates.setdefault(key, {
                                "id": int(u_id), "name": u_name, "is_unique": True,
                                "entity": unique_t, "score": sim,
                            })
                            candidate["score"] = max(candidate["score"], sim)
                    else:
                        t_id = entity.get("id")
                        sim = _similarity(league_name, t_name)
                        if t_id and sim >= 0.72:
                            key = (int(t_id), False)
                            candidate = candidates.setdefault(key, {
                                "id": int(t_id), "name": t_name, "is_unique": False,
                                "entity": entity, "score": sim,
                            })
                            candidate["score"] = max(candidate["score"], sim)
        except requests.HTTPError as exc:
            search_errors += 1
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code == 403:
                return {"status": "unavailable", "reason": "provider_access_challenge"}
            if status_code == 429:
                return {"status": "unavailable", "reason": "provider_rate_limited"}
            continue
        except Exception:
            search_errors += 1
            continue

    ranked = sorted(candidates.values(), key=lambda item: item["score"], reverse=True)
    if not ranked:
        if not search_succeeded and search_errors:
            return {"status": "unavailable", "reason": "provider_unavailable"}
        return {"status": "unresolved", "reason": "competition_not_resolved"}
    # Search may return both a regional competition and a similarly named cup.
    # Refuse close matches instead of caching an arbitrary provider ID.
    if len(ranked) > 1 and ranked[0]["score"] - ranked[1]["score"] < 0.08:
        return {
            "status": "ambiguous",
            "reason": "competition_ambiguous",
            "candidates": [
                {"id": item["id"], "name": item["name"], "is_unique": item["is_unique"], "score": round(item["score"], 3)}
                for item in ranked[:5]
            ],
        }

    # A plausible search hit is not yet a durable cross-provider mapping. Keep it
    # ephemeral here; callers can verify the live entity and an operator can add a
    # curated alias only after the mapping has independent support.
    resolved = ranked[0]
    return {**resolved, "status": "resolved", "source": "search"}


def _fetch_tournament_seasons(tournament_id: Any, is_unique: bool = True) -> Tuple[List[Dict[str, Any]], bool]:
    paths = [
        (f"/unique-tournament/{tournament_id}/seasons", True),
        (f"/tournament/{tournament_id}/seasons", False),
    ] if is_unique else [
        (f"/tournament/{tournament_id}/seasons", False),
        (f"/unique-tournament/{tournament_id}/seasons", True),
    ]
    for path, flag in paths:
        try:
            payload = _api_get(path)
            seasons = payload.get("seasons") or []
            if seasons:
                return seasons, flag
        except Exception:
            continue
    for path, flag in [(f"/tournament/{tournament_id}", False), (f"/unique-tournament/{tournament_id}", True)]:
        try:
            payload = _api_get(path)
            t_obj = payload.get("tournament") or payload.get("uniqueTournament") or {}
            season = t_obj.get("currentSeason") or t_obj.get("season")
            if season and isinstance(season, dict) and season.get("id"):
                return [season], flag
        except Exception:
            continue
    return [], is_unique


def _flatten_standings(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(payload, dict):
        return rows

    standings_list = payload.get("standings") or []
    if not isinstance(standings_list, list):
        if isinstance(standings_list, dict):
            standings_list = [standings_list]
        else:
            standings_list = []

    if not standings_list and payload.get("rows"):
        standings_list = [{"name": "", "rows": payload.get("rows")}]

    for table in standings_list:
        if not isinstance(table, dict):
            continue
        group_name = table.get("name") or ""
        table_rows = table.get("rows") or table.get("table") or table.get("items") or []
        for raw in table_rows:
            if not isinstance(raw, dict):
                continue
            team = raw.get("team") or {}
            team_name = team.get("name") or team.get("teamName") or (team if isinstance(team, str) else "") or raw.get("teamName") or ""
            if not team_name:
                continue
            scores_for = raw.get("scoresFor")
            scores_against = raw.get("scoresAgainst")
            try:
                goal_difference = int(scores_for) - int(scores_against)
            except (TypeError, ValueError):
                goal_difference = raw.get("scoreDiffFormatted") or raw.get("scoreDiff") or "-"

            # Form / Last 5
            form_items: List[str] = []
            raw_form = raw.get("form") or raw.get("recentForm") or raw.get("last5") or []
            if isinstance(raw_form, str):
                for c in raw_form.upper():
                    if c in ("W", "V"):
                        form_items.append("W")
                    elif c in ("D", "E"):
                        form_items.append("D")
                    elif c in ("L", "D"):
                        form_items.append("L")
            elif isinstance(raw_form, list):
                for item in raw_form:
                    if isinstance(item, str):
                        val = item.strip().upper()
                        if val in ("W", "WIN", "V"):
                            form_items.append("W")
                        elif val in ("D", "DRAW", "E"):
                            form_items.append("D")
                        elif val in ("L", "LOSS"):
                            form_items.append("L")
                    elif isinstance(item, dict):
                        val = str(item.get("result") or item.get("value") or "").strip().upper()
                        if val in ("W", "WIN", "V"):
                            form_items.append("W")
                        elif val in ("D", "DRAW", "E"):
                            form_items.append("D")
                        elif val in ("L", "LOSS"):
                            form_items.append("L")

            promotion = raw.get("promotion") or {}
            rows.append({
                "group": group_name,
                "position": raw.get("position"),
                "team_id": team.get("id"),
                "team": team_name,
                "short_name": team.get("shortName") or team_name,
                "matches": raw.get("matches", raw.get("played", 0)),
                "wins": raw.get("wins", 0),
                "draws": raw.get("draws", 0),
                "losses": raw.get("losses", 0),
                "scores_for": scores_for if scores_for is not None else 0,
                "scores_against": scores_against if scores_against is not None else 0,
                "goal_difference": goal_difference,
                "points": raw.get("points", 0),
                "form": form_items,
                "promotion": promotion.get("text") or "",
                "promotion_id": promotion.get("id"),
            })
    return rows


def _extract_views_from_payload(payload: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    views: Dict[str, List[Dict[str, Any]]] = {}
    if not isinstance(payload, dict):
        return views

    standings_list = payload.get("standings") or []
    if isinstance(standings_list, list):
        for item in standings_list:
            if not isinstance(item, dict):
                continue
            table_type = str(item.get("type") or "total").lower()
            table_rows = _flatten_standings({"standings": [item]})
            if table_rows:
                if table_type in ("total", "home", "away"):
                    views[table_type] = table_rows
                elif "total" not in views:
                    views["total"] = table_rows

    if not views.get("total"):
        flat = _flatten_standings(payload)
        if flat:
            views["total"] = flat

    return views


def _public_tournament_page(tournament_id: Any) -> Dict[str, Any]:
    """Read the same server-rendered standings shown on the public tournament page.

    SofaScore's JSON API can reject a server while its public page remains available.
    The numeric ID is validated and the returned tournament ID is checked below.
    """
    if not str(tournament_id).isdigit():
        return {}
    url = f"https://www.sofascore.com/football/tournament/x/x/{int(tournament_id)}"
    response = _http_session().get(url, timeout=REQUEST_TIMEOUT_SECONDS, verify=VERIFY_SSL)
    response.raise_for_status()
    match = re.search(
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        response.text, re.S,
    )
    if not match:
        return {}
    data = json.loads(unescape(match.group(1)))
    props = (data.get("props") or {}).get("pageProps") or {}
    entity = props.get("uniqueTournament") or {}
    if str(entity.get("id")) != str(tournament_id):
        return {}
    return props


def _fetch_tournament_standings_views(tournament_id: Any, season_id: Any, is_unique: bool = True) -> Dict[str, List[Dict[str, Any]]]:
    views: Dict[str, List[Dict[str, Any]]] = {}
    prefixes = ["unique-tournament", "tournament"] if is_unique else ["tournament", "unique-tournament"]

    # Estrategia principal: endpoints individuales por vista (más fiables)
    for view in ("total", "home", "away"):
        for prefix in prefixes:
            paths = []
            if season_id:
                paths.append(f"/{prefix}/{tournament_id}/season/{season_id}/standings/{view}")
            paths.append(f"/{prefix}/{tournament_id}/standings/{view}")
            for path in paths:
                try:
                    payload = _api_get(path)
                    if payload and payload.get("standings"):
                        extracted = _extract_views_from_payload(payload)
                        if extracted:
                            for k, v in extracted.items():
                                if v and k not in views:
                                    views[k] = v
                            if views.get("total"):
                                break
                        else:
                            flat = _flatten_standings(payload)
                            if flat:
                                views[view] = flat
                                break
                except Exception:
                    continue
            if view in views:
                break

    return views


def _fetch_tournament_events(tournament_id: Any, season_id: Any, is_unique: bool = True, max_pages: int = 2) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    seen: set[str] = set()
    prefixes = ["unique-tournament", "tournament"] if is_unique else ["tournament", "unique-tournament"]
    for prefix in prefixes:
        for page in range(max_pages):
            try:
                payload = _api_get(f"/{prefix}/{tournament_id}/season/{season_id}/events/last/{page}")
            except Exception:
                break
            page_events = payload.get("events") or []
            for event in page_events:
                event_id = str(event.get("id") or "")
                if not event_id or event_id in seen:
                    continue
                seen.add(event_id)
                events.append(event)
            if not payload.get("hasNextPage") or not page_events:
                break
        if events:
            break
    return events


def _enrich_with_recent_form(views: Dict[str, List[Dict[str, Any]]], season_events: List[Dict[str, Any]]) -> None:
    if not season_events or not views.get("total"):
        return
    # Construir historial de partidos por equipo
    team_history: Dict[str, List[str]] = {}
    for event in sorted(season_events, key=lambda e: int(e.get("startTimestamp") or 0)):
        h_team = event.get("homeTeam") or {}
        a_team = event.get("awayTeam") or {}
        h_id, a_id = str(h_team.get("id") or ""), str(a_team.get("id") or "")
        h_score = (event.get("homeScore") or {}).get("current")
        a_score = (event.get("awayScore") or {}).get("current")
        if h_score is None or a_score is None:
            continue
        try:
            hs, as_ = int(h_score), int(a_score)
            if hs > as_:
                h_res, a_res = "W", "L"
            elif hs < as_:
                h_res, a_res = "L", "W"
            else:
                h_res, a_res = "D", "D"
            if h_id:
                team_history.setdefault(h_id, []).append(h_res)
            if a_id:
                team_history.setdefault(a_id, []).append(a_res)
        except (ValueError, TypeError):
            continue

    for v_rows in views.values():
        for row in v_rows:
            t_id = str(row.get("team_id") or "")
            if not row.get("form") and t_id in team_history:
                # Tomar los últimos 5 partidos
                row["form"] = team_history[t_id][-5:]


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

        score = team_score * 0.72 + date_score * 0.18 + league_score * 0.10
        ranked.append((score, event))

    ranked.sort(key=lambda item: item[0], reverse=True)
    if not ranked or ranked[0][0] < 0.72:
        return None
    return ranked[0][1]


def _event_score(event: Dict[str, Any], side: str) -> Optional[int]:
    score_obj = event.get(f"{side}Score") or {}
    for key in ("current", "normaltime", "display"):
        value = score_obj.get(key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return None


def _build_ou_views(
    events: Iterable[Dict[str, Any]],
    line: float = 2.5,
) -> Dict[str, List[Dict[str, Any]]]:
    stats: Dict[str, Dict[str, Any]] = {
        "total": {},
        "home": {},
        "away": {},
    }

    def init_row(team_id: Any, name: str) -> Dict[str, Any]:
        return {
            "team_id": team_id,
            "team": name,
            "matches": 0,
            "over": 0,
            "under": 0,
            "push": 0,
            "goals_for": 0,
            "goals_against": 0,
        }

    for event in events:
        status = (event.get("status") or {}).get("type")
        if status != "finished":
            continue
        home_team = event.get("homeTeam") or {}
        away_team = event.get("awayTeam") or {}
        home_id = home_team.get("id")
        away_id = away_team.get("id")
        home_name = home_team.get("name")
        away_name = away_team.get("name")
        if not home_id or not away_id or not home_name or not away_name:
            continue

        home_goals = _event_score(event, "home")
        away_goals = _event_score(event, "away")
        if home_goals is None or away_goals is None:
            continue

        total_goals = home_goals + away_goals

        def record(view_key: str, tid: Any, tname: str, gf: int, ga: int) -> None:
            view = stats[view_key]
            if tid not in view:
                view[tid] = init_row(tid, tname)
            entry = view[tid]
            entry["matches"] += 1
            entry["goals_for"] += gf
            entry["goals_against"] += ga
            if total_goals > line:
                entry["over"] += 1
            elif total_goals < line:
                entry["under"] += 1
            else:
                entry["push"] += 1

        record("total", home_id, home_name, home_goals, away_goals)
        record("total", away_id, away_name, away_goals, home_goals)
        record("home", home_id, home_name, home_goals, away_goals)
        record("away", away_id, away_name, away_goals, home_goals)

    rendered: Dict[str, List[Dict[str, Any]]] = {}
    for view_key, team_map in stats.items():
        rows: List[Dict[str, Any]] = []
        for entry in team_map.values():
            matches = entry["matches"]
            if not matches:
                continue
            effective = entry["over"] + entry["under"]
            over_pct = round((entry["over"] / effective) * 100, 1) if effective else 0.0
            avg_goals = round((entry["goals_for"] + entry["goals_against"]) / matches, 2)
            rows.append({
                "team_id": entry["team_id"],
                "team": entry["team"],
                "matches": matches,
                "over": entry["over"],
                "under": entry["under"],
                "push": entry["push"],
                "over_pct": over_pct,
                "avg_goals": avg_goals,
            })
        rows.sort(key=lambda r: (r["over_pct"], r["avg_goals"], r["over"]), reverse=True)
        for idx, row in enumerate(rows, start=1):
            row["position"] = idx
        rendered[view_key] = rows

    return rendered


def _ou_signal(
    views: Dict[str, List[Dict[str, Any]]],
    home_team_id: Any,
    away_team_id: Any,
) -> Dict[str, Any]:
    home_row = next((r for r in views.get("home", []) if str(r.get("team_id")) == str(home_team_id)), None)
    away_row = next((r for r in views.get("away", []) if str(r.get("team_id")) == str(away_team_id)), None)
    if not home_row and not away_row:
        home_row = next((r for r in views.get("total", []) if str(r.get("team_id")) == str(home_team_id)), None)
        away_row = next((r for r in views.get("total", []) if str(r.get("team_id")) == str(away_team_id)), None)

    home_pct = home_row.get("over_pct") if home_row else None
    away_pct = away_row.get("over_pct") if away_row else None
    home_matches = home_row.get("matches", 0) if home_row else 0
    away_matches = away_row.get("matches", 0) if away_row else 0
    sample = home_matches + away_matches

    if home_pct is not None and away_pct is not None:
        combined = round((home_pct + away_pct) / 2.0, 1)
    elif home_pct is not None:
        combined = round(float(home_pct), 1)
    elif away_pct is not None:
        combined = round(float(away_pct), 1)
    else:
        return {"label": "SIN MUESTRA SUFICIENTE", "tone": "neutral", "over_pct": None, "sample": sample}

    if combined >= 65.0 and sample >= 6:
        label = "TENDENCIA OVER CONTEXTUAL"
        tone = "over"
    elif combined <= 35.0 and sample >= 6:
        label = "TENDENCIA UNDER CONTEXTUAL"
        tone = "under"
    else:
        label = "PERFIL EQUILIBRADO"
        tone = "neutral"

    return {
        "label": label,
        "tone": tone,
        "over_pct": combined,
        "home_pct": home_pct,
        "away_pct": away_pct,
        "sample": sample,
    }


def _cache_key(
    home_name: str,
    away_name: str,
    league_name: str,
    match_date: Any,
    ou_line: float,
    season_id: Any = None,
) -> str:
    seed = f"{CACHE_VERSION}:{_normalize_name(home_name)}:{_normalize_name(away_name)}:{_normalize_name(league_name)}:{str(match_date or '')[:10]}:{ou_line:.1f}:{str(season_id or '')}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"sofascore_table:{digest}"


def _read_cache(key: str) -> Optional[Dict[str, Any]]:
    with _cache_lock:
        cached = _memory_cache.get(key)
        if cached and (time.time() - cached.get("cached_at_epoch", 0)) < CACHE_TTL_SECONDS:
            payload = cached.get("payload")
            if isinstance(payload, dict) and payload.get("available") is True and payload.get("views", {}).get("total"):
                return payload
    if sql_store is not None:
        try:
            persisted = sql_store.get_json_state(key, default=None)
            if isinstance(persisted, dict) and persisted.get("payload"):
                epoch = persisted.get("cached_at_epoch", 0)
                if (time.time() - epoch) < CACHE_TTL_SECONDS:
                    payload = persisted.get("payload")
                    if isinstance(payload, dict) and payload.get("available") is True and payload.get("views", {}).get("total"):
                        with _cache_lock:
                            _memory_cache[key] = persisted
                        return payload
        except Exception:
            pass
    return None


def _write_cache(key: str, payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict) or payload.get("available") is not True or not payload.get("views", {}).get("total"):
        return
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
    tournament_id: Any = None,
    season_id: Any = None,
) -> Dict[str, Any]:
    """Resuelve el torneo y devuelve tablas total/local/visitante, temporadas y rachas."""
    if not str(home_name or "").strip() or not str(away_name or "").strip():
        return _unavailable("missing_teams")

    try:
        ou_line = float(goal_line)
    except (TypeError, ValueError):
        ou_line = 2.5
    ou_line = min(8.5, max(0.5, ou_line))

    key = _cache_key(home_name, away_name, league_name, match_date, ou_line, season_id)
    cached = _read_cache(key)
    if cached:
        result = dict(cached)
        result["cached"] = True
        return result

    try:
        event = None
        unique_id = tournament_id
        unique: Dict[str, Any] = {}
        tournament: Dict[str, Any] = {}
        season: Dict[str, Any] = {}
        requested_home_id = None
        requested_away_id = None
        is_unique_tournament = True
        seasons_list: List[Dict[str, Any]] = []
        resolution_reason = "competition_not_resolved"

        # Si ya nos pasan un tournament_id directamente
        if unique_id:
            is_unique_tournament = True

        # Intento 1: Resolver por equipos y partido
        if not unique_id:
            anchor_team = _resolve_team(home_name) or _resolve_team(away_name)
            if anchor_team:
                events: List[Dict[str, Any]] = []
                for direction in ("next", "last"):
                    try:
                        payload = _api_get(f"/team/{anchor_team['id']}/events/{direction}/0")
                        events.extend(payload.get("events") or [])
                    except Exception:
                        continue
                event = _select_event(events, home_name, away_name, league_name, match_date)

            if event:
                tournament = event.get("tournament") or {}
                unique = tournament.get("uniqueTournament") or {}
                season = event.get("season") or {}
                unique_id = unique.get("id") or tournament.get("id")
                if not season_id:
                    season_id = season.get("id")
                is_unique_tournament = bool(unique.get("id"))
                event_home = event.get("homeTeam") or {}
                event_away = event.get("awayTeam") or {}
                requested_home_id = event_home.get("id")
                requested_away_id = event_away.get("id")
                if _similarity(home_name, event_away.get("name")) > _similarity(home_name, event_home.get("name")):
                    requested_home_id, requested_away_id = requested_away_id, requested_home_id

        # Intento 2: Resolver por nombre de liga / torneo
        if not unique_id and league_name:
            t_entity = _resolve_tournament(league_name)
            if t_entity and t_entity.get("id"):
                unique_id = t_entity["id"]
                unique = t_entity
                is_unique_tournament = t_entity.get("is_unique", True)
            elif t_entity:
                resolution_reason = t_entity.get("reason", resolution_reason)

        if not unique_id:
            return _unavailable(resolution_reason)

        # Obtener lista de temporadas disponibles
        seasons_list, detected_is_unique = _fetch_tournament_seasons(unique_id, is_unique_tournament)
        is_unique_tournament = detected_is_unique

        if not season_id and seasons_list:
            season = seasons_list[0]
            season_id = season.get("id")
        elif season_id and seasons_list:
            season = next((s for s in seasons_list if str(s.get("id")) == str(season_id)), seasons_list[0])

        # Cargar clasificaciones completas (total, home, away)
        views = _fetch_tournament_standings_views(unique_id, season_id, is_unique=is_unique_tournament)

        # The public page is server-rendered even when the JSON API challenges
        # this host. Accept it only for the same tournament and season.
        if not views.get("total") and is_unique_tournament:
            try:
                page = _public_tournament_page(unique_id)
                page_seasons = page.get("seasons") or []
                page_season = page_seasons[0] if page_seasons else {}
                if page and (not season_id or str(season_id) == str(page_season.get("id"))):
                    views = _extract_views_from_payload({"standings": page.get("standings") or []})
                    if views.get("total"):
                        seasons_list = page_seasons or seasons_list
                        season = page_season or season
                        season_id = page_season.get("id") or season_id
                        unique = page.get("uniqueTournament") or unique
            except Exception:
                pass

        if not views.get("total"):
            return _unavailable("standings_not_available")

        # Si aún no tenemos los IDs de los equipos, encontrarlos en las filas de la tabla
        table_rows = [row for row in views.get("total", []) if row.get("team_id")]
        table_ids = {str(row["team_id"]) for row in table_rows}
        if (not requested_home_id or not requested_away_id
                or str(requested_home_id) not in table_ids
                or str(requested_away_id) not in table_ids
                or str(requested_home_id) == str(requested_away_id)):
            def best_team_id(name: str, excluded: Any = None):
                ranked = sorted((
                    (max(_table_team_similarity(name, row.get("team")),
                         _table_team_similarity(name, row.get("short_name"))), row.get("team_id"))
                    for row in table_rows if str(row.get("team_id")) != str(excluded)
                ), key=lambda item: item[0], reverse=True)
                return ranked[0][1] if ranked and ranked[0][0] >= 0.75 else None

            requested_home_id = best_team_id(home_name)
            requested_away_id = best_team_id(away_name, requested_home_id)

        # Cargar eventos de la temporada para calcular O/U y enriquecer rachas
        try:
            season_events = _fetch_tournament_events(unique_id, season_id, is_unique=is_unique_tournament)
            _enrich_with_recent_form(views, season_events)

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
        except Exception:
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

        formatted_seasons = [
            {
                "id": s.get("id"),
                "name": s.get("name") or s.get("year") or str(s.get("id")),
                "year": s.get("year") or s.get("name") or str(s.get("id")),
            }
            for s in seasons_list
        ]

        result = {
            "available": True,
            "cached": False,
            "source": "SofaScore",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "tournament": unique.get("name") or tournament.get("name") or league_name,
            "season": season.get("name") or season.get("year") or "",
            "tournament_id": unique_id,
            "season_id": season_id,
            "seasons": formatted_seasons,
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
        return result
    except Exception:
        return _unavailable("provider_unavailable")


__all__ = ["get_league_table_context", "_lookup_league_alias", "_load_league_aliases"]
