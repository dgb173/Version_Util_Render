"""Proveedor universal y resiliente de tablas de clasificacion de ligas desde NowGoal.

Permite consultar cualquier liga del mundo soportada por NowGoal descargando y parseando
en tiempo real (o desde cache local/SQLite) el archivo JSON de clasificacion de la competicion.
Genera tablas normalizadas para las vistas Total, Local y Visitante compatibles con el frontend.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LEAGUE_TABLES_DIR = DATA_DIR / "league_tables"
VERIFIED_TABLES_PATH = DATA_DIR / "nowgoal_verified_tables.json"

NOWGOAL_BASE_URL = os.getenv("NOWGOAL_FOOTBALL_URL", "https://football.nowgoal.net").rstrip("/")
CACHE_TTL_SECONDS = 43200  # 12 horas de cache para clasificaciones de liga
REQUEST_TIMEOUT = 8

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    ),
    "Referer": f"{NOWGOAL_BASE_URL}/",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

_session: Optional[requests.Session] = None
_session_lock = threading.Lock()
_memory_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_cache_lock = threading.Lock()
_league_name_to_id_cache: Optional[Dict[str, str]] = None


def _get_session() -> requests.Session:
    global _session
    with _session_lock:
        if _session is None:
            s = requests.Session()
            s.headers.update(HEADERS)
            _session = s
    return _session


def _normalize_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char)).lower()
    text = re.sub(r"\b(fc|cf|sc|afc|club|deportivo|futbol|football|futebol)\b", " ", text)
    text = re.sub(r"\b(w|women|woman|f|femenino|femenina|ladies)\b", " ", text)
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


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


def _get_sql_store():
    try:
        from . import sql_store
        return sql_store
    except Exception:
        return None


def _load_league_name_index() -> Dict[str, str]:
    """Crea un indice en memoria de nombres normalizados de liga -> league_id."""
    global _league_name_to_id_cache
    if _league_name_to_id_cache is not None:
        return _league_name_to_id_cache

    index: Dict[str, str] = {}
    # 1. Desde nowgoal_verified_tables.json
    if VERIFIED_TABLES_PATH.exists():
        try:
            verified = json.loads(VERIFIED_TABLES_PATH.read_text(encoding="utf-8"))
            for lid, val in verified.items():
                lname = val.get("league")
                if lname:
                    norm = _normalize_name(lname)
                    if norm:
                        index[norm] = str(lid)
        except Exception as e:
            LOGGER.debug("Error leyendo nowgoal_verified_tables.json: %s", e)

    # 2. Desde data/league_tables/*_latest.json
    if LEAGUE_TABLES_DIR.exists():
        try:
            for f in LEAGUE_TABLES_DIR.glob("*_latest.json"):
                lid_match = re.match(r"^(\d+)_latest\.json$", f.name)
                if not lid_match:
                    continue
                lid = lid_match.group(1)
                try:
                    raw = json.loads(f.read_text(encoding="utf-8"))
                    payload_dict = raw[1] if isinstance(raw, list) and len(raw) > 1 and isinstance(raw[1], dict) else (raw if isinstance(raw, dict) else {})
                    info = payload_dict.get("LeagueInfo")
                    if isinstance(info, list) and len(info) > 1 and info[1]:
                        norm = _normalize_name(info[1])
                        if norm:
                            index[norm] = str(lid)
                except Exception:
                    continue
        except Exception as e:
            LOGGER.debug("Error indexando league_tables: %s", e)

    _league_name_to_id_cache = index
    return index


def resolve_league_id(league_name: str = "", nowgoal_league_id: Any = None, match_id: Any = None) -> Optional[str]:
    """Resuelve el ID numerico de la liga NowGoal."""
    if nowgoal_league_id is not None:
        cleaned = str(nowgoal_league_id).strip()
        if cleaned.isdigit() and int(cleaned) > 0:
            return cleaned

    # Si tenemos match_id, intentar recuperarlo de SQLite
    if match_id:
        store = _get_sql_store()
        if store:
            try:
                cached = store.get_match(str(match_id))
                if isinstance(cached, dict):
                    lid = cached.get("league_id")
                    if lid and str(lid).strip().isdigit():
                        return str(lid).strip()
                    # Buscar en pre_match_context
                    pre = cached.get("pre_match_context") or {}
                    if isinstance(pre, dict):
                        plid = pre.get("league_id") or (pre.get("current") or {}).get("league_id")
                        if plid and str(plid).strip().isdigit():
                            return str(plid).strip()
            except Exception:
                pass

    # Si tenemos el nombre de la liga, buscar en el indice
    if league_name:
        index = _load_league_name_index()
        norm_target = _normalize_name(league_name)
        if norm_target in index:
            return index[norm_target]

        # Busqueda por maxima similitud
        best_id, best_score = None, 0.0
        for stored_name, stored_id in index.items():
            score = _similarity(norm_target, stored_name)
            if score > best_score:
                best_score = score
                best_id = stored_id
        if best_score >= 0.82:
            return best_id

    return None


def _parse_nowgoal_score_list(
    score_container: Any,
    sub_names: Dict[str, str],
    teams: Dict[str, Dict[str, str]],
    color_container: Any = None,
    is_total: bool = False,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    # Normalizar score_container a dict de {sub_key: [filas]}
    groups: Dict[str, List[Any]] = {}
    if isinstance(score_container, dict):
        groups = score_container
    elif isinstance(score_container, list):
        groups = {"sub_0": score_container}

    for sub_key, sub_rows in groups.items():
        if not isinstance(sub_rows, list):
            continue

        group_name = sub_names.get(sub_key, "Clasificación")
        # Color list puede ser dict o list
        if isinstance(color_container, dict):
            color_entries = color_container.get(sub_key) or []
        elif isinstance(color_container, list):
            color_entries = color_container
        else:
            color_entries = []

        for r in sub_rows:
            if not isinstance(r, list) or len(r) < 9:
                continue

            if is_total and len(r) >= 17:
                # Formato Total: [color_idx, rank, team_id, ?, matches, wins, draws, losses, scores_for, scores_against, diff, ... points at idx 16]
                color_idx = int(r[0]) if str(r[0]).isdigit() else -1
                pos = int(r[1]) if str(r[1]).isdigit() else 0
                tid = r[2]
                pj = int(r[4]) if str(r[4]).isdigit() else 0
                v = int(r[5]) if str(r[5]).isdigit() else 0
                e = int(r[6]) if str(r[6]).isdigit() else 0
                d = int(r[7]) if str(r[7]).isdigit() else 0
                gf = int(r[8]) if str(r[8]).isdigit() else 0
                gc = int(r[9]) if str(r[9]).isdigit() else 0
                dg = int(r[10]) if str(r[10]).lstrip("-").isdigit() else (gf - gc)
                pts = int(r[16]) if str(r[16]).isdigit() else (v * 3 + e)

                promotion = ""
                if isinstance(color_entries, list) and 0 <= color_idx < len(color_entries):
                    item = color_entries[color_idx]
                    if isinstance(item, list) and len(item) > 1:
                        promotion = str(item[1])
            else:
                # Formato Home/Away: [rank, team_id, matches, wins, draws, losses, scores_for, scores_against, diff, ... points at idx 14]
                pos = int(r[0]) if str(r[0]).isdigit() else 0
                tid = r[1]
                pj = int(r[2]) if str(r[2]).isdigit() else 0
                v = int(r[3]) if str(r[3]).isdigit() else 0
                e = int(r[4]) if str(r[4]).isdigit() else 0
                d = int(r[5]) if str(r[5]).isdigit() else 0
                gf = int(r[6]) if str(r[6]).isdigit() else 0
                gc = int(r[7]) if str(r[7]).isdigit() else 0
                dg = int(r[8]) if str(r[8]).lstrip("-").isdigit() else (gf - gc)
                pts = int(r[14]) if len(r) > 14 and str(r[14]).isdigit() else (v * 3 + e)
                promotion = ""

            t_info = teams.get(str(tid), {})
            t_name = t_info.get("name") or str(tid)
            s_name = t_info.get("short_name") or t_name

            rows.append({
                "position": pos,
                "team_id": tid,
                "team": t_name,
                "short_name": s_name,
                "group": group_name,
                "matches": pj,
                "wins": v,
                "draws": e,
                "losses": d,
                "scores_for": gf,
                "scores_against": gc,
                "goal_difference": dg,
                "points": pts,
                "promotion": promotion,
            })

    return sorted(rows, key=lambda row: (
        str(row.get("group") or ""),
        int(row.get("position") or 9999),
        str(row.get("team") or ""),
    ))


def _parse_nowgoal_ou_pan(
    big_small_dict: Dict[str, Any],
    teams: Dict[str, Dict[str, str]],
    home_team_id: Any = None,
    away_team_id: Any = None,
    target_line: float = 2.5,
) -> Dict[str, Any]:
    """Parsea BigSmallPan de NowGoal para las vistas O/U y la señal de tendencia."""
    ou_views: Dict[str, List[Dict[str, Any]]] = {}
    mapping = {
        "total": big_small_dict.get("TotalBs") or [],
        "home": big_small_dict.get("HomeBs") or [],
        "away": big_small_dict.get("GuestBs") or [],
    }

    for view_key, raw_rows in mapping.items():
        parsed_rows: List[Dict[str, Any]] = []
        if isinstance(raw_rows, list):
            for r in raw_rows:
                # Formato: [pos, team_id, matches, over, push, under, over_pct, push_pct, under_pct]
                if not isinstance(r, list) or len(r) < 7:
                    continue
                pos = int(r[0]) if str(r[0]).isdigit() else 0
                tid = r[1]
                matches = int(r[2]) if str(r[2]).isdigit() else 0
                over = int(r[3]) if str(r[3]).isdigit() else 0
                push = int(r[4]) if str(r[4]).isdigit() else 0
                under = int(r[5]) if str(r[5]).isdigit() else 0
                try:
                    over_pct = float(r[6])
                except (ValueError, TypeError):
                    over_pct = round((over / matches) * 100, 1) if matches else 0.0

                t_info = teams.get(str(tid), {})
                parsed_rows.append({
                    "position": pos,
                    "team_id": tid,
                    "team": t_info.get("name") or str(tid),
                    "short_name": t_info.get("short_name") or str(tid),
                    "matches": matches,
                    "over": over,
                    "under": under,
                    "push": push,
                    "over_pct": over_pct,
                    "avg_goals": 0.0,
                })
        if parsed_rows:
            ou_views[view_key] = sorted(parsed_rows, key=lambda x: -x["over_pct"])

    # Calcular señal de tendencia
    signal = {"label": "SIN DATOS", "tone": "neutral", "over_pct": None, "sample": 0}
    home_row = next((r for r in ou_views.get("home", []) if str(r.get("team_id")) == str(home_team_id)), None)
    if not home_row:
        home_row = next((r for r in ou_views.get("total", []) if str(r.get("team_id")) == str(home_team_id)), None)
    away_row = next((r for r in ou_views.get("away", []) if str(r.get("team_id")) == str(away_team_id)), None)
    if not away_row:
        away_row = next((r for r in ou_views.get("total", []) if str(r.get("team_id")) == str(away_team_id)), None)

    samples = [r for r in (home_row, away_row) if r and r.get("matches")]
    if samples:
        total_m = sum(int(r["matches"]) for r in samples)
        weighted = sum(float(r["over_pct"]) * int(r["matches"]) for r in samples)
        pct = round(weighted / total_m, 1) if total_m else 0.0
        if pct >= 62:
            label, tone = "TENDENCIA OVER", "over"
        elif pct <= 38:
            label, tone = "TENDENCIA UNDER", "under"
        else:
            label, tone = "TENDENCIA NEUTRA", "neutral"
        signal = {"label": label, "tone": tone, "over_pct": pct, "sample": total_m}

    table_line = f"{target_line:g}"
    return {
        "line": target_line,
        "views": ou_views,
        "matches_analyzed": len(ou_views.get("total", [])),
        "signal": signal,
        "tables": {table_line: {"line": target_line, "views": ou_views, "signal": signal}},
    }


def _fetch_nowgoal_league_json(league_id: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Descubre la temporada y descarga el archivo JSON de clasificacion de NowGoal."""
    session = _get_session()
    season = ""
    data_path = ""

    # 1. Intentar descubrir ruta exacta desde /league/{league_id}
    try:
        url_league = f"{NOWGOAL_BASE_URL}/league/{league_id}"
        resp = session.get(url_league, timeout=REQUEST_TIMEOUT, verify=False)
        if resp.status_code == 200 and len(resp.text) > 500:
            sn_m = re.search(r'const\s+_season\s*=\s*"([^"]+)"', resp.text)
            dp_m = re.search(r'const\s+_dataPath\s*=\s*"([^"]+)"', resp.text)
            if sn_m:
                season = sn_m.group(1).strip()
            if dp_m:
                data_path = dp_m.group(1).strip()
    except Exception as e:
        LOGGER.debug("Error descubriendo liga %s: %s", league_id, e)

    # 2. Si se encontro data_path, descargarlo
    if data_path:
        try:
            full_url = f"{NOWGOAL_BASE_URL}{data_path}"
            r = session.get(full_url, timeout=REQUEST_TIMEOUT, verify=False)
            raw = r.text.lstrip("\ufeff")
            if r.status_code == 200 and raw.startswith("{"):
                js = json.loads(raw)
                return season or "Actual", js
        except Exception as e:
            LOGGER.debug("Error descargando data_path %s: %s", data_path, e)

    # 3. Probar temporadas directas comunes (anuales y bianuales)
    current_year = datetime.now().year
    seasons_to_try = [
        f"{current_year}-{current_year + 1}",
        f"{current_year}",
        f"{current_year - 1}-{current_year}",
        f"{current_year - 1}",
    ]
    for s_candidate in seasons_to_try:
        try:
            try_url = f"{NOWGOAL_BASE_URL}/jsData/matchResult/json/{s_candidate}/s{league_id}_en.json"
            r = session.get(try_url, timeout=REQUEST_TIMEOUT, verify=False)
            raw = r.text.lstrip("\ufeff")
            if r.status_code == 200 and raw.startswith("{"):
                js = json.loads(raw)
                return s_candidate, js
        except Exception:
            continue

    return None


def _load_local_league_table(league_id: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Carga una copia local de la tabla si existe en el disco."""
    local_f = LEAGUE_TABLES_DIR / f"{league_id}_latest.json"
    if local_f.exists():
        try:
            raw = json.loads(local_f.read_text(encoding="utf-8"))
            if isinstance(raw, list) and len(raw) > 1 and isinstance(raw[1], dict):
                season = str(raw[1].get("LeagueInfo", ["", "", ""])[2]) if len(raw[1].get("LeagueInfo", [])) > 2 else "Local"
                return season, raw[1]
            elif isinstance(raw, dict):
                return "Local", raw
        except Exception:
            pass

    if VERIFIED_TABLES_PATH.exists():
        try:
            v_all = json.loads(VERIFIED_TABLES_PATH.read_text(encoding="utf-8"))
            table = v_all.get(str(league_id))
            if table and table.get("views"):
                return table.get("season") or "Verificada", {"_pre_normalized": table}
        except Exception:
            pass

    return None


def _resolve_team_id_in_rows(rows: List[Dict[str, Any]], name: str, exclude_id: Any = None) -> Optional[Any]:
    if not name or not rows:
        return None

    def score(row: Dict[str, Any]) -> float:
        if exclude_id is not None and str(row.get("team_id")) == str(exclude_id):
            return 0.0
        return max(
            _similarity(name, row.get("team") or ""),
            _similarity(name, row.get("short_name") or ""),
        )

    ranked = sorted(((score(r), r.get("team_id")) for r in rows if r.get("team_id") is not None),
                    key=lambda item: item[0], reverse=True)
    return ranked[0][1] if ranked and ranked[0][0] >= 0.70 else None


def get_league_table_context(
    home_name: str,
    away_name: str,
    league_name: str = "",
    nowgoal_league_id: Any = None,
    match_id: Any = None,
    goal_line: Any = 2.5,
) -> Dict[str, Any]:
    """Punto de entrada principal para obtener la tabla de clasificacion desde NowGoal."""
    league_id = resolve_league_id(league_name, nowgoal_league_id, match_id)
    cache_key = f"nowgoal_v2_table_{league_id or league_name}_{_normalize_name(home_name)}_{_normalize_name(away_name)}"

    try:
        ou_line = float(goal_line)
    except (TypeError, ValueError):
        ou_line = 2.5
    ou_line = min(8.5, max(0.5, ou_line))

    # 1. Cache en memoria
    now = time.time()
    with _cache_lock:
        if cache_key in _memory_cache:
            entry_time, cached_payload = _memory_cache[cache_key]
            if now - entry_time < CACHE_TTL_SECONDS:
                return dict(cached_payload, cached=True)

    # 2. Cache en SQLite kv_store
    store = _get_sql_store()
    if store:
        try:
            cached_json = store.get_json_state(cache_key)
            if isinstance(cached_json, dict) and cached_json.get("available"):
                with _cache_lock:
                    _memory_cache[cache_key] = (now, cached_json)
                return dict(cached_json, cached=True)
        except Exception:
            pass

    # Si no se pudo resolver el ID de liga, no podemos consultar la base de datos de NowGoal
    if not league_id:
        return {"available": False, "reason": "league_not_linked", "views": {}}

    season = ""
    payload_raw: Optional[Dict[str, Any]] = None

    # 3. Intentar descarga en vivo de NowGoal
    live_res = _fetch_nowgoal_league_json(league_id)
    if live_res:
        season, payload_raw = live_res

    # 4. Si la descarga en vivo falla o la liga no respondio, usar copia local si existe
    if not payload_raw:
        local_res = _load_local_league_table(league_id)
        if local_res:
            season, payload_raw = local_res

    if not payload_raw:
        return {"available": False, "reason": "standings_not_available", "views": {}}

    # Caso en que ya estaba pre-normalizado (ej. de nowgoal_verified_tables.json)
    if "_pre_normalized" in payload_raw:
        t = payload_raw["_pre_normalized"]
        views = t.get("views") or {}
        rows = views.get("total") or []
        home_id = _resolve_team_id_in_rows(rows, home_name)
        away_id = _resolve_team_id_in_rows(rows, away_name, exclude_id=home_id)
        result = {
            "available": True,
            "cached": True,
            "source": "NowGoal",
            "fetched_at": t.get("captured_at") or datetime.now(timezone.utc).isoformat(),
            "tournament": t.get("league") or league_name or f"Liga {league_id}",
            "season": t.get("season") or season,
            "tournament_id": league_id,
            "home_team_id": home_id,
            "away_team_id": away_id,
            "home_name": home_name,
            "away_name": away_name,
            "views": views,
            "ou": {"views": {}, "matches_analyzed": 0, "tables": {}},
            "external_links": {"nowgoal": f"{NOWGOAL_BASE_URL}/league/{league_id}"},
        }
        with _cache_lock:
            _memory_cache[cache_key] = (now, result)
        if store:
            try:
                store.set_json_state(cache_key, result)
            except Exception:
                pass
        return result

    # 5. Parsear payload_raw descargado de NowGoal
    league_info = payload_raw.get("LeagueInfo") or []
    official_name = league_info[1] if isinstance(league_info, list) and len(league_info) > 1 and league_info[1] else (league_name or f"Liga {league_id}")

    teams: Dict[str, Dict[str, str]] = {}
    for t in payload_raw.get("TeamInfo") or []:
        if isinstance(t, list) and len(t) > 1:
            tid = t[0]
            tname = str(t[1])
            sname = str(t[2]) if len(t) > 2 and t[2] else tname
            teams[str(tid)] = {"name": tname, "short_name": sname}

    sub_names: Dict[str, str] = {}
    for sub in payload_raw.get("SubLeagueInfo") or []:
        if isinstance(sub, list) and len(sub) > 1:
            sub_names[f"sub_{sub[0]}"] = str(sub[1])

    color_container = payload_raw.get("ColorList") or []

    # Compatibilidad con TotalScore / TotalScoreList, HomeScore / HomeScoreList, GuestScore / GuestScoreList
    total_data = payload_raw.get("TotalScore") if payload_raw.get("TotalScore") is not None else payload_raw.get("TotalScoreList")
    home_data = payload_raw.get("HomeScore") if payload_raw.get("HomeScore") is not None else payload_raw.get("HomeScoreList")
    away_data = payload_raw.get("GuestScore") if payload_raw.get("GuestScore") is not None else payload_raw.get("GuestScoreList")

    views = {
        "total": _parse_nowgoal_score_list(total_data, sub_names, teams, color_container, is_total=True),
        "home": _parse_nowgoal_score_list(home_data, sub_names, teams, color_container, is_total=False),
        "away": _parse_nowgoal_score_list(away_data, sub_names, teams, color_container, is_total=False),
    }

    if not views["total"]:
        return {"available": False, "reason": "standings_not_available", "views": {}}

    total_rows = views["total"]
    home_id = _resolve_team_id_in_rows(total_rows, home_name)
    away_id = _resolve_team_id_in_rows(total_rows, away_name, exclude_id=home_id)

    # Parsea BigSmallPan para analisis O/U
    ou_data = _parse_nowgoal_ou_pan(
        payload_raw.get("BigSmallPan") or {},
        teams,
        home_team_id=home_id,
        away_team_id=away_id,
        target_line=ou_line,
    )

    result = {
        "available": True,
        "cached": False,
        "source": "NowGoal",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "tournament": official_name,
        "season": season,
        "tournament_id": league_id,
        "home_team_id": home_id,
        "away_team_id": away_id,
        "home_name": home_name,
        "away_name": away_name,
        "views": views,
        "ou": ou_data,
        "external_links": {"nowgoal": f"{NOWGOAL_BASE_URL}/league/{league_id}"},
    }

    # Guardar en caches
    with _cache_lock:
        _memory_cache[cache_key] = (now, result)
    if store:
        try:
            store.set_json_state(cache_key, result)
        except Exception:
            pass

    return result


def build_h2h_standings_fallback(
    home_name: str,
    away_name: str,
    league_name: str = "",
    home_standings: Any = None,
    away_standings: Any = None,
    match_id: Any = None,
) -> Dict[str, Any]:
    """Genera una tabla normalizada basada en los datos de clasificación del enfrentamiento."""
    store = _get_sql_store()
    cached_match = (store.get_match(str(match_id)) or {}) if (store and match_id) else {}

    if not isinstance(home_standings, dict):
        home_standings = cached_match.get("home_standings")
    if not isinstance(away_standings, dict):
        away_standings = cached_match.get("away_standings")

    total_rows: List[Dict[str, Any]] = []
    home_rows: List[Dict[str, Any]] = []
    away_rows: List[Dict[str, Any]] = []

    pairs = [
        (home_name, home_standings, "home"),
        (away_name, away_standings, "away"),
    ]

    for default_pos, (team_name, raw, side) in enumerate(pairs, start=1):
        if not isinstance(raw, dict):
            continue

        def _num(key: str) -> int:
            try:
                val = raw.get(key)
                return int(val) if val is not None and str(val).isdigit() else 0
            except (TypeError, ValueError):
                return 0

        pos_raw = raw.get("ranking")
        pos = int(pos_raw) if pos_raw is not None and str(pos_raw).isdigit() else default_pos

        gf = _num("total_gf")
        gc = _num("total_gc")
        v = _num("total_v")
        e = _num("total_e")
        d = _num("total_d")
        pj = _num("total_pj")
        pts = v * 3 + e

        # Solo incluir si hay partidos jugados o ranking o goles
        if pj > 0 or gf > 0 or gc > 0 or (pos_raw and str(pos_raw).isdigit()):
            total_rows.append({
                "group": league_name or "Clasificación",
                "position": pos,
                "team_id": 1 if side == "home" else 2,
                "team": team_name,
                "short_name": team_name,
                "matches": pj,
                "wins": v,
                "draws": e,
                "losses": d,
                "scores_for": gf,
                "scores_against": gc,
                "goal_difference": gf - gc,
                "points": pts,
                "promotion": "",
            })

            # Estadisticas especificas como local o visitante
            sp_pj = _num("specific_pj")
            if sp_pj > 0:
                sp_gf = _num("specific_gf")
                sp_gc = _num("specific_gc")
                sp_v = _num("specific_v")
                sp_e = _num("specific_e")
                sp_d = _num("specific_d")
                row_spec = {
                    "group": league_name or "Clasificación",
                    "position": pos,
                    "team_id": 1 if side == "home" else 2,
                    "team": team_name,
                    "short_name": team_name,
                    "matches": sp_pj,
                    "wins": sp_v,
                    "draws": sp_e,
                    "losses": sp_d,
                    "scores_for": sp_gf,
                    "scores_against": sp_gc,
                    "goal_difference": sp_gf - sp_gc,
                    "points": sp_v * 3 + sp_e,
                    "promotion": "",
                }
                if side == "home":
                    home_rows.append(row_spec)
                else:
                    away_rows.append(row_spec)

    total_rows.sort(key=lambda row: (row["position"], -row["points"], row["team"]))

    if not total_rows:
        return {"available": False, "reason": "standings_not_available", "views": {}}

    views: Dict[str, List[Dict[str, Any]]] = {"total": total_rows}
    if home_rows:
        views["home"] = home_rows
    if away_rows:
        views["away"] = away_rows

    return {
        "available": True,
        "cached": True,
        "partial": True,
        "source": "NowGoal",
        "tournament": league_name or "Clasificación",
        "season": "",
        "home_team_id": 1,
        "away_team_id": 2,
        "home_name": home_name,
        "away_name": away_name,
        "views": views,
        "ou": {},
        "external_links": {},
    }

