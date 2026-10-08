"""Modulo para consultar y formatear el historial reciente de cualquier equipo rival.

Permite consultar los partidos de un equipo (como Finlandia, Bielorrusia, etc.)
a partir del match_id del partido donde jugo o del historico cacheado,
devolviendo sus partidos con condicion (Casa/Fuera), resultado, marcador,
lineas de handicap asiatico y evaluacion de cobertura AH.
"""

from __future__ import annotations

import gzip
import json
import logging
import re
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import data_manager
from . import estudio_scraper as es
from . import sql_store

LOGGER = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CACHE_PATH = DATA_DIR / "team_recent_history_cache.json"

_CACHE_LOCK = threading.Lock()
_CACHE: Dict[str, Any] = {}
_CACHE_LOADED = False


def _read_cache() -> Dict[str, Any]:
    global _CACHE, _CACHE_LOADED
    with _CACHE_LOCK:
        if _CACHE_LOADED:
            return _CACHE
        if CACHE_PATH.exists():
            try:
                with CACHE_PATH.open("r", encoding="utf-8") as fh:
                    _CACHE = json.load(fh)
            except Exception as e:
                LOGGER.warning("No se pudo leer team_recent_history_cache.json: %s", e)
                _CACHE = {}
        _CACHE_LOADED = True
        return _CACHE


def _write_cache(cache_data: Dict[str, Any]) -> None:
    with _CACHE_LOCK:
        try:
            tmp_path = CACHE_PATH.with_suffix(".tmp")
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(cache_data, fh, ensure_ascii=False, indent=2)
            tmp_path.replace(CACHE_PATH)
        except Exception as e:
            LOGGER.warning("No se pudo guardar team_recent_history_cache.json: %s", e)


def _same_team(left: str, right: str) -> bool:
    a = es._normalize_team_name(left or "")
    b = es._normalize_team_name(right or "")
    return bool(a and b and (a == b or a in b or b in a))


def _score_parts(value: str) -> Optional[Tuple[int, int]]:
    text = str(value or "").replace(":", "-").strip()
    if "?" in text:
        return None
    parts = text.split("-")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except (TypeError, ValueError):
        return None


def _calc_ah_evaluation(score_str: str, ah_str: str, is_home: bool) -> Dict[str, Any]:
    score = _score_parts(score_str)
    line = es.parse_ah_to_number_of(str(ah_str or ""))
    if not score or line is None:
        return {"code": "SIN_DATO", "label": "-", "covered": False, "net": None}

    # En NowGoal AH se expresa habitualmente desde la perspectiva del equipo local.
    # Si el equipo analizado es Home: net = (goles_home - goles_away) + line
    # Si el equipo analizado es Away: net = (goles_away - goles_home) - line
    team_goals = score[0] if is_home else score[1]
    rival_goals = score[1] if is_home else score[0]
    goal_diff = team_goals - rival_goals

    # team_line: linea que tiene el equipo
    team_line = line if is_home else -line
    net = goal_diff + team_line

    if net > 0.25 + 1e-9:
        code, label = "COVER", "Cubre"
    elif abs(net - 0.25) <= 1e-9:
        code, label = "HALF_WIN", "Medio Cubre"
    elif abs(net) <= 1e-9:
        code, label = "PUSH", "Nulo"
    elif abs(net + 0.25) <= 1e-9:
        code, label = "HALF_LOSS", "Medio Pierde"
    else:
        code, label = "NO_COVER", "No Cubre"

    return {"code": code, "label": label, "covered": net > 0, "net": net}


def _find_opponent_match_against_rival(
    opponent_name: str,
    primary_matches: List[Dict[str, Any]],
    secondary_matches: List[Dict[str, Any]],
    is_neutral: bool,
    rival_name: str,
) -> Optional[Dict[str, Any]]:
    """Busca en los partidos del oponente el enfrentamiento más representativo contra el rival en común.
    
    Si el partido es en estadio neutral, busca en ambos conjuntos sin restricción de condición de campo.
    Si no es neutral, prioriza la condición primaria (misma localía de hoy) y si no hay,
    recurre a la secundaria.
    """
    if not opponent_name or not rival_name:
        return None

    def _parse_candidate(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None
        m_home = str(item.get("home") or item.get("home_team") or "").strip()
        m_away = str(item.get("away") or item.get("away_team") or "").strip()
        is_h = _same_team(opponent_name, m_home)
        is_a = _same_team(opponent_name, m_away)
        if not (is_h or is_a):
            return None
        m_rival = m_away if is_h else m_home
        if not _same_team(rival_name, m_rival):
            return None

        m_date = str(item.get("date") or item.get("match_date") or item.get("date_txt") or "").strip()
        m_score = str(item.get("score") or item.get("score_raw") or item.get("result") or "").replace("-", ":").strip()
        m_id = "".join(filter(str.isdigit, str(item.get("matchIndex") or item.get("match_id") or item.get("id") or "")))
        m_ah = str(item.get("ahLine") or item.get("ahLine_raw") or item.get("ah_line") or item.get("ah") or "-").strip()

        score_parts = _score_parts(m_score)
        wdl = "D"
        res_letter = "E"
        score_disp = m_score
        if score_parts:
            tg = score_parts[0] if is_h else score_parts[1]
            rg = score_parts[1] if is_h else score_parts[0]
            score_disp = f"{tg}:{rg}"
            if tg > rg:
                wdl = "W"
                res_letter = "V"
            elif tg < rg:
                wdl = "L"
                res_letter = "D"
            else:
                wdl = "D"
                res_letter = "E"

        ah_eval = _calc_ah_evaluation(m_score, m_ah, is_h)

        return {
            "opponent_name": opponent_name,
            "match_id": m_id,
            "date": m_date,
            "home_team": m_home,
            "away_team": m_away,
            "is_home": is_h,
            "venue": "home" if is_h else "away",
            "score": m_score,
            "score_display": score_disp,
            "wdl": wdl,
            "res_letter": res_letter,
            "ah": m_ah,
            "ah_eval": ah_eval,
        }

    def _best_match(candidates_list: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        parsed = []
        for raw in candidates_list:
            c = _parse_candidate(raw)
            if c:
                parsed.append(c)
        if not parsed:
            return None

        def _d_key(m):
            d = m.get("date") or ""
            if "/" in d:
                p = d.split("/")
                if len(p) == 3:
                    return f"{p[2]}-{p[1]}-{p[0]}"
            return d

        parsed.sort(key=_d_key, reverse=True)
        return parsed[0]

    if is_neutral:
        combined = list(primary_matches or []) + list(secondary_matches or [])
        return _best_match(combined)
    else:
        res = _best_match(primary_matches or [])
        if res:
            return res
        return _best_match(secondary_matches or [])


def resolve_team_id(
    team_name: str,
    parent_match_id: Optional[str] = None,
    parent_data: Optional[Dict[str, Any]] = None,
    match_id: Optional[str] = None,
) -> Optional[str]:
    """Resuelve el ID de equipo de NowGoal a partir del contexto del partido padre o de su H2H soup."""
    team_clean = es._normalize_team_name(team_name)
    if not team_clean:
        return None

    # 1. De parent_data si se pasa o si podemos recuperarlo
    if not parent_data and (parent_match_id or match_id):
        lookup_id = "".join(filter(str.isdigit, str(parent_match_id or match_id or "")))
        if lookup_id:
            try:
                parent_data = data_manager.get_precacheo_match(lookup_id) or sql_store.get_match(lookup_id) or {}
            except Exception:
                parent_data = None

    if parent_data and isinstance(parent_data, dict):
        p_home = str(parent_data.get("home_name") or parent_data.get("home") or "")
        p_away = str(parent_data.get("away_name") or parent_data.get("away") or "")
        if _same_team(team_clean, p_home):
            tid = parent_data.get("home_id") or parent_data.get("home_team_id")
            if tid:
                return str(tid).strip()
        if _same_team(team_clean, p_away):
            tid = parent_data.get("away_id") or parent_data.get("away_team_id")
            if tid:
                return str(tid).strip()

        ctx = parent_data.get("pre_match_context", {})
        if isinstance(ctx, dict):
            curr = ctx.get("current", {})
            if isinstance(curr, dict):
                for k in ["home_matches_all", "away_matches_all", "home_matches", "away_matches"]:
                    for m in curr.get(k, []):
                        if not isinstance(m, dict):
                            continue
                        if _same_team(m.get("home", ""), team_clean) and m.get("home_id"):
                            return str(m.get("home_id")).strip()
                        if _same_team(m.get("away", ""), team_clean) and m.get("away_id"):
                            return str(m.get("away_id")).strip()

    # 2. De la pagina H2H de parent_match_id o match_id
    search_ids = []
    if parent_match_id:
        search_ids.append("".join(filter(str.isdigit, str(parent_match_id))))
    if match_id:
        search_ids.append("".join(filter(str.isdigit, str(match_id))))

    for mid in search_ids:
        if not mid:
            continue
        try:
            soup = es._load_main_match_soup(mid)
            if not soup:
                continue
            for a in soup.find_all("a"):
                t = a.get_text(strip=True)
                if t and _same_team(t, team_clean):
                    onclick = a.get("onclick", "") + " " + a.get("href", "")
                    m = re.search(r'(?:team|openFbTeam)\((\d+)\)', onclick)
                    if m:
                        return m.group(1).strip()
                    m2 = re.search(r'/team/summary/(\d+)', onclick)
                    if m2:
                        return m2.group(1).strip()
        except Exception as exc:
            LOGGER.debug("Error resolviendo team_id en soup de match %s: %s", mid, exc)

    return None


def fetch_live_team_matches(team_name: str, team_id: str) -> List[Dict[str, Any]]:
    """Consulta la API de NowGoal para obtener el calendario y partidos mas recientes de un equipo."""
    clean_id = "".join(filter(str.isdigit, str(team_id or "")))
    if not clean_id:
        return []

    url = f"https://football.nowgoal50.com/ajax/getteamschedulebypageno?id={clean_id}&pageNo=1&sclassId=0&init=1"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": f"https://football.nowgoal50.com/team/summary/{clean_id}",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    req = urllib.request.Request(url, headers=headers)
    match_list: List[Dict[str, Any]] = []
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            raw_bytes = resp.read()
            try:
                content = gzip.decompress(raw_bytes).decode("utf-8-sig", errors="ignore")
            except Exception:
                content = raw_bytes.decode("utf-8-sig", errors="ignore")
            data = json.loads(content)
            match_list = data.get("MatchList", []) or []
    except Exception as exc:
        LOGGER.warning("Error consultando getteamschedulebypageno para team_id %s: %s", clean_id, exc)
        return []

    if not match_list:
        return []

    # Localizar el partido mas reciente jugado o programado para extraer el H2H con cuotas AH
    recent_id = None
    for m in match_list:
        mid = str(m.get("ID") or "")
        state = str(m.get("State") or "")
        if mid and state in ("-1", "0"):
            recent_id = mid
            break

    h2h_matches: List[Dict[str, Any]] = []
    if recent_id:
        try:
            soup = es._load_main_match_soup(recent_id)
            if soup:
                odds_map = es.extract_vs_odds(soup)
                m1 = es.extract_recent_matches(soup, "table_v1", team_name, None, False, odds_map, limit=40, is_neutral_venue=True)
                m2 = es.extract_recent_matches(soup, "table_v2", team_name, None, False, odds_map, limit=40, is_neutral_venue=True)
                h2h_matches = m1 if len(m1) >= len(m2) else m2
        except Exception as exc:
            LOGGER.warning("Error extrayendo H2H reciente de match %s: %s", recent_id, exc)

    # Si hay partidos finalizados en match_list que no estan en h2h_matches, complementarlos
    existing_keys = set()
    for hm in h2h_matches:
        d = str(hm.get("date") or hm.get("match_date") or "")
        h = str(hm.get("home") or hm.get("home_team") or "")
        a = str(hm.get("away") or hm.get("away_team") or "")
        existing_keys.add(f"{d}:{h}:{a}")
        mid = str(hm.get("matchIndex") or hm.get("match_id") or "")
        if mid:
            existing_keys.add(mid)

    for m in match_list:
        state = str(m.get("State") or "")
        if state != "-1":
            continue
        mid = str(m.get("ID") or "")
        m_time = str(m.get("Time") or "").split(" ")[0].replace("/", "-")
        h_name = str(m.get("HomeName") or "")
        a_name = str(m.get("AwayName") or "")
        if mid in existing_keys or f"{m_time}:{h_name}:{a_name}" in existing_keys:
            continue

        h2h_matches.append({
            "matchIndex": mid,
            "date": m_time,
            "league_id": str(m.get("SclassID") or ""),
            "home": h_name,
            "away": a_name,
            "score": f"{m.get('HomeScore', '')}:{m.get('AwayScore', '')}",
            "ahLine": "-",
        })

    return h2h_matches


def get_team_recent_history(
    team_name: str,
    match_id: Optional[str] = None,
    parent_match_id: Optional[str] = None,
    league_id: Optional[str] = None,
    team_id: Optional[str] = None,
    force_refresh: bool = False,
    force_live: bool = False,
) -> Dict[str, Any]:
    clean_team = str(team_name or "").strip()
    if not clean_team:
        return {"status": "error", "error": "Falta el nombre del equipo"}

    clean_mid = "".join(filter(str.isdigit, str(match_id or "")))
    clean_parent = "".join(filter(str.isdigit, str(parent_match_id or "")))
    cache_key = f"v4:{clean_team.lower()}:{clean_mid}:{clean_parent}"

    cache = _read_cache()
    if not force_refresh and not force_live and cache_key in cache:
        cached_entry = cache[cache_key]
        if cached_entry and (time.time() - cached_entry.get("timestamp", 0)) < 86400:
            return {"status": "success", "cached": True, **cached_entry.get("data", {})}

    raw_matches: List[Dict[str, Any]] = []

    # Recuperar datos del partido padre para inferir contexto de oponente y estadios neutrales
    parent_data: Dict[str, Any] = {}
    opponent_name = ""
    opponent_is_home = False
    is_neutral = False
    opp_matches_primary: List[Dict[str, Any]] = []
    opp_matches_secondary: List[Dict[str, Any]] = []

    lookup_parent_id = clean_parent or clean_mid
    if lookup_parent_id:
        try:
            parent_data = data_manager.get_precacheo_match(lookup_parent_id) or sql_store.get_match(lookup_parent_id) or {}
            if parent_data:
                p_home = str(parent_data.get("home_name") or parent_data.get("home") or "").strip()
                p_away = str(parent_data.get("away_name") or parent_data.get("away") or "").strip()
                is_neutral = bool(parent_data.get("is_neutral_venue") or parent_data.get("neutral") or False)

                if _same_team(clean_team, p_away):
                    opponent_name = p_home
                    opponent_is_home = True
                    opp_matches_primary = parent_data.get("recent_home_matches_all") or parent_data.get("recent_home_matches") or []
                    opp_matches_secondary = parent_data.get("recent_away_matches_all") or parent_data.get("recent_away_matches") or []
                elif _same_team(clean_team, p_home):
                    opponent_name = p_away
                    opponent_is_home = False
                    opp_matches_primary = parent_data.get("recent_away_matches_all") or parent_data.get("recent_away_matches") or []
                    opp_matches_secondary = parent_data.get("recent_home_matches_all") or parent_data.get("recent_home_matches") or []
        except Exception as exc:
            LOGGER.warning("Error resolviendo oponente desde parent %s: %s", lookup_parent_id, exc)

    # 0. Si se solicita en vivo (force_live) o se dispone de team_id, consultar NowGoal directamente
    resolved_team_id = team_id or resolve_team_id(clean_team, parent_match_id=lookup_parent_id, parent_data=parent_data, match_id=clean_mid)
    is_live_loaded = False
    if force_live and resolved_team_id:
        try:
            live_matches = fetch_live_team_matches(clean_team, resolved_team_id)
            if live_matches:
                raw_matches = live_matches
                is_live_loaded = True
        except Exception as exc:
            LOGGER.warning("Error obteniendo partidos en vivo para %s (ID %s): %s", clean_team, resolved_team_id, exc)

    # 1. Intentar scrapear directamente desde el match_id indicado si difiere del equipo padre
    if not raw_matches and clean_mid:
        try:
            soup = es._load_main_match_soup(clean_mid)
            if soup:
                odds_map = es.extract_vs_odds(soup)
                m1 = es.extract_recent_matches(soup, "table_v1", clean_team, None, False, odds_map, limit=100, is_neutral_venue=True)
                m2 = es.extract_recent_matches(soup, "table_v2", clean_team, None, False, odds_map, limit=100, is_neutral_venue=True)
                raw_matches = m1 if len(m1) >= len(m2) else m2
        except Exception as exc:
            LOGGER.warning("Error extrayendo partidos para %s con match_id %s: %s", clean_team, clean_mid, exc)

    # 2. Si no hubo partidos directos, usar los partidos cacheados del parent_data
    if not raw_matches and parent_data:
        p_home = str(parent_data.get("home_name") or parent_data.get("home") or "").strip()
        p_away = str(parent_data.get("away_name") or parent_data.get("away") or "").strip()
        if _same_team(clean_team, p_home):
            raw_matches = parent_data.get("recent_home_matches_all") or parent_data.get("recent_home_matches") or []
        elif _same_team(clean_team, p_away):
            raw_matches = parent_data.get("recent_away_matches_all") or parent_data.get("recent_away_matches") or []

    # 3. Formatear y enriquecer los partidos
    formatted_matches = []
    seen_keys = set()

    for item in raw_matches:
        if not isinstance(item, dict):
            continue

        m_date = str(item.get("date") or item.get("match_date") or item.get("date_txt") or "").strip()
        m_home = str(item.get("home") or item.get("home_team") or "").strip()
        m_away = str(item.get("away") or item.get("away_team") or "").strip()
        m_score = str(item.get("score") or item.get("score_raw") or item.get("result") or "").replace("-", ":").strip()
        m_id = "".join(filter(str.isdigit, str(item.get("matchIndex") or item.get("match_id") or item.get("id") or "")))
        m_lid = str(item.get("league_id_hist") or item.get("league_id") or item.get("league") or "").strip()
        m_ah = str(item.get("ahLine") or item.get("ahLine_raw") or item.get("ah_line") or item.get("ah") or "-").strip()

        dedup_key = f"{m_date}:{m_home}:{m_away}"
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        is_home = _same_team(clean_team, m_home)
        is_away = _same_team(clean_team, m_away)
        if not (is_home or is_away):
            continue

        venue = "home" if is_home else "away"
        rival = m_away if is_home else m_home

        # Marcador y W/D/L
        score_parts = _score_parts(m_score)
        wdl = "D"
        res_letter = "E"
        if score_parts:
            tg = score_parts[0] if is_home else score_parts[1]
            rg = score_parts[1] if is_home else score_parts[0]
            if tg > rg:
                wdl = "W"
                res_letter = "V"
            elif tg < rg:
                wdl = "L"
                res_letter = "D"
            else:
                wdl = "D"
                res_letter = "E"

        # Evaluacion AH
        ah_eval = _calc_ah_evaluation(m_score, m_ah, is_home)

        # Comparativa indirecta si el oponente tambien enfrento a este mismo rival
        comparative_match = None
        has_common_rival = False
        if opponent_name:
            comparative_match = _find_opponent_match_against_rival(
                opponent_name=opponent_name,
                primary_matches=opp_matches_primary,
                secondary_matches=opp_matches_secondary,
                is_neutral=is_neutral,
                rival_name=rival,
            )
            has_common_rival = comparative_match is not None

        formatted_matches.append({
            "match_id": m_id,
            "date": m_date,
            "league_id": m_lid,
            "venue": venue,
            "is_home": is_home,
            "home_team": m_home,
            "away_team": m_away,
            "rival": rival,
            "score": m_score,
            "wdl": wdl,
            "res_letter": res_letter,
            "ah": m_ah,
            "ah_eval": ah_eval,
            "comparative_match": comparative_match,
            "has_common_rival": has_common_rival,
        })

    # Ordenar por fecha descendente
    def _date_sort_key(m):
        d = m.get("date") or ""
        if "/" in d:
            parts = d.split("/")
            if len(parts) == 3:
                return f"{parts[2]}-{parts[1]}-{parts[0]}"
        return d

    formatted_matches.sort(key=_date_sort_key, reverse=True)

    # Calcular estadisticas resumen
    def calc_stats(subset: List[Dict]):
        w = sum(1 for m in subset if m["wdl"] == "W")
        d = sum(1 for m in subset if m["wdl"] == "D")
        l = sum(1 for m in subset if m["wdl"] == "L")
        cover_w = sum(1 for m in subset if m["ah_eval"]["code"] in ("COVER", "HALF_WIN"))
        cover_l = sum(1 for m in subset if m["ah_eval"]["code"] in ("NO_COVER", "HALF_LOSS"))
        cover_push = sum(1 for m in subset if m["ah_eval"]["code"] == "PUSH")
        total = len(subset)
        cover_pct = round((cover_w / total * 100), 1) if total > 0 else 0
        return {
            "total": total,
            "wins": w,
            "draws": d,
            "losses": l,
            "cover_wins": cover_w,
            "cover_losses": cover_l,
            "cover_push": cover_push,
            "cover_pct": cover_pct,
        }

    home_subset = [m for m in formatted_matches if m["venue"] == "home"]
    away_subset = [m for m in formatted_matches if m["venue"] == "away"]

    common_matches = [m for m in formatted_matches if m.get("has_common_rival")]

    result_data = {
        "team_name": clean_team,
        "team_id": resolved_team_id,
        "is_live": is_live_loaded,
        "opponent_name": opponent_name,
        "opponent_is_home": opponent_is_home,
        "is_neutral_venue": is_neutral,
        "has_common_rivals": len(common_matches) > 0,
        "common_rivals_count": len(common_matches),
        "total_matches": len(formatted_matches),
        "matches": formatted_matches,
        "stats_all": calc_stats(formatted_matches),
        "stats_home": calc_stats(home_subset),
        "stats_away": calc_stats(away_subset),
    }

    # Guardar en cache
    cache[cache_key] = {
        "timestamp": time.time(),
        "data": result_data,
    }
    _write_cache(cache)

    return {"status": "success", "cached": False, **result_data}
