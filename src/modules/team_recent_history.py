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
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
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

_OU_KV_KEY = "ou_lines_by_match_id_v1"
_OU_CACHE_LOCK = threading.Lock()
_OU_CACHE: Dict[str, str] = {}
_OU_CACHE_LOADED = False
_OU_MISS_TS: Dict[str, float] = {}
_OU_MISS_TTL_SECONDS = 600


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


def _parse_ou_num(ou_str: str) -> Optional[float]:
    raw = str(ou_str or "").strip().replace(",", ".")
    if not raw or raw in ("-", "N/A", "?"):
        return None
    if "/" in raw:
        parts = [p.strip() for p in raw.split("/") if p.strip()]
        try:
            nums = [float(p) for p in parts]
            return sum(nums) / len(nums) if nums else None
        except ValueError:
            return None
    try:
        return float(raw)
    except ValueError:
        return None


def _normalize_ou_display(raw_ou: Any) -> Optional[str]:
    """Valida y normaliza una linea O/U para visualizacion rapida."""
    text = str(raw_ou or "").strip().replace(",", ".")
    if not text or text in ("-", "?", "N/A", "null", "None"):
        return None
    if _parse_ou_num(text) is None:
        return None
    return text


def _eval_ou_result(score_str: str, ou_str: str) -> str:
    """Evalua OVER/UNDER/PUSH para un marcador y una linea O/U."""
    score_parts = _score_parts(score_str)
    ou_num = _parse_ou_num(ou_str)
    if not score_parts or ou_num is None:
        return "SIN_DATO"
    total_goals = score_parts[0] + score_parts[1]
    diff = total_goals - ou_num
    if diff > 1e-9:
        return "OVER"
    if diff < -1e-9:
        return "UNDER"
    return "PUSH"


def _load_ou_cache() -> Dict[str, str]:
    """Carga en memoria el indice persistente match_id -> linea O/U desde SQLite kv_store."""
    global _OU_CACHE_LOADED, _OU_CACHE
    with _OU_CACHE_LOCK:
        if not _OU_CACHE_LOADED:
            try:
                stored = sql_store.get_json_state(_OU_KV_KEY, default={})
                if isinstance(stored, dict):
                    _OU_CACHE = {
                        str(k).strip(): vn
                        for k, v in stored.items()
                        for vn in [_normalize_ou_display(v)]
                        if str(k).strip().isdigit() and vn is not None
                    }
            except Exception as exc:
                LOGGER.warning("No se pudo cargar cache O/U desde kv_store: %s", exc)
            _OU_CACHE_LOADED = True
        return dict(_OU_CACHE)


def _save_ou_cache(new_entries: Dict[str, str]) -> None:
    """Persiste nuevas lineas O/U resueltas en SQLite kv_store."""
    if not new_entries:
        return
    with _OU_CACHE_LOCK:
        if not _OU_CACHE_LOADED:
            try:
                stored = sql_store.get_json_state(_OU_KV_KEY, default={})
                if isinstance(stored, dict):
                    for k, v in stored.items():
                        ks = str(k).strip()
                        vn = _normalize_ou_display(v)
                        if ks.isdigit() and vn is not None and ks not in _OU_CACHE:
                            _OU_CACHE[ks] = vn
            except Exception:
                pass
            _OU_CACHE_LOADED = True
        changed = False
        for k, v in new_entries.items():
            ks = "".join(ch for ch in str(k or "") if ch.isdigit())
            vn = _normalize_ou_display(v)
            if ks and vn is not None and _OU_CACHE.get(ks) != vn:
                _OU_CACHE[ks] = vn
                _OU_MISS_TS.pop(ks, None)
                changed = True
        if changed:
            try:
                sql_store.set_json_state(_OU_KV_KEY, _OU_CACHE)
            except Exception as exc:
                LOGGER.warning("No se pudo guardar cache O/U en kv_store: %s", exc)


def _extract_ou_from_match_payload(payload: Any) -> Optional[str]:
    """Extrae la linea O/U desde un payload de partido almacenado en SQLite."""
    if not isinstance(payload, dict):
        return None
    odds = payload.get("main_match_odds")
    if isinstance(odds, dict):
        for key in ("goals_linea", "goals_linea_raw", "ou", "ou_line", "ouLine"):
            norm = _normalize_ou_display(odds.get(key))
            if norm is not None:
                return norm
    for key in ("goals_linea", "goals_linea_raw", "ou", "ou_line", "ouLine"):
        norm = _normalize_ou_display(payload.get(key))
        if norm is not None:
            return norm
    return None


def _lookup_ou_lines_fast(match_ids: List[str]) -> Dict[str, str]:
    """Resuelve lineas O/U en local sin red: Capa 1 kv_store + Capa 2 tabla SQLite matches."""
    clean_ids: List[str] = []
    seen = set()
    for mid in match_ids or []:
        cid = "".join(ch for ch in str(mid or "") if ch.isdigit())
        if cid and cid not in seen:
            seen.add(cid)
            clean_ids.append(cid)
    if not clean_ids:
        return {}

    ou_cache = _load_ou_cache()
    result: Dict[str, str] = {}
    missing_ids: List[str] = []
    for cid in clean_ids:
        cached_val = _normalize_ou_display(ou_cache.get(cid))
        if cached_val is not None:
            result[cid] = cached_val
        else:
            missing_ids.append(cid)

    if missing_ids:
        newly_found: Dict[str, str] = {}
        try:
            rows = sql_store.fetch_matches_by_ids(missing_ids)
        except Exception as exc:
            LOGGER.warning("Error consultando SQLite para O/U rapido: %s", exc)
            rows = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            rmid = "".join(
                ch for ch in str(row.get("match_id") or row.get("id") or "") if ch.isdigit()
            )
            if not rmid:
                continue
            ou_val = _extract_ou_from_match_payload(row)
            if ou_val is not None:
                result[rmid] = ou_val
                newly_found[rmid] = ou_val
        if newly_found:
            _save_ou_cache(newly_found)

    return result


def _enrich_matches_ou(matches: List[Dict[str, Any]]) -> None:
    """Completa instantaneamente el campo `ou` y `ou_res` usando caches locales (sin red)."""
    if not matches:
        return
    missing_ids: List[str] = []
    for m in matches:
        if not isinstance(m, dict):
            continue
        existing = _normalize_ou_display(m.get("ou"))
        if existing is not None:
            m["ou"] = existing
            if not m.get("ou_res") or m.get("ou_res") == "SIN_DATO":
                m["ou_res"] = _eval_ou_result(str(m.get("score") or ""), existing)
            continue
        mid = "".join(ch for ch in str(m.get("match_id") or "") if ch.isdigit())
        if mid:
            missing_ids.append(mid)

    if not missing_ids:
        return

    found_map = _lookup_ou_lines_fast(missing_ids)
    if not found_map:
        return

    for m in matches:
        if not isinstance(m, dict):
            continue
        mid = "".join(ch for ch in str(m.get("match_id") or "") if ch.isdigit())
        if mid and mid in found_map and _normalize_ou_display(m.get("ou")) is None:
            ou_val = found_map[mid]
            m["ou"] = ou_val
            m["ou_res"] = _eval_ou_result(str(m.get("score") or ""), ou_val)


def resolve_ou_lines_batch(match_ids: List[str], max_live: int = 24) -> Dict[str, Any]:
    """Resuelve en lote las lineas O/U para partidos faltantes usando Capa 1/2 local y Ajax en paralelo."""
    clean_ids: List[str] = []
    seen = set()
    for mid in match_ids or []:
        cid = "".join(ch for ch in str(mid or "") if ch.isdigit())
        if cid and cid not in seen:
            seen.add(cid)
            clean_ids.append(cid)

    if not clean_ids:
        return {"status": "success", "ou_lines": {}, "unavailable": [], "remaining": 0}

    resolved = _lookup_ou_lines_fast(clean_ids)
    now = time.time()

    with _OU_CACHE_LOCK:
        # Limpiar misses expirados ocasionalmente
        expired = [k for k, ts in _OU_MISS_TS.items() if (now - ts) >= _OU_MISS_TTL_SECONDS]
        for k in expired:
            _OU_MISS_TS.pop(k, None)
        recent_misses = {
            cid for cid in clean_ids if cid not in resolved and cid in _OU_MISS_TS
        }

    pending_for_live = [
        cid for cid in clean_ids if cid not in resolved and cid not in recent_misses
    ]
    cap = max(0, min(int(max_live or 24), 40))
    to_fetch = pending_for_live[:cap]
    remaining = max(0, len(pending_for_live) - len(to_fetch))

    live_found: Dict[str, str] = {}
    live_misses: List[str] = []
    if to_fetch:
        def _fetch_one(mid: str) -> Tuple[str, Optional[str]]:
            try:
                odds = es.fetch_odds_from_ajax(mid)
                if isinstance(odds, dict):
                    val = _normalize_ou_display(
                        odds.get("goals_linea_raw") or odds.get("goals_linea")
                    )
                    return mid, val
            except Exception as exc:
                LOGGER.debug("Error obteniendo O/U Ajax para %s: %s", mid, exc)
            return mid, None

        workers = min(6, len(to_fetch))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_fetch_one, mid) for mid in to_fetch]
            for fut in as_completed(futures):
                mid, val = fut.result()
                if val is not None:
                    live_found[mid] = val
                else:
                    live_misses.append(mid)

        if live_found:
            resolved.update(live_found)
            _save_ou_cache(live_found)
        if live_misses:
            stamp = time.time()
            with _OU_CACHE_LOCK:
                for mid in live_misses:
                    _OU_MISS_TS[mid] = stamp

    unavailable = sorted(set(recent_misses) | set(live_misses))
    return {
        "status": "success",
        "ou_lines": resolved,
        "unavailable": unavailable,
        "remaining": remaining,
    }


def _calc_ah_evaluation(score_str: str, ah_str: str, is_home: bool) -> Dict[str, Any]:
    score = _score_parts(score_str)
    line = es.parse_ah_to_number_of(str(ah_str or ""))
    if not score or line is None:
        return {"code": "SIN_DATO", "label": "-", "covered": False, "net": None}

    # Convencion NowGoal: ah > 0 significa que el LOCAL cede goles (es favorito).
    # Por tanto, el equipo analizado cede: line si juega en casa, -line si juega fuera.
    team_goals = score[0] if is_home else score[1]
    rival_goals = score[1] if is_home else score[0]
    goal_diff = team_goals - rival_goals

    team_gives_line = line if is_home else -line
    net = round(goal_diff - team_gives_line, 4)

    # Desdoblar lineas de cuarto (.25 / .75) igual que pattern_search_v2._eval_split_net
    frac = round((abs(team_gives_line) * 4) % 4)
    if frac in (1, 3):
        sub_lines = [team_gives_line - 0.25, team_gives_line + 0.25]
    else:
        sub_lines = [team_gives_line]

    outcomes = []
    for sub in sub_lines:
        sub_net = goal_diff - sub
        if sub_net > 1e-9:
            outcomes.append(1.0)
        elif sub_net < -1e-9:
            outcomes.append(-1.0)
        else:
            outcomes.append(0.0)

    avg = sum(outcomes) / len(outcomes)
    if avg >= 0.99:
        code, label = "COVER", "Cubre"
    elif avg >= 0.49:
        code, label = "HALF_WIN", "Medio Cubre"
    elif abs(avg) < 0.01:
        code, label = "PUSH", "Nulo"
    elif avg <= -0.99:
        code, label = "NO_COVER", "No Cubre"
    elif avg <= -0.49:
        code, label = "HALF_LOSS", "Medio Pierde"
    else:
        code, label = "SIN_DATO", "-"

    return {"code": code, "label": label, "covered": avg > 0, "net": net}


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
                    onclick = (a.get("onclick") or "") + " " + (a.get("href") or "")
                    m = re.search(r'(?:team|openFbTeam)\s*\(\s*(\d+)\s*\)', onclick, re.IGNORECASE)
                    if m:
                        return m.group(1).strip()
                    m2 = re.search(r'/team/(?:summary/)?(\d+)', onclick, re.IGNORECASE)
                    if m2:
                        return m2.group(1).strip()
        except Exception as exc:
            LOGGER.debug("Error resolviendo team_id en soup de match %s: %s", mid, exc)

    return None


def fetch_live_team_matches_via_h2h(team_name: str, match_id: Optional[str] = None, parent_match_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Extrae los partidos recientes de un equipo y sus cuotas AH directamente de NowGoal mediante H2H."""
    mids_to_try = []
    if match_id:
        mids_to_try.append("".join(filter(str.isdigit, str(match_id))))
    if parent_match_id:
        p_clean = "".join(filter(str.isdigit, str(parent_match_id)))
        if p_clean and p_clean not in mids_to_try:
            mids_to_try.append(p_clean)

    for mid in mids_to_try:
        if not mid:
            continue
        try:
            soup = es._load_main_match_soup(mid)
            if not soup:
                continue
            odds_map = es.extract_vs_market_odds(soup)
            m1 = es.extract_recent_matches(soup, "table_v1", team_name, None, False, odds_map, limit=100, is_neutral_venue=True)
            m2 = es.extract_recent_matches(soup, "table_v2", team_name, None, False, odds_map, limit=100, is_neutral_venue=True)
            candidate = m1 if len(m1) >= len(m2) else m2
            if candidate and len(candidate) > 0:
                return candidate
        except Exception as exc:
            LOGGER.warning("Error extrayendo matches live para %s desde H2H %s: %s", team_name, mid, exc)
    return []


def fetch_live_team_matches(team_name: str, team_id: str) -> List[Dict[str, Any]]:
    """Consulta la API de NowGoal para obtener el calendario y partidos mas recientes de un equipo."""
    clean_id = "".join(filter(str.isdigit, str(team_id or "")))
    if not clean_id:
        return []

    url = f"https://football.nowgoal.net/ajax/getteamschedulebypageno?id={clean_id}&pageNo=1&sclassId=0&init=1"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Referer": f"https://football.nowgoal.net/team/summary/{clean_id}",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    req = urllib.request.Request(url, headers=headers)
    match_list: List[Dict[str, Any]] = []
    ssl_ctx = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(req, timeout=8, context=ssl_ctx) as resp:
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
                odds_map = es.extract_vs_market_odds(soup)
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
    opponent_name: Optional[str] = None,
) -> Dict[str, Any]:
    clean_team = str(team_name or "").strip()
    if not clean_team:
        return {"status": "error", "error": "Falta el nombre del equipo"}

    clean_mid = "".join(filter(str.isdigit, str(match_id or "")))
    clean_parent = "".join(filter(str.isdigit, str(parent_match_id or "")))
    clean_opp = str(opponent_name or "").strip()
    cache_key = f"v2|v5:{clean_team.lower()}:{clean_mid}:{clean_parent}:{clean_opp.lower()}"

    cache = _read_cache()
    if not force_refresh and not force_live and cache_key in cache:
        cached_entry = cache[cache_key]
        cached_matches = cached_entry.get("data", {}).get("matches", []) if cached_entry else []
        if cached_entry and len(cached_matches) > 1 and (time.time() - cached_entry.get("timestamp", 0)) < 86400:
            _enrich_matches_ou(cached_matches)
            return {"status": "success", "cached": True, **cached_entry.get("data", {})}

    raw_matches: List[Dict[str, Any]] = []

    # Recuperar datos del partido padre para inferir contexto de oponente y estadios neutrales
    parent_data: Dict[str, Any] = {}
    opponent_is_home = False
    is_neutral = False
    opp_matches_primary: List[Dict[str, Any]] = []
    opp_matches_secondary: List[Dict[str, Any]] = []

    lookup_parent_id = clean_parent
    if lookup_parent_id:
        try:
            parent_data = data_manager.get_precacheo_match(lookup_parent_id) or sql_store.get_match(lookup_parent_id) or {}
            if parent_data:
                p_home = str(parent_data.get("home_name") or parent_data.get("home") or "").strip()
                p_away = str(parent_data.get("away_name") or parent_data.get("away") or "").strip()
                is_neutral = bool(parent_data.get("is_neutral_venue") or parent_data.get("neutral") or False)
                p_league = str(parent_data.get("league_name") or parent_data.get("league") or "").strip()
                if not is_neutral and p_league:
                    try:
                        from src.modules import estudio_scraper
                        is_neutral = bool(estudio_scraper._is_neutral_league(p_league))
                    except Exception:
                        pass

                has_explicit_custom_opp = bool(clean_opp and not _same_team(clean_opp, p_home) and not _same_team(clean_opp, p_away))

                if not has_explicit_custom_opp:
                    if _same_team(clean_team, p_away):
                        clean_opp = p_home
                        opponent_is_home = True
                    elif _same_team(clean_team, p_home):
                        clean_opp = p_away
                        opponent_is_home = False
                    elif not clean_opp:
                        clean_opp = p_home
                        opponent_is_home = True
                    else:
                        opponent_is_home = _same_team(clean_opp, p_home)
                else:
                    opponent_is_home = _same_team(clean_opp, p_home)

                if clean_opp:
                    if opponent_is_home:
                        opp_matches_primary = parent_data.get("recent_home_matches_all") or parent_data.get("recent_home_matches") or []
                        opp_matches_secondary = parent_data.get("recent_away_matches_all") or parent_data.get("recent_away_matches") or []
                    else:
                        opp_matches_primary = parent_data.get("recent_away_matches_all") or parent_data.get("recent_away_matches") or []
                        opp_matches_secondary = parent_data.get("recent_home_matches_all") or parent_data.get("recent_home_matches") or []
        except Exception as exc:
            LOGGER.warning("Error resolviendo oponente desde parent %s: %s", lookup_parent_id, exc)

    if not is_neutral and league_id:
        try:
            from src.modules import estudio_scraper
            is_neutral = bool(estudio_scraper._is_neutral_league(str(league_id).strip()))
        except Exception:
            pass

    opponent_name = clean_opp

    # 1. Si no es live/refresh forzado, intentar recuperar de parent_data si el equipo pertenece al partido padre
    p_home = str(parent_data.get("home_name") or parent_data.get("home") or "").strip() if parent_data else ""
    p_away = str(parent_data.get("away_name") or parent_data.get("away") or "").strip() if parent_data else ""
    is_parent_home = bool(parent_data and _same_team(clean_team, p_home))
    is_parent_away = bool(parent_data and _same_team(clean_team, p_away))

    if not force_live and not force_refresh:
        if is_parent_home:
            raw_matches = parent_data.get("recent_home_matches_all") or parent_data.get("recent_home_matches") or []
        elif is_parent_away:
            raw_matches = parent_data.get("recent_away_matches_all") or parent_data.get("recent_away_matches") or []

    # 2. Verificar si tenemos cuotas AH en los partidos obtenidos
    has_any_ah = any(
        str(m.get("ahLine") or m.get("ah_line") or m.get("ahLine_raw") or "").strip() not in ("", "-", "N/A", "?")
        for m in (raw_matches or [])
    )

    should_fetch_live = force_live or force_refresh or not raw_matches or not has_any_ah
    is_live_loaded = False
    resolved_team_id = team_id

    if should_fetch_live:
        # Intentar consultar directamente NowGoal mediante H2H de clean_mid o lookup_parent_id
        live_matches = fetch_live_team_matches_via_h2h(clean_team, match_id=clean_mid, parent_match_id=lookup_parent_id)
        if live_matches:
            raw_matches = live_matches
            is_live_loaded = True
        elif force_live:
            if not resolved_team_id:
                resolved_team_id = resolve_team_id(clean_team, parent_match_id=lookup_parent_id, parent_data=parent_data, match_id=clean_mid)
            if resolved_team_id:
                try:
                    fallback_matches = fetch_live_team_matches(clean_team, resolved_team_id)
                    if fallback_matches:
                        raw_matches = fallback_matches
                        is_live_loaded = True
                except Exception as exc:
                    LOGGER.warning("Error en fetch_live_team_matches fallback para %s: %s", clean_team, exc)

    # 3. Respaldo adicional con parent_data si no se cargó nada en vivo
    if not raw_matches and parent_data:
        if is_parent_home or _same_team(clean_team, p_home):
            raw_matches = parent_data.get("recent_home_matches_all") or parent_data.get("recent_home_matches") or []
        elif is_parent_away or _same_team(clean_team, p_away):
            raw_matches = parent_data.get("recent_away_matches_all") or parent_data.get("recent_away_matches") or []

    if not resolved_team_id:
        resolved_team_id = resolve_team_id(clean_team, parent_match_id=lookup_parent_id, parent_data=parent_data, match_id=clean_mid)

    # 4. Formatear y enriquecer los partidos
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
        raw_ah = item.get("ahLine") or item.get("ahLine_raw") or item.get("ah_line") or item.get("ah") or "-"
        m_ah = str(raw_ah).strip() if raw_ah not in (None, "", "N/A", "?") else "-"

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

        # Evaluacion AH y perspectiva del equipo
        ah_eval = _calc_ah_evaluation(m_score, m_ah, is_home)
        ah_num = es.parse_ah_to_number_of(str(m_ah or ""))
        team_ah: Optional[float] = None
        team_role = "N/D"
        if ah_num is not None:
            team_ah = round(ah_num if is_home else -ah_num, 4)
            if team_ah > 0.01:
                team_role = "FAV"
            elif team_ah < -0.01:
                team_role = "DOG"
            else:
                team_role = "PK"

        goals_for = (score_parts[0] if is_home else score_parts[1]) if score_parts else None
        goals_against = (score_parts[1] if is_home else score_parts[0]) if score_parts else None

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

        raw_ou = item.get("ouLine") or item.get("ou_line") or item.get("ou") or item.get("goals_line") or "-"
        m_ou = str(raw_ou).strip() if raw_ou not in (None, "", "N/A") else "-"
        ou_num = _parse_ou_num(m_ou)
        ou_res = "SIN_DATO"
        if score_parts and ou_num is not None:
            total_goals = score_parts[0] + score_parts[1]
            diff_ou = total_goals - ou_num
            if diff_ou > 1e-9:
                ou_res = "OVER"
            elif diff_ou < -1e-9:
                ou_res = "UNDER"
            else:
                ou_res = "PUSH"

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
            "goals_for": goals_for,
            "goals_against": goals_against,
            "wdl": wdl,
            "res_letter": res_letter,
            "ah": m_ah,
            "team_ah": team_ah,
            "team_role": team_role,
            "ou": m_ou,
            "ou_res": ou_res,
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
    _enrich_matches_ou(formatted_matches)

    # Calcular estadisticas resumen
    def calc_stats(subset: List[Dict]):
        w = sum(1 for m in subset if m["wdl"] == "W")
        d = sum(1 for m in subset if m["wdl"] == "D")
        l = sum(1 for m in subset if m["wdl"] == "L")
        cover_w = sum(1 for m in subset if m["ah_eval"]["code"] in ("COVER", "HALF_WIN"))
        cover_l = sum(1 for m in subset if m["ah_eval"]["code"] in ("NO_COVER", "HALF_LOSS"))
        cover_push = sum(1 for m in subset if m["ah_eval"]["code"] == "PUSH")
        decided = cover_w + cover_l
        total = len(subset)
        cover_pct = round((cover_w / decided * 100), 1) if decided > 0 else 0

        overs = sum(1 for m in subset if m.get("ou_res") == "OVER")
        unders = sum(1 for m in subset if m.get("ou_res") == "UNDER")
        ou_push = sum(1 for m in subset if m.get("ou_res") == "PUSH")
        ou_decided = overs + unders
        over_pct = round((overs / ou_decided * 100), 1) if ou_decided > 0 else 0

        gf_vals = [m["goals_for"] for m in subset if m.get("goals_for") is not None]
        ga_vals = [m["goals_against"] for m in subset if m.get("goals_against") is not None]
        goals_for_avg = round(sum(gf_vals) / len(gf_vals), 2) if gf_vals else 0.0
        goals_against_avg = round(sum(ga_vals) / len(ga_vals), 2) if ga_vals else 0.0

        fav_sub = [m for m in subset if m.get("team_role") == "FAV"]
        fav_cw = sum(1 for m in fav_sub if m["ah_eval"]["code"] in ("COVER", "HALF_WIN"))
        fav_cl = sum(1 for m in fav_sub if m["ah_eval"]["code"] in ("NO_COVER", "HALF_LOSS"))
        fav_dec = fav_cw + fav_cl
        fav_cover_pct = round((fav_cw / fav_dec * 100), 1) if fav_dec > 0 else 0

        dog_sub = [m for m in subset if m.get("team_role") == "DOG"]
        dog_cw = sum(1 for m in dog_sub if m["ah_eval"]["code"] in ("COVER", "HALF_WIN"))
        dog_cl = sum(1 for m in dog_sub if m["ah_eval"]["code"] in ("NO_COVER", "HALF_LOSS"))
        dog_dec = dog_cw + dog_cl
        dog_cover_pct = round((dog_cw / dog_dec * 100), 1) if dog_dec > 0 else 0

        return {
            "total": total,
            "wins": w,
            "draws": d,
            "losses": l,
            "cover_wins": cover_w,
            "cover_losses": cover_l,
            "cover_push": cover_push,
            "cover_pct": cover_pct,
            "overs": overs,
            "unders": unders,
            "ou_push": ou_push,
            "over_pct": over_pct,
            "goals_for_avg": goals_for_avg,
            "goals_against_avg": goals_against_avg,
            "fav_n": len(fav_sub),
            "fav_cover_wins": fav_cw,
            "fav_cover_pct": fav_cover_pct,
            "dog_n": len(dog_sub),
            "dog_cover_wins": dog_cw,
            "dog_cover_pct": dog_cover_pct,
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
