"""NowGoal data fetcher and parser using the direct JS data endpoint (/gf/data/bf_en-idn.js).

NowGoal serves real-time and scheduled match data via compact JavaScript files
(e.g., /gf/data/bf_en-idn.js or /gf/data/bf_en-idn1.js) rather than embedding match rows in HTML.
This module performs a session handshake (to acquire the LS_ACCESS_TOKEN cookie),
downloads the JS file, sanitizes its syntax so it can be parsed with json.loads,
converts UTC dates to exact Spain timezone (Europe/Madrid),
and constructs structured dictionaries for upcoming, live, and finished matches.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
import threading
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
import requests
from requests.adapters import HTTPAdapter
import urllib3
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LOGGER = logging.getLogger(__name__)
SPAIN_TZ = ZoneInfo("Europe/Madrid")
UTC_TZ = dt.timezone.utc

NOWGOAL_BASE_HOSTS = [
    "https://live10.nowgoal26.com",
    "https://live20.nowgoal25.com",
    "https://www.nowgoal26.com",
    "https://www.nowgoal25.com",
]

_session_lock = threading.Lock()
_shared_session: Optional[requests.Session] = None
_session_initialized_at: float = 0


def get_requests_session() -> requests.Session:
    global _shared_session
    with _session_lock:
        if _shared_session is None:
            session = requests.Session()
            retries = Retry(
                total=2,
                connect=2,
                read=2,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504],
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retries)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
            })
            _shared_session = session
        return _shared_session


def _sanitize_js_row(raw_row: str) -> Optional[List[Any]]:
    """Sanitiza una fila JS de NowGoal (A[x]=[...] o B[x]=[...]) a una lista Python."""
    if not raw_row:
        return None
    clean = raw_row.replace("'", '"')
    while ",," in clean:
        clean = clean.replace(",,", ",null,")
    clean = clean.strip()
    if clean.startswith(","):
        clean = "null" + clean
    if clean.endswith(","):
        clean = clean + "null"
    try:
        parsed = json.loads(f"[{clean}]")
        return parsed if isinstance(parsed, list) else None
    except Exception:
        return None


def fetch_bf_data_raw() -> Optional[str]:
    """Descarga el archivo JS de datos en vivo / portada realizando el handshake de cookies necesario."""
    session = get_requests_session()
    endpoints = ["/gf/data/bf_en-idn.js", "/gf/data/bf_en-idn1.js", "/gf/data/bf_en1.js"]

    for host in NOWGOAL_BASE_HOSTS:
        try:
            # Handshake previo en la portada para obtener cookies (LS_ACCESS_TOKEN)
            session.headers.update({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            })
            home_resp = session.get(f"{host}/", timeout=8, verify=False)
            if home_resp.status_code != 200:
                continue

            # Headers para la descarga de los scripts de datos
            session.headers.update({
                "Accept": "*/*",
                "Referer": f"{host}/",
                "Sec-Fetch-Dest": "script",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "same-origin",
            })

            for ep in endpoints:
                url = f"{host}{ep}"
                try:
                    resp = session.get(url, timeout=8, verify=False)
                    if resp.status_code == 200 and ("A[" in resp.text or "var A" in resp.text):
                        return resp.text
                except Exception as exc:
                    LOGGER.debug("Error fetching %s: %s", url, exc)
                    continue
        except Exception as exc:
            LOGGER.debug("Error during handshake on %s: %s", host, exc)
            continue

    return None


def _parse_nowgoal_date_to_spain(date_raw: Any) -> Optional[dt.datetime]:
    """
    Parsea la fecha NowGoal en UTC a datetime con zona horaria de España (Europe/Madrid).
    Formatos admitidos: '2026-08-28 19:00:00', '2026,7,28,19,0,0' (mes base 0 en JS).
    """
    if not date_raw:
        return None
    text = str(date_raw).strip()

    # Formato estándar 'YYYY-MM-DD HH:MM:SS'
    m_std = re.match(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?", text)
    if m_std:
        try:
            y, mo, d, h, mi = (int(x) for x in m_std.groups()[:5])
            s = int(m_std.group(6) or 0)
            dt_utc = dt.datetime(y, mo, d, h, mi, s, tzinfo=UTC_TZ)
            return dt_utc.astimezone(SPAIN_TZ)
        except Exception:
            pass

    # Formato separado por comas de JS (año, mes_0_idx, dia, hora, minuto...)
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if len(parts) >= 5:
        try:
            year = int(parts[0])
            js_month = int(parts[1])
            month = js_month + 1 if 0 <= js_month <= 11 else js_month
            day = int(parts[2])
            hour = int(parts[3])
            minute = int(parts[4])
            dt_utc = dt.datetime(year, month, day, hour, minute, tzinfo=UTC_TZ)
            return dt_utc.astimezone(SPAIN_TZ)
        except Exception:
            pass

    return None


def parse_matches_from_bf_content(
    content: str,
    status_filter: str = "all",
    handicap_filter: Optional[str] = None,
    goal_line_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Parsea el contenido de bf_en-idn.js y devuelve una lista de diccionarios normalizados."""
    if not content:
        return []

    leagues_map: Dict[str, str] = {}
    league_pattern = re.compile(r"B\[\d+\]=\[(.*?)\];")
    for match in league_pattern.finditer(content):
        row = _sanitize_js_row(match.group(1))
        if row and len(row) >= 2:
            lid = str(row[0])
            lname = str(row[1] or "")
            if lid and lname:
                leagues_map[lid] = lname

    match_pattern = re.compile(r"A\[\d+\]=\[(.*?)\];")
    matches: List[Dict[str, Any]] = []
    now_spain = dt.datetime.now(SPAIN_TZ)

    for match in match_pattern.finditer(content):
        row = _sanitize_js_row(match.group(1))
        if not row or len(row) < 7:
            continue

        match_id = str(row[0])
        if not match_id:
            continue

        league_id = str(row[1]) if len(row) > 1 and row[1] is not None else ""
        league_name = str(row[2]) if len(row) > 2 and row[2] else leagues_map.get(league_id, "Unknown League")
        if league_id in leagues_map and len(leagues_map[league_id]) > len(league_name):
            league_name = leagues_map[league_id]

        home_team = str(row[4]) if len(row) > 4 and row[4] is not None else "Local"
        away_team = str(row[5]) if len(row) > 5 and row[5] is not None else "Visitante"

        try:
            status_val = int(row[8]) if len(row) > 8 and row[8] is not None else 0
        except (ValueError, TypeError):
            status_val = 0

        home_score = str(row[9]) if len(row) > 9 and row[9] is not None else ""
        away_score = str(row[10]) if len(row) > 10 and row[10] is not None else ""
        score_str = f"{home_score}-{away_score}" if (home_score != "" and away_score != "") else "-:-"

        match_dt = _parse_nowgoal_date_to_spain(row[6] if len(row) > 6 else None)
        if not match_dt:
            match_dt = now_spain

        time_str = match_dt.strftime("%H:%M")
        date_str = match_dt.strftime("%m/%d/%Y")
        date_iso_str = match_dt.strftime("%Y-%m-%d")
        start_time_iso = match_dt.isoformat()

        # Odds: AH en columna 21, O/U en columna 25 (o 23/24 según formato)
        ah_raw = row[21] if len(row) > 21 and row[21] is not None else None
        ou_raw = row[25] if len(row) > 25 and row[25] is not None else (
            row[23] if len(row) > 23 and isinstance(row[23], (int, float)) else None
        )

        ah_str = str(ah_raw) if ah_raw is not None and str(ah_raw) != "" else "N/A"
        ou_str = str(ou_raw) if ou_raw is not None and str(ou_raw) != "" else "N/A"

        is_finished = status_val == -1
        is_upcoming = status_val == 0 or (status_val != -1 and match_dt > now_spain)
        is_live = status_val > 0

        if status_filter == "upcoming" and not is_upcoming:
            continue
        if status_filter == "finished" and not is_finished:
            continue
        if status_filter == "live" and not is_live:
            continue

        if handicap_filter:
            try:
                ah_float = float(ah_str)
                target_ah = float(handicap_filter)
                if abs(ah_float - target_ah) > 0.05:
                    continue
            except Exception:
                pass

        if goal_line_filter:
            try:
                ou_float = float(ou_str)
                target_ou = float(goal_line_filter)
                if abs(ou_float - target_ou) > 0.05:
                    continue
            except Exception:
                pass

        match_dict = {
            "id": match_id,
            "match_id": match_id,
            "home_team": home_team,
            "away_team": away_team,
            "home_name": home_team,
            "away_name": away_team,
            "league": league_name,
            "league_name": league_name,
            "league_id": league_id,
            "time": time_str,
            "date": date_str,
            "match_date": date_str,
            "start_time": start_time_iso,
            "handicap": ah_str,
            "goal_line": ou_str,
            "score": score_str,
            "final_score": score_str if is_finished else None,
            "status": status_val,
            "main_match_odds": {
                "ah_linea": ah_str,
                "goals_linea": ou_str,
            },
            "specialist_picks": [],
        }
        matches.append(match_dict)

    if status_filter == "finished":
        matches.sort(key=lambda m: m.get("start_time") or "", reverse=True)
    else:
        matches.sort(key=lambda m: m.get("start_time") or "")

    return matches


def fetch_main_page_matches_direct(
    status_filter: str = "upcoming",
    limit: Optional[int] = None,
    offset: int = 0,
    handicap_filter: Optional[str] = None,
    goal_line_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Descarga de forma síncrona los partidos directamente desde bf_en-idn.js."""
    content = fetch_bf_data_raw()
    if not content:
        return []
    matches = parse_matches_from_bf_content(
        content,
        status_filter=status_filter,
        handicap_filter=handicap_filter,
        goal_line_filter=goal_line_filter,
    )
    if offset > 0:
        matches = matches[offset:]
    if limit is not None and limit > 0:
        matches = matches[:limit]
    return matches


def fetch_main_page() -> str:
    """Función de compatibilidad que devuelve el contenido crudo de datos NowGoal."""
    content = fetch_bf_data_raw()
    return content or ""
