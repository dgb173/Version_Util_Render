"""NowGoal data fetcher and parser using direct JS extraction and Playwright fallback.

NowGoal serves real-time match arrays (variables A and B) through data scripts (e.g., /gf/data/bf_en-idn.js).
This module first tries ultra-fast direct HTTP endpoints with session cookies; if challenged or empty,
it uses headless Playwright to evaluate the global `A` and `B` JS objects directly from browser memory,
providing 100% reliable real-time upcoming, live, and finished match schedules.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import re
import threading
import xml.etree.ElementTree as ET
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
NOWGOAL_RESULTS_PATH = "/football/results"

_session_lock = threading.Lock()
_shared_session: Optional[requests.Session] = None


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


def _is_valid_numeric_handicap(val: Any) -> bool:
    """Devuelve True solo si val representa un handicap asiático numérico válido."""
    if val in (None, "", "N/A", "null", "None", "-"):
        return False
    try:
        float(str(val).strip().replace("−", "-"))
        return True
    except (TypeError, ValueError):
        return False


def fetch_bf_data_raw() -> Optional[str]:
    """Descarga el contenido JS de partidos próximos unificando ligas principales y secundarias."""
    session = get_requests_session()
    endpoints = [
        "/gf/data/bf_en-idn.js",
        "/gf/data/bf_en-idn1.js",
    ]

    for host in NOWGOAL_BASE_HOSTS:
        try:
            home_resp = session.get(f"{host}/", timeout=5, verify=False)
            if home_resp.status_code != 200:
                continue

            session.headers.update({
                "Accept": "*/*",
                "Referer": f"{host}/",
                "Sec-Fetch-Dest": "script",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "same-origin",
            })

            combined = []
            for ep in endpoints:
                url = f"{host}{ep}"
                try:
                    resp = session.get(url, timeout=6, verify=False)
                    if resp.status_code == 200 and ("A[" in resp.text or "var A" in resp.text):
                        combined.append(resp.text)
                except Exception as exc:
                    LOGGER.debug("Error fetching %s: %s", url, exc)
                    continue

            if combined:
                return "\n".join(combined)

            # Fallback a bf_en.js si los anteriores fallaron
            for ep in ["/gf/data/bf_en.js", "/gf/data/bf_change_en.js"]:
                url = f"{host}{ep}"
                try:
                    resp = session.get(url, timeout=6, verify=False)
                    if resp.status_code == 200 and ("A[" in resp.text or "var A" in resp.text):
                        return resp.text
                except Exception:
                    continue
        except Exception as exc:
            LOGGER.debug("Error during handshake on %s: %s", host, exc)
            continue

    return None


def _parse_finished_odds_xml(content: str) -> Dict[str, Dict[str, str]]:
    """Convierte goal8.xml en un mapa match_id -> líneas AH/O-U."""
    odds: Dict[str, Dict[str, str]] = {}
    if not content:
        return odds
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return odds

    for node in root.findall(".//m"):
        parts = [part.strip() for part in (node.text or "").split(",")]
        if len(parts) < 11 or not parts[0]:
            continue
        odds[parts[0]] = {"handicap": parts[2], "goal_line": parts[10]}
    return odds


def fetch_finished_data_raw() -> Tuple[Optional[str], Dict[str, Dict[str, str]]]:
    """Descarga la lista completa de /football/results unificando principales y secundarias con cuotas."""
    session = get_requests_session()
    bf_endpoints = (
        "/gf/data/finish/bf_en-idn.js",
        "/gf/data/finish/bf_en-idn1.js",
    )
    for host in NOWGOAL_BASE_HOSTS:
        results_url = f"{host}{NOWGOAL_RESULTS_PATH}"
        try:
            page_resp = session.get(results_url, timeout=8, verify=False)
            if page_resp.status_code != 200:
                continue
            headers = {
                "Accept": "*/*",
                "Referer": results_url,
                "Sec-Fetch-Dest": "script",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "same-origin",
            }
            combined_texts = []
            for endpoint in bf_endpoints:
                try:
                    bf_resp = session.get(
                        f"{host}{endpoint}", timeout=10, verify=False, headers=headers
                    )
                    if bf_resp.status_code == 200 and "A[" in bf_resp.text:
                        combined_texts.append(bf_resp.text)
                except Exception as exc:
                    LOGGER.debug("Error fetching finished endpoint %s on %s: %s", endpoint, host, exc)

            if combined_texts:
                try:
                    odds_resp = session.get(
                        f"{host}/gf/data/finish/goal8.xml",
                        timeout=10,
                        verify=False,
                        headers=headers,
                    )
                    odds = _parse_finished_odds_xml(odds_resp.text) if odds_resp.status_code == 200 else {}
                except Exception as exc:
                    LOGGER.debug("Error fetching goal8.xml on %s: %s", host, exc)
                    odds = {}
                return "\n".join(combined_texts), odds
        except Exception as exc:
            LOGGER.debug("Error during results handshake on %s: %s", host, exc)
    return None, {}


def parse_matches_from_bf_content(
    content: str,
    status_filter: str = "all",
    handicap_filter: Optional[str] = None,
    goal_line_filter: Optional[str] = None,
    odds_by_match: Optional[Dict[str, Dict[str, str]]] = None,
    require_handicap: bool = True,
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
    seen_ids = set()
    now_spain = dt.datetime.now(SPAIN_TZ)

    for match in match_pattern.finditer(content):
        row = _sanitize_js_row(match.group(1))
        if not row or len(row) < 7:
            continue

        match_id = str(row[0])
        if not match_id or match_id in seen_ids:
            continue
        seen_ids.add(match_id)

        league_id = str(row[1]) if len(row) > 1 and row[1] is not None else ""
        league_name = leagues_map.get(league_id) or (
            str(row[2]) if len(row) > 2 and row[2] else "Unknown League"
        )

        home_team = str(row[4]) if len(row) > 4 and row[4] is not None else "Local"
        away_team = str(row[5]) if len(row) > 5 and row[5] is not None else "Visitante"

        try:
            status_val = int(row[8]) if len(row) > 8 and row[8] is not None else 0
        except (ValueError, TypeError):
            status_val = 0

        home_score = str(row[9]) if len(row) > 9 and row[9] is not None else ""
        away_score = str(row[10]) if len(row) > 10 and row[10] is not None else ""

        if status_val == 0:
            score_str = None
        else:
            score_str = f"{home_score}-{away_score}" if (home_score != "" and away_score != "") else "-:-"

        match_dt = _parse_nowgoal_date_to_spain(row[6] if len(row) > 6 else None)
        if not match_dt:
            match_dt = now_spain

        time_str = match_dt.strftime("%H:%M")
        date_str = match_dt.strftime("%m/%d/%Y")
        start_time_iso = match_dt.isoformat()

        # Odds: AH en columna 21, O/U en columna 25 (o 23 según versión JS)
        ah_raw = row[21] if len(row) > 21 and row[21] is not None else None
        ou_raw = row[25] if len(row) > 25 and row[25] is not None else (
            row[23] if len(row) > 23 and isinstance(row[23], (int, float)) else None
        )

        ah_str = str(ah_raw) if ah_raw is not None and str(ah_raw) != "" else "N/A"
        ou_str = str(ou_raw) if ou_raw is not None and str(ou_raw) != "" else "N/A"
        finished_odds = (odds_by_match or {}).get(match_id, {})
        if finished_odds.get("handicap") not in (None, ""):
            ah_str = str(finished_odds["handicap"])
        if finished_odds.get("goal_line") not in (None, ""):
            ou_str = str(finished_odds["goal_line"])

        # Descarte estricto: Todo partido sin línea de hándicap válida queda excluido
        if require_handicap and not _is_valid_numeric_handicap(ah_str):
            continue

        if status_filter == "finished" and odds_by_match is not None:
            # En el feed /finish: -1=FT; -10=cancelado, -11=pendiente y -14=aplazado.
            is_finished = status_val == -1
        else:
            is_finished = status_val in (-1, 13, 14, 15) or (
                score_str is not None
                and score_str != "-:-"
                and home_score != ""
                and away_score != ""
                and match_dt <= now_spain
            )
        is_upcoming = status_val == 0 or (status_val not in (-1, 13, 14, 15) and match_dt > now_spain)
        is_live = status_val > 0 and status_val not in (-1, 13, 14, 15)

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


def fetch_matches_with_playwright(status_filter: str = "all") -> List[Dict[str, Any]]:
    """Extrae partidos y cuotas evaluando los arrays JS de memoria mediante Chromium (Playwright) como fallback."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        LOGGER.warning("Playwright no está instalado.")
        return []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto("https://live10.nowgoal26.com/", wait_until="domcontentloaded", timeout=25000)
                page.wait_for_timeout(4000)

                raw_data = page.evaluate("""() => {
                    const matches = [];
                    const leagues = {};
                    if (typeof B !== 'undefined' && Array.isArray(B)) {
                        for (let i = 0; i < B.length; i++) {
                            const row = B[i];
                            if (row && row.length >= 2) {
                                leagues[String(row[0])] = String(row[1] || '');
                            }
                        }
                    }
                    if (typeof A !== 'undefined' && Array.isArray(A)) {
                        for (let i = 0; i < A.length; i++) {
                            const row = A[i];
                            if (row && row.length >= 7) {
                                matches.push(row);
                            }
                        }
                    }
                    return { matches, leagues };
                }""")
            finally:
                browser.close()

        raw_matches = raw_data.get("matches", [])
        leagues_map = raw_data.get("leagues", {})
        now_spain = dt.datetime.now(SPAIN_TZ)
        parsed_matches: List[Dict[str, Any]] = []
        seen_ids = set()

        for row in raw_matches:
            match_id = str(row[0])
            if not match_id or match_id in seen_ids:
                continue
            seen_ids.add(match_id)

            league_id = str(row[1]) if len(row) > 1 and row[1] is not None else ""
            league_name = leagues_map.get(league_id) or (str(row[2]) if len(row) > 2 and row[2] else "Unknown League")
            home_team = str(row[4]) if len(row) > 4 and row[4] is not None else "Local"
            away_team = str(row[5]) if len(row) > 5 and row[5] is not None else "Visitante"

            try:
                status_val = int(row[8]) if len(row) > 8 and row[8] is not None else 0
            except Exception:
                status_val = 0

            home_score = str(row[9]) if len(row) > 9 and row[9] is not None else ""
            away_score = str(row[10]) if len(row) > 10 and row[10] is not None else ""

            if status_val == 0:
                score_str = None
            else:
                score_str = f"{home_score}-{away_score}" if (home_score != "" and away_score != "") else "-:-"

            match_dt = _parse_nowgoal_date_to_spain(row[6] if len(row) > 6 else None) or now_spain
            time_str = match_dt.strftime("%H:%M")
            date_str = match_dt.strftime("%m/%d/%Y")
            start_time_iso = match_dt.isoformat()

            ah_raw = row[21] if len(row) > 21 and row[21] is not None else None
            ou_raw = row[25] if len(row) > 25 and row[25] is not None else (
                row[23] if len(row) > 23 and isinstance(row[23], (int, float)) else None
            )
            ah_str = str(ah_raw) if ah_raw is not None and str(ah_raw) != "" else "N/A"
            ou_str = str(ou_raw) if ou_raw is not None and str(ou_raw) != "" else "N/A"

            is_finished = status_val in (-1, 13, 14, 15) or (score_str is not None and score_str != "-:-" and home_score != "" and away_score != "" and match_dt <= now_spain)
            is_upcoming = status_val == 0 or (status_val not in (-1, 13, 14, 15) and match_dt > now_spain)
            is_live = status_val > 0 and status_val not in (-1, 13, 14, 15)

            if status_filter == "upcoming" and not is_upcoming:
                continue
            if status_filter == "finished" and not is_finished:
                continue
            if status_filter == "live" and not is_live:
                continue

            parsed_matches.append({
                "id": match_id,
                "match_id": match_id,
                "home_team": home_team,
                "away_team": away_team,
                "home_name": home_team,
                "away_name": away_team,
                "league": league_name,
                "league_name": league_name,
                "league_id": league_id,
                "date": date_str,
                "match_date": date_str,
                "time": time_str,
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
            })

        if status_filter == "finished":
            parsed_matches.sort(key=lambda m: m.get("start_time") or "", reverse=True)
        else:
            parsed_matches.sort(key=lambda m: m.get("start_time") or "")

        return parsed_matches
    except Exception as exc:
        LOGGER.error("Error en extracción con Playwright: %s", exc)
        return []


def fetch_finished_matches_with_playwright() -> List[Dict[str, Any]]:
    """Fallback que lee exclusivamente las filas FT de /football/results."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        LOGGER.warning("Playwright no está instalado.")
        return []

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(
                    f"https://live10.nowgoal26.com{NOWGOAL_RESULTS_PATH}",
                    wait_until="domcontentloaded",
                    timeout=25000,
                )
                page.wait_for_selector('tr[id^="tr1_"]', timeout=15000)
                rows = page.evaluate(r"""() => Array.from(document.querySelectorAll('tr[id^="tr1_"]')).map(tr => {
                    const id = tr.id.replace('tr1_', '');
                    const odds = (tr.getAttribute('odds') || '').split(',');
                    const status = (tr.querySelector(`#time_${id}`)?.textContent || '').trim();
                    const meta = (tr.querySelector('.toolimg')?.getAttribute('data-match') || '').split(',');
                    return {
                        id,
                        home_team: (tr.querySelector(`#team1_${id}`)?.textContent || '').trim(),
                        away_team: (tr.querySelector(`#team2_${id}`)?.textContent || '').trim(),
                        league: meta.slice(2).join(',').trim(),
                        date_raw: tr.querySelector(`#mt_${id}`)?.getAttribute('data-t') || '',
                        status,
                        score: (tr.querySelector('.f-b b')?.textContent || '').replace(/\s+/g, ''),
                        handicap: odds.length > 2 ? odds[2] : '',
                        goal_line: odds.length > 10 ? odds[10] : ''
                    };
                }).filter(row => row.id && /^(FT|AET|Pen)/i.test(row.status))""")
            finally:
                browser.close()

        parsed = []
        for item in rows:
            match_dt = _parse_nowgoal_date_to_spain(item.get("date_raw")) or dt.datetime.now(SPAIN_TZ)
            score = item.get("score") or "-:-"
            handicap = str(item.get("handicap") or "N/A")
            goal_line = str(item.get("goal_line") or "N/A")
            parsed.append({
                "id": str(item["id"]),
                "match_id": str(item["id"]),
                "home_team": item.get("home_team") or "Local",
                "away_team": item.get("away_team") or "Visitante",
                "home_name": item.get("home_team") or "Local",
                "away_name": item.get("away_team") or "Visitante",
                "league": item.get("league") or "Unknown League",
                "league_name": item.get("league") or "Unknown League",
                "date": match_dt.strftime("%m/%d/%Y"),
                "match_date": match_dt.strftime("%m/%d/%Y"),
                "time": match_dt.strftime("%H:%M"),
                "start_time": match_dt.isoformat(),
                "handicap": handicap,
                "goal_line": goal_line,
                "score": score,
                "final_score": score,
                "status": -1,
                "main_match_odds": {"ah_linea": handicap, "goals_linea": goal_line},
                "specialist_picks": [],
            })
        parsed.sort(key=lambda match: match.get("start_time") or "", reverse=True)
        return parsed
    except Exception as exc:
        LOGGER.error("Error leyendo la página de resultados con Playwright: %s", exc)
        return []


def fetch_main_page_matches_direct(
    status_filter: str = "upcoming",
    limit: Optional[int] = None,
    offset: int = 0,
    handicap_filter: Optional[str] = None,
    goal_line_filter: Optional[str] = None,
    require_handicap: bool = True,
) -> List[Dict[str, Any]]:
    """Descarga de forma ultrarrápida los partidos (HTTP directo con fallback a Playwright)."""
    # 1. Intentar vía HTTP directo (50-100ms)
    odds_by_match: Dict[str, Dict[str, str]] = {}
    if status_filter == "finished":
        content, odds_by_match = fetch_finished_data_raw()
    else:
        content = fetch_bf_data_raw()
    matches: List[Dict[str, Any]] = []
    if content:
        matches = parse_matches_from_bf_content(
            content,
            status_filter=status_filter,
            handicap_filter=handicap_filter,
            goal_line_filter=goal_line_filter,
            odds_by_match=odds_by_match,
            require_handicap=require_handicap,
        )

    # 2. Fallback a Playwright si HTTP directo falló o no devolvió partidos
    if not matches:
        matches = (
            fetch_finished_matches_with_playwright()
            if status_filter == "finished"
            else fetch_matches_with_playwright(status_filter=status_filter)
        )
        if require_handicap:
            matches = [m for m in matches if _is_valid_numeric_handicap(m.get("handicap"))]
        if handicap_filter:
            try:
                target_ah = float(handicap_filter)
                matches = [m for m in matches if abs(float(m.get("handicap", 999)) - target_ah) < 0.05]
            except Exception:
                pass
        if goal_line_filter:
            try:
                target_ou = float(goal_line_filter)
                matches = [m for m in matches if abs(float(m.get("goal_line", 999)) - target_ou) < 0.05]
            except Exception:
                pass

    if offset > 0:
        matches = matches[offset:]
    if limit is not None and limit > 0:
        matches = matches[:limit]

    return matches


def fetch_main_page() -> str:
    """Función de compatibilidad que devuelve el contenido crudo de datos NowGoal."""
    content = fetch_bf_data_raw()
    return content or ""
