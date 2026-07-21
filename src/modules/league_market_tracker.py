"""Seguimiento temporal de una liga y de la colocacion Over/Under de las casas.

El modulo conserva el dato crudo, separa prepartido/directo y reconstruye la
clasificacion previa a cada encuentro. Las conclusiones son descriptivas y
siempre incluyen muestra; no se etiqueta una correlacion como causalidad.
"""

from __future__ import annotations

import concurrent.futures
import json
import math
import re
import threading
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import sql_store

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


INFO_BASE = "https://football.nowgoal26.com"
LIVE_BASE = "https://live10.nowgoal26.com"
DEFAULT_LEAGUE_ID = "381"
DEFAULT_COMPANIES = (8, 31, 3)  # Bet365, SBOBET, Crown
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
}
_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False


def _session() -> requests.Session:
    session = requests.Session()
    session.verify = False
    retry = Retry(total=3, backoff_factor=.35, status_forcelist=(429, 500, 502, 503, 504))
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=12, pool_maxsize=12))
    session.headers.update(HEADERS)
    return session


def _ensure_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        sql_store.ensure_bootstrap()
        with sql_store._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS league_market_matches (
                    league_id TEXT NOT NULL,
                    season TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    round_label TEXT,
                    match_date TEXT,
                    home_team_id TEXT,
                    home_team TEXT NOT NULL,
                    away_team_id TEXT,
                    away_team TEXT NOT NULL,
                    home_score INTEGER,
                    away_score INTEGER,
                    source_state INTEGER,
                    context_json TEXT NOT NULL DEFAULT '{}',
                    source_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (league_id, season, match_id)
                );
                CREATE INDEX IF NOT EXISTS idx_lmm_season_date
                    ON league_market_matches(league_id, season, match_date);

                CREATE TABLE IF NOT EXISTS league_market_odds (
                    league_id TEXT NOT NULL,
                    season TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    company_id INTEGER NOT NULL,
                    company_name TEXT NOT NULL,
                    market TEXT NOT NULL DEFAULT 'OU',
                    phase TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    sequence_no INTEGER NOT NULL,
                    line REAL,
                    over_price REAL,
                    under_price REAL,
                    home_score INTEGER,
                    away_score INTEGER,
                    minute TEXT,
                    is_closed INTEGER NOT NULL DEFAULT 0,
                    source_kind TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (league_id, season, match_id, company_id, market, phase, observed_at, sequence_no)
                );
                CREATE INDEX IF NOT EXISTS idx_lmo_match_company
                    ON league_market_odds(league_id, season, match_id, company_id, phase);
                CREATE INDEX IF NOT EXISTS idx_lmo_analysis
                    ON league_market_odds(league_id, season, market, phase, line);

                CREATE TABLE IF NOT EXISTS league_market_sync_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_id TEXT NOT NULL,
                    seasons_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    matches_seen INTEGER NOT NULL DEFAULT 0,
                    matches_with_odds INTEGER NOT NULL DEFAULT 0,
                    snapshots_saved INTEGER NOT NULL DEFAULT 0,
                    errors_json TEXT NOT NULL DEFAULT '[]',
                    started_at TEXT NOT NULL,
                    finished_at TEXT
                );
                """
            )
        _SCHEMA_READY = True


def _get_json(session: requests.Session, url: str, referer: str = "") -> Dict[str, Any]:
    headers = {"Referer": referer, "X-Requested-With": "XMLHttpRequest"} if referer else {}
    response = session.get(url, headers=headers, timeout=35)
    response.raise_for_status()
    return json.loads(response.text.lstrip("\ufeff"))


def available_seasons(league_id: str = DEFAULT_LEAGUE_ID) -> List[str]:
    try:
        data = _get_json(_session(), f"{INFO_BASE}/jsData/leagueSeason/sea{league_id}.json")
        return [str(value) for value in data.get("SeasonList", [])]
    except Exception:
        return []


def _discover_season(session: requests.Session, league_id: str, season: str) -> Tuple[str, str, str]:
    page_url = f"{INFO_BASE}/league/{season}/{league_id}"
    response = session.get(page_url, timeout=35)
    response.raise_for_status()
    html = response.text
    season_match = re.search(r'const\s+_season\s*=\s*"([^"]+)"', html)
    path_match = re.search(r'const\s+_dataPath\s*=\s*"([^"]+)"', html)
    name_match = re.search(r'const\s+_sclassName\s*=\s*escapeChar\("([^"]+)"\)', html)
    if not season_match or not path_match:
        raise RuntimeError(f"NowGoal no expuso el calendario de {season}")
    actual = season_match.group(1)
    if str(actual) != str(season):
        raise RuntimeError(f"NowGoal devolvio {actual} al solicitar {season}")
    return actual, urljoin(INFO_BASE, path_match.group(1)), (name_match.group(1) if name_match else f"Liga {league_id}")


def _score(value: Any) -> Tuple[Optional[int], Optional[int]]:
    match = re.search(r"(\d+)\s*[-:]\s*(\d+)", str(value or ""))
    return (int(match.group(1)), int(match.group(2))) if match else (None, None)


def _walk_matches(node: Any, league_id: str, round_label: str = "") -> Iterable[Tuple[str, list]]:
    if isinstance(node, dict):
        for key, value in node.items():
            next_round = str(key)[2:] if str(key).startswith("R_") else round_label
            yield from _walk_matches(value, league_id, next_round)
    elif isinstance(node, list):
        if len(node) >= 8 and str(node[1]) == str(league_id) and str(node[0]).isdigit():
            yield round_label, node
            return
        for child in node:
            yield from _walk_matches(child, league_id, round_label)


def _flatten_calendar(data: Dict[str, Any], league_id: str) -> List[Dict[str, Any]]:
    teams = {str(row[0]): str(row[1]) for row in data.get("TeamInfo", []) if isinstance(row, list) and len(row) > 1}
    found: Dict[str, Dict[str, Any]] = {}
    for round_label, row in _walk_matches(data.get("ScheduleList") or {}, league_id):
        match_id = str(row[0])
        hs, away_score = _score(row[6] if len(row) > 6 else "")
        found[match_id] = {
            "match_id": match_id,
            "round": round_label,
            "match_date": str(row[3]),
            "home_team_id": str(row[4]),
            "home_team": teams.get(str(row[4]), str(row[4])),
            "away_team_id": str(row[5]),
            "away_team": teams.get(str(row[5]), str(row[5])),
            "home_score": hs,
            "away_score": away_score,
            "source_state": int(row[2]) if str(row[2]).lstrip("-").isdigit() else 0,
            "source_home_rank": row[8] if len(row) > 8 else None,
            "source_away_rank": row[9] if len(row) > 9 else None,
            "visible_ah": row[10] if len(row) > 10 else None,
            "visible_ou": row[12] if len(row) > 12 else None,
        }
    return sorted(found.values(), key=lambda item: (item["match_date"], item["match_id"]))


def _team_snapshot(stats: Dict[str, Dict[str, int]], team_id: str) -> Dict[str, Any]:
    row = stats.get(team_id, {})
    played = int(row.get("played", 0))
    return {
        "played": played,
        "wins": int(row.get("wins", 0)),
        "draws": int(row.get("draws", 0)),
        "losses": int(row.get("losses", 0)),
        "gf": int(row.get("gf", 0)),
        "ga": int(row.get("ga", 0)),
        "gd": int(row.get("gf", 0)) - int(row.get("ga", 0)),
        "points": int(row.get("points", 0)),
        "ppg": round(int(row.get("points", 0)) / played, 3) if played else None,
    }


def _add_result(stats: Dict[str, Dict[str, int]], team: str, gf: int, ga: int) -> None:
    row = stats.setdefault(team, defaultdict(int))
    row["played"] += 1
    row["gf"] += gf
    row["ga"] += ga
    if gf > ga:
        row["wins"] += 1
        row["points"] += 3
    elif gf == ga:
        row["draws"] += 1
        row["points"] += 1
    else:
        row["losses"] += 1


def _standings_context(matches: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    total: Dict[str, Dict[str, int]] = {}
    home: Dict[str, Dict[str, int]] = {}
    away: Dict[str, Dict[str, int]] = {}
    contexts: Dict[str, Dict[str, Any]] = {}
    goals_so_far: List[int] = []
    for match in matches:
        ranked = sorted(
            {match["home_team_id"], match["away_team_id"], *total.keys()},
            key=lambda tid: (
                -int(total.get(tid, {}).get("points", 0)),
                -(int(total.get(tid, {}).get("gf", 0)) - int(total.get(tid, {}).get("ga", 0))),
                -int(total.get(tid, {}).get("gf", 0)),
                tid,
            ),
        )
        ranks = {tid: index + 1 for index, tid in enumerate(ranked)}
        home_id, away_id = match["home_team_id"], match["away_team_id"]
        home_total, away_total = _team_snapshot(total, home_id), _team_snapshot(total, away_id)
        contexts[match["match_id"]] = {
            "home": {**home_total, "rank": ranks.get(home_id), "venue": _team_snapshot(home, home_id)},
            "away": {**away_total, "rank": ranks.get(away_id), "venue": _team_snapshot(away, away_id)},
            "rank_gap": (ranks.get(away_id) or 0) - (ranks.get(home_id) or 0),
            "league_matches_before": len(goals_so_far),
            "league_goal_avg_before": round(sum(goals_so_far) / len(goals_so_far), 3) if goals_so_far else None,
        }
        hs, aws = match.get("home_score"), match.get("away_score")
        if hs is None or aws is None:
            continue
        _add_result(total, home_id, hs, aws)
        _add_result(total, away_id, aws, hs)
        _add_result(home, home_id, hs, aws)
        _add_result(away, away_id, aws, hs)
        goals_so_far.append(hs + aws)
    return contexts


def _as_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _summary_odds(session: requests.Session, match_id: str) -> List[Dict[str, Any]]:
    referer = f"{LIVE_BASE}/oddscomp/{match_id}"
    url = f"{LIVE_BASE}/ajax/soccerajax?type=14&t=1&id={match_id}&h=0&s=-1"
    payload = _get_json(session, url, referer)
    data = payload.get("Data") or {}
    return data.get("mixodds") or data.get("oddsList") or []


def _detail_odds(session: requests.Session, match_id: str, company_id: int) -> Dict[str, List[Dict[str, Any]]]:
    referer = f"{LIVE_BASE}/oddscomp/{match_id}"
    url = f"{LIVE_BASE}/ajax/soccerajax?type=14&id={match_id}&t=20&cid={company_id}&h=0&r1=0&r2=0&r3=0"
    payload = _get_json(session, url, referer)
    data = payload.get("Data") or {}
    return {"AH": data.get("ah") or [], "OU": data.get("ou") or []}


def _snapshot(match: Dict[str, Any], company_id: int, company: str, phase: str,
              observed_at: str, sequence: int, odds: Dict[str, Any], source: str,
              record: Optional[Dict[str, Any]] = None, market: str = "OU") -> Dict[str, Any]:
    record = record or {}
    return {
        "league_id": match["league_id"], "season": match["season"], "match_id": match["match_id"],
        "company_id": int(company_id), "company_name": company, "market": market, "phase": phase,
        "observed_at": observed_at, "sequence_no": sequence,
        "line": _as_float(odds.get("g")), "over_price": _as_float(odds.get("u")),
        "under_price": _as_float(odds.get("d")), "home_score": record.get("hs"),
        "away_score": record.get("gs"), "minute": str(record.get("ht") or ""),
        "is_closed": 1 if record.get("close") else 0, "source_kind": source,
    }


def _collect_match_odds(match: Dict[str, Any], companies: Tuple[int, ...]) -> Tuple[List[Dict[str, Any]], List[str]]:
    session = _session()
    snapshots: List[Dict[str, Any]] = []
    errors: List[str] = []
    company_names: Dict[int, str] = {}
    try:
        summary = _summary_odds(session, match["match_id"])
        for item in summary:
            cid = int(item.get("cid"))
            name = str(item.get("cn") or f"Casa {cid}")
            company_names[cid] = name
            for market, market_key in (("AH", "ah"), ("OU", "ou")):
                odds = item.get(market_key) or (item.get("odds") if market == "OU" else {}) or {}
                for seq, (label, key) in enumerate((("opening", "f"), ("closing", "l"))):
                    values = odds.get(key) or {}
                    if _as_float(values.get("g")) is None:
                        continue
                    snapshots.append(_snapshot(match, cid, name, "pre_match", label, seq, values, "summary", market=market))
    except Exception as exc:
        errors.append(f"{match['match_id']}: resumen: {exc}")

    for cid in companies:
        try:
            records_by_market = _detail_odds(session, match["match_id"], cid)
            name = company_names.get(cid, f"Casa {cid}")
            for market, records in records_by_market.items():
                snapshots.extend(_rows_from_detail(match, cid, name, records, market=market))
        except Exception as exc:
            errors.append(f"{match['match_id']}: casa {cid}: {exc}")
    return snapshots, errors


def _rows_from_summary(match: Dict[str, Any], summary: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[int, str]]:
    rows: List[Dict[str, Any]] = []
    names: Dict[int, str] = {}
    for item in summary or []:
        cid = int(item.get("cid"))
        name = str(item.get("cn") or f"Casa {cid}")
        names[cid] = name
        for market, market_key in (("AH", "ah"), ("OU", "ou")):
            odds = item.get(market_key) or (item.get("odds") if market == "OU" else {}) or {}
            for seq, (label, key) in enumerate((("opening", "f"), ("closing", "l"))):
                values = odds.get(key) or {}
                if _as_float(values.get("g")) is not None:
                    rows.append(_snapshot(match, cid, name, "pre_match", label, seq, values, "summary", market=market))
    return rows, names


def _rows_from_detail(match: Dict[str, Any], cid: int, name: str,
                      records: List[Dict[str, Any]], market: str = "OU") -> List[Dict[str, Any]]:
    rows = []
    for sequence, record in enumerate(reversed(records or [])):
        values = record.get("odds") or {}
        if _as_float(values.get("g")) is None or record.get("close"):
            continue
        stamp = str(record.get("mt") or sequence)
        phase = "pre_match" if int(record.get("type") or 0) in (1, 2) else "in_play"
        rows.append(_snapshot(match, cid, name, phase, stamp, sequence, values, "timeline", record, market=market))
    return rows


def _browser_backfill(matches: List[Dict[str, Any]], companies: Tuple[int, ...],
                      progress: Optional[Callable[[Dict[str, Any]], None]] = None) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Fallback local con navegador real cuando NowGoal devuelve ``code 1001`` a requests."""
    if not matches:
        return [], []
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return [], [f"Playwright no disponible: {exc}"]

    chrome_candidates = (
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    )
    executable = next((str(path) for path in chrome_candidates if path.exists()), None)
    rows: List[Dict[str, Any]] = []
    errors: List[str] = []
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True, executable_path=executable) if executable else playwright.chromium.launch(headless=True)
            page = browser.new_page()
            first_id = matches[0]["match_id"]
            page.goto(f"{LIVE_BASE}/oddscomp/{first_id}", wait_until="domcontentloaded", timeout=90000)
            for index, match in enumerate(matches, start=1):
                try:
                    summary = page.evaluate(
                        "async u => { const r = await fetch(u); return await r.json(); }",
                        f"/ajax/soccerajax?type=14&t=1&id={match['match_id']}&h=0&s=-1",
                    )
                    summary_rows, names = _rows_from_summary(
                        match, (((summary.get("Data") or {}).get("mixodds") or
                                 (summary.get("Data") or {}).get("oddsList") or []) if isinstance(summary, dict) else [])
                    )
                    rows.extend(summary_rows)
                    for cid in companies:
                        detail = page.evaluate(
                            "async u => { const r = await fetch(u); return await r.json(); }",
                            f"/ajax/soccerajax?type=14&id={match['match_id']}&t=20&cid={cid}&h=0&r1=0&r2=0&r3=0",
                        )
                        detail_data = (detail.get("Data") or {}) if isinstance(detail, dict) else {}
                        for market in ("AH", "OU"):
                            records = detail_data.get(market.lower()) or []
                            rows.extend(_rows_from_detail(match, cid, names.get(cid, f"Casa {cid}"), records, market=market))
                except Exception as exc:
                    errors.append(f"{match['match_id']}: navegador: {exc}")
                if progress and (index == 1 or index % 5 == 0 or index == len(matches)):
                    progress({"browser_fallback": True, "completed": index, "total": len(matches), "snapshots": len(rows)})
            browser.close()
    except Exception as exc:
        errors.append(f"Fallback navegador: {exc}")
    return rows, errors


def _upsert_season(league_id: str, season: str, league_name: str,
                   matches: List[Dict[str, Any]], odds: List[Dict[str, Any]]) -> None:
    _ensure_schema()
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    contexts = _standings_context(matches)
    with sql_store._connect() as conn:
        for match in matches:
            source = {key: match.get(key) for key in ("source_home_rank", "source_away_rank", "visible_ah", "visible_ou")}
            source["league_name"] = league_name
            conn.execute(
                """INSERT INTO league_market_matches(
                    league_id,season,match_id,round_label,match_date,home_team_id,home_team,
                    away_team_id,away_team,home_score,away_score,source_state,context_json,source_json,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(league_id,season,match_id) DO UPDATE SET
                    round_label=excluded.round_label,match_date=excluded.match_date,
                    home_team_id=excluded.home_team_id,home_team=excluded.home_team,
                    away_team_id=excluded.away_team_id,away_team=excluded.away_team,
                    home_score=excluded.home_score,away_score=excluded.away_score,
                    source_state=excluded.source_state,context_json=excluded.context_json,
                    source_json=excluded.source_json,updated_at=excluded.updated_at""",
                (league_id, season, match["match_id"], match["round"], match["match_date"],
                 match["home_team_id"], match["home_team"], match["away_team_id"], match["away_team"],
                 match["home_score"], match["away_score"], match["source_state"],
                 json.dumps(contexts.get(match["match_id"], {}), ensure_ascii=False),
                 json.dumps(source, ensure_ascii=False), now, now),
            )
        for item in odds:
            conn.execute(
                """INSERT INTO league_market_odds(
                    league_id,season,match_id,company_id,company_name,market,phase,observed_at,
                    sequence_no,line,over_price,under_price,home_score,away_score,minute,is_closed,source_kind,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(league_id,season,match_id,company_id,market,phase,observed_at,sequence_no)
                DO UPDATE SET line=excluded.line,over_price=excluded.over_price,under_price=excluded.under_price,
                    home_score=excluded.home_score,away_score=excluded.away_score,minute=excluded.minute,
                    is_closed=excluded.is_closed,company_name=excluded.company_name""",
                (item["league_id"], item["season"], item["match_id"], item["company_id"],
                 item["company_name"], item["market"], item["phase"], item["observed_at"],
                 item["sequence_no"], item["line"], item["over_price"], item["under_price"],
                 item["home_score"], item["away_score"], item["minute"], item["is_closed"],
                 item["source_kind"], now),
            )


def sync_league(league_id: str = DEFAULT_LEAGUE_ID, seasons: Iterable[str] = ("2025",),
                companies: Iterable[int] = DEFAULT_COMPANIES, workers: int = 6,
                progress: Optional[Callable[[Dict[str, Any]], None]] = None) -> Dict[str, Any]:
    """Descarga temporadas completas y persiste todos los partidos y movimientos."""
    _ensure_schema()
    season_values = [str(value) for value in seasons]
    company_values = tuple(int(value) for value in companies)
    started = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with sql_store._connect() as conn:
        cursor = conn.execute(
            "INSERT INTO league_market_sync_runs(league_id,seasons_json,status,started_at) VALUES(?,?,?,?)",
            (str(league_id), json.dumps(season_values), "running", started),
        )
        run_id = cursor.lastrowid

    all_errors: List[str] = []
    total_matches = total_with_odds = total_snapshots = 0
    try:
        for season in season_values:
            session = _session()
            actual, data_url, league_name = _discover_season(session, str(league_id), season)
            calendar = _flatten_calendar(_get_json(session, data_url), str(league_id))
            for match in calendar:
                match.update({"league_id": str(league_id), "season": actual})
            total_matches += len(calendar)
            season_odds: List[Dict[str, Any]] = []
            completed = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(int(workers), 10))) as executor:
                futures = {executor.submit(_collect_match_odds, match, company_values): match for match in calendar}
                for future in concurrent.futures.as_completed(futures):
                    rows, errors = future.result()
                    season_odds.extend(rows)
                    all_errors.extend(errors)
                    completed += 1
                    if rows:
                        total_with_odds += 1
                    if progress:
                        progress({"season": season, "completed": completed, "total": len(calendar),
                                  "matches_seen": total_matches, "snapshots": total_snapshots + len(season_odds)})
            covered = {row["match_id"] for row in season_odds if row["source_kind"] == "summary"}
            missing = [match for match in calendar if match["match_id"] not in covered]
            if missing:
                browser_rows, browser_errors = _browser_backfill(missing, company_values, progress)
                season_odds.extend(browser_rows)
                all_errors.extend(browser_errors)
                browser_covered = {row["match_id"] for row in browser_rows if row["source_kind"] == "summary"}
                total_with_odds += len(browser_covered)
            total_snapshots += len(season_odds)
            _upsert_season(str(league_id), actual, league_name, calendar, season_odds)
        coverage = (total_with_odds / total_matches) if total_matches else 0
        if coverage < .9:
            all_errors.append(f"Cobertura de cuotas incompleta: {total_with_odds}/{total_matches}")
        status = "complete" if not all_errors else "partial"
    except Exception as exc:
        all_errors.append(str(exc))
        status = "failed"
    finished = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with sql_store._connect() as conn:
        conn.execute(
            """UPDATE league_market_sync_runs SET status=?,matches_seen=?,matches_with_odds=?,
               snapshots_saved=?,errors_json=?,finished_at=? WHERE id=?""",
            (status, total_matches, total_with_odds, total_snapshots,
             json.dumps(all_errors[:200], ensure_ascii=False), finished, run_id),
        )
    return {"run_id": run_id, "status": status, "league_id": str(league_id), "seasons": season_values,
            "matches_seen": total_matches, "matches_with_odds": total_with_odds,
            "snapshots_saved": total_snapshots, "errors": all_errors[:25]}


def _movement_band(delta: Optional[float]) -> str:
    if delta is None:
        return "unknown"
    if delta >= .24:
        return "line_up"
    if delta <= -.24:
        return "line_down"
    return "stable"


def _rank_band(value: Any) -> str:
    try:
        gap = int(value)
    except (TypeError, ValueError):
        return "unknown"
    if gap >= 5:
        return "home_stronger"
    if gap <= -5:
        return "away_stronger"
    return "similar"


def get_overview(league_id: str = DEFAULT_LEAGUE_ID, season: str = "2025",
                 company_id: int = 8) -> Dict[str, Any]:
    _ensure_schema()
    with sql_store._connect() as conn:
        match_rows = conn.execute(
            "SELECT * FROM league_market_matches WHERE league_id=? AND season=? ORDER BY match_date,match_id",
            (str(league_id), str(season)),
        ).fetchall()
        season_rows = conn.execute(
            "SELECT season,COUNT(*) matches FROM league_market_matches WHERE league_id=? GROUP BY season ORDER BY season DESC",
            (str(league_id),),
        ).fetchall()
        sync_row = conn.execute(
            "SELECT * FROM league_market_sync_runs WHERE league_id=? ORDER BY id DESC LIMIT 1", (str(league_id),)
        ).fetchone()

        historical_raw = conn.execute(
            """SELECT m.season,m.home_score,m.away_score,m.context_json,
               MAX(CASE WHEN o.observed_at='opening' THEN o.line END) AS open_line,
               MAX(CASE WHEN o.observed_at='closing' THEN o.line END) AS close_line
               FROM league_market_matches m
               LEFT JOIN league_market_odds o ON o.league_id=m.league_id AND o.season=m.season
                    AND o.match_id=m.match_id AND o.company_id=? AND o.phase='pre_match' AND o.market='OU'
                    AND o.source_kind='summary'
               WHERE m.league_id=? AND m.season<?
               GROUP BY m.season,m.match_id,m.home_score,m.away_score,m.context_json""",
            (int(company_id), str(league_id), str(season)),
        ).fetchall()

        matches: List[Dict[str, Any]] = []
        for raw in match_rows:
            row = dict(raw)
            context = json.loads(row.pop("context_json") or "{}")
            source = json.loads(row.pop("source_json") or "{}")
            odds = conn.execute(
                """SELECT company_name,observed_at,line,over_price,under_price,source_kind
                   FROM league_market_odds WHERE league_id=? AND season=? AND match_id=?
                   AND company_id=? AND phase='pre_match' AND market='OU' ORDER BY
                   CASE observed_at WHEN 'opening' THEN 0 WHEN 'closing' THEN 999999 ELSE CAST(observed_at AS INTEGER) END,
                   sequence_no""",
                (str(league_id), str(season), row["match_id"], int(company_id)),
            ).fetchall()
            odds_values = [dict(item) for item in odds]
            timeline = [item for item in odds_values if item["source_kind"] == "timeline"]
            opening = next((item for item in odds_values if item["observed_at"] == "opening"), None)
            closing = next((item for item in odds_values if item["observed_at"] == "closing"), None)
            if not opening and timeline:
                opening = timeline[0]
            if not closing and timeline:
                closing = timeline[-1]
            open_line, close_line = (opening or {}).get("line"), (closing or {}).get("line")
            delta = round(close_line - open_line, 2) if open_line is not None and close_line is not None else None
            goals = None if row["home_score"] is None or row["away_score"] is None else row["home_score"] + row["away_score"]
            outcome = None
            if goals is not None and close_line is not None:
                outcome = "over" if goals > close_line else ("under" if goals < close_line else "push")
            prior_avg = context.get("league_goal_avg_before")
            matches.append({
                **row, "context": context, "source": source, "opening": opening, "closing": closing,
                "open_line": open_line, "close_line": close_line, "line_delta": delta,
                "movement": _movement_band(delta), "rank_context": _rank_band(context.get("rank_gap")),
                "goals": goals, "outcome": outcome, "timeline_points": len(timeline),
                "line_vs_prior_avg": round(open_line - prior_avg, 2) if open_line is not None and prior_avg is not None else None,
            })

    historical: List[Dict[str, Any]] = []
    for raw in historical_raw:
        row = dict(raw)
        if row["home_score"] is None or row["away_score"] is None or row["close_line"] is None:
            continue
        goals = row["home_score"] + row["away_score"]
        outcome = "over" if goals > row["close_line"] else ("under" if goals < row["close_line"] else "push")
        if outcome == "push":
            continue
        context = json.loads(row.get("context_json") or "{}")
        delta = round(row["close_line"] - row["open_line"], 2) if row["open_line"] is not None else None
        prior_avg = context.get("league_goal_avg_before")
        historical.append({
            "season": row["season"], "outcome": outcome, "movement": _movement_band(delta),
            "rank_context": _rank_band(context.get("rank_gap")), "open_line": row["open_line"],
            "line_vs_prior_avg": round(row["open_line"] - prior_avg, 2)
            if row["open_line"] is not None and prior_avg is not None else None,
        })

    settled = [row for row in matches if row["outcome"] in ("over", "under")]
    training_rows = historical or settled
    movement_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in training_rows:
        movement_groups[row["movement"]].append(row)
    patterns = []
    labels = {"line_up": "La casa sube la línea", "line_down": "La casa baja la línea", "stable": "La línea permanece estable"}
    for key in ("line_up", "line_down", "stable"):
        sample = movement_groups.get(key, [])
        if not sample:
            continue
        overs = sum(item["outcome"] == "over" for item in sample)
        unders = sum(item["outcome"] == "under" for item in sample)
        patterns.append({"key": key, "label": labels[key], "sample": len(sample),
                         "over_pct": round(overs * 100 / len(sample), 1),
                         "under_pct": round(unders * 100 / len(sample), 1),
                         "reliability": "alta" if len(sample) >= 30 else ("media" if len(sample) >= 15 else "baja")})

    contextual_specs = (
        ("high_placement", "Línea por encima de la media previa", lambda row: row.get("line_vs_prior_avg") is not None and row["line_vs_prior_avg"] >= .25),
        ("low_placement", "Línea por debajo de la media previa", lambda row: row.get("line_vs_prior_avg") is not None and row["line_vs_prior_avg"] <= -.25),
        ("home_gap", "Local claramente mejor clasificado", lambda row: row.get("rank_context") == "home_stronger"),
        ("away_gap", "Visitante claramente mejor clasificado", lambda row: row.get("rank_context") == "away_stronger"),
    )
    for key, label, predicate in contextual_specs:
        sample = [row for row in training_rows if predicate(row)]
        if len(sample) < 8:
            continue
        overs = sum(item["outcome"] == "over" for item in sample)
        patterns.append({"key": key, "label": label, "sample": len(sample),
                         "over_pct": round(overs * 100 / len(sample), 1),
                         "under_pct": round((len(sample) - overs) * 100 / len(sample), 1),
                         "reliability": "alta" if len(sample) >= 30 else ("media" if len(sample) >= 15 else "baja")})

    rounds: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in settled:
        rounds[str(row.get("round_label") or "-")].append(row)
    evolution = []
    for round_label, sample in rounds.items():
        evolution.append({"round": round_label, "matches": len(sample),
                          "avg_open": round(sum(x["open_line"] for x in sample if x["open_line"] is not None) /
                                            max(1, sum(x["open_line"] is not None for x in sample)), 2),
                          "avg_goals": round(sum(x["goals"] for x in sample) / len(sample), 2),
                          "over_pct": round(sum(x["outcome"] == "over" for x in sample) * 100 / len(sample), 1)})
    evolution.sort(key=lambda item: int(item["round"]) if str(item["round"]).isdigit() else 999)

    league_name = next((item["source"].get("league_name") for item in matches if item["source"].get("league_name")), "Iceland Division 1")
    return {
        "league_id": str(league_id), "league_name": league_name, "season": str(season),
        "available_seasons": [dict(item) for item in season_rows], "company_id": int(company_id),
        "matches": matches, "patterns": patterns, "evolution": evolution,
        "summary": {"matches": len(matches), "settled": len(settled),
                    "with_odds": sum(row["open_line"] is not None for row in matches),
                    "over_pct": round(sum(row["outcome"] == "over" for row in settled) * 100 / len(settled), 1) if settled else None,
                    "avg_goals": round(sum(row["goals"] for row in settled) / len(settled), 2) if settled else None,
                    "historical_training_matches": len(historical),
                    "training_seasons": sorted({row["season"] for row in historical}),
                    "avg_open_line": round(sum(row["open_line"] for row in settled if row["open_line"] is not None) /
                                           max(1, sum(row["open_line"] is not None for row in settled)), 2) if settled else None},
        "last_sync": dict(sync_row) if sync_row else None,
        "methodology": ("Patrones entrenados solo con temporadas anteriores: "
                        + (", ".join(sorted({row['season'] for row in historical})) if historical else "sin histórico previo")
                        + ". Clasificación reconstruida solo con partidos anteriores. Movimiento = cierre - apertura. Directo separado del prepartido."),
    }


def get_match_timeline(league_id: str, season: str, match_id: str) -> Dict[str, Any]:
    _ensure_schema()
    with sql_store._connect() as conn:
        match = conn.execute(
            "SELECT * FROM league_market_matches WHERE league_id=? AND season=? AND match_id=?",
            (str(league_id), str(season), str(match_id)),
        ).fetchone()
        if not match:
            return {"available": False}
        odds = conn.execute(
            """SELECT company_id,company_name,market,phase,observed_at,sequence_no,line,over_price,
               under_price,home_score,away_score,minute,is_closed,source_kind
               FROM league_market_odds WHERE league_id=? AND season=? AND match_id=?
               ORDER BY company_id,CASE market WHEN 'AH' THEN 0 ELSE 1 END,
               CASE phase WHEN 'pre_match' THEN 0 ELSE 1 END,
               CASE observed_at WHEN 'opening' THEN 0 WHEN 'closing' THEN 9999999999
               ELSE CAST(observed_at AS INTEGER) END,sequence_no""",
            (str(league_id), str(season), str(match_id)),
        ).fetchall()
    result = dict(match)
    result["context"] = json.loads(result.pop("context_json") or "{}")
    result["source"] = json.loads(result.pop("source_json") or "{}")
    result["odds"] = [dict(row) for row in odds]
    result["available"] = True
    return result


__all__ = ["available_seasons", "get_match_timeline", "get_overview", "sync_league"]
