"""Fast server-side query helpers for the Pre-Cacheo pending-results view."""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import math
import os
import re
import threading
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import ijson
import requests

from . import data_manager, sql_store


UTC = dt.timezone.utc
SPAIN_TZ = ZoneInfo("Europe/Madrid")


def _http_arg(value: Any) -> Dict[str, Any]:
    """Encode one positional argument for Turso's SQL-over-HTTP API."""
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "integer", "value": "1" if value else "0"}
    if isinstance(value, int):
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        return {"type": "float", "value": str(value)}
    if isinstance(value, (bytes, bytearray)):
        return {"type": "blob", "base64": base64.b64encode(value).decode("ascii")}
    return {"type": "text", "value": str(value)}


def _http_value(cell: Any) -> Any:
    """Decode one Hrana value returned by Turso."""
    if not isinstance(cell, dict):
        return cell
    value_type = cell.get("type")
    if value_type == "null":
        return None
    if value_type == "integer":
        try:
            return int(cell.get("value"))
        except (TypeError, ValueError):
            return cell.get("value")
    if value_type == "float":
        try:
            return float(cell.get("value"))
        except (TypeError, ValueError):
            return cell.get("value")
    if value_type == "blob":
        try:
            return base64.b64decode(cell.get("base64") or "")
        except (TypeError, ValueError):
            return b""
    return cell.get("value")


def _remote_query(sql: str, params: Sequence[Any]) -> Optional[List[Dict[str, Any]]]:
    """Run a small read directly on Turso, without downloading an embedded replica."""
    database_url = str(os.getenv("LIBSQL_URL") or "").strip()
    auth_token = str(os.getenv("LIBSQL_AUTH_TOKEN") or "").strip()
    if not database_url or not auth_token:
        return None

    if database_url.startswith("libsql://"):
        database_url = "https://" + database_url[len("libsql://"):]
    elif database_url.startswith("turso://"):
        database_url = "https://" + database_url[len("turso://"):]

    response = requests.post(
        f"{database_url.rstrip('/')}/v2/pipeline",
        headers={
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        },
        json={
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": sql,
                        "args": [_http_arg(value) for value in params],
                    },
                },
                {"type": "close"},
            ]
        },
        timeout=(4, 15),
    )
    response.raise_for_status()
    payload = response.json()
    results = payload.get("results") if isinstance(payload, dict) else None
    first = results[0] if isinstance(results, list) and results else None
    if not isinstance(first, dict) or first.get("type") != "ok":
        raise RuntimeError(f"Turso query failed: {first}")

    result = (((first.get("response") or {}).get("result")) or {})
    columns = [
        str(column.get("name") or "")
        for column in result.get("cols", [])
        if isinstance(column, dict)
    ]
    output: List[Dict[str, Any]] = []
    for raw_row in result.get("rows", []) or []:
        if not isinstance(raw_row, list):
            continue
        output.append({
            name: _http_value(raw_row[index]) if index < len(raw_row) else None
            for index, name in enumerate(columns)
        })
    return output


_FILE_STORE_LOCK = threading.Lock()
_FILE_STORE: Optional[Tuple[List[Dict[str, Any]], bool]] = None
_FAST_STORE_DIR = sql_store.DATA_DIR / ".precacheo_fast"
_FAST_INDEX_FILE = _FAST_STORE_DIR / "index.json"


def _fast_payload_path(match_id: str):
    digest = hashlib.sha256(str(match_id).encode("utf-8")).hexdigest()
    return _FAST_STORE_DIR / f"{digest}.json"


def _handicap_value_matches(raw_value: Any, selected_values: Optional[Sequence[str]]) -> bool:
    """Mirror the UI handicap buckets for JSON-backed reads."""
    if not selected_values:
        return True
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return False

    for raw_target in selected_values:
        try:
            target = float(raw_target)
        except (TypeError, ValueError):
            continue
        magnitude = abs(target)
        if magnitude >= 2.49:
            if (target > 0 and value >= 2.24) or (target < 0 and value <= -2.24):
                return True
        elif abs(magnitude - 2.0) < 0.01:
            if target - 0.01 <= value <= target + 0.01:
                return True
        elif abs(magnitude - 1.5) < 0.1:
            low, high = ((1.24, 1.76) if target > 0 else (-1.76, -1.24))
            if low <= value <= high:
                return True
        elif abs(magnitude - 1.0) < 0.1:
            if target - 0.1 <= value <= target + 0.1:
                return True
        elif abs(magnitude - 0.5) < 0.1:
            low, high = ((0.24, 0.76) if target > 0 else (-0.76, -0.24))
            if low <= value <= high:
                return True
        elif magnitude < 0.1:
            if -0.1 <= value <= 0.1:
                return True
        elif target - 0.01 <= value <= target + 0.01:
            return True
    return False


def _iter_deployed_rows(path: Any):
    """Yield one JSON-array row at a time with bounded memory."""
    with path.open("rb") as handle:
        for row in ijson.items(handle, "item", use_float=True):
            if isinstance(row, dict):
                yield row


def _load_json_file_store() -> Tuple[List[Dict[str, Any]], bool]:
    """Cache only lightweight headers from the deployed live buckets."""
    global _FILE_STORE
    with _FILE_STORE_LOCK:
        if _FILE_STORE is not None:
            return _FILE_STORE

        if _FAST_INDEX_FILE.exists():
            try:
                with _FAST_INDEX_FILE.open("r", encoding="utf-8") as handle:
                    fast_headers = json.load(handle)
                if isinstance(fast_headers, list):
                    _FILE_STORE = (
                        [row for row in fast_headers if isinstance(row, dict)],
                        True,
                    )
                    return _FILE_STORE
            except Exception:
                pass

        headers_by_id: Dict[str, Dict[str, Any]] = {}
        loaded_any = False
        for bucket in (data_manager.PRECACHEO_BUCKET, data_manager.PENDING_RESULTS_BUCKET):
            path = sql_store.DATA_DIR / bucket
            if not path.exists():
                continue
            loaded_any = True
            try:
                for row in _iter_deployed_rows(path):
                    match_id = str(row.get("match_id") or row.get("id") or "").strip()
                    if not match_id:
                        continue
                    odds = row.get("main_match_odds") if isinstance(row.get("main_match_odds"), dict) else {}
                    handicap = row.get("handicap")
                    if handicap in (None, ""):
                        handicap = odds.get("ah_linea")
                    headers_by_id[match_id] = {
                        "match_id": match_id,
                        "handicap": handicap,
                        "score": row.get("score") or row.get("final_score"),
                        "match_date": row.get("match_date") or row.get("date"),
                        "start_time": row.get("start_time"),
                        "time": row.get("time"),
                    }
            except Exception:
                continue

        _FILE_STORE = (list(headers_by_id.values()), loaded_any)
        return _FILE_STORE


def _load_json_payloads_by_ids(match_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    """Stream large buckets and retain only rows required by the visible page."""
    wanted = {str(match_id) for match_id in match_ids if str(match_id)}
    output: Dict[str, Dict[str, Any]] = {}
    if not wanted:
        return output

    if _FAST_INDEX_FILE.exists():
        for match_id in wanted:
            path = _fast_payload_path(match_id)
            if not path.exists():
                continue
            try:
                with path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                if isinstance(payload, dict):
                    output[match_id] = payload
            except Exception:
                continue
        return output

    for bucket in (data_manager.PRECACHEO_BUCKET, data_manager.PENDING_RESULTS_BUCKET):
        path = sql_store.DATA_DIR / bucket
        if not path.exists():
            continue
        try:
            for row in _iter_deployed_rows(path):
                match_id = str(row.get("match_id") or row.get("id") or "").strip()
                if match_id in wanted:
                    output[match_id] = row
        except Exception:
            continue
    return output


def _handicap_bucket_sql(selected_values: Optional[Sequence[str]]) -> Tuple[str, List[float]]:
    """Build SQL matching the handicap buckets exposed by the UI."""
    clauses: List[str] = []
    params: List[float] = []
    seen: set[float] = set()

    for raw in selected_values or []:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        rounded = round(value, 4)
        if rounded in seen:
            continue
        seen.add(rounded)

        magnitude = abs(value)
        if magnitude >= 2.49:
            clauses.append("handicap >= ?" if value > 0 else "handicap <= ?")
            params.append(2.24 if value > 0 else -2.24)
        elif abs(magnitude - 2.0) < 0.01:
            clauses.append("handicap BETWEEN ? AND ?")
            center = 2.0 if value > 0 else -2.0
            params.extend([center - 0.01, center + 0.01])
        elif abs(magnitude - 1.5) < 0.1:
            clauses.append("handicap BETWEEN ? AND ?")
            params.extend([1.24, 1.76] if value > 0 else [-1.76, -1.24])
        elif abs(magnitude - 1.0) < 0.1:
            clauses.append("handicap BETWEEN ? AND ?")
            params.extend([value - 0.1, value + 0.1])
        elif abs(magnitude - 0.5) < 0.1:
            clauses.append("handicap BETWEEN ? AND ?")
            params.extend([0.24, 0.76] if value > 0 else [-0.76, -0.24])
        elif magnitude < 0.1:
            clauses.append("handicap BETWEEN ? AND ?")
            params.extend([-0.1, 0.1])
        else:
            clauses.append("handicap BETWEEN ? AND ?")
            params.extend([value - 0.01, value + 0.01])

    if not clauses:
        return "", []
    return "(" + " OR ".join(clauses) + ")", params


def _fetch_candidates(handicap_buckets: Optional[Sequence[str]]) -> List[Dict[str, Any]]:
    """Fetch only lightweight headers needed to filter, count and sort."""
    buckets = [data_manager.PRECACHEO_BUCKET, data_manager.PENDING_RESULTS_BUCKET]
    query = """
        SELECT
            match_id,
            handicap,
            COALESCE(score, json_extract(payload_json, '$.final_score')) AS score,
            COALESCE(match_date, json_extract(payload_json, '$.date')) AS match_date,
            json_extract(payload_json, '$.start_time') AS start_time,
            json_extract(payload_json, '$.time') AS match_time
        FROM matches
        WHERE bucket IN (?, ?)
    """
    params: List[Any] = list(buckets)

    handicap_sql, handicap_params = _handicap_bucket_sql(handicap_buckets)
    if handicap_sql:
        query += f" AND {handicap_sql}"
        params.extend(handicap_params)
    query += " ORDER BY updated_at DESC"

    remote_rows = _remote_query(query, params)
    if remote_rows is not None:
        return remote_rows

    file_headers, files_loaded = _load_json_file_store()
    if files_loaded:
        return [
            row for row in file_headers
            if _handicap_value_matches(row.get("handicap"), handicap_buckets)
        ]

    sql_store.ensure_bootstrap()
    with sql_store._connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [
        {
            "match_id": row["match_id"],
            "handicap": row["handicap"],
            "score": row["score"],
            "match_date": row["match_date"],
            "start_time": row["start_time"],
            "time": row["match_time"],
        }
        for row in rows
    ]


def _fetch_payloads_by_ids(match_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    ordered_ids = [str(match_id) for match_id in match_ids or [] if str(match_id)]
    if not ordered_ids:
        return {}
    placeholders = ", ".join("?" for _ in ordered_ids)
    query = (
        "SELECT match_id, payload_json, explorer_json "
        f"FROM matches WHERE match_id IN ({placeholders})"
    )
    remote_rows = _remote_query(query, ordered_ids)
    if remote_rows is not None:
        rows = remote_rows
    else:
        _, files_loaded = _load_json_file_store()
        if files_loaded:
            return _load_json_payloads_by_ids(ordered_ids)
        with sql_store._connect() as conn:
            rows = conn.execute(query, ordered_ids).fetchall()

    output: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        # Start with payload_json (contains full scrape data with h2h stats)
        base: Dict[str, Any] = {}
        try:
            base = json.loads(row["payload_json"]) if row["payload_json"] else {}
        except (json.JSONDecodeError, TypeError):
            pass
        if not isinstance(base, dict):
            base = {}

        # Overlay explorer_json if it exists (has compact/explorer-oriented fields)
        explorer: Dict[str, Any] = {}
        try:
            explorer = json.loads(row["explorer_json"]) if row["explorer_json"] else {}
        except (json.JSONDecodeError, TypeError):
            pass
        if not isinstance(explorer, dict):
            explorer = {}

        if explorer:
            # Merge: explorer keys override base, but preserve base keys that
            # explorer doesn't have (like h2h_stadium, h2h_general with stats_rows,
            # last_home_match, last_away_match, comparativas_indirectas, etc.)
            merged = {**base, **explorer}
            # Restore deep analysis keys from base when explorer nullifies them
            _DEEP_KEYS = (
                'h2h_stadium', 'h2h_general', 'h2h_col3',
                'last_home_match', 'last_away_match',
                'comparativas_indirectas', 'pre_match_context',
                'market_analysis_data',
            )
            for key in _DEEP_KEYS:
                base_val = base.get(key)
                merged_val = merged.get(key)
                if base_val and not merged_val:
                    merged[key] = base_val
            payload = merged
        else:
            payload = base

        if payload:
            output[str(row["match_id"])] = payload
    return output


def _parse_clock(value: Any) -> Tuple[int, int]:
    match = re.search(r"(\d{1,2}):(\d{2})", str(value or ""))
    if not match:
        return 0, 0
    hour, minute = int(match.group(1)), int(match.group(2))
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return 0, 0
    return hour, minute


def _parse_date(value: Any) -> Optional[Tuple[int, int, int]]:
    text = str(value or "").strip()
    if not text:
        return None

    iso_candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = dt.datetime.fromisoformat(iso_candidate)
        return parsed.year, parsed.month, parsed.day
    except (TypeError, ValueError):
        pass

    iso_match = re.match(r"^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$", text)
    if iso_match:
        return tuple(int(part) for part in iso_match.groups())

    short_match = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})$", text)
    if not short_match:
        return None
    first, second = int(short_match.group(1)), int(short_match.group(2))
    year_text = short_match.group(3)
    year = int(year_text)
    if len(year_text) == 2:
        year += 2000 if year < 70 else 1900

    if first > 12 and second <= 12:
        day, month = first, second
    elif second > 12 and first <= 12:
        month, day = first, second
    else:
        # Nowgoal/Pre-Cacheo stores ambiguous dates as month/day/year.
        month, day = first, second
    return year, month, day


def _scheduled_at_utc(match: Dict[str, Any]) -> Optional[dt.datetime]:
    start_time = match.get("start_time")
    if start_time:
        candidate = str(start_time).strip()
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            parsed = dt.datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            pass

    date_parts = _parse_date(match.get("match_date") or match.get("date"))
    if not date_parts:
        return None
    hour, minute = _parse_clock(match.get("time"))
    try:
        local = dt.datetime(*date_parts, hour, minute, tzinfo=SPAIN_TZ)
    except ValueError:
        return None
    return local.astimezone(UTC)


def _has_final_score(match: Dict[str, Any]) -> bool:
    score = str(match.get("score") or match.get("final_score") or "").strip()
    if not score or "?" in score:
        return False
    return ":" in score or "-" in score


def fetch_pending_page(
    page: int = 1,
    per_page: int = 100,
    handicap_buckets: Optional[Sequence[str]] = None,
    now: Optional[dt.datetime] = None,
    min_age_minutes: int = 30,
    max_age_hours: int = 48,
) -> Dict[str, Any]:
    """Return one recent-first page after filtering candidates on the server."""
    page = max(1, int(page or 1))
    per_page = max(1, min(int(per_page or 100), 100))
    now_utc = now or dt.datetime.now(UTC)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=UTC)
    else:
        now_utc = now_utc.astimezone(UTC)

    pending: List[Tuple[dt.datetime, str]] = []
    headers_by_id: Dict[str, Dict[str, Any]] = {}
    seen_ids: set[str] = set()
    for row in _fetch_candidates(handicap_buckets):
        raw_id = row.get("match_id") or row.get("id")
        match_id = str(raw_id or "").strip()
        if not match_id or match_id in seen_ids or _has_final_score(row):
            continue
        scheduled_at = _scheduled_at_utc(row)
        if not scheduled_at:
            continue
        age = now_utc - scheduled_at
        if age < dt.timedelta(minutes=min_age_minutes) or age > dt.timedelta(hours=max_age_hours):
            continue
        seen_ids.add(match_id)
        headers_by_id[match_id] = row
        pending.append((scheduled_at, match_id))

    pending.sort(key=lambda item: (item[0], item[1]), reverse=True)
    total = len(pending)
    total_pages = max(1, math.ceil(total / per_page))
    page = min(page, total_pages)
    start = (page - 1) * per_page
    page_ids = [item[1] for item in pending[start:start + per_page]]
    payloads = _fetch_payloads_by_ids(page_ids)
    matches = []
    for match_id in page_ids:
        payload = payloads.get(match_id)
        if not isinstance(payload, dict):
            continue
        normalized = dict(payload)
        normalized.pop("market_analysis_html", None)
        normalized.pop("historical_matches_html", None)
        header = headers_by_id.get(match_id, {})
        for field in ("start_time", "match_date", "time", "score", "handicap"):
            if normalized.get(field) in (None, "") and header.get(field) not in (None, ""):
                normalized[field] = header[field]
        normalized["id"] = match_id
        normalized["match_id"] = match_id
        normalized.setdefault("state", "pending_results")
        normalized.setdefault("specialist_picks", [])
        matches.append(normalized)
    return {
        "matches": matches,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "handicap_filters": [str(value) for value in handicap_buckets or []],
    }


def fetch_upcoming_page(
    page: int = 1,
    per_page: int = 100,
    handicap_buckets: Optional[Sequence[str]] = None,
    now: Optional[dt.datetime] = None,
) -> Dict[str, Any]:
    """Return one chronological page of upcoming Pre-Cacheo rows from SQL."""
    page = max(1, int(page or 1))
    per_page = max(1, min(int(per_page or 100), 100))
    now_utc = now or dt.datetime.now(UTC)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=UTC)
    else:
        now_utc = now_utc.astimezone(UTC)

    upcoming: List[Tuple[dt.datetime, str]] = []
    headers_by_id: Dict[str, Dict[str, Any]] = {}
    seen_ids: set[str] = set()
    for row in _fetch_candidates(handicap_buckets):
        match_id = str(row.get("match_id") or row.get("id") or "").strip()
        if not match_id or match_id in seen_ids or _has_final_score(row):
            continue
        scheduled_at = _scheduled_at_utc(row)
        if not scheduled_at:
            continue
        # Próximos termina exactamente al llegar la hora programada. El margen
        # de 30 minutos pertenece únicamente a Resultados Pendientes.
        if scheduled_at <= now_utc:
            continue
        seen_ids.add(match_id)
        headers_by_id[match_id] = row
        upcoming.append((scheduled_at, match_id))

    upcoming.sort(key=lambda item: (item[0], item[1]))
    total = len(upcoming)
    total_pages = max(1, math.ceil(total / per_page))
    page = min(page, total_pages)
    start = (page - 1) * per_page
    page_ids = [item[1] for item in upcoming[start:start + per_page]]
    payloads = _fetch_payloads_by_ids(page_ids)
    matches = []
    for match_id in page_ids:
        payload = payloads.get(match_id)
        if not isinstance(payload, dict):
            continue
        normalized = dict(payload)
        normalized.pop("market_analysis_html", None)
        normalized.pop("historical_matches_html", None)
        header = headers_by_id.get(match_id, {})
        for field in ("start_time", "match_date", "time", "score", "handicap"):
            if normalized.get(field) in (None, "") and header.get(field) not in (None, ""):
                normalized[field] = header[field]
        normalized["id"] = match_id
        normalized["match_id"] = match_id
        normalized.setdefault("state", "precacheo")
        normalized.setdefault("specialist_picks", [])
        matches.append(normalized)
    return {
        "matches": matches,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "handicap_filters": [str(value) for value in handicap_buckets or []],
    }
