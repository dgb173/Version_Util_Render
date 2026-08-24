"""Fast server-side query helpers for the Pre-Cacheo pending-results view."""

from __future__ import annotations

import datetime as dt
import json
import math
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from . import data_manager, precache_fast_store, sql_store


UTC = dt.timezone.utc
SPAIN_TZ = ZoneInfo("Europe/Madrid")


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


def _fetch_sql_candidates(
    handicap_buckets: Optional[Sequence[str]],
    buckets: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Fetch lightweight candidate headers directly from SQL."""
    sql_store.ensure_bootstrap()
    selected_buckets = list(
        buckets or [data_manager.PRECACHEO_BUCKET, data_manager.PENDING_RESULTS_BUCKET]
    )
    if not selected_buckets:
        return []
    placeholders = ", ".join("?" for _ in selected_buckets)
    query = """
        SELECT
            match_id,
            handicap,
            COALESCE(score, json_extract(payload_json, '$.final_score')) AS score,
            COALESCE(match_date, json_extract(payload_json, '$.date')) AS match_date,
            json_extract(payload_json, '$.start_time') AS start_time,
            json_extract(payload_json, '$.time') AS match_time
        FROM matches
        WHERE bucket IN ({placeholders})
    """.format(placeholders=placeholders)
    params: List[Any] = selected_buckets

    handicap_sql, handicap_params = _handicap_bucket_sql(handicap_buckets)
    if handicap_sql:
        query += f" AND {handicap_sql}"
        params.extend(handicap_params)
    query += " ORDER BY updated_at DESC"

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


def _fetch_candidates(handicap_buckets: Optional[Sequence[str]]) -> List[Dict[str, Any]]:
    """Fetch only lightweight headers needed to filter, count and sort."""
    if precache_fast_store.available():
        return precache_fast_store.load_headers(handicap_buckets)
    return _fetch_sql_candidates(handicap_buckets)


def _fetch_payloads_by_ids(match_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    ordered_ids = [str(match_id) for match_id in match_ids or [] if str(match_id)]
    if not ordered_ids:
        return {}
    if precache_fast_store.available():
        return precache_fast_store.load_payloads(ordered_ids)

    placeholders = ", ".join("?" for _ in ordered_ids)
    query = (
        "SELECT match_id, payload_json, explorer_json "
        f"FROM matches WHERE match_id IN ({placeholders})"
    )
    output: Dict[str, Dict[str, Any]] = {}
    # Iterate the cursor instead of fetchall(): otherwise 100 full JSON strings,
    # their compact copies and their parsed dictionaries coexist in memory.
    with sql_store._connect() as conn:
        for row in conn.execute(query, ordered_ids):
            base: Dict[str, Any] = {}
            if row["explorer_json"]:
                try:
                    base = json.loads(row["explorer_json"])
                except Exception:
                    base = {}

            if not base and row["payload_json"]:
                try:
                    base = json.loads(row["payload_json"])
                except Exception:
                    base = {}

            if not isinstance(base, dict) or not base:
                continue

            # Strip bloated HTML fields to maintain low memory profile (<100MB RAM)
            base.pop('historical_matches_html', None)
            base.pop('market_analysis_html', None)
            base.pop('raw_html', None)
            base.pop('full_html', None)

            output[str(row["match_id"])] = base
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


def fetch_upcoming_ids_from_sql(
    buckets: Optional[Sequence[str]] = None,
    limit: int = 200,
    now: Optional[dt.datetime] = None,
) -> List[str]:
    """Return chronological upcoming IDs directly from SQL, bypassing fast snapshots."""
    now_utc = now or dt.datetime.now(UTC)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=UTC)
    else:
        now_utc = now_utc.astimezone(UTC)

    upcoming: List[Tuple[dt.datetime, str]] = []
    seen_ids: set[str] = set()
    for row in _fetch_sql_candidates(None, buckets=buckets):
        match_id = str(row.get("match_id") or row.get("id") or "").strip()
        if not match_id or match_id in seen_ids or _has_final_score(row):
            continue
        scheduled_at = _scheduled_at_utc(row)
        if not scheduled_at or scheduled_at <= now_utc:
            continue
        seen_ids.add(match_id)
        upcoming.append((scheduled_at, match_id))

    upcoming.sort(key=lambda item: (item[0], item[1]))
    safe_limit = max(1, int(limit or 200))
    return [match_id for _, match_id in upcoming[:safe_limit]]


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
    per_page = max(1, min(int(per_page or 100), 2000))
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
    per_page = max(1, min(int(per_page or 100), 2000))
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
