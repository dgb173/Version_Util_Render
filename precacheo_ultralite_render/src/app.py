from __future__ import annotations

import json
import math
import os
import sqlite3
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from flask import Flask, jsonify, redirect, render_template, request, url_for

try:
    import libsql as _libsql  # type: ignore
except Exception:
    _libsql = None


APP_DIR = Path(__file__).resolve().parent
SERVICE_ROOT = APP_DIR.parent
PROJECT_ROOT = SERVICE_ROOT.parent
DATA_DIR = PROJECT_ROOT / "data"

PRECACHEO_BUCKET = "data_precacheo.json"
DEFAULT_DB_PATH = DATA_DIR / "app_data.db"
JSON_FALLBACKS = (
    DATA_DIR / "data_precacheo.json",
    DATA_DIR / "data_precacheo.json.bak",
)

DEFAULT_PER_PAGE = max(1, int(os.getenv("PRECACHEO_UI_ITEMS_PER_PAGE", "100")))
MAX_PER_PAGE = max(DEFAULT_PER_PAGE, int(os.getenv("PRECACHEO_UI_MAX_PER_PAGE", "250")))
LIBSQL_URL = os.getenv("LIBSQL_URL", "").strip()
LIBSQL_AUTH_TOKEN = os.getenv("LIBSQL_AUTH_TOKEN", "").strip()

app = Flask(__name__, template_folder=str(APP_DIR / "templates"))


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text in {"None", "null"}:
        return ""
    return text


def _coalesce(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return ""


def _compact_match(raw: Dict[str, Any], updated_at: str = "") -> Dict[str, str]:
    main_odds = raw.get("main_match_odds") or {}
    last_home = raw.get("last_home_match") or {}
    last_away = raw.get("last_away_match") or {}
    h2h_col3 = raw.get("h2h_col3") or {}
    home_standings = raw.get("home_standings") or {}
    away_standings = raw.get("away_standings") or {}

    h2h_score = ""
    h2h_home_goals = _clean_text(h2h_col3.get("goles_home"))
    h2h_away_goals = _clean_text(h2h_col3.get("goles_away"))
    if h2h_home_goals or h2h_away_goals:
        h2h_score = f"{h2h_home_goals}:{h2h_away_goals}".strip(":")

    return {
        "match_id": _coalesce(raw.get("match_id"), raw.get("id")),
        "match_date": _coalesce(raw.get("match_date"), raw.get("date")),
        "kickoff_time": _coalesce(raw.get("time"), raw.get("time_obj")),
        "league_name": _coalesce(raw.get("league_name")),
        "home_name": _coalesce(raw.get("home_name"), raw.get("home_team")),
        "away_name": _coalesce(raw.get("away_name"), raw.get("away_team")),
        "ah_line": _coalesce(raw.get("handicap"), main_odds.get("ah_linea")),
        "goal_line": _coalesce(raw.get("goal_line"), main_odds.get("goals_linea")),
        "score": _coalesce(raw.get("score"), raw.get("final_score")),
        "last_home": _coalesce(last_home.get("score")),
        "last_home_ah": _coalesce(last_home.get("handicap_line_raw")),
        "last_away": _coalesce(last_away.get("score")),
        "last_away_ah": _coalesce(last_away.get("handicap_line_raw")),
        "h2h_col3": h2h_score,
        "h2h_col3_ah": _coalesce(h2h_col3.get("handicap")),
        "home_rank": _coalesce(home_standings.get("ranking")),
        "away_rank": _coalesce(away_standings.get("ranking")),
        "updated_at": _coalesce(raw.get("precacheo_date"), raw.get("cached_at"), updated_at),
    }


def _parse_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, min(parsed, maximum))


def _normalized_filters(args: Dict[str, str]) -> Dict[str, str]:
    return {
        "q": _clean_text(args.get("q")),
        "league": _clean_text(args.get("league")),
        "date": _clean_text(args.get("date")),
    }


def _build_where_clause(filters: Dict[str, str]) -> Tuple[str, List[str]]:
    clauses = ["bucket = ?"]
    params: List[str] = [PRECACHEO_BUCKET]

    if filters["q"]:
        q = f"%{filters['q'].lower()}%"
        clauses.append(
            """
            (
                lower(COALESCE(json_extract(payload_json, '$.home_name'), json_extract(payload_json, '$.home_team'), '')) LIKE ?
                OR lower(COALESCE(json_extract(payload_json, '$.away_name'), json_extract(payload_json, '$.away_team'), '')) LIKE ?
                OR lower(COALESCE(json_extract(payload_json, '$.league_name'), '')) LIKE ?
                OR CAST(match_id AS TEXT) LIKE ?
            )
            """.strip()
        )
        params.extend([q, q, q, f"%{filters['q']}%"])

    if filters["league"]:
        clauses.append("lower(COALESCE(json_extract(payload_json, '$.league_name'), '')) LIKE ?")
        params.append(f"%{filters['league'].lower()}%")

    if filters["date"]:
        clauses.append("COALESCE(json_extract(payload_json, '$.match_date'), '') LIKE ?")
        params.append(f"%{filters['date']}%")

    return " AND ".join(clauses), params


def _connect_db() -> sqlite3.Connection:
    db_path = Path(os.getenv("APP_SQLITE_PATH", str(DEFAULT_DB_PATH)))
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if LIBSQL_URL:
        if _libsql is None:
            raise RuntimeError("LIBSQL_URL esta configurado pero el paquete libsql no esta disponible.")
        kwargs: Dict[str, Any] = {"sync_url": LIBSQL_URL}
        if LIBSQL_AUTH_TOKEN:
            kwargs["auth_token"] = LIBSQL_AUTH_TOKEN
        conn = _libsql.connect(str(db_path), **kwargs)
        sync_fn = getattr(conn, "sync", None)
        if callable(sync_fn):
            sync_fn()
    else:
        conn = sqlite3.connect(str(db_path), timeout=30)

    conn.row_factory = sqlite3.Row
    return conn


def _query_precacheo_page_db(page: int, per_page: int, filters: Dict[str, str]) -> Tuple[List[Dict[str, str]], int, str]:
    where_sql, params = _build_where_clause(filters)
    offset = (page - 1) * per_page

    count_sql = f"SELECT COUNT(*) AS total FROM matches WHERE {where_sql}"
    rows_sql = f"""
        SELECT
            match_id,
            updated_at,
            COALESCE(json_extract(payload_json, '$.match_date'), '') AS match_date,
            COALESCE(json_extract(payload_json, '$.time'), json_extract(payload_json, '$.time_obj'), '') AS kickoff_time,
            COALESCE(json_extract(payload_json, '$.league_name'), '') AS league_name,
            COALESCE(json_extract(payload_json, '$.home_name'), json_extract(payload_json, '$.home_team'), '') AS home_name,
            COALESCE(json_extract(payload_json, '$.away_name'), json_extract(payload_json, '$.away_team'), '') AS away_name,
            COALESCE(json_extract(payload_json, '$.handicap'), json_extract(payload_json, '$.main_match_odds.ah_linea'), '') AS ah_line,
            COALESCE(json_extract(payload_json, '$.goal_line'), json_extract(payload_json, '$.main_match_odds.goals_linea'), '') AS goal_line,
            COALESCE(json_extract(payload_json, '$.score'), json_extract(payload_json, '$.final_score'), '') AS score,
            COALESCE(json_extract(payload_json, '$.last_home_match.score'), '') AS last_home,
            COALESCE(json_extract(payload_json, '$.last_home_match.handicap_line_raw'), '') AS last_home_ah,
            COALESCE(json_extract(payload_json, '$.last_away_match.score'), '') AS last_away,
            COALESCE(json_extract(payload_json, '$.last_away_match.handicap_line_raw'), '') AS last_away_ah,
            CASE
                WHEN json_extract(payload_json, '$.h2h_col3.goles_home') IS NULL
                     AND json_extract(payload_json, '$.h2h_col3.goles_away') IS NULL
                THEN ''
                ELSE CAST(COALESCE(json_extract(payload_json, '$.h2h_col3.goles_home'), '') AS TEXT)
                     || ':' ||
                     CAST(COALESCE(json_extract(payload_json, '$.h2h_col3.goles_away'), '') AS TEXT)
            END AS h2h_col3,
            COALESCE(json_extract(payload_json, '$.h2h_col3.handicap'), '') AS h2h_col3_ah,
            COALESCE(json_extract(payload_json, '$.home_standings.ranking'), '') AS home_rank,
            COALESCE(json_extract(payload_json, '$.away_standings.ranking'), '') AS away_rank
        FROM matches
        WHERE {where_sql}
        ORDER BY updated_at DESC
        LIMIT ? OFFSET ?
    """

    with _connect_db() as conn:
        total = int(conn.execute(count_sql, params).fetchone()["total"])
        rows = conn.execute(rows_sql, [*params, per_page, offset]).fetchall()

    items = []
    for row in rows:
        items.append({key: _clean_text(row[key]) for key in row.keys()})
    source = f"sqlite:{Path(os.getenv('APP_SQLITE_PATH', str(DEFAULT_DB_PATH))).name}"
    return items, total, source


def _json_cache_signature() -> Tuple[Tuple[str, float], ...]:
    signature: List[Tuple[str, float]] = []
    for path in JSON_FALLBACKS:
        if path.exists():
            signature.append((str(path), path.stat().st_mtime))
    return tuple(signature)


@lru_cache(maxsize=1)
def _load_json_fallback_cached(_signature: Tuple[Tuple[str, float], ...]) -> Tuple[str, Tuple[Dict[str, str], ...]]:
    for path in JSON_FALLBACKS:
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except Exception:
            continue

        if not isinstance(payload, list):
            continue

        rows = tuple(_compact_match(item) for item in payload if isinstance(item, dict))
        return str(path.name), rows

    return "none", tuple()


def _load_json_fallback_rows() -> Tuple[str, Tuple[Dict[str, str], ...]]:
    return _load_json_fallback_cached(_json_cache_signature())


def _match_filters(row: Dict[str, str], filters: Dict[str, str]) -> bool:
    haystack = " ".join(
        [
            row.get("match_id", ""),
            row.get("league_name", ""),
            row.get("home_name", ""),
            row.get("away_name", ""),
        ]
    ).lower()

    if filters["q"] and filters["q"].lower() not in haystack:
        return False
    if filters["league"] and filters["league"].lower() not in row.get("league_name", "").lower():
        return False
    if filters["date"] and filters["date"] not in row.get("match_date", ""):
        return False
    return True


def _query_precacheo_page_json(page: int, per_page: int, filters: Dict[str, str]) -> Tuple[List[Dict[str, str]], int, str]:
    source_name, rows = _load_json_fallback_rows()
    filtered = [row for row in rows if _match_filters(row, filters)]
    total = len(filtered)
    offset = (page - 1) * per_page
    page_rows = filtered[offset : offset + per_page]
    return page_rows, total, f"json:{source_name}"


def _query_precacheo_page(page: int, per_page: int, filters: Dict[str, str]) -> Tuple[List[Dict[str, str]], int, str]:
    try:
        rows, total, source = _query_precacheo_page_db(page, per_page, filters)
        if rows or total:
            return rows, total, source
    except Exception:
        pass
    return _query_precacheo_page_json(page, per_page, filters)


def _league_options(rows: Iterable[Dict[str, str]]) -> List[str]:
    leagues = sorted({row.get("league_name", "") for row in rows if row.get("league_name")})
    return leagues[:500]


@app.route("/")
def home():
    return redirect(url_for("precacheo"))


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True})


@app.route("/api/precacheo")
def api_precacheo():
    page = _parse_int(request.args.get("page"), default=1, minimum=1, maximum=100000)
    per_page = _parse_int(request.args.get("per_page"), default=DEFAULT_PER_PAGE, minimum=10, maximum=MAX_PER_PAGE)
    filters = _normalized_filters(request.args)
    rows, total, source = _query_precacheo_page(page, per_page, filters)
    total_pages = max(1, math.ceil(total / per_page)) if total else 1
    return jsonify(
        {
            "items": rows,
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": total_pages,
            "source": source,
            "filters": filters,
        }
    )


@app.route("/precacheo")
def precacheo():
    page = _parse_int(request.args.get("page"), default=1, minimum=1, maximum=100000)
    per_page = _parse_int(request.args.get("per_page"), default=DEFAULT_PER_PAGE, minimum=10, maximum=MAX_PER_PAGE)
    filters = _normalized_filters(request.args)
    rows, total, source = _query_precacheo_page(page, per_page, filters)
    total_pages = max(1, math.ceil(total / per_page)) if total else 1

    if page > total_pages:
        page = total_pages
        rows, total, source = _query_precacheo_page(page, per_page, filters)

    query_for_links = {k: v for k, v in filters.items() if v}
    query_for_links["per_page"] = per_page
    pagination = {
        "first": url_for("precacheo", **query_for_links, page=1),
        "prev": url_for("precacheo", **query_for_links, page=max(1, page - 1)),
        "next": url_for("precacheo", **query_for_links, page=min(total_pages, page + 1)),
        "last": url_for("precacheo", **query_for_links, page=total_pages),
    }
    api_url = url_for("api_precacheo", **query_for_links, page=page)

    return render_template(
        "precacheo.html",
        rows=rows,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        filters=filters,
        source=source,
        league_options=_league_options(rows),
        query_for_links=query_for_links,
        pagination=pagination,
        api_url=api_url,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
