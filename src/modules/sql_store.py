import json
import logging
import os
import sqlite3
import sys
import threading
import time
from datetime import datetime
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .red_cards import normalize_red_card_stats_payload

LOGGER = logging.getLogger(__name__)

try:
    import libsql as _libsql  # type: ignore
except Exception:
    _libsql = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

STUDIED_MATCHES_DIR = PROJECT_ROOT / "studied_matches"
HISTORY_FILE = STUDIED_MATCHES_DIR / "history.json"

DB_PATH = Path(os.getenv("APP_SQLITE_PATH", str(DATA_DIR / "app_data.db")))
LEGACY_SYNC_ENABLED = os.getenv("DATA_LEGACY_SYNC", "0").strip().lower() not in {"0", "false", "no", "off"}
BOOTSTRAP_LOCK_FILE = Path(
    os.getenv("APP_SQLITE_BOOTSTRAP_LOCK", str(DATA_DIR / ".sql_bootstrap.lock"))
)
SQL_BOOTSTRAP_MODE = os.getenv("SQL_BOOTSTRAP_MODE", "full").strip().lower()
SQL_BOOTSTRAP_SKIP_LEGACY = SQL_BOOTSTRAP_MODE in {"none", "schema_only", "no_legacy"}
SQL_BOOTSTRAP_HISTORY_ONLY = SQL_BOOTSTRAP_MODE == "history_only"
LIBSQL_URL = os.getenv("LIBSQL_URL", "").strip()
LIBSQL_AUTH_TOKEN = os.getenv("LIBSQL_AUTH_TOKEN", "").strip()
LIBSQL_SYNC_INTERVAL_SECONDS = max(0, int(os.getenv("LIBSQL_SYNC_INTERVAL_SECONDS", "60")))
EXPLORER_BACKFILL_KEY = "explorer_payload_backfill_v1"

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

MANAGED_BUCKETS = [
    "data_cloud_league.json",
    "data_ah_0.json",
    "data_ah_0.5.json",
    "data_ah_1.5.json",
    "data_ah_2_plus.json",
    "data_minus_ah_0.5.json",
    "data_minus_ah_1.5.json",
    "data_minus_ah_2_plus.json",
    "data_unknown.json",
    "data_others.json",
    "data_precacheo.json",
    "data_pending_results.json",
]


def _parse_bootstrap_only_buckets(raw: str) -> Tuple[str, ...]:
    if not raw:
        return tuple()

    selected: List[str] = []
    allowed = set(MANAGED_BUCKETS)

    for token in str(raw).replace(";", ",").split(","):
        name = token.strip()
        if not name:
            continue
        if not name.endswith(".json"):
            name = f"{name}.json"
        if name not in allowed:
            LOGGER.warning("Ignoring unknown SQL bootstrap bucket: %s", name)
            continue
        if name not in selected:
            selected.append(name)

    return tuple(selected)


SQL_BOOTSTRAP_ONLY_BUCKETS = _parse_bootstrap_only_buckets(
    os.getenv("SQL_BOOTSTRAP_ONLY_BUCKETS", "")
)

MATCH_STATE_BY_BUCKET = {
    "data_precacheo.json": "precacheo",
    "data_pending_results.json": "pending_results",
}

DEFAULT_MATCH_STATE = "historical"

_BOOTSTRAP_LOCK = threading.Lock()
_BOOTSTRAPPED = False
_LIBSQL_SYNC_LOCK = threading.Lock()
_LIBSQL_INITIAL_SYNC_DONE = False


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


@contextmanager
def _process_bootstrap_lock():
    """
    Inter-process lock to avoid parallel JSON->SQL bootstrap runs.
    """
    BOOTSTRAP_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    handle = open(BOOTSTRAP_LOCK_FILE, "a+", encoding="utf-8")
    try:
        handle.seek(0)
        if handle.tell() == 0:
            handle.write("0")
            handle.flush()
        handle.seek(0)
        while True:
            try:
                if sys.platform == "win32":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (OSError, IOError):
                time.sleep(0.1)
        yield
    finally:
        try:
            handle.seek(0)
            if sys.platform == "win32":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except (OSError, IOError):
            pass
        handle.close()


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = None

    if LIBSQL_URL:
        if _libsql is None:
            raise RuntimeError(
                "LIBSQL_URL está configurado, pero el paquete 'libsql' no está instalado."
            )

        connect_kwargs: Dict[str, Any] = {
            "sync_url": LIBSQL_URL,
        }
        if LIBSQL_AUTH_TOKEN:
            connect_kwargs["auth_token"] = LIBSQL_AUTH_TOKEN
        if LIBSQL_SYNC_INTERVAL_SECONDS > 0:
            connect_kwargs["sync_interval"] = LIBSQL_SYNC_INTERVAL_SECONDS

        conn = _libsql.connect(str(DB_PATH), **connect_kwargs)
    else:
        conn = sqlite3.connect(DB_PATH, timeout=30)

    try:
        conn.row_factory = sqlite3.Row
    except Exception:
        pass

    for pragma in (
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA foreign_keys = ON",
    ):
        try:
            conn.execute(pragma)
        except Exception:
            # Some libsql modes may not accept every PRAGMA; continue with safe defaults.
            pass

    # First sync on process boot ensures local replica has latest remote data.
    if LIBSQL_URL:
        sync_fn = getattr(conn, "sync", None)
        if callable(sync_fn):
            with _LIBSQL_SYNC_LOCK:
                global _LIBSQL_INITIAL_SYNC_DONE
                if not _LIBSQL_INITIAL_SYNC_DONE:
                    try:
                        sync_fn()
                    except Exception as exc:
                        LOGGER.warning("Initial libsql sync failed: %s", exc)
                    _LIBSQL_INITIAL_SYNC_DONE = True

    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS kv_store (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            bucket TEXT NOT NULL,
            state TEXT NOT NULL,
            handicap REAL,
            score TEXT,
            match_date TEXT,
            payload_json TEXT NOT NULL,
            explorer_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_matches_bucket ON matches(bucket);
        CREATE INDEX IF NOT EXISTS idx_matches_state ON matches(state);
        CREATE INDEX IF NOT EXISTS idx_matches_match_date ON matches(match_date);
        CREATE INDEX IF NOT EXISTS idx_matches_updated_at ON matches(updated_at);

        CREATE TABLE IF NOT EXISTS history_pending (
            season TEXT NOT NULL,
            league_id TEXT NOT NULL,
            match_id TEXT NOT NULL,
            item_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (season, league_id, match_id)
        );

        CREATE TABLE IF NOT EXISTS history_cached (
            season TEXT NOT NULL,
            league_id TEXT NOT NULL,
            match_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (season, league_id, match_id)
        );

        CREATE INDEX IF NOT EXISTS idx_history_pending_season_league ON history_pending(season, league_id);
        CREATE INDEX IF NOT EXISTS idx_history_cached_season_league ON history_cached(season, league_id);

        CREATE TABLE IF NOT EXISTS uefa_qualifying_matches (
            match_id TEXT PRIMARY KEY,
            competition_id TEXT NOT NULL,
            competition_name TEXT NOT NULL,
            season TEXT NOT NULL,
            stage_id TEXT NOT NULL,
            stage_name TEXT NOT NULL,
            stage_order INTEGER NOT NULL DEFAULT 0,
            match_date TEXT,
            home_team_id TEXT,
            home_team TEXT NOT NULL,
            away_team_id TEXT,
            away_team TEXT NOT NULL,
            score TEXT,
            half_time_score TEXT,
            source_state INTEGER,
            ah_line REAL,
            ou_line REAL,
            home_odds_decimal REAL,
            away_odds_decimal REAL,
            company_id INTEGER NOT NULL DEFAULT 8,
            source_url TEXT,
            source_json TEXT NOT NULL,
            deep_status TEXT NOT NULL DEFAULT 'catalogued',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_uefa_qualifying_competition_season
            ON uefa_qualifying_matches(competition_id, season);
        CREATE INDEX IF NOT EXISTS idx_uefa_qualifying_stage
            ON uefa_qualifying_matches(stage_name, stage_order);
        CREATE INDEX IF NOT EXISTS idx_uefa_qualifying_date
            ON uefa_qualifying_matches(match_date);
        CREATE INDEX IF NOT EXISTS idx_uefa_qualifying_home
            ON uefa_qualifying_matches(home_team);
        """
    )
    _ensure_matches_explorer_column(conn)


def _ensure_matches_explorer_column(conn: sqlite3.Connection) -> None:
    cols = conn.execute("PRAGMA table_info(matches)").fetchall()
    names = {row["name"] for row in cols}
    if "explorer_json" not in names:
        conn.execute("ALTER TABLE matches ADD COLUMN explorer_json TEXT")


def _compact_main_match_odds(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    return {
        "ah_linea": raw.get("ah_linea"),
        "goals_linea": raw.get("goals_linea"),
    }


def _compact_prev_match(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    return {
        "match_id": raw.get("match_id"),
        "score": raw.get("score"),
        "handicap_line_raw": raw.get("handicap_line_raw"),
        "over_under_line_raw": raw.get("over_under_line_raw"),
        "over_under_result": raw.get("over_under_result"),
        "home_team": raw.get("home_team"),
        "away_team": raw.get("away_team"),
        "date": raw.get("date"),
        "league_id_hist": raw.get("league_id_hist"),
        "history_scope": raw.get("history_scope"),
        "subject_is_home": raw.get("subject_is_home"),
        "is_general_fallback": raw.get("is_general_fallback"),
        "is_different_league": raw.get("is_different_league"),
        "stats_rows": raw.get("stats_rows") if isinstance(raw.get("stats_rows"), list) else [],
    }


def _compact_market_analysis_data(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None

    out: Dict[str, Any] = {}
    stadium = raw.get("stadium")
    if isinstance(stadium, dict):
        out["stadium"] = {
            "movement": stadium.get("movement"),
            "result": stadium.get("result"),
            "score": stadium.get("score"),
            "date": stadium.get("date"),
        }

    general = raw.get("general")
    if isinstance(general, dict):
        out["general"] = {
            "movement": general.get("movement"),
            "result": general.get("result"),
            "score": general.get("score"),
            "date": general.get("date"),
        }

    return out or None


def _compact_indirect_side(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    return {
        "match_id": raw.get("match_id"),
        "ah_line": raw.get("ah_line"),
        "ah": raw.get("ah"),
        "score": raw.get("score"),
        "localia": raw.get("localia"),
        "date": raw.get("date"),
        "home_team": raw.get("home_team"),
        "away_team": raw.get("away_team"),
        "cover_status": raw.get("cover_status"),
        "league_id_hist": raw.get("league_id_hist"),
        "league_id": raw.get("league_id"),
        "history_scope": raw.get("history_scope"),
        "same_league": raw.get("same_league"),
        "is_general_fallback": bool(raw.get("is_general_fallback")),
        "is_different_league": raw.get("is_different_league"),
        "stats_rows": raw.get("stats_rows") if isinstance(raw.get("stats_rows"), list) else [],
    }


def _compact_comparativas(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    left = _compact_indirect_side(raw.get("left"))
    right = _compact_indirect_side(raw.get("right"))
    if not left and not right:
        return None
    return {"left": left, "right": right}


def _compact_recent_matches(raw: Any) -> List[Dict[str, Any]]:
    """Conserva solo lo necesario para calcular V/O/U en los listados."""
    if not isinstance(raw, list):
        return []
    return [
        {
            "home": row.get("home"),
            "away": row.get("away"),
            "score": row.get("score") or row.get("score_raw"),
        }
        for row in raw
        if isinstance(row, dict)
    ]


def _compact_context_matches(raw: Any) -> List[Dict[str, Any]]:
    """Filas minimas para pintar el contexto previo en local y en Render."""
    if not isinstance(raw, list):
        return []
    return [
        {
            "date": row.get("date"),
            "home": row.get("home"),
            "away": row.get("away"),
            "score": row.get("score") or row.get("score_raw"),
            "ahLine": row.get("ahLine") or row.get("ahLine_raw"),
            "league_id_hist": row.get("league_id_hist"),
        }
        for row in raw
        if isinstance(row, dict)
    ]


def _compact_pre_match_context(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}

    def compact_moment(moment: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(moment, dict):
            return None
        return {
            key: moment.get(key)
            for key in (
                "match_id", "date", "home_name", "away_name", "score",
                "ah_line", "league_id", "league_name",
            )
        } | {
            "home_matches": _compact_context_matches(moment.get("home_matches")),
            "away_matches": _compact_context_matches(moment.get("away_matches")),
        }

    return {
        "current": compact_moment(raw.get("current")),
        "previous": compact_moment(raw.get("previous")),
    }


def _build_explorer_payload(match_data: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "match_id": match_data.get("match_id"),
        "history_data_version": match_data.get("history_data_version"),
        "id": match_data.get("id"),
        "home_name": match_data.get("home_name"),
        "away_name": match_data.get("away_name"),
        "home_team": match_data.get("home_team"),
        "away_team": match_data.get("away_team"),
        "league_name": match_data.get("league_name"),
        "league_id": match_data.get("league_id"),
        "competition_type": match_data.get("competition_type"),
        "competition_stage": match_data.get("competition_stage"),
        "competition_stage_id": match_data.get("competition_stage_id"),
        "season": match_data.get("season"),
        "match_date": match_data.get("match_date"),
        "date": match_data.get("date"),
        "cached_at": match_data.get("cached_at"),
        "time_obj": match_data.get("time_obj"),
        "handicap": match_data.get("handicap"),
        "score": match_data.get("score"),
        "final_score": match_data.get("final_score"),
        "stats_rows": match_data.get("stats_rows") if isinstance(match_data.get("stats_rows"), list) else [],
        "stats_status": match_data.get("stats_status"),
        "stats_updated_at": match_data.get("stats_updated_at"),
        "main_match_odds": _compact_main_match_odds(match_data.get("main_match_odds")),
        "last_home_match": _compact_prev_match(match_data.get("last_home_match")),
        "last_away_match": _compact_prev_match(match_data.get("last_away_match")),
        "market_analysis_data": _compact_market_analysis_data(match_data.get("market_analysis_data")),
        "market_analysis_html": match_data.get("market_analysis_html"),
        "h2h_col3": match_data.get("h2h_col3"),
        "comparativas_indirectas": _compact_comparativas(match_data.get("comparativas_indirectas")),
        "home_standings": match_data.get("home_standings"),
        "away_standings": match_data.get("away_standings"),
        "home_ou_stats": match_data.get("home_ou_stats"),
        "away_ou_stats": match_data.get("away_ou_stats"),
        "home_ou_stats_specific": match_data.get("home_ou_stats_specific"),
        "away_ou_stats_specific": match_data.get("away_ou_stats_specific"),
        "home_ou_stats_general": match_data.get("home_ou_stats_general"),
        "away_ou_stats_general": match_data.get("away_ou_stats_general"),
        "pre_match_context": _compact_pre_match_context(match_data.get("pre_match_context")),
        "recent_home_matches_same_league_specific": _compact_recent_matches(
            match_data.get("recent_home_matches_same_league_specific")
        ),
        "recent_away_matches_same_league_specific": _compact_recent_matches(
            match_data.get("recent_away_matches_same_league_specific")
        ),
        "recent_home_matches_same_league_general": _compact_recent_matches(
            match_data.get("recent_home_matches_same_league_general")
        ),
        "recent_away_matches_same_league_general": _compact_recent_matches(
            match_data.get("recent_away_matches_same_league_general")
        ),
    }
    return payload


def _backfill_explorer_payload(
    conn: sqlite3.Connection,
    batch_size: int = 500,
    max_rows: Optional[int] = None,
) -> int:
    updated = 0
    while True:
        if isinstance(max_rows, int) and max_rows > 0 and updated >= max_rows:
            break

        current_limit = int(batch_size)
        if isinstance(max_rows, int) and max_rows > 0:
            current_limit = min(current_limit, max_rows - updated)
        if current_limit <= 0:
            break

        rows = conn.execute(
            """
            SELECT match_id, payload_json
            FROM matches
            WHERE explorer_json IS NULL
            LIMIT ?
            """,
            (current_limit,),
        ).fetchall()
        if not rows:
            break

        for row in rows:
            raw = row["payload_json"]
            try:
                match_data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(match_data, dict):
                continue

            explorer_payload = _build_explorer_payload(match_data)
            conn.execute(
                "UPDATE matches SET explorer_json = ? WHERE match_id = ?",
                (json.dumps(explorer_payload, ensure_ascii=False), row["match_id"]),
            )
            updated += 1

    return updated


def _get_kv(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM kv_store WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def _set_kv(conn: sqlite3.Connection, key: str, value: str) -> None:
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO kv_store(key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        (key, value, ts),
    )


def _normalize_score(match_data: Dict) -> Optional[str]:
    score = match_data.get("score")
    if score is None:
        score = match_data.get("final_score")
    if score is None:
        return None
    return str(score)


def _extract_handicap(match_data: Dict) -> Optional[float]:
    raw = match_data.get("handicap")
    if raw is None:
        raw = (match_data.get("main_match_odds") or {}).get("ah_linea")
    if raw in (None, "", "N/A", "-"):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _extract_match_date(match_data: Dict) -> Optional[str]:
    value = match_data.get("match_date")
    if value in (None, "", "N/A"):
        return None
    return str(value)


def _upsert_match(
    conn: sqlite3.Connection,
    match_data: Dict,
    bucket: str,
    state: str,
) -> Tuple[Optional[str], str]:
    normalize_red_card_stats_payload(match_data)
    match_id_raw = match_data.get("match_id")
    if match_id_raw in (None, ""):
        raise ValueError("match_data requires 'match_id'")

    match_id = str(match_id_raw)
    previous_row = conn.execute(
        "SELECT bucket FROM matches WHERE match_id = ?",
        (match_id,),
    ).fetchone()
    previous_bucket = previous_row["bucket"] if previous_row else None

    payload = json.dumps(match_data, ensure_ascii=False)
    explorer_payload = json.dumps(_build_explorer_payload(match_data), ensure_ascii=False)
    ts = now_iso()

    conn.execute(
        """
        INSERT INTO matches(
            match_id, bucket, state, handicap, score, match_date, payload_json, explorer_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(match_id) DO UPDATE SET
            bucket = excluded.bucket,
            state = excluded.state,
            handicap = excluded.handicap,
            score = excluded.score,
            match_date = excluded.match_date,
            payload_json = excluded.payload_json,
            explorer_json = excluded.explorer_json,
            updated_at = excluded.updated_at
        """,
        (
            match_id,
            bucket,
            state,
            _extract_handicap(match_data),
            _normalize_score(match_data),
            _extract_match_date(match_data),
            payload,
            explorer_payload,
            ts,
            ts,
        ),
    )
    return previous_bucket, match_id


def _fetch_matches_rows(
    conn: sqlite3.Connection,
    bucket: Optional[str] = None,
    state: Optional[str] = None,
    limit: Optional[int] = None,
    prefer_explorer_payload: bool = False,
) -> List[sqlite3.Row]:
    payload_expr = "COALESCE(explorer_json, payload_json)" if prefer_explorer_payload else "payload_json"
    query = f"SELECT {payload_expr} AS payload_json FROM matches"
    params: List[str] = []
    clauses: List[str] = []

    if bucket:
        clauses.append("bucket = ?")
        params.append(bucket)
    if state:
        clauses.append("state = ?")
        params.append(state)

    if clauses:
        query += " WHERE " + " AND ".join(clauses)

    query += " ORDER BY updated_at DESC"
    if isinstance(limit, int) and limit > 0:
        query += " LIMIT ?"
        params.append(limit)
    return conn.execute(query, params).fetchall()


def _fetch_distinct_buckets(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT DISTINCT bucket FROM matches").fetchall()
    return [row["bucket"] for row in rows]


def _load_json_file(path: Path):
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        LOGGER.warning("Unable to read legacy json %s: %s", path, exc)
        return None


def _import_legacy_matches(
    conn: sqlite3.Connection,
    data_dir: Path,
    buckets: Optional[Sequence[str]] = None,
) -> int:
    imported = 0

    # General historical first, then live buckets. The explicit cloud archive
    # is imported last so finished league extractions remain historical even
    # when the same ID still lingers in a precache snapshot.
    ordered_files: List[str] = []
    requested_buckets = [b for b in (buckets or MANAGED_BUCKETS) if b in MANAGED_BUCKETS]

    for bucket in requested_buckets:
        if bucket not in (
            "data_cloud_league.json",
            "data_pending_results.json",
            "data_precacheo.json",
        ):
            ordered_files.append(bucket)
    if "data_pending_results.json" in requested_buckets:
        ordered_files.append("data_pending_results.json")
    if "data_precacheo.json" in requested_buckets:
        ordered_files.append("data_precacheo.json")
    if "data_cloud_league.json" in requested_buckets:
        ordered_files.append("data_cloud_league.json")

    for bucket in ordered_files:
        path = data_dir / bucket
        payload = _load_json_file(path)
        if not isinstance(payload, list):
            continue

        state = MATCH_STATE_BY_BUCKET.get(bucket, DEFAULT_MATCH_STATE)
        for item in payload:
            if not isinstance(item, dict):
                continue
            try:
                _upsert_match(conn, item, bucket=bucket, state=state)
                imported += 1
            except Exception:
                continue

    return imported


def _import_legacy_history(conn: sqlite3.Connection, history_file: Path) -> Tuple[int, int]:
    pending_count = 0
    cached_count = 0

    payload = _load_json_file(history_file)
    if not isinstance(payload, dict):
        return pending_count, cached_count

    pending = payload.get("pending", {})
    cached = payload.get("cached", {})

    ts = now_iso()

    if isinstance(pending, dict):
        for season, leagues in pending.items():
            if not isinstance(leagues, dict):
                continue
            for league_id, entries in leagues.items():
                if not isinstance(entries, list):
                    continue
                for item in entries:
                    if isinstance(item, dict):
                        match_id = str(item.get("id") or item.get("match_id") or "")
                        item_data = item
                    else:
                        match_id = str(item)
                        item_data = {"id": match_id, "ah": "N/A"}
                    if not match_id:
                        continue
                    conn.execute(
                        """
                        INSERT INTO history_pending(season, league_id, match_id, item_json, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(season, league_id, match_id) DO UPDATE SET
                            item_json = excluded.item_json,
                            updated_at = excluded.updated_at
                        """,
                        (str(season), str(league_id), match_id, json.dumps(item_data, ensure_ascii=False), ts, ts),
                    )
                    pending_count += 1

    if isinstance(cached, dict):
        for season, leagues in cached.items():
            if not isinstance(leagues, dict):
                continue
            for league_id, entries in leagues.items():
                if not isinstance(entries, list):
                    continue
                for item in entries:
                    match_id = str(item)
                    if not match_id:
                        continue
                    conn.execute(
                        """
                        INSERT INTO history_cached(season, league_id, match_id, created_at)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(season, league_id, match_id) DO NOTHING
                        """,
                        (str(season), str(league_id), match_id, ts),
                    )
                    cached_count += 1

    return pending_count, cached_count


def ensure_bootstrap(force_import: bool = False) -> None:
    global _BOOTSTRAPPED

    with _BOOTSTRAP_LOCK:
        if _BOOTSTRAPPED and not force_import:
            return

        with _process_bootstrap_lock():
            with _connect() as conn:
                _init_schema(conn)

                done_flag = _get_kv(conn, "legacy_json_bootstrap_v1")
                should_import = force_import or done_flag != "done"

                if should_import:
                    if force_import:
                        conn.execute("DELETE FROM matches")
                        conn.execute("DELETE FROM history_pending")
                        conn.execute("DELETE FROM history_cached")

                    imported_matches = 0
                    pending_count = 0
                    cached_count = 0

                    if SQL_BOOTSTRAP_SKIP_LEGACY:
                        LOGGER.info(
                            "SQL bootstrap running in lightweight mode. "
                            "Skipping legacy JSON import (SQL_BOOTSTRAP_MODE=%s).",
                            SQL_BOOTSTRAP_MODE,
                        )
                    else:
                        if not SQL_BOOTSTRAP_HISTORY_ONLY:
                            if SQL_BOOTSTRAP_ONLY_BUCKETS:
                                LOGGER.info(
                                    "SQL bootstrap importing selected buckets: %s",
                                    ", ".join(SQL_BOOTSTRAP_ONLY_BUCKETS),
                                )
                            imported_matches = _import_legacy_matches(
                                conn,
                                DATA_DIR,
                                buckets=SQL_BOOTSTRAP_ONLY_BUCKETS or None,
                            )
                        pending_count, cached_count = _import_legacy_history(conn, HISTORY_FILE)

                    _set_kv(conn, "legacy_json_bootstrap_v1", "done")

                    LOGGER.info(
                        "SQL bootstrap complete. Imported matches=%s pending=%s cached=%s",
                        imported_matches,
                        pending_count,
                        cached_count,
                    )

                backfill_done = _get_kv(conn, EXPLORER_BACKFILL_KEY)
                if force_import or backfill_done != "done":
                    # Keep first-request latency bounded; complete in chunks.
                    updated = _backfill_explorer_payload(conn, max_rows=1000)
                    pending_row = conn.execute(
                        "SELECT 1 FROM matches WHERE explorer_json IS NULL LIMIT 1"
                    ).fetchone()
                    if pending_row is None:
                        _set_kv(conn, EXPLORER_BACKFILL_KEY, "done")
                    if updated:
                        if pending_row is None:
                            LOGGER.info("Explorer payload backfill complete. Updated rows=%s", updated)
                        else:
                            LOGGER.info("Explorer payload backfill chunk updated rows=%s", updated)

        _BOOTSTRAPPED = True


def upsert_match(match_data: Dict, bucket: str, state: str) -> Tuple[Optional[str], str]:
    ensure_bootstrap()
    with _connect() as conn:
        return _upsert_match(conn, match_data, bucket, state)


def delete_match(match_id: str, bucket: Optional[str] = None, state: Optional[str] = None) -> bool:
    ensure_bootstrap()
    clauses = ["match_id = ?"]
    params: List[str] = [str(match_id)]

    if bucket:
        clauses.append("bucket = ?")
        params.append(bucket)
    if state:
        clauses.append("state = ?")
        params.append(state)

    with _connect() as conn:
        cur = conn.execute(
            f"DELETE FROM matches WHERE {' AND '.join(clauses)}",
            params,
        )
        return cur.rowcount > 0


def get_match(match_id: str, bucket: Optional[str] = None, state: Optional[str] = None) -> Optional[Dict]:
    ensure_bootstrap()

    query = "SELECT payload_json FROM matches WHERE match_id = ?"
    params: List[str] = [str(match_id)]

    if bucket:
        query += " AND bucket = ?"
        params.append(bucket)
    if state:
        query += " AND state = ?"
        params.append(state)

    with _connect() as conn:
        row = conn.execute(query, params).fetchone()

    if not row:
        return None

    try:
        return json.loads(row["payload_json"])
    except json.JSONDecodeError:
        return None


def get_match_bucket(match_id: str) -> Optional[str]:
    ensure_bootstrap()
    with _connect() as conn:
        row = conn.execute(
            "SELECT bucket FROM matches WHERE match_id = ?",
            (str(match_id),),
        ).fetchone()
    return row["bucket"] if row else None


def fetch_matches(
    bucket: Optional[str] = None,
    state: Optional[str] = None,
    limit: Optional[int] = None,
    prefer_explorer_payload: bool = False,
) -> List[Dict]:
    ensure_bootstrap()
    with _connect() as conn:
        rows = _fetch_matches_rows(
            conn,
            bucket=bucket,
            state=state,
            limit=limit,
            prefer_explorer_payload=prefer_explorer_payload,
        )

    output: List[Dict] = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            output.append(payload)
    return output


def fetch_matches_by_ids(
    match_ids: Sequence[str],
    bucket: Optional[str] = None,
    state: Optional[str] = None,
    limit: Optional[int] = None,
    prefer_explorer_payload: bool = False,
) -> List[Dict]:
    """
    Fetch matches restricted to the provided match IDs and preserve input order.
    Useful when UI needs rows aligned with a visible matches list.
    """
    ensure_bootstrap()

    ordered_ids: List[str] = []
    seen: set[str] = set()
    for raw in match_ids or []:
        mid = str(raw).strip()
        if not mid or mid in seen:
            continue
        seen.add(mid)
        ordered_ids.append(mid)

    if not ordered_ids:
        return []

    if isinstance(limit, int) and limit > 0:
        ordered_ids = ordered_ids[:limit]

    payload_expr = "COALESCE(explorer_json, payload_json)" if prefer_explorer_payload else "payload_json"
    rows_by_id: Dict[str, Dict] = {}

    with _connect() as conn:
        chunk_size = 400
        for start in range(0, len(ordered_ids), chunk_size):
            chunk = ordered_ids[start:start + chunk_size]
            if not chunk:
                continue

            placeholders = ", ".join(["?"] * len(chunk))
            query = f"SELECT match_id, {payload_expr} AS payload_json FROM matches WHERE match_id IN ({placeholders})"
            params: List[str] = list(chunk)

            if bucket:
                query += " AND bucket = ?"
                params.append(bucket)
            if state:
                query += " AND state = ?"
                params.append(state)

            rows = conn.execute(query, params).fetchall()
            for row in rows:
                try:
                    payload = json.loads(row["payload_json"])
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows_by_id[str(row["match_id"])] = payload

    output: List[Dict] = []
    for mid in ordered_ids:
        payload = rows_by_id.get(mid)
        if payload is not None:
            output.append(payload)

    return output


def fetch_all_matches() -> List[Dict]:
    return fetch_matches(bucket=None, state=None)


def fetch_distinct_buckets() -> List[str]:
    ensure_bootstrap()
    with _connect() as conn:
        return _fetch_distinct_buckets(conn)


def export_bucket_to_json(bucket: str) -> Path:
    ensure_bootstrap()
    path = DATA_DIR / bucket
    
    # Limitar buckets históricos acumulativos a un máximo de 2500 partidos para evitar superar el límite de 100MB de GitHub.
    limit_val = None
    if bucket.startswith("data_") and bucket not in ("data_precacheo.json", "data_pending_results.json"):
        limit_val = 2500
        
    rows = fetch_matches(bucket=bucket, limit=limit_val)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        with temporary.open("w", encoding="utf-8") as fh:
            json.dump(rows, fh, indent=2, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())

        last_error = None
        for attempt in range(5):
            try:
                os.replace(temporary, path)
                last_error = None
                break
            except OSError as exc:
                last_error = exc
                time.sleep(0.15 * (attempt + 1))
        if last_error is not None:
            raise last_error
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
    return path


def export_all_buckets_to_json() -> List[Path]:
    ensure_bootstrap()
    written: List[Path] = []

    known = set(MANAGED_BUCKETS)
    for bucket in fetch_distinct_buckets():
        if bucket.startswith("data_"):
            known.add(bucket)

    for bucket in sorted(known):
        written.append(export_bucket_to_json(bucket))
    return written


def import_legacy_json_to_db(reset_first: bool = False) -> None:
    ensure_bootstrap(force_import=reset_first)


def _history_rows_to_structure(rows: Sequence[sqlite3.Row]) -> Dict[str, Dict[str, List[Dict]]]:
    data: Dict[str, Dict[str, List[Dict]]] = {}
    for row in rows:
        season = row["season"]
        league_id = row["league_id"]
        raw_item = row["item_json"]
        try:
            item = json.loads(raw_item)
        except json.JSONDecodeError:
            item = {"id": row["match_id"], "ah": "N/A"}

        data.setdefault(season, {}).setdefault(league_id, []).append(item)
    return data


def history_get_full() -> Dict:
    ensure_bootstrap()
    with _connect() as conn:
        pending_rows = conn.execute(
            "SELECT season, league_id, match_id, item_json FROM history_pending ORDER BY season, league_id"
        ).fetchall()
        cached_rows = conn.execute(
            "SELECT season, league_id, match_id FROM history_cached ORDER BY season, league_id"
        ).fetchall()

    pending = _history_rows_to_structure(pending_rows)

    cached: Dict[str, Dict[str, List[str]]] = {}
    for row in cached_rows:
        cached.setdefault(row["season"], {}).setdefault(row["league_id"], []).append(row["match_id"])

    return {"pending": pending, "cached": cached}


def history_export_to_json() -> Path:
    ensure_bootstrap()
    payload = history_get_full()
    STUDIED_MATCHES_DIR.mkdir(parents=True, exist_ok=True)
    with HISTORY_FILE.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    return HISTORY_FILE


def history_is_cached(season: str, league_id: str, match_id: str) -> bool:
    ensure_bootstrap()
    with _connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM history_cached WHERE season = ? AND league_id = ? AND match_id = ?",
            (str(season), str(league_id), str(match_id)),
        ).fetchone()
    return row is not None


def history_add_pending_matches(season: str, league_id: str, match_data_list: Iterable) -> int:
    ensure_bootstrap()

    season_s = str(season)
    league_s = str(league_id)

    added = 0
    ts = now_iso()

    with _connect() as conn:
        for item in match_data_list:
            if isinstance(item, dict):
                match_id = str(item.get("id") or item.get("match_id") or "")
                payload = item
            else:
                match_id = str(item)
                payload = {"id": match_id, "ah": "N/A"}

            if not match_id:
                continue

            is_cached = conn.execute(
                "SELECT 1 FROM history_cached WHERE season = ? AND league_id = ? AND match_id = ?",
                (season_s, league_s, match_id),
            ).fetchone()
            if is_cached:
                continue

            already_pending = conn.execute(
                "SELECT 1 FROM history_pending WHERE season = ? AND league_id = ? AND match_id = ?",
                (season_s, league_s, match_id),
            ).fetchone()
            if already_pending:
                continue

            conn.execute(
                """
                INSERT INTO history_pending(season, league_id, match_id, item_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (season_s, league_s, match_id, json.dumps(payload, ensure_ascii=False), ts, ts),
            )
            added += 1

    return added


def history_move_to_cached(season: str, league_id: str, match_id: str) -> None:
    ensure_bootstrap()

    season_s = str(season)
    league_s = str(league_id)
    match_s = str(match_id)

    with _connect() as conn:
        conn.execute(
            "DELETE FROM history_pending WHERE season = ? AND league_id = ? AND match_id = ?",
            (season_s, league_s, match_s),
        )
        conn.execute(
            """
            INSERT INTO history_cached(season, league_id, match_id, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(season, league_id, match_id) DO NOTHING
            """,
            (season_s, league_s, match_s, now_iso()),
        )


def history_replace(payload: Dict) -> None:
    ensure_bootstrap()

    pending = payload.get("pending", {}) if isinstance(payload, dict) else {}
    cached = payload.get("cached", {}) if isinstance(payload, dict) else {}

    ts = now_iso()

    with _connect() as conn:
        conn.execute("DELETE FROM history_pending")
        conn.execute("DELETE FROM history_cached")

        if isinstance(pending, dict):
            for season, leagues in pending.items():
                if not isinstance(leagues, dict):
                    continue
                for league_id, entries in leagues.items():
                    if not isinstance(entries, list):
                        continue
                    for item in entries:
                        if isinstance(item, dict):
                            match_id = str(item.get("id") or item.get("match_id") or "")
                            item_payload = item
                        else:
                            match_id = str(item)
                            item_payload = {"id": match_id, "ah": "N/A"}
                        if not match_id:
                            continue
                        conn.execute(
                            """
                            INSERT INTO history_pending(season, league_id, match_id, item_json, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                str(season),
                                str(league_id),
                                match_id,
                                json.dumps(item_payload, ensure_ascii=False),
                                ts,
                                ts,
                            ),
                        )

        if isinstance(cached, dict):
            for season, leagues in cached.items():
                if not isinstance(leagues, dict):
                    continue
                for league_id, entries in leagues.items():
                    if not isinstance(entries, list):
                        continue
                    for item in entries:
                        match_id = str(item)
                        if not match_id:
                            continue
                        conn.execute(
                            """
                            INSERT INTO history_cached(season, league_id, match_id, created_at)
                            VALUES (?, ?, ?, ?)
                            """,
                            (str(season), str(league_id), match_id, ts),
                        )


def history_get_pending() -> Dict:
    return history_get_full().get("pending", {})


def get_json_state(key: str, default=None):
    """
    Generic JSON state storage in kv_store.
    """
    ensure_bootstrap()
    with _connect() as conn:
        raw = _get_kv(conn, key)
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def set_json_state(key: str, payload) -> None:
    """
    Persist generic JSON-serializable payload in kv_store.
    """
    ensure_bootstrap()
    with _connect() as conn:
        _set_kv(conn, key, json.dumps(payload, ensure_ascii=False))


def delete_state(key: str) -> None:
    ensure_bootstrap()
    with _connect() as conn:
        conn.execute("DELETE FROM kv_store WHERE key = ?", (key,))


def upsert_uefa_qualifying_matches(rows: Iterable[Dict[str, Any]]) -> int:
    """Persiste el catalogo especializado de fases previas UEFA en el SQL principal."""
    ensure_bootstrap()
    timestamp = now_iso()
    saved = 0
    with _connect() as conn:
        for raw in rows or []:
            if not isinstance(raw, dict):
                continue
            match_id = str(raw.get("match_id") or raw.get("id") or "").strip()
            if not match_id:
                continue
            conn.execute(
                """
                INSERT INTO uefa_qualifying_matches (
                    match_id, competition_id, competition_name, season,
                    stage_id, stage_name, stage_order, match_date,
                    home_team_id, home_team, away_team_id, away_team,
                    score, half_time_score, source_state, ah_line, ou_line,
                    home_odds_decimal, away_odds_decimal, company_id,
                    source_url, source_json, deep_status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(match_id) DO UPDATE SET
                    competition_id = excluded.competition_id,
                    competition_name = excluded.competition_name,
                    season = excluded.season,
                    stage_id = excluded.stage_id,
                    stage_name = excluded.stage_name,
                    stage_order = excluded.stage_order,
                    match_date = excluded.match_date,
                    home_team_id = excluded.home_team_id,
                    home_team = excluded.home_team,
                    away_team_id = excluded.away_team_id,
                    away_team = excluded.away_team,
                    score = excluded.score,
                    half_time_score = excluded.half_time_score,
                    source_state = excluded.source_state,
                    ah_line = excluded.ah_line,
                    ou_line = excluded.ou_line,
                    home_odds_decimal = excluded.home_odds_decimal,
                    away_odds_decimal = excluded.away_odds_decimal,
                    company_id = excluded.company_id,
                    source_url = excluded.source_url,
                    source_json = excluded.source_json,
                    deep_status = CASE
                        WHEN uefa_qualifying_matches.deep_status = 'enriched' THEN 'enriched'
                        ELSE excluded.deep_status
                    END,
                    updated_at = excluded.updated_at
                """,
                (
                    match_id,
                    str(raw.get("competition_id") or ""),
                    str(raw.get("competition_name") or ""),
                    str(raw.get("season") or ""),
                    str(raw.get("stage_id") or ""),
                    str(raw.get("stage_name") or ""),
                    int(raw.get("stage_order") or 0),
                    raw.get("match_date"),
                    str(raw.get("home_team_id") or ""),
                    str(raw.get("home_team") or ""),
                    str(raw.get("away_team_id") or ""),
                    str(raw.get("away_team") or ""),
                    raw.get("score"),
                    raw.get("half_time_score"),
                    raw.get("source_state"),
                    raw.get("ah_line"),
                    raw.get("ou_line"),
                    raw.get("home_odds_decimal"),
                    raw.get("away_odds_decimal"),
                    int(raw.get("company_id") or 8),
                    raw.get("source_url"),
                    json.dumps(raw, ensure_ascii=False),
                    str(raw.get("deep_status") or "catalogued"),
                    timestamp,
                    timestamp,
                ),
            )
            saved += 1
    return saved


def fetch_uefa_qualifying_matches(
    competition_ids: Optional[Sequence[str]] = None,
    seasons: Optional[Sequence[str]] = None,
    stages: Optional[Sequence[str]] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    ensure_bootstrap()
    clauses: List[str] = []
    params: List[Any] = []

    def add_in_clause(column: str, values: Optional[Sequence[str]]) -> None:
        clean = [str(value).strip() for value in (values or []) if str(value).strip()]
        if not clean:
            return
        clauses.append(f"{column} IN ({', '.join(['?'] * len(clean))})")
        params.extend(clean)

    add_in_clause("competition_id", competition_ids)
    add_in_clause("season", seasons)
    add_in_clause("stage_name", stages)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    safe_limit = max(1, min(int(limit or 5000), 20000))

    with _connect() as conn:
        rows = conn.execute(
            f"SELECT source_json, deep_status FROM uefa_qualifying_matches{where} "
            "ORDER BY match_date DESC, match_id DESC LIMIT ?",
            [*params, safe_limit],
        ).fetchall()

    output: List[Dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row["source_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            payload["deep_status"] = row["deep_status"]
            output.append(payload)
    return output


def mark_uefa_qualifying_deep_status(match_id: str, status: str) -> None:
    ensure_bootstrap()
    with _connect() as conn:
        conn.execute(
            "UPDATE uefa_qualifying_matches SET deep_status = ?, updated_at = ? WHERE match_id = ?",
            (str(status), now_iso(), str(match_id)),
        )


def update_uefa_qualifying_stats(
    match_id: str,
    stats_rows: Sequence[Dict[str, Any]],
    status: str,
) -> bool:
    """Persiste las estadisticas del partido tambien en el catalogo UEFA separado."""
    ensure_bootstrap()
    timestamp = now_iso()
    with _connect() as conn:
        row = conn.execute(
            "SELECT source_json FROM uefa_qualifying_matches WHERE match_id = ?",
            (str(match_id),),
        ).fetchone()
        if not row:
            return False
        try:
            payload = json.loads(row["source_json"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        payload["stats_rows"] = list(stats_rows or [])
        payload["stats_status"] = str(status)
        payload["stats_updated_at"] = timestamp
        conn.execute(
            "UPDATE uefa_qualifying_matches SET source_json = ?, updated_at = ? WHERE match_id = ?",
            (json.dumps(payload, ensure_ascii=False), timestamp, str(match_id)),
        )
    return True


def bulk_update_uefa_qualifying_stats(items: Sequence[Dict[str, Any]]) -> int:
    """Actualiza matches y catalogo UEFA en una sola transaccion por lote."""
    ensure_bootstrap()
    updated = 0
    with _connect() as conn:
        for item in items or []:
            match_id = str(item.get("match_id") or "")
            if not match_id:
                continue
            stats_rows = list(item.get("stats_rows") or [])
            status = str(item.get("status") or "unavailable")
            timestamp = str(item.get("updated_at") or now_iso())

            match_row = conn.execute(
                "SELECT payload_json, bucket, state FROM matches WHERE match_id = ?",
                (match_id,),
            ).fetchone()
            if match_row:
                try:
                    match_payload = json.loads(match_row["payload_json"])
                except (TypeError, json.JSONDecodeError):
                    match_payload = {"match_id": match_id}
                if not isinstance(match_payload, dict):
                    match_payload = {"match_id": match_id}
                match_payload["stats_rows"] = stats_rows
                match_payload["stats_status"] = status
                match_payload["stats_updated_at"] = timestamp
                _upsert_match(
                    conn,
                    match_payload,
                    bucket=str(match_row["bucket"] or "data_uefa_qualifying.json"),
                    state=str(match_row["state"] or "historical"),
                )

            catalogue_row = conn.execute(
                "SELECT source_json FROM uefa_qualifying_matches WHERE match_id = ?",
                (match_id,),
            ).fetchone()
            if catalogue_row:
                try:
                    catalogue_payload = json.loads(catalogue_row["source_json"])
                except (TypeError, json.JSONDecodeError):
                    catalogue_payload = {}
                if not isinstance(catalogue_payload, dict):
                    catalogue_payload = {}
                catalogue_payload["stats_rows"] = stats_rows
                catalogue_payload["stats_status"] = status
                catalogue_payload["stats_updated_at"] = timestamp
                conn.execute(
                    "UPDATE uefa_qualifying_matches SET source_json = ?, updated_at = ? WHERE match_id = ?",
                    (json.dumps(catalogue_payload, ensure_ascii=False), timestamp, match_id),
                )
            updated += 1
    return updated


def get_db_path() -> Path:
    return DB_PATH


def is_libsql_enabled() -> bool:
    return bool(LIBSQL_URL)


def sync_replica() -> bool:
    """
    Triggers a manual sync when using libsql embedded replicas.
    Returns True if sync was attempted.
    """
    if not is_libsql_enabled():
        return False
    with _connect() as conn:
        sync_fn = getattr(conn, "sync", None)
        if callable(sync_fn):
            sync_fn()
            return True
    return False
