import argparse
import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

try:
    from modules.youth_filter import is_unbettable_youth_match
except Exception:
    is_unbettable_youth_match = lambda m: False

PRECACHE_BUCKET = "data_precacheo.json"


def _normalize_text(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_name(value):
    text = _normalize_text(value)
    return text.casefold() if text else None


def _normalize_line(value):
    text = _normalize_text(value)
    if text is None:
        return None
    if "/" in text:
        parts = text.split("/")
        if len(parts) == 2:
            try:
                p1 = float(parts[0].strip().replace("−", "-").replace(",", "."))
                p2 = float(parts[1].strip().replace("−", "-").replace(",", "."))
                parsed = (p1 + p2) / 2.0
                normalized = f"{parsed:.2f}".rstrip("0").rstrip(".")
                return normalized or "0"
            except Exception:
                pass
    try:
        parsed = float(text.replace("−", "-").replace(",", "."))
    except Exception:
        return text
    normalized = f"{parsed:.2f}".rstrip("0").rstrip(".")
    return normalized or "0"


def _match_snapshot_handicap(match):
    return _normalize_line(
        match.get("handicap")
        or (match.get("main_match_odds") or {}).get("ah_linea")
    )


def _has_valid_handicap(match):
    """Verifica si el partido tiene una línea de hándicap asiático numérica válida."""
    if not isinstance(match, dict):
        return False
    raw = match.get("handicap")
    if raw in (None, "", "-", "N/A", "null", "None", "?", "--", "undefined"):
        odds = match.get("main_match_odds") or {}
        raw = odds.get("ah_linea")
    if raw in (None, "", "-", "N/A", "null", "None", "?", "--", "undefined"):
        return False
    raw_str = str(raw).strip()
    if "/" in raw_str:
        parts = raw_str.split("/")
        if len(parts) == 2:
            try:
                float(parts[0].strip().replace("−", "-").replace(",", "."))
                float(parts[1].strip().replace("−", "-").replace(",", "."))
                return True
            except (TypeError, ValueError):
                return False
    try:
        val = raw_str.replace("−", "-").replace(",", ".")
        float(val)
        return True
    except (TypeError, ValueError):
        return False


def _match_snapshot_goal_line(match):
    return _normalize_line(
        match.get("goal_line")
        or match.get("goal_line_alt")
        or match.get("goal_line_decimal")
        or (match.get("main_match_odds") or {}).get("goals_linea")
    )


def _cached_handicap(match):
    return _normalize_line(
        match.get("handicap")
        or (match.get("main_match_odds") or {}).get("ah_linea")
    )


def _cached_goal_line(match):
    return _normalize_line(
        match.get("goal_line")
        or match.get("goal_line_alt")
        or match.get("goal_line_decimal")
        or (match.get("main_match_odds") or {}).get("goals_linea")
    )


def _looks_like_complete_precache(match):
    if not isinstance(match, dict):
        return False
    if match.get("error") or match.get("precache_placeholder"):
        return False
    try:
        history_data_version = int(match.get("history_data_version") or 0)
    except (TypeError, ValueError):
        history_data_version = 0
    if history_data_version < 2:
        return False

    heavyweight_fields = (
        match.get("historical_matches_html"),
        match.get("market_analysis_html"),
    )
    if any(value for value in heavyweight_fields):
        return True

    dict_sections = (
        match.get("last_home_match"),
        match.get("last_away_match"),
        match.get("h2h_stadium"),
        match.get("h2h_general"),
        match.get("comparativas_indirectas"),
    )
    return any(isinstance(section, dict) and section for section in dict_sections)


def _load_existing_precache(conn: sqlite3.Connection, match_ids):
    existing = {}
    ordered_ids = []
    seen = set()
    for raw_id in match_ids:
        mid = str(raw_id).strip()
        if not mid or mid in seen:
            continue
        seen.add(mid)
        ordered_ids.append(mid)

    if not ordered_ids:
        return existing

    chunk_size = 400
    for start in range(0, len(ordered_ids), chunk_size):
        chunk = ordered_ids[start:start + chunk_size]
        placeholders = ", ".join(["?"] * len(chunk))
        rows = conn.execute(
            f"""
            SELECT match_id, payload_json
            FROM matches
            WHERE bucket = ?
              AND match_id IN ({placeholders})
            """,
            [PRECACHE_BUCKET, *chunk],
        ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row[1])
            except Exception:
                continue
            if isinstance(payload, dict):
                existing[str(row[0])] = payload
    return existing


def _can_reuse_precache(snapshot_match, cached_match):
    if not _looks_like_complete_precache(cached_match):
        return False

    snapshot_home = _normalize_name(snapshot_match.get("home_team") or snapshot_match.get("home_name"))
    snapshot_away = _normalize_name(snapshot_match.get("away_team") or snapshot_match.get("away_name"))
    cached_home = _normalize_name(cached_match.get("home_name") or cached_match.get("home_team"))
    cached_away = _normalize_name(cached_match.get("away_name") or cached_match.get("away_team"))

    if snapshot_home and cached_home and snapshot_home != cached_home:
        return False
    if snapshot_away and cached_away and snapshot_away != cached_away:
        return False

    snapshot_ah = _match_snapshot_handicap(snapshot_match)
    cached_ah = _cached_handicap(cached_match)
    if snapshot_ah and cached_ah and snapshot_ah != cached_ah:
        return False

    snapshot_ou = _match_snapshot_goal_line(snapshot_match)
    cached_ou = _cached_goal_line(cached_match)
    if snapshot_ou and cached_ou and snapshot_ou != cached_ou:
        return False

    return True


def build_jobs(
    db_path: Path,
    cache_key: str,
    out_path: Path,
    include_existing: bool = False,
    max_jobs: int = 0,
) -> int:
    if not db_path.exists():
        print(f"ERROR: No existe base SQL: {db_path}")
        return 2

    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT value FROM kv_store WHERE key = ?",
            (cache_key,),
        ).fetchone()

        if not row or not row[0]:
            print("ERROR: No hay snapshot de partidos en SQL. Ejecuta primero generar data.")
            return 3

        payload = json.loads(row[0])
        if isinstance(payload, str):
            payload = json.loads(payload)

        # Filtrar estrictamente solo partidos con handicap asiatico valido y no juveniles
        upcoming = [
            m for m in (raw_upcoming or [])
            if isinstance(m, dict) and _has_valid_handicap(m) and not is_unbettable_youth_match(m)
        ]
        if max_jobs and int(max_jobs) > 0:
            upcoming = upcoming[:int(max_jobs)]

        candidate_ids = []
        for match in upcoming:
            mid = match.get("id") or match.get("match_id")
            if mid is not None:
                candidate_ids.append(str(mid).strip())

        existing_precache = {}
        if not include_existing:
            existing_precache = _load_existing_precache(conn, candidate_ids)

        jobs = []
        seen = set()
        skipped_existing = 0
        forced_refresh = 0

        for match in upcoming:
            mid = match.get("id") or match.get("match_id")
            if mid is None:
                continue

            mid = str(mid).strip()
            if not mid or mid in seen:
                continue

            seen.add(mid)

            if not include_existing:
                cached = existing_precache.get(mid)
                if cached:
                    if _can_reuse_precache(match, cached):
                        skipped_existing += 1
                        continue
                    forced_refresh += 1

            ah_val = match.get("handicap") or (match.get("main_match_odds") or {}).get("ah_linea")
            ou_val = match.get("goal_line") or (match.get("main_match_odds") or {}).get("goals_linea")
            jobs.append(
                {
                    "id": mid,
                    "ah": str(ah_val) if ah_val is not None else "N/A",
                    "ou": str(ou_val) if ou_val is not None else "N/A",
                    "season": "json_snapshot",
                    "league_id": "json_snapshot",
                }
            )

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(jobs, fh, ensure_ascii=False, indent=2)

        total_upcoming = len(seen)
        print(f"Snapshot upcoming: {total_upcoming}")
        if include_existing:
            print("Modo completo: se incluyen tambien los ya cacheados.")
        else:
            print(f"Saltados por precache reutilizable: {skipped_existing}")
            print(f"Marcados para refresco por cambios/incompletos: {forced_refresh}")
        print(f"Partidos exportados a JSON: {len(jobs)} -> {out_path}")
        if not jobs:
            return 4
        return 0
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Genera temp_matches_job.json desde snapshot SQL.")
    parser.add_argument("--db", default="data/app_data.db", help="Ruta de la base SQLite")
    parser.add_argument("--cache-key", default="app_main_page_cache_v1", help="Clave en kv_store")
    parser.add_argument("--out", default="temp_matches_job.json", help="Ruta del JSON de salida")
    parser.add_argument(
        "--include-existing",
        action="store_true",
        help="Incluye tambien partidos ya analizados en precacheo (fuerza reproceso).",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=0,
        help="Límite de partidos a procesar (0 o negativo para todos los disponibles)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return build_jobs(
        db_path=Path(args.db),
        cache_key=args.cache_key,
        out_path=Path(args.out),
        include_existing=bool(args.include_existing),
        max_jobs=args.max_jobs,
    )


if __name__ == "__main__":
    sys.exit(main())
