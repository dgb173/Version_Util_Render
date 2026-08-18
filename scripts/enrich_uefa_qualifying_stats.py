"""Completa tiros/ataques de los historicos UEFA y los guarda en ambas tablas SQL."""

from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from modules import sql_store  # noqa: E402
from modules.estudio_scraper import _df_to_rows, get_match_progression_stats_data  # noqa: E402


REQUIRED_LABELS = {"tiros", "tiros a puerta", "ataques", "ataques peligrosos"}


def _labels(rows):
    return {str(row.get("label") or "").strip().lower() for row in (rows or []) if isinstance(row, dict)}


def _complete(rows) -> bool:
    return REQUIRED_LABELS.issubset(_labels(rows))


def _fetch_stats(match_id: str):
    try:
        rows = _df_to_rows(get_match_progression_stats_data(str(match_id)))
        return str(match_id), rows, None
    except Exception as exc:  # el lote debe continuar aunque falle una pagina
        return str(match_id), [], str(exc)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--retry-unavailable", action="store_true")
    parser.add_argument("--competitions", default="")
    args = parser.parse_args()

    competitions = [value.strip() for value in args.competitions.split(",") if value.strip()]
    catalogue = sql_store.fetch_uefa_qualifying_matches(
        competition_ids=competitions or None,
        limit=20000,
    )
    ids = [str(row.get("match_id")) for row in catalogue if row.get("match_id")]
    matches = {
        str(row.get("match_id")): row
        for row in sql_store.fetch_matches_by_ids(ids, state="historical", limit=20000)
    }

    pending = []
    already_complete = 0
    skipped_unavailable = 0
    for row in catalogue:
        match_id = str(row.get("match_id") or "")
        stored = matches.get(match_id) or {}
        stats = stored.get("stats_rows") or row.get("stats_rows") or []
        if _complete(stats):
            already_complete += 1
            continue
        status = str(stored.get("stats_status") or row.get("stats_status") or "")
        if status == "unavailable" and not args.retry_unavailable:
            skipped_unavailable += 1
            continue
        pending.append(match_id)

    if args.limit > 0:
        pending = pending[: args.limit]

    summary = {
        "catalogue": len(catalogue),
        "already_complete": already_complete,
        "skipped_unavailable": skipped_unavailable,
        "pending": len(pending),
        "complete": 0,
        "partial": 0,
        "unavailable": 0,
        "errors": 0,
    }
    print(json.dumps({"event": "start", **summary}, ensure_ascii=False), flush=True)

    workers = max(1, min(int(args.workers), 32))
    write_buffer = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_stats, match_id): match_id for match_id in pending}
        for index, future in enumerate(as_completed(futures), start=1):
            match_id, rows, error = future.result()
            if error:
                summary["errors"] += 1
                status = "error"
            elif _complete(rows):
                summary["complete"] += 1
                status = "complete"
            elif rows:
                summary["partial"] += 1
                status = "partial"
            else:
                summary["unavailable"] += 1
                status = "unavailable"
            write_buffer.append({
                "match_id": match_id,
                "stats_rows": rows,
                "status": status,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            if len(write_buffer) >= 25:
                sql_store.bulk_update_uefa_qualifying_stats(write_buffer)
                write_buffer.clear()
            if index % 25 == 0 or index == len(pending):
                print(json.dumps({"event": "progress", "processed": index, **summary}, ensure_ascii=False), flush=True)

    if write_buffer:
        sql_store.bulk_update_uefa_qualifying_stats(write_buffer)

    print(json.dumps({"event": "done", **summary}, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
