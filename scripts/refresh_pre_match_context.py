#!/usr/bin/env python
"""Genera de forma incremental el contexto previo de los partidos próximos."""

from __future__ import annotations

import argparse
import concurrent.futures
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from modules import sql_store  # noqa: E402
from modules.estudio_scraper import analizar_contexto_previo_rapido  # noqa: E402


PRECACHE_BUCKET = "data_precacheo.json"
SNAPSHOT_KEY = "app_main_page_cache_v1"


def _match_id(match):
    return str((match or {}).get("match_id") or (match or {}).get("id") or "").strip()


def _context_is_reusable(match, now_epoch=None, ttl_hours=8):
    context = (match or {}).get("pre_match_context")
    if not isinstance(context, dict):
        return False
    current = context.get("current")
    if not isinstance(current, dict):
        return False
    if not isinstance(current.get("home_matches"), list) or not isinstance(current.get("away_matches"), list):
        return False
    try:
        generated = float(context.get("generated_at_epoch") or 0)
    except (TypeError, ValueError):
        return False
    now_value = float(now_epoch if now_epoch is not None else time.time())
    age = now_value - generated
    return generated > 0 and 0 <= age < max(1, int(ttl_hours)) * 3600


def _select_context_jobs(snapshot, cached_by_id, force=False, now_epoch=None, ttl_hours=8):
    upcoming = snapshot.get("upcoming_matches", []) if isinstance(snapshot, dict) else []
    jobs = []
    seen = set()
    for item in upcoming:
        match_id = _match_id(item)
        if not match_id or match_id in seen:
            continue
        seen.add(match_id)
        cached = cached_by_id.get(match_id)
        if not isinstance(cached, dict):
            continue
        if not force and _context_is_reusable(cached, now_epoch=now_epoch, ttl_hours=ttl_hours):
            continue
        jobs.append(cached)
    return jobs


def _scrape_context(match):
    match_id = _match_id(match)
    main_odds = match.get("main_match_odds") or {}
    context = analizar_contexto_previo_rapido(
        match_id,
        current_ah=main_odds.get("ah_linea") or match.get("handicap"),
        current_goal_line=main_odds.get("goals_linea") or match.get("goal_line"),
    )
    if not isinstance(context, dict) or context.get("error"):
        return match_id, None, (context or {}).get("error", "contexto vacío")
    return match_id, context, None


def refresh_contexts(workers=6, force=False, ttl_hours=8):
    snapshot = sql_store.get_json_state(SNAPSHOT_KEY, {})
    if not isinstance(snapshot, dict):
        snapshot = {}
    cached_rows = sql_store.fetch_matches(bucket=PRECACHE_BUCKET)
    cached_by_id = {_match_id(row): row for row in cached_rows if _match_id(row)}
    jobs = _select_context_jobs(
        snapshot,
        cached_by_id,
        force=force,
        ttl_hours=ttl_hours,
    )

    print(
        f"Contexto previo: próximos={len(snapshot.get('upcoming_matches', []))} "
        f"cacheados={len(cached_by_id)} pendientes={len(jobs)}"
    )
    if not jobs:
        print("Todos los contextos próximos siguen siendo reutilizables.")
        return 0, 0

    completed = failed = 0
    max_workers = max(1, min(int(workers or 1), 10))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_scrape_context, match): match for match in jobs}
        for future in concurrent.futures.as_completed(futures):
            source = futures[future]
            match_id = _match_id(source)
            try:
                _, context, error = future.result()
            except Exception as exc:
                context, error = None, str(exc)
            if error or not context:
                failed += 1
                print(f"  Contexto {match_id}: ERROR {error}")
                continue

            merged = dict(source)
            merged["pre_match_context"] = context
            merged["context_scraped_at"] = context.get("generated_at")
            sql_store.upsert_match(merged, bucket=PRECACHE_BUCKET, state="precacheo")
            completed += 1
            if (completed + failed) % 10 == 0 or completed + failed == len(jobs):
                print(
                    f"  Progreso contexto: {completed + failed}/{len(jobs)} "
                    f"(ok={completed}, fail={failed})"
                )
    return completed, failed


def main():
    parser = argparse.ArgumentParser(
        description="Actualiza casa/fuera, último H2H y AH similares del precacheo próximo."
    )
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--ttl-hours", type=int, default=8)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    completed, failed = refresh_contexts(
        workers=args.workers,
        force=args.force,
        ttl_hours=args.ttl_hours,
    )
    print(f"Contexto previo finalizado: ok={completed}, fail={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
