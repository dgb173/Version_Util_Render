#!/usr/bin/env python
"""Extrae una liga en GitHub Actions y publica un histórico ligero para Render."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from modules import league_extraction_registry, sql_store  # noqa: E402
from modules.league_handicap_scraper import (  # noqa: E402
    preview_league_handicap,
    sanitize_selected_matches,
    scrape_match_to_sql,
)


CLOUD_BUCKET = "data_cloud_league.json"
MAX_GITHUB_FILE_BYTES = 95 * 1024 * 1024


def _optional_float(value: str) -> float | None:
    cleaned = str(value or "").strip()
    return None if not cleaned else float(cleaned)


def _store_in_cloud_bucket(match_id: str) -> bool:
    stored = sql_store.get_match(match_id)
    if not isinstance(stored, dict) or stored.get("error"):
        return False
    sql_store.upsert_match(stored, bucket=CLOUD_BUCKET, state="historical")
    return True


def _process_match(
    match: Dict[str, Any],
    league_id: str,
    force: bool,
) -> Dict[str, Any]:
    match_id = str(match.get("id") or "")
    existing = sql_store.get_match(match_id)
    if isinstance(existing, dict) and not existing.get("error") and not force:
        _store_in_cloud_bucket(match_id)
        return {"id": match_id, "status": "exists", "bucket": CLOUD_BUCKET}

    result = scrape_match_to_sql(match, league_id, force=force)
    if result.get("status") in {"saved", "exists"} and _store_in_cloud_bucket(match_id):
        result["bucket"] = CLOUD_BUCKET
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extrae una liga y conserva los análisis en data_cloud_league.json."
    )
    parser.add_argument("--league-reference", required=True, help="URL o ID de liga NowGoal")
    parser.add_argument("--season", default="", help="Temporada; vacía para autodetectar")
    parser.add_argument("--ah", default="", help="AH visible exacto; vacío para todos")
    parser.add_argument("--company-id", type=int, default=8)
    parser.add_argument(
        "--match-status",
        choices=("finished", "all", "upcoming"),
        default="finished",
    )
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--label", default="")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--max-matches",
        type=int,
        default=0,
        help="0 procesa todos; un valor positivo limita esta ejecución",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    workers = max(1, min(10, args.workers))
    target_ah = _optional_float(args.ah)

    print("Consultando calendario y cuotas de la liga...", flush=True)
    preview = preview_league_handicap(
        league_reference=args.league_reference,
        target_ah=target_ah,
        season=args.season,
        company_id=args.company_id,
        match_status=args.match_status,
    )
    registered_matches = sanitize_selected_matches(
        preview.get("matches") or [],
        args.company_id,
    )
    if not registered_matches:
        raise RuntimeError("La consulta no devolvió partidos con esos filtros.")
    matches = (
        registered_matches[: args.max_matches]
        if args.max_matches > 0
        else registered_matches
    )

    extraction = league_extraction_registry.register_existing_league(
        league_id=preview["league_id"],
        league_name=preview["league_name"],
        season=preview["season"],
        company_id=preview["company_id"],
        target_ah=preview["target_ah"],
        matches=registered_matches,
        label=args.label,
    )
    extraction_id = extraction["extraction_id"]
    league_extraction_registry.update_extraction_status(extraction_id, "running")

    counts: Counter[str] = Counter()
    print(
        f"Analizando {len(matches)} partidos de {preview['league_name']} "
        f"({preview['season']}) con {workers} workers...",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_process_match, match, preview["league_id"], args.force): match
            for match in matches
        }
        for index, future in enumerate(as_completed(futures), start=1):
            source = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "id": str(source.get("id") or ""),
                    "status": "error",
                    "error": str(exc),
                }
            counts[str(result.get("status") or "error")] += 1
            league_extraction_registry.update_match(
                extraction_id,
                str(result.get("id") or source.get("id") or ""),
                result,
            )
            print(
                f"[{index}/{len(matches)}] {result.get('id')}: "
                f"{result.get('status')} {result.get('error') or ''}",
                flush=True,
            )

    output_path = sql_store.export_bucket_to_json(CLOUD_BUCKET)
    if output_path.stat().st_size > MAX_GITHUB_FILE_BYTES:
        raise RuntimeError(
            f"{output_path.name} supera 95 MB. Reduce el lote antes de publicarlo."
        )

    final_status = "completed" if len(matches) == len(registered_matches) else "registered"
    league_extraction_registry.update_extraction_status(extraction_id, final_status)
    summary = {
        "extraction_id": extraction_id,
        "league_id": preview["league_id"],
        "league_name": preview["league_name"],
        "season": preview["season"],
        "registered": len(registered_matches),
        "processed": len(matches),
        "counts": dict(counts),
        "cloud_matches": len(sql_store.fetch_matches(bucket=CLOUD_BUCKET)),
        "cloud_file_mb": round(output_path.stat().st_size / 1024 / 1024, 2),
    }
    print("\n" + json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
