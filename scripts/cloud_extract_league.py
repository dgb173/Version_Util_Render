#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Extrae una liga completa en GitHub Actions y publica su archivo cloud."""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import sys
from pathlib import Path
from typing import Any, Dict


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
SRC_DIR = PROJECT_DIR / "src"
sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(PROJECT_DIR))
os.chdir(PROJECT_DIR)

from modules import league_extraction_registry, league_handicap_scraper, sql_store
from modules.league_handicap_scraper import scrape_match_to_sql


CLOUD_BUCKET = "data_cloud_league.json"
CURRENT_HISTORY_VERSION = 3


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Extraer liga en la nube (GitHub Actions)")
    parser.add_argument(
        "--league-reference",
        "--league",
        dest="league_reference",
        required=True,
        help="ID o URL completa de la liga NowGoal",
    )
    parser.add_argument("--season", default="", help="Temporada opcional")
    parser.add_argument(
        "--match-status",
        choices=("finished", "all", "upcoming"),
        default="finished",
        help="Estado de los partidos que se analizaran",
    )
    parser.add_argument("--ah", type=float, default=None, help="AH visible exacto")
    parser.add_argument("--label", default="", help="Etiqueta visible de la extraccion")
    parser.add_argument("--company-id", type=int, default=8, help="ID de la casa de apuestas")
    parser.add_argument("--workers", type=int, default=5, help="Procesos simultaneos")
    parser.add_argument(
        "--max-matches",
        type=int,
        default=0,
        help="0 procesa la liga completa; otro valor limita la ejecucion para pruebas",
    )
    parser.add_argument("--force", action="store_true", help="Fuerza el reanalisis")
    return parser.parse_args(argv)


def _store_in_cloud_bucket(match_id: str) -> bool:
    """Mueve la copia SQL de un partido al archivo historico exclusivo de la nube."""
    stored = sql_store.get_match(str(match_id))
    if not isinstance(stored, dict):
        return False
    stored["match_id"] = str(stored.get("match_id") or match_id)
    sql_store.upsert_match(stored, bucket=CLOUD_BUCKET, state="historical")
    return True


def _process_match(match: Dict[str, Any], league_id: str, force: bool = False) -> Dict[str, Any]:
    """Analiza un partido y garantiza que termine en el bucket cloud."""
    match_id = str(match.get("id") or match.get("match_id") or "")
    existing = sql_store.get_match(match_id) if match_id else None
    try:
        history_version = int((existing or {}).get("history_data_version") or 0)
    except (TypeError, ValueError):
        history_version = 0

    # Los registros antiguos deben regenerarse para incorporar el historial de liga.
    effective_force = bool(force or (existing and history_version < CURRENT_HISTORY_VERSION))
    result = scrape_match_to_sql(match, str(league_id), effective_force)
    if result.get("status") in {"saved", "exists"} and _store_in_cloud_bucket(match_id):
        result["bucket"] = CLOUD_BUCKET
    return result


def main(argv=None) -> int:
    args = parse_args(argv)
    workers = max(1, min(10, int(args.workers)))
    max_matches = max(0, int(args.max_matches))

    print(
        f"Iniciando extraccion cloud: liga={args.league_reference!r}, "
        f"temporada={args.season or 'automatica'}, estado={args.match_status}, "
        f"AH={args.ah if args.ah is not None else 'todos'}"
    )
    preview = league_handicap_scraper.preview_league_handicap(
        league_reference=args.league_reference,
        target_ah=args.ah,
        season=args.season,
        company_id=args.company_id,
        match_status=args.match_status,
    )
    matches = league_handicap_scraper.sanitize_selected_matches(
        preview.get("matches") or [],
        company_id=args.company_id,
    )
    if max_matches:
        matches = matches[:max_matches]

    league_id = str(preview.get("league_id") or args.league_reference)
    league_name = str(preview.get("league_name") or f"Liga {league_id}")
    season = str(preview.get("season") or args.season)
    print(f"Calendario recuperado: {league_name} ({season}) - {len(matches)} partidos")
    if not matches:
        raise RuntimeError("La liga no devolvio partidos con los filtros solicitados")

    extraction = league_extraction_registry.create_extraction(
        league_id=league_id,
        league_name=league_name,
        season=season,
        company_id=args.company_id,
        target_ah=args.ah,
        matches=matches,
        label=args.label,
    )
    extraction_id = extraction["extraction_id"]
    league_extraction_registry.update_extraction_status(extraction_id, "running")

    counts: Dict[str, int] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_match, match, league_id, args.force): match
            for match in matches
        }
        for processed, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            source = futures[future]
            try:
                result = future.result()
            except Exception as exc:  # pragma: no cover - proteccion del runner cloud
                result = {"id": source.get("id"), "status": "error", "error": str(exc)}
            result["round"] = str(source.get("round") or "")
            league_extraction_registry.update_match(
                extraction_id,
                str(source.get("id") or ""),
                result,
            )
            status = str(result.get("status") or "error")
            counts[status] = counts.get(status, 0) + 1
            print(f"[{processed}/{len(matches)}] {source.get('id')}: {status}")

    failed = counts.get("error", 0)
    league_extraction_registry.update_extraction_status(
        extraction_id,
        "failed" if failed == len(matches) else "completed",
    )
    sql_store.export_bucket_to_json(CLOUD_BUCKET)
    print(f"Archivo publicado: data/{CLOUD_BUCKET}")
    print(f"Registro publicado: data/league_extractions.json")
    print(f"Resumen: {counts}")
    if failed == len(matches):
        raise RuntimeError("Todos los partidos de la liga fallaron")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
