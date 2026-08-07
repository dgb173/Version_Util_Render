#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de Extracción de Liga en la Nube (GitHub Actions).
Scrapea una liga completa por ID o URL y guarda los JSONs actualizados.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import os
import sys
from pathlib import Path

# Paths
script_dir = Path(__file__).resolve().parent
project_dir = script_dir.parent
src_dir = project_dir / 'src'

sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(project_dir))

os.chdir(project_dir)

from modules import data_manager, league_extraction_registry, league_handicap_scraper, sql_store

def parse_args():
    parser = argparse.ArgumentParser(description="Extraer liga en la nube (GitHub Actions)")
    parser.add_argument("--league", required=True, help="ID o URL de la liga (ej: 36)")
    parser.add_argument("--season", default="", help="Temporada opcional (ej: 2025-2026)")
    parser.add_argument("--company-id", type=int, default=8, help="ID casa de apuestas")
    parser.add_argument("--workers", type=int, default=10, help="Número de hilos de extracción")
    parser.add_argument("--force", action="store_true", help="Forzar reanálisis de partidos existentes")
    return parser.parse_args()

def main():
    args = parse_args()
    print(f"🚀 Iniciando extracción cloud para liga '{args.league}' (Temporada: '{args.season or 'actual'}')...")
    
    # 1. Previsualizar/Obtener partidos de la liga
    preview = league_handicap_scraper.preview_league_handicap(
        league_reference=args.league,
        season=args.season,
        company_id=args.company_id,
        match_status="all",
        competition_type="league"
    )
    
    matches = preview.get("matches", [])
    league_id = preview.get("league_id", args.league)
    season = preview.get("season", args.season)
    league_name = preview.get("league_name", f"Liga {league_id}")
    
    print(f"📋 Encontrados {len(matches)} partidos para {league_name} ({season}).")
    if not matches:
        print("⚠️ No se encontraron partidos para procesar.")
        return

    # 2. Crear o recuperar registro de extracción
    sanitized = league_handicap_scraper.sanitize_selected_matches(matches, company_id=args.company_id)
    extraction = league_extraction_registry.create_extraction(
        league_id=league_id,
        season=season,
        matches=sanitized,
        league_name=league_name,
        company_id=args.company_id
    )
    extraction_id = extraction["id"]
    print(f"🆔 ID de Registro de Extracción: {extraction_id}")

    league_extraction_registry.update_extraction_status(extraction_id, "running")

    # 3. Scrapear en paralelo con ThreadPoolExecutor
    processed = 0
    total = len(sanitized)
    
    def scrape_single(match):
        return league_handicap_scraper.scrape_match_to_sql(match, league_id, args.force)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(scrape_single, match): match for match in sanitized}
        for future in concurrent.futures.as_completed(futures):
            source_match = futures[future]
            try:
                res = future.result()
            except Exception as exc:
                res = {"id": source_match.get("id"), "status": "error", "error": str(exc)}
            
            res["round"] = str(source_match.get("round") or "")
            league_extraction_registry.update_match(extraction_id, str(source_match.get("id") or ""), res)
            
            processed += 1
            if processed % 10 == 0 or processed == total:
                print(f"⏳ Progreso: {processed}/{total} partidos procesados ({res.get('status')})")

    league_extraction_registry.update_extraction_status(extraction_id, "completed")
    print("✅ Extracción de liga completada con éxito.")

    # 4. Exportar JSONs para sincronizar con local
    try:
        data_manager.export_precacheo_json()
        print("💾 data_precacheo.json exportado correctamente.")
    except Exception as exc:
        print(f"⚠️ Aviso al exportar precacheo: {exc}")

if __name__ == "__main__":
    main()
