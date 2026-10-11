#!/usr/bin/env python
"""
Fase 1 — Medición de cobertura de AH en calendarios livianos de NowGoal.

Objetivo:
    Evaluar de forma reproducible y con rigor estadístico la disponibilidad de
    la línea de Hándicap Asiático (AH) en los calendarios livianos oficiales
    (ScheduleList de NowGoal), contrastando el universo total de partidos frente
    al subconjunto de partidos finalizados.

Metodología:
    1. Denominadores explícitos: se calcula cobertura tanto sobre partidos totales
       como sobre partidos finalizados (candidatos reales para estudio histórico).
    2. Identificación estricta de candidatos:
       - Criterio Oficial (Exacto): |row_ah - target_ah| < 1e-6
       - Criterio Exploratorio (Rango ±0.25): |row_ah - target_ah| <= 0.25 + 1e-6
    3. Verificación de presencia en SQLite:
       - Se comprueba la existencia en SQLite ID por ID únicamente sobre los
         partidos identificados como candidatos (sin restar totales agregados).
    4. Deduplicación de temporadas:
       - Se registra la temporada oficial descubierta por NowGoal para evitar
         duplicidades derivadas de probar múltiples formatos (ej. 2024 vs 2024-2025).
       - Las temporadas no resueltas se documentan explícitamente en el informe.
    5. Métricas descriptivas puras:
       - No se aplican umbrales arbitrarios de suficiencia; se exponen los números brutos.

Uso:
    py scripts/measure_calendar_ah_coverage.py
    py scripts/measure_calendar_ah_coverage.py --target-ah 0.25
    py scripts/measure_calendar_ah_coverage.py --target-ah -0.75 --leagues 36 455 273
    py scripts/measure_calendar_ah_coverage.py --target-ah 0.5 --leagues 36 --years-back 3
    py scripts/measure_calendar_ah_coverage.py --output-json docs/informe_cobertura_ah.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
import sqlite3

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

os.environ.setdefault("PYTHONUTF8", "1")

from modules.league_handicap_scraper import (
    DEFAULT_COMPANY_ID,
    _discover_league,
    _flatten_schedule,
    parse_league_reference,
)


def _check_matches_in_sqlite_readonly(match_ids: Set[str]) -> Tuple[Set[str], Optional[str]]:
    """
    Comprueba si los IDs existen en SQLite en modo estrictamente de solo lectura (mode=ro).
    No ejecuta ensure_bootstrap(), DDL, migraciones ni escrituras de ningún tipo.
    Devuelve (found_ids, error_message). Si ocurre un error, se registra explícitamente
    para no enmascarar un fallo de base de datos como 'cero coincidencias'.
    """
    if not match_ids:
        return set(), None
    db_path = PROJECT_ROOT / "data" / "app_data.db"
    if not db_path.exists():
        return set(), f"DB_NOT_FOUND: no existe el archivo {db_path.as_posix()}"
    try:
        uri = f"file:{db_path.as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as conn:
            table_check = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='matches'"
            ).fetchone()
            if not table_check:
                return set(), "TABLE_NOT_FOUND: la tabla 'matches' no existe en SQLite"
            found: Set[str] = set()
            id_list = list(match_ids)
            for i in range(0, len(id_list), 500):
                chunk = id_list[i : i + 500]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT match_id FROM matches WHERE match_id IN ({placeholders})",
                    chunk,
                ).fetchall()
                for r in rows:
                    found.add(str(r[0]))
            return found, None
    except Exception as exc:
        return set(), f"SQLITE_READ_ERROR: {type(exc).__name__} - {str(exc)}"


import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Catálogo inicial de referencia para pruebas controladas
SAMPLE_LEAGUES: Dict[str, str] = {
    "36": "English Premier League",
    "31": "Spanish La Liga",
    "34": "Italian Serie A",
    "8": "German Bundesliga",
    "11": "French Ligue 1",
    "455": "Japan Nadeshiko League Women",
    "273": "Australian A-League",
    "29": "Portuguese Primeira Liga",
    "17": "Dutch Eredivisie",
    "196": "USA Major League Soccer",
}


def measure_single_season(
    league_id: str,
    target_ah: Optional[float],
    season_override: str = "",
    company_id: int = DEFAULT_COMPANY_ID,
) -> Dict[str, Any]:
    """Descarga y audita el calendario liviano para una combinación liga-temporada específica."""
    result: Dict[str, Any] = {
        "league_id": str(league_id),
        "league_name": SAMPLE_LEAGUES.get(str(league_id), f"Liga {league_id}"),
        "requested_season": season_override or "(auto-descubierta)",
        "discovered_season": None,
        "download_time_ms": 0,
        # Conteos de universo
        "total_matches": 0,
        "finished_matches": 0,
        # Cobertura AH
        "total_with_ah": 0,
        "total_without_ah": 0,
        "finished_with_ah": 0,
        "finished_without_ah": 0,
        # Tasas relativas (porcentajes)
        "pct_ah_over_total": 0.0,
        "pct_ah_over_finished": 0.0,
        # Candidatos Criterio Oficial (Exacto)
        "candidates_exact": 0,
        "candidates_exact_in_sql": 0,
        "candidates_exact_new": 0,
        "candidates_exact_ids": [],
        # Candidatos Criterio Exploratorio (Bucket ±0.25)
        "candidates_bucket": 0,
        "candidates_bucket_in_sql": 0,
        "candidates_bucket_new": 0,
        "candidates_bucket_ids": [],
        # Referencia general de la liga en SQLite local
        "total_league_matches_in_sql": 0,
        # Distribución de líneas AH encontradas
        "ah_distribution_finished": {},
        "error": None,
    }

    session = requests.Session()
    t0 = time.perf_counter()

    try:
        league_ref, req_season = parse_league_reference(league_id, season_override)
        discovered_season, league_data = _discover_league(session, league_ref, req_season)
        result["discovered_season"] = discovered_season

        match_map, rounds = _flatten_schedule(league_data)
        result["download_time_ms"] = round((time.perf_counter() - t0) * 1000)

        total_matches = len(match_map)
        finished_matches = 0
        total_with_ah = 0
        finished_with_ah = 0
        total_in_sql = 0

        exact_candidates: List[str] = []
        exact_in_sql: List[str] = []

        bucket_candidates: List[str] = []
        bucket_in_sql: List[str] = []

        ah_distribution: Dict[str, int] = {}

        all_match_ids: Set[str] = set()

        for match_id_raw, m in match_map.items():
            match_id = str(match_id_raw)
            all_match_ids.add(match_id)
            is_finished = int(m.get("source_state", 0)) == -1
            score = str(m.get("score", "-")).strip()
            has_score = bool(score and score not in ("-", "?:?", "?-?"))
            finished = is_finished and has_score

            if finished:
                finished_matches += 1

            row_ah = m.get("row_ah")
            if row_ah is not None:
                total_with_ah += 1
                if finished:
                    finished_with_ah += 1
                    ah_key = f"{float(row_ah):.2f}"
                    ah_distribution[ah_key] = ah_distribution.get(ah_key, 0) + 1

                    if target_ah is not None:
                        diff = abs(float(row_ah) - float(target_ah))
                        # 1. Criterio Oficial (Exacto)
                        if diff < 1e-6:
                            exact_candidates.append(match_id)

                        # 2. Criterio Exploratorio (Rango de tolerancia ±0.25)
                        if diff <= 0.25001:
                            bucket_candidates.append(match_id)

        # Consultas de presencia en SQLite estrictamente de solo lectura (mode=ro)
        sql_present_all, sql_err = _check_matches_in_sqlite_readonly(all_match_ids)
        total_in_sql = len(sql_present_all)
        result["sqlite_error"] = sql_err

        exact_in_sql = [mid for mid in exact_candidates if mid in sql_present_all]
        bucket_in_sql = [mid for mid in bucket_candidates if mid in sql_present_all]

        result["total_matches"] = total_matches
        result["finished_matches"] = finished_matches
        result["total_with_ah"] = total_with_ah
        result["total_without_ah"] = total_matches - total_with_ah
        result["finished_with_ah"] = finished_with_ah
        result["finished_without_ah"] = finished_matches - finished_with_ah

        result["pct_ah_over_total"] = (
            round((total_with_ah / total_matches) * 100, 2) if total_matches > 0 else 0.0
        )
        result["pct_ah_over_finished"] = (
            round((finished_with_ah / finished_matches) * 100, 2)
            if finished_matches > 0
            else 0.0
        )

        result["candidates_exact"] = len(exact_candidates)
        result["candidates_exact_in_sql"] = len(exact_in_sql)
        result["candidates_exact_new"] = len(exact_candidates) - len(exact_in_sql)
        result["candidates_exact_ids"] = exact_candidates

        result["candidates_bucket"] = len(bucket_candidates)
        result["candidates_bucket_in_sql"] = len(bucket_in_sql)
        result["candidates_bucket_new"] = len(bucket_candidates) - len(bucket_in_sql)
        result["candidates_bucket_ids"] = bucket_candidates

        result["total_league_matches_in_sql"] = total_in_sql
        result["ah_distribution_finished"] = dict(
            sorted(ah_distribution.items(), key=lambda item: float(item[0]))
        )

    except Exception as exc:
        result["download_time_ms"] = round((time.perf_counter() - t0) * 1000)
        result["error"] = str(exc)

    return result


def measure_league_with_history(
    league_id: str,
    target_ah: Optional[float],
    years_back: int = 0,
    company_id: int = DEFAULT_COMPANY_ID,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """
    Evalúa una liga y sus temporadas anteriores deduplicando temporadas idénticas
    y registrando explícitamente cualquier fallo de resolución.
    """
    measured_seasons: List[Dict[str, Any]] = []
    unresolved_attempts: List[Dict[str, str]] = []
    seen_discovered: Set[str] = set()

    current_year = dt.datetime.utcnow().year

    # Generar candidatos de temporada ordenados de más reciente a más antiguo
    candidate_tokens: List[str] = [""]  # "" indica la temporada por defecto/actual
    if years_back > 0:
        for offset in range(years_back + 1):
            y = current_year - offset
            candidate_tokens.extend([f"{y-1}-{y}", str(y), f"{y}-{y+1}"])

    # Preservar orden único
    deduped_tokens: List[str] = []
    for t in candidate_tokens:
        if t not in deduped_tokens:
            deduped_tokens.append(t)

    for token in deduped_tokens:
        metric = measure_single_season(
            league_id=league_id,
            target_ah=target_ah,
            season_override=token,
            company_id=company_id,
        )

        if metric["error"]:
            unresolved_attempts.append({
                "league_id": str(league_id),
                "attempted_season": token or "(actual)",
                "error": str(metric["error"]),
            })
            continue

        disc = metric["discovered_season"]
        if not disc or disc in seen_discovered:
            # Temporada ya medida mediante otro formato o no válida
            continue

        seen_discovered.add(disc)
        measured_seasons.append(metric)

    return measured_seasons, unresolved_attempts


def print_formal_report(
    measured: List[Dict[str, Any]],
    unresolved: List[Dict[str, str]],
    target_ah: Optional[float],
) -> None:
    """Imprime el informe formal con denominadores explícitos y desglose riguroso."""
    print()
    print("=" * 115)
    print("INFORME DE MEDICIÓN DE COBERTURA: LÍNEA AH EN CALENDARIOS LIVIANOS (FASE 1)")
    print("=" * 115)
    print(f"Fecha de ejecución UTC: {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Hándicap objetivo analizado: {target_ah if target_ah is not None else 'Ninguno (análisis global)'}")
    print(f"Total combinaciones liga-temporada auditadas con éxito: {len(measured)}")
    print(f"Total intentos de temporada no resueltos/rechazados: {len(unresolved)}")
    print("-" * 115)

    headers = (
        f"{'Liga / Competición':<28} "
        f"{'Temporada':<10} "
        f"{'Totales':>8} "
        f"{'Fin':>6} "
        f"{'AH(Fin)':>8} "
        f"{'% Cov(Fin)':>11} "
        f"{'Exact':>7} "
        f"{'Ex(New)':>8} "
        f"{'Rango±.25':>10} "
        f"{'Rng(New)':>9} "
        f"{'Desc(ms)':>9}"
    )
    print(headers)
    print("-" * 115)

    tot_matches = 0
    tot_finished = 0
    tot_fin_with_ah = 0
    tot_exact = 0
    tot_exact_new = 0
    tot_bucket = 0
    tot_bucket_new = 0

    for m in measured:
        tot_matches += m["total_matches"]
        tot_finished += m["finished_matches"]
        tot_fin_with_ah += m["finished_with_ah"]
        tot_exact += m["candidates_exact"]
        tot_exact_new += m["candidates_exact_new"]
        tot_bucket += m["candidates_bucket"]
        tot_bucket_new += m["candidates_bucket_new"]

        row = (
            f"{m['league_name'][:27]:<28} "
            f"{str(m['discovered_season'])[:9]:<10} "
            f"{m['total_matches']:>8} "
            f"{m['finished_matches']:>6} "
            f"{m['finished_with_ah']:>8} "
            f"{m['pct_ah_over_finished']:>10.2f}% "
            f"{m['candidates_exact']:>7} "
            f"{m['candidates_exact_new']:>8} "
            f"{m['candidates_bucket']:>10} "
            f"{m['candidates_bucket_new']:>9} "
            f"{m['download_time_ms']:>9}"
        )
        print(row)

    print("-" * 115)
    global_pct_fin = (
        round((tot_fin_with_ah / tot_finished) * 100, 2) if tot_finished > 0 else 0.0
    )
    tot_row = (
        f"{'TOTAL CONSOLIDADO':<28} "
        f"{'-':<10} "
        f"{tot_matches:>8} "
        f"{tot_finished:>6} "
        f"{tot_fin_with_ah:>8} "
        f"{global_pct_fin:>10.2f}% "
        f"{tot_exact:>7} "
        f"{tot_exact_new:>8} "
        f"{tot_bucket:>10} "
        f"{tot_bucket_new:>9} "
        f"{'-':>9}"
    )
    print(tot_row)
    print("=" * 115)

    # Detalle metodológico de lectura
    print("\nNOTAS METODOLÓGICAS DE LECTURA:")
    print("  1. % Cov(Fin) = (Partidos Finalizados con AH / Total Partidos Finalizados) * 100.")
    print("     Este es el denominador analítico correcto; los partidos no disputados no forman parte de la muestra.")
    print("  2. Exact = Coincidencia estricta (|AH_partido - target_ah| < 1e-6). Es el Criterio Oficial.")
    print("  3. Ex(New) = Partidos candidatos exactos que NO están en SQLite y requerirían scraping profundo.")
    print("  4. Rango±.25 = Coincidencia exploratoria (|AH_partido - target_ah| <= 0.25).")
    print("  5. Rng(New) = Partidos en rango exploratorio que NO están en SQLite.")

    if unresolved:
        print("\nINTENTOS DE TEMPORADA NO RESUELTOS (AUDITORÍA DE ORIGEN):")
        for u in unresolved[:10]:
            print(f"  - Liga ID {u['league_id']} | Token '{u['attempted_season']}': {u['error'][:80]}")
        if len(unresolved) > 10:
            print(f"  ... y {len(unresolved) - 10} intentos más no resueltos.")

    sqlite_errors = [m for m in measured if m.get("sqlite_error")]
    if sqlite_errors:
        print("\n⚠️ AVISOS DE LECTURA SQLITE (NO SE PUDO CONSULTAR DATA/APP_DATA.DB):")
        for se in sqlite_errors[:5]:
            print(f"  - {se['league_name']} ({se['discovered_season']}): {se['sqlite_error']}")
        print("  * Los conteos de candidatos nuevos (New) no deben considerarse definitivos si la BD no fue accesible.")
    print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Medición rigurosa de cobertura AH en calendarios de NowGoal (Fase 1)."
    )
    parser.add_argument(
        "--target-ah",
        type=float,
        default=None,
        help="Línea AH objetivo a evaluar (ej. 0.25, -0.75, 0.5).",
    )
    parser.add_argument(
        "--leagues",
        nargs="+",
        default=None,
        help="IDs numéricos de ligas a auditar. Por defecto usa catálogo de 10 ligas muestra.",
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=0,
        help="Número de temporadas históricas hacia atrás a auditar por cada liga (0 = temporada actual).",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Ruta de archivo para guardar el dataset completo de la auditoría en formato JSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    league_ids = [str(lid).strip() for lid in (args.leagues or list(SAMPLE_LEAGUES.keys()))]

    all_measured: List[Dict[str, Any]] = []
    all_unresolved: List[Dict[str, str]] = []

    for lid in league_ids:
        measured, unresolved = measure_league_with_history(
            league_id=lid,
            target_ah=args.target_ah,
            years_back=max(0, int(args.years_back)),
        )
        all_measured.extend(measured)
        all_unresolved.extend(unresolved)

    print_formal_report(all_measured, all_unresolved, args.target_ah)

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "metadata": {
                "generated_at_utc": dt.datetime.utcnow().isoformat(),
                "target_ah": args.target_ah,
                "audited_leagues_count": len(league_ids),
                "successful_seasons_count": len(all_measured),
                "unresolved_attempts_count": len(all_unresolved),
            },
            "successful_audits": all_measured,
            "unresolved_audits": all_unresolved,
        }
        out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[OK] Archivo JSON de auditoría guardado exitosamente en: {out_path.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
