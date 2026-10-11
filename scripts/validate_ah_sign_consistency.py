#!/usr/bin/env python
"""
Validación Controlada de Consistencia de AH y Convención de Signos.

Objetivo:
    Comparar de forma empírica una muestra representativa de partidos existentes
    en SQLite frente a sus correspondientes registros en el calendario liviano
    (ScheduleList row[10]) de NowGoal, para:
    1. Identificar con rigor técnico qué mide cada fuente:
       - Cal(AH): Línea registrada en el calendario liviano NowGoal (ScheduleList row[10]).
       - SQL(AH): Línea earlyOdds de Bet365/Sbobet almacenada en SQLite (main_match_odds.ah_linea).
    2. Comprobar los 3 casos de mercado:
       - Local Favorito (AH > 0)
       - Visitante Favorito (AH < 0)
       - Partido Nivelado (AH == 0)

    3. Verificar si el signo y rol del favorito concuerdan o si existe inversión entre fuentes.

Uso:
    py scripts/validate_ah_sign_consistency.py
    py scripts/validate_ah_sign_consistency.py --output-json docs/validacion_signos_ah.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

os.environ.setdefault("PYTHONUTF8", "1")

from modules.league_handicap_scraper import (
    _discover_league,
    _flatten_schedule,
    parse_league_reference,
)
from modules.nowgoal_fetcher import parse_handicap_numeric

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Ligas principales para la muestra de validación con sus nombres exactos en SQLite
VALIDATION_LEAGUES: Dict[str, Dict[str, Any]] = {
    "36": {
        "name": "English Premier League",
        "exact_name": "English Premier League",
        "seasons": ["2026-2027", "2025-2026"],
    },
    "31": {
        "name": "Spanish La Liga",
        "exact_name": "Spanish La Liga",
        "seasons": ["2026-2027", "2025-2026"],
    },
    "34": {
        "name": "Italian Serie A",
        "exact_name": "Italian Serie A",
        "seasons": ["2026-2027", "2025-2026"],
    },
    "8": {
        "name": "German Bundesliga",
        "exact_name": "German Bundesliga",
        "seasons": ["2026-2027", "2025-2026"],
    },
}


def _match_league_id(payload: Dict[str, Any]) -> Optional[str]:
    raw_name = str(payload.get("league_name") or payload.get("league") or "").strip()
    for lid, cfg in VALIDATION_LEAGUES.items():
        if raw_name == cfg["exact_name"]:
            return lid
    return None


def get_cached_matches_stratified() -> Dict[str, List[Dict[str, Any]]]:
    """
    Obtiene partidos de SQLite estratificados por signo:
    - Local Favorito (AH > 0)
    - Visitante Favorito (AH < 0)
    - Partido Nivelado (AH == 0)
    """
    db_path = PROJECT_ROOT / "data" / "app_data.db"
    if not db_path.exists():
        return {}

    uri = f"file:{db_path.as_posix()}?mode=ro"
    stratified: Dict[str, Dict[str, List[Dict[str, Any]]]] = {
        lid: {"pos": [], "neg": [], "zero": []} for lid in VALIDATION_LEAGUES
    }

    try:
        with sqlite3.connect(uri, uri=True) as conn:
            conn.row_factory = sqlite3.Row
            target_names = [cfg["exact_name"] for cfg in VALIDATION_LEAGUES.values()]
            placeholders = ",".join("?" for _ in target_names)
            query = f"""
                SELECT match_id, payload_json, handicap, score
                FROM matches
                WHERE json_extract(payload_json, '$.league_name') IN ({placeholders})
                ORDER BY match_id DESC
                LIMIT 1000
            """
            rows = conn.execute(query, target_names).fetchall()

            for r in rows:
                try:
                    payload = json.loads(r["payload_json"])
                except Exception:
                    continue

                lid = _match_league_id(payload)
                if not lid:
                    continue

                m_odds = payload.get("main_match_odds") or {}
                ah_raw = m_odds.get("ah_linea") or r["handicap"]
                if ah_raw is None or ah_raw == "" or ah_raw == "N/A":
                    continue

                try:
                    ah_num = float(ah_raw)
                except (ValueError, TypeError):
                    continue

                match_info = {
                    "match_id": str(r["match_id"]),
                    "league_id": lid,
                    "home_name": payload.get("home_name") or payload.get("home_team"),
                    "away_name": payload.get("away_name") or payload.get("away_team"),
                    "score": r["score"] or payload.get("score"),
                    "sqlite_early_ah": ah_num,
                }

                if ah_num > 0 and len(stratified[lid]["pos"]) < 2:
                    stratified[lid]["pos"].append(match_info)
                elif ah_num < 0 and len(stratified[lid]["neg"]) < 2:
                    stratified[lid]["neg"].append(match_info)
                elif abs(ah_num) < 1e-6 and len(stratified[lid]["zero"]) < 2:
                    stratified[lid]["zero"].append(match_info)

    except Exception as exc:
        print(f"[ERROR SQLite] {exc}", file=sys.stderr)

    # Aplanar por liga
    flat_results: Dict[str, List[Dict[str, Any]]] = {}
    for lid, buckets in stratified.items():
        flat_results[lid] = buckets["pos"] + buckets["neg"] + buckets["zero"]

    return flat_results


def run_cross_validation() -> List[Dict[str, Any]]:
    """Descarga calendarios livianos y contrasta ID por ID contra SQLite."""
    cached_groups = get_cached_matches_stratified()
    validation_records: List[Dict[str, Any]] = []

    session = requests.Session()

    for lid, cached_list in cached_groups.items():
        if not cached_list:
            continue

        cfg = VALIDATION_LEAGUES[lid]
        lname = cfg["name"]

        combined_match_map: Dict[str, Dict[str, Any]] = {}
        resolved_season = ""

        for s_token in cfg["seasons"]:
            try:
                lref, req_s = parse_league_reference(lid, s_token)
                disc_s, ldata = _discover_league(session, lref, req_s)
                m_map, _ = _flatten_schedule(ldata)
                combined_match_map.update(m_map)
                if not resolved_season:
                    resolved_season = disc_s
            except Exception:
                continue

        for cm in cached_list:
            mid = cm["match_id"]
            cal_match = combined_match_map.get(mid)

            record: Dict[str, Any] = {
                "match_id": mid,
                "league_id": lid,
                "league_name": lname,
                "season": resolved_season,
                "home": cm["home_name"],
                "away": cm["away_name"],
                "score": cm["score"],
                "in_calendar": cal_match is not None,
                "calendar_ah": cal_match.get("row_ah") if cal_match else None,
                "sqlite_early_ah": cm["sqlite_early_ah"],
                "sign_agreement": "UNKNOWN",
                "line_comparison": "N/D",
                "calendar_fav": "UNKNOWN",
                "sqlite_fav": "UNKNOWN",
            }

            if cal_match and cal_match.get("row_ah") is not None and cm["sqlite_early_ah"] is not None:
                cal_ah = float(cal_match["row_ah"])
                sql_ah = float(cm["sqlite_early_ah"])

                # Determinar rol de favorito en calendario y SQLite
                record["calendar_fav"] = (
                    "LOCAL" if cal_ah > 0 else ("VISITANTE" if cal_ah < 0 else "NIVELADO (0)")
                )
                record["sqlite_fav"] = (
                    "LOCAL" if sql_ah > 0 else ("VISITANTE" if sql_ah < 0 else "NIVELADO (0)")
                )

                # Comparación de signos
                if abs(cal_ah) < 1e-6 and abs(sql_ah) < 1e-6:
                    record["sign_agreement"] = "AMBOS_CERO (Nivelado)"
                elif (cal_ah > 0 and sql_ah > 0) or (cal_ah < 0 and sql_ah < 0):
                    record["sign_agreement"] = "MISMO_SIGNO"
                elif (cal_ah > 0 and sql_ah < 0) or (cal_ah < 0 and sql_ah > 0):
                    record["sign_agreement"] = "SIGNO_OPUESTO"
                else:
                    record["sign_agreement"] = "TRANSICION_CERO"

                # Diferencia entre fuentes
                diff = sql_ah - cal_ah

                if abs(diff) < 1e-6:
                    record["line_comparison"] = f"{cal_ah:+.2f} (Idéntico)"
                else:
                    record["line_comparison"] = f"Cal {cal_ah:+.2f} vs SQL {sql_ah:+.2f} (dif {diff:+.2f})"

            validation_records.append(record)

    return validation_records


def print_comparison_table(records: List[Dict[str, Any]]) -> None:
    """Imprime tabla comparativa con formato formal entre la línea del calendario NowGoal y earlyOdds de SQLite."""
    print()
    print("=" * 125)
    print("VALIDACIÓN ESTRATIFICADA: LÍNEA CALENDARIO NOWGOAL vs EARLYODDS SQLITE")
    print("=" * 125)
    print(f"Total partidos auditados: {len(records)}")
    print("-" * 125)

    headers = (
        f"{'ID':<9} "
        f"{'Competición':<22} "
        f"{'Local vs Visitante':<30} "
        f"{'Marc':>5} "
        f"{'Cal(AH)':>9} "
        f"{'SQL(AH)':>8} "
        f"{'Fav(Cal)':>9} "
        f"{'Fav(SQL)':>9} "
        f"{'Consistencia Signo':>20}"
    )
    print(headers)
    print("-" * 125)


    same_sign_count = 0
    zero_count = 0
    opposite_sign_count = 0
    transition_count = 0

    for r in records:
        h_vs_a = f"{str(r['home'])[:13]} v {str(r['away'])[:13]}"
        cal_str = f"{r['calendar_ah']:+.2f}" if r["calendar_ah"] is not None else "N/D"
        sql_str = f"{r['sqlite_early_ah']:+.2f}" if r["sqlite_early_ah"] is not None else "N/D"

        sig = r["sign_agreement"]
        if sig == "MISMO_SIGNO":
            same_sign_count += 1
        elif sig == "AMBOS_CERO (Nivelado)":
            zero_count += 1
        elif sig == "SIGNO_OPUESTO":
            opposite_sign_count += 1
        elif sig == "TRANSICION_CERO":
            transition_count += 1

        row = (
            f"{r['match_id']:<9} "
            f"{r['league_name'][:21]:<22} "
            f"{h_vs_a:<30} "
            f"{str(r['score'] or '-'):>5} "
            f"{cal_str:>9} "
            f"{sql_str:>8} "
            f"{r['calendar_fav'][:9]:>9} "
            f"{r['sqlite_fav'][:9]:>9} "
            f"{sig:>20}"
        )
        print(row)


    print("-" * 125)
    print("RESUMEN DE CONSISTENCIA:")
    print(f"  - Mismo signo y mismo favorito (MISMO_SIGNO): {same_sign_count}")
    print(f"  - Partidos nivelados en ambas fuentes (AMBOS_CERO): {zero_count}")
    print(f"  - Movimiento hacia/desde línea cero (TRANSICION_CERO): {transition_count}")
    print(f"  - Signos opuestos / inversión de favoritismo (SIGNO_OPUESTO): {opposite_sign_count}")
    print("=" * 125)
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Validación estratificada de AH y signos.")
    parser.add_argument("--output-json", type=str, default=None, help="Ruta para guardar JSON.")
    args = parser.parse_args()

    records = run_cross_validation()
    print_comparison_table(records)

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "metadata": {
                "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "total_audited": len(records),
            },
            "records": records,
        }
        out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[OK] Archivo guardado en: {out_path.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
