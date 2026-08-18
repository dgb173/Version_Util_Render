#!/usr/bin/env python3
"""Predice AH y O/U con explicacion y salida NO BET obligatoria cuando falta edge."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import joblib
import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.universal_autotrainer.features import build_feature_row, load_matches_from_db  # type: ignore
else:
    from .features import build_feature_row, load_matches_from_db


def _format_line(value: float) -> str:
    if abs(value) < 1e-9:
        return "0"
    text = f"{value:+.2f}".rstrip("0").rstrip(".")
    return text


def _confidence(edge: float, calibration: List[Dict[str, Any]]) -> Dict[str, Any]:
    magnitude = abs(edge)
    if not calibration:
        return {"band": "SIN CALIBRAR", "positive_rate_test": None, "n_test": 0}
    best = min(calibration, key=lambda item: 0.0 if item["edge_min"] <= magnitude <= item["edge_max"] else min(abs(magnitude - item["edge_min"]), abs(magnitude - item["edge_max"])))
    rate = best.get("positive_rate")
    band = "ALTA" if rate is not None and rate >= 0.58 else ("MEDIA" if rate is not None and rate >= 0.52 else "BAJA")
    return {"band": band, "positive_rate_test": rate, "mean_profit_even_odds_test": best.get("mean_profit_even_odds"), "n_test": best.get("n")}


def _reasons(row: Dict[str, Any], market: str, direction: str) -> List[str]:
    reasons: List[str] = []
    if market == "AH":
        if row.get("flag_home_result_inflation") == 1.0:
            reasons.append("El local llega revalorizado por un resultado mejor que su dominio real.")
        if row.get("flag_hidden_resistant_away") == 1.0:
            reasons.append("El visitante presenta alta resistencia fuera (tasa de no derrota específica).")
        if row.get("flag_weak_home_condition") == 1.0:
            reasons.append("La fortaleza global del local no se confirma en su estadio.")
        if row.get("flag_common_market_home") == 1.0:
            reasons.append("Las líneas contra rivales comunes valoraban mejor al local.")
        if row.get("flag_common_market_away") == 1.0:
            reasons.append("Las líneas contra rivales comunes valoraban mejor al visitante.")
        revaluation = row.get("h2h_general_revaluation")
        if isinstance(revaluation, float) and math.isfinite(revaluation) and abs(revaluation) >= 0.25:
            side = "local" if revaluation > 0 else "visitante"
            reasons.append(f"La línea revaloriza al {side} en {abs(revaluation):.2f} respecto al H2H comparable.")
    else:
        if row.get("flag_ou_inflated_recent_score") == 1.0:
            reasons.append("La línea total puede estar inflada por un marcador reciente extremo.")
        if row.get("flag_local_low_production_high_ou") == 1.0:
            reasons.append("El total es alto pese a una producción peligrosa inferior del local.")
        home_gf = row.get("home_specific_gf_pg")
        away_ga = row.get("away_specific_ga_pg")
        if all(isinstance(v, float) and math.isfinite(v) for v in (home_gf, away_ga)):
            reasons.append(f"Cruce condición: local {home_gf:.2f} GF/casa y visitante {away_ga:.2f} GC/fuera.")
    if not reasons:
        reasons.append(f"La combinación multivariable favorece {direction}; ninguna regla aislada decide el pick.")
    return reasons[:5]


def format_prediction(row: Dict[str, Any], artifact: Dict[str, Any], ah_edge: float, ou_edge: float) -> Dict[str, Any]:
    ah = float(row["current_ah"])
    ou = row.get("current_ou")
    ah_selected = artifact.get("ah_enabled", False) and abs(ah_edge) >= float(artifact["ah_threshold"])
    ou_selected = artifact.get("ou_enabled", False) and abs(ou_edge) >= float(artifact["ou_threshold"])
    if ah_edge >= 0:
        ah_side = row["home_name"]
        ah_bet_line = -ah
        ah_direction = "LOCAL"
    else:
        ah_side = row["away_name"]
        ah_bet_line = ah
        ah_direction = "VISITANTE"
    ou_direction = "OVER" if ou_edge >= 0 else "UNDER"
    result = {
        "match_id": row["match_id"],
        "match": f"{row['home_name']} vs {row['away_name']}",
        "league": row["league"],
        "date": row["match_date"],
        "market_lines": {"ah_nowgoal": ah, "ou": ou},
        "ah": {
            "pick": f"{ah_side} {_format_line(ah_bet_line)}" if ah_selected else "NO BET",
            "candidate": f"{ah_side} {_format_line(ah_bet_line)}",
            "direction": ah_direction,
            "expected_unit_edge_even_odds": ah_edge if ah_edge >= 0 else -ah_edge,
            "threshold": artifact["ah_threshold"],
            "enabled_by_untouched_test": artifact.get("ah_enabled", False),
            "confidence": _confidence(ah_edge, artifact.get("calibration", {}).get("ah", [])),
            "reasons": _reasons(row, "AH", ah_direction),
        },
        "ou": {
            "pick": f"{ou_direction} {ou:g}" if ou_selected and isinstance(ou, float) and math.isfinite(ou) else "NO BET",
            "candidate": f"{ou_direction} {ou:g}" if isinstance(ou, float) and math.isfinite(ou) else "SIN LINEA",
            "expected_unit_edge_even_odds": abs(ou_edge),
            "threshold": artifact["ou_threshold"],
            "enabled_by_untouched_test": artifact.get("ou_enabled", False),
            "confidence": _confidence(ou_edge, artifact.get("calibration", {}).get("ou", [])),
            "reasons": _reasons(row, "OU", ou_direction),
        },
        "guardrail": "Pronóstico selectivo, no garantía. NO BET significa que el edge no superó el umbral validado fuera de muestra.",
    }
    return result


def predict_match(match: Dict[str, Any], artifact: Dict[str, Any]) -> Dict[str, Any]:
    row = build_feature_row(match, include_targets=False)
    if row is None:
        return {"error": "Partido sin línea AH válida"}
    columns = artifact["feature_columns"]
    x = pd.DataFrame([{column: row.get(column, np.nan) for column in columns}])
    ah_edge = float(np.clip(artifact["ah_model"].predict(x)[0], -1.0, 1.0))
    ou_edge = float(np.clip(artifact["ou_model"].predict(x)[0], -1.0, 1.0))
    return format_prediction(row, artifact, ah_edge, ou_edge)


def main() -> int:
    parser = argparse.ArgumentParser(description="Predice un partido con el modelo universal")
    root = Path(__file__).resolve().parents[2]
    parser.add_argument("--db", type=Path, default=root / "data" / "app_data.db")
    parser.add_argument("--model", type=Path, default=root / "models" / "universal_autotrainer" / "universal_market_model.joblib")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--match-id")
    group.add_argument("--team-query")
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()
    matches = load_matches_from_db(args.db, states=("precacheo", "pending_results", "historical"), require_result=False)
    selected: Optional[Dict[str, Any]] = None
    if args.match_id:
        selected = next((m for m in matches if str(m.get("match_id")) == str(args.match_id)), None)
    else:
        query = str(args.team_query).lower()
        selected = next((m for m in matches if query in str(m.get("home_name", "")).lower() or query in str(m.get("away_name", "")).lower()), None)
    if selected is None:
        raise SystemExit("Partido no encontrado")
    artifact = joblib.load(args.model)
    result = predict_match(selected, artifact)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
