#!/usr/bin/env python3
"""Ciclo reproducible: reentrena, audita y pronostica el precacheo actual."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.universal_autotrainer.features import build_feature_row, load_matches_from_db  # type: ignore
    from scripts.universal_autotrainer.predict import format_prediction  # type: ignore
    from scripts.universal_autotrainer.train import train  # type: ignore
else:
    from .features import build_feature_row, load_matches_from_db
    from .predict import format_prediction
    from .train import train


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecuta autoentrenamiento y pronosticos selectivos")
    root = Path(__file__).resolve().parents[2]
    parser.add_argument("--db", type=Path, default=root / "data" / "app_data.db")
    parser.add_argument("--model-dir", type=Path, default=root / "models" / "universal_autotrainer")
    parser.add_argument("--reports-dir", type=Path, default=root / "reports" / "universal_autotrainer")
    parser.add_argument("--skip-train", action="store_true", help="Usa el artefacto ya auditado")
    args = parser.parse_args()
    if not args.skip_train:
        train(args.db, args.model_dir)
    artifact_path = args.model_dir / "universal_market_model.joblib"
    artifact = joblib.load(artifact_path)
    matches = load_matches_from_db(args.db, states=("precacheo", "pending_results"), require_result=False, compact=True)
    rows = [row for match in matches if (row := build_feature_row(match, include_targets=False)) is not None]
    columns = artifact["feature_columns"]
    frame = pd.DataFrame([{column: row.get(column, np.nan) for column in columns} for row in rows])
    if len(frame):
        ah_edges = np.clip(artifact["ah_model"].predict(frame), -1.0, 1.0)
        ou_edges = np.clip(artifact["ou_model"].predict(frame), -1.0, 1.0)
        predictions = [format_prediction(row, artifact, float(ah), float(ou)) for row, ah, ou in zip(rows, ah_edges, ou_edges)]
    else:
        predictions = []
    ah_picks = [item for item in predictions if item.get("ah", {}).get("pick") not in {None, "NO BET"}]
    ou_picks = [item for item in predictions if item.get("ou", {}).get("pick") not in {None, "NO BET"}]
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.reports_dir / stamp
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now().isoformat(),
        "model_trained_at": artifact.get("trained_at_utc"),
        "ah_enabled": artifact.get("ah_enabled", False),
        "ou_enabled": artifact.get("ou_enabled", False),
        "matches_evaluated": len(predictions),
        "ah_bets": len(ah_picks),
        "ou_bets": len(ou_picks),
        "predictions": predictions,
    }
    (output_dir / "predictions.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Ciclo universal AH + O/U",
        "",
        f"- Partidos evaluados: {len(predictions)}",
        f"- AH habilitado por test temporal: {artifact.get('ah_enabled', False)}",
        f"- O/U habilitado por test temporal: {artifact.get('ou_enabled', False)}",
        f"- Apuestas AH: {len(ah_picks)}",
        f"- Apuestas O/U: {len(ou_picks)}",
        "",
        "Si un mercado esta deshabilitado, todos sus candidatos quedan como NO BET aunque el modelo bruto muestre direccion.",
    ]
    (output_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"output_dir": str(output_dir), "matches": len(predictions), "ah_bets": len(ah_picks), "ou_bets": len(ou_picks)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
