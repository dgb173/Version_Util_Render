#!/usr/bin/env python3
"""Entrena y valida cronologicamente los modelos universales AH y O/U."""

from __future__ import annotations

import argparse
import gc
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.universal_autotrainer.features import (  # type: ignore
        FEATURE_VERSION,
        build_feature_row,
        feature_columns,
        load_matches_from_db,
        parse_date,
    )
else:
    from .features import FEATURE_VERSION, build_feature_row, feature_columns, load_matches_from_db, parse_date


RANDOM_STATE = 20260715
FLAG_COLUMNS = (
    "flag_home_result_inflation",
    "flag_hidden_resistant_away",
    "flag_weak_home_condition",
    "flag_common_market_home",
    "flag_common_market_away",
    "flag_ou_inflated_recent_score",
    "flag_local_low_production_high_ou",
)


@dataclass
class Split:
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (np.floating, float)):
        return None if not math.isfinite(float(value)) else round(float(value), 6)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def _build_frame(matches: Sequence[Dict[str, Any]]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    invalid_date = 0
    invalid_features = 0
    for match in matches:
        row = build_feature_row(match, include_targets=True)
        if row is None or "ah_profit_home" not in row:
            invalid_features += 1
            continue
        date = parse_date(row.get("match_date"))
        if date is None:
            invalid_date += 1
            continue
        row["_date"] = date
        rows.append(row)
    frame = pd.DataFrame(rows).sort_values(["_date", "match_id"]).drop_duplicates("match_id", keep="last")
    quality = {
        "db_rows_loaded": len(matches),
        "usable_rows": len(frame),
        "invalid_date": invalid_date,
        "invalid_features_or_result": invalid_features,
        "duplicates_removed": max(0, len(rows) - len(frame)),
        "date_min": frame["match_date"].min() if len(frame) else None,
        "date_max": frame["match_date"].max() if len(frame) else None,
    }
    return frame, quality


def chronological_split(frame: pd.DataFrame, train_fraction: float = 0.64, validation_fraction: float = 0.16) -> Split:
    if len(frame) < 500:
        raise ValueError(f"Se necesitan al menos 500 partidos; disponibles: {len(frame)}")
    n = len(frame)
    train_end = max(1, int(n * train_fraction))
    validation_end = max(train_end + 1, int(n * (train_fraction + validation_fraction)))
    # Una fecha nunca se reparte entre dos bloques: reduce la fuga por jornada.
    while train_end < n and frame.iloc[train_end - 1]["_date"] == frame.iloc[train_end]["_date"]:
        train_end += 1
    while validation_end < n and frame.iloc[validation_end - 1]["_date"] == frame.iloc[validation_end]["_date"]:
        validation_end += 1
    return Split(frame.iloc[:train_end].copy(), frame.iloc[train_end:validation_end].copy(), frame.iloc[validation_end:].copy())


def make_model(min_samples_leaf: int, max_features: float, n_estimators: int = 90) -> Pipeline:
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
        ("model", RandomForestRegressor(
        n_estimators=n_estimators,
        max_depth=9,
        min_samples_leaf=min_samples_leaf,
        max_features=max_features,
        n_jobs=-1,
        random_state=RANDOM_STATE,
        )),
    ])


def _candidate_thresholds(predictions: np.ndarray) -> List[float]:
    magnitude = np.abs(predictions)
    candidates = {0.0, 0.025, 0.05, 0.075, 0.10, 0.125, 0.15, 0.20, 0.25, 0.30}
    for q in (0.35, 0.45, 0.55, 0.65, 0.75, 0.82, 0.88, 0.92):
        candidates.add(float(np.quantile(magnitude, q)))
    return sorted(value for value in candidates if math.isfinite(value))


def evaluate_selective(actual: np.ndarray, predicted: np.ndarray, threshold: float) -> Dict[str, Any]:
    selected = np.abs(predicted) >= threshold
    directions = np.where(predicted >= 0, 1.0, -1.0)
    profits = actual * directions
    picked = profits[selected]
    n = int(selected.sum())
    if not n:
        return {"n": 0, "coverage": 0.0, "mean_profit_even_odds": None, "win_rate": None, "loss_rate": None, "push_rate": None, "stderr": None}
    return {
        "n": n,
        "coverage": n / len(actual),
        "mean_profit_even_odds": float(np.mean(picked)),
        "win_rate": float(np.mean(picked > 0)),
        "loss_rate": float(np.mean(picked < 0)),
        "push_rate": float(np.mean(picked == 0)),
        "stderr": float(np.std(picked, ddof=1) / math.sqrt(n)) if n > 1 else None,
        "home_picks": int(np.sum(selected & (predicted >= 0))),
        "away_picks": int(np.sum(selected & (predicted < 0))),
    }


def choose_threshold(actual: np.ndarray, predicted: np.ndarray) -> Tuple[float, Dict[str, Any]]:
    minimum = max(80, int(len(actual) * 0.08))
    best: Optional[Tuple[float, float, Dict[str, Any]]] = None
    for threshold in _candidate_thresholds(predicted):
        metrics = evaluate_selective(actual, predicted, threshold)
        if metrics["n"] < minimum or metrics["mean_profit_even_odds"] is None:
            continue
        stderr = metrics.get("stderr") or 0.0
        # Criterio conservador: beneficio menos una penalizacion por incertidumbre y baja cobertura.
        conservative = metrics["mean_profit_even_odds"] - 0.75 * stderr + 0.02 * metrics["coverage"]
        if best is None or conservative > best[0]:
            best = (conservative, threshold, metrics)
    if best is None:
        metrics = evaluate_selective(actual, predicted, float(np.quantile(np.abs(predicted), 0.8)))
        return float(np.quantile(np.abs(predicted), 0.8)), metrics
    return best[1], best[2]


def _fit_market(
    split: Split, columns: Sequence[str], target: str, market: str
) -> Tuple[Pipeline, float, Dict[str, Any], np.ndarray, np.ndarray]:
    train = split.train.dropna(subset=[target])
    validation = split.validation.dropna(subset=[target])
    test = split.test.dropna(subset=[target])
    x_train, y_train = train[list(columns)], train[target].to_numpy(dtype=float)
    x_val, y_val = validation[list(columns)], validation[target].to_numpy(dtype=float)
    grid = ((12, 0.45), (20, 0.60), (32, 0.75))
    trials = []
    best = None
    for min_leaf, max_features in grid:
        model = make_model(min_leaf, max_features)
        model.fit(x_train, y_train)
        pred = np.clip(model.predict(x_val), -1.0, 1.0)
        threshold, selective = choose_threshold(y_val, pred)
        mae = float(np.mean(np.abs(y_val - pred)))
        score = (selective.get("mean_profit_even_odds") or -1.0) - 0.25 * (selective.get("stderr") or 0.0)
        trial = {"min_samples_leaf": min_leaf, "max_features": max_features, "threshold": threshold, "validation": selective, "mae": mae, "score": score}
        trials.append(trial)
        if best is None or score > best[0]:
            best = (score, min_leaf, max_features, threshold, selective)
    assert best is not None
    _, min_leaf, max_features, threshold, validation_metrics = best
    audit_model = make_model(min_leaf, max_features, n_estimators=140)
    combined = pd.concat([split.train, split.validation], ignore_index=True).dropna(subset=[target])
    audit_model.fit(combined[list(columns)], combined[target].to_numpy(dtype=float))
    y_test = test[target].to_numpy(dtype=float)
    pred_test = np.clip(audit_model.predict(test[list(columns)]), -1.0, 1.0)
    test_metrics = evaluate_selective(y_test, pred_test, threshold)
    test_metrics["mae"] = float(np.mean(np.abs(y_test - pred_test)))
    metrics = {
        "market": market,
        "target": target,
        "selected_params": {"min_samples_leaf": min_leaf, "max_features": max_features},
        "threshold": threshold,
        "validation_at_selection": validation_metrics,
        "untouched_test": test_metrics,
        "all_trials": trials,
        "warning": "mean_profit_even_odds supone cuota decimal 2.00; no es ROI real sin cuotas historicas.",
    }
    # Modelo operativo final: toda la historia disponible, sin cambiar el umbral auditado.
    final_model = make_model(min_leaf, max_features, n_estimators=180)
    full = pd.concat([split.train, split.validation, split.test], ignore_index=True).dropna(subset=[target])
    final_model.fit(full[list(columns)], full[target].to_numpy(dtype=float))
    return final_model, threshold, metrics, pred_test, y_test


def _calibration(predicted: np.ndarray, actual: np.ndarray, bins: int = 5) -> List[Dict[str, Any]]:
    magnitude = np.abs(predicted)
    if len(magnitude) < bins:
        return []
    edges = np.unique(np.quantile(magnitude, np.linspace(0, 1, bins + 1)))
    output = []
    for low, high in zip(edges[:-1], edges[1:]):
        mask = (magnitude >= low) & (magnitude <= high if high == edges[-1] else magnitude < high)
        if not mask.any():
            continue
        profits = actual[mask] * np.where(predicted[mask] >= 0, 1.0, -1.0)
        output.append({"edge_min": float(low), "edge_max": float(high), "n": int(mask.sum()), "mean_profit_even_odds": float(np.mean(profits)), "positive_rate": float(np.mean(profits > 0))})
    return output


def _mine_patterns(test: pd.DataFrame, predicted: np.ndarray, actual: np.ndarray, market: str, threshold: float) -> List[Dict[str, Any]]:
    selected = np.abs(predicted) >= threshold
    direction = np.where(predicted >= 0, 1.0, -1.0)
    profits = actual * direction
    patterns: List[Dict[str, Any]] = []
    for flag in FLAG_COLUMNS:
        if flag not in test:
            continue
        for flag_value in (0.0, 1.0):
            mask = selected & (test[flag].fillna(0).to_numpy(dtype=float) == flag_value)
            n = int(mask.sum())
            if n < 30:
                continue
            mean = float(np.mean(profits[mask]))
            stderr = float(np.std(profits[mask], ddof=1) / math.sqrt(n)) if n > 1 else 0.0
            patterns.append({"market": market, "condition": f"{flag}={int(flag_value)}", "n_test": n, "mean_profit_even_odds": mean, "stderr": stderr, "conservative_edge": mean - stderr})
    return sorted(patterns, key=lambda item: item["conservative_edge"], reverse=True)


def train(db_path: Path, output_dir: Path) -> Dict[str, Any]:
    matches = load_matches_from_db(db_path, states=("historical",), require_result=True, compact=True)
    frame, quality = _build_frame(matches)
    del matches
    gc.collect()
    columns = feature_columns(frame.to_dict("records"))
    # Variables de mercado actuales son validas; targets y metadatos quedan excluidos en feature_columns.
    split = chronological_split(frame)
    ah_model, ah_threshold, ah_metrics, ah_pred, ah_actual = _fit_market(split, columns, "ah_profit_home", "AH")
    ou_model, ou_threshold, ou_metrics, ou_pred, ou_actual = _fit_market(split, columns, "ou_profit_over", "OU")

    ah_test = split.test.dropna(subset=["ah_profit_home"])
    ou_test = split.test.dropna(subset=["ou_profit_over"])
    metrics = {
        "feature_version": FEATURE_VERSION,
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "database": str(db_path.resolve()),
        "quality": quality,
        "split": {
            "train_n": len(split.train), "validation_n": len(split.validation), "test_n": len(split.test),
            "train_end": split.train["match_date"].max(),
            "validation_start": split.validation["match_date"].min(),
            "validation_end": split.validation["match_date"].max(),
            "test_start": split.test["match_date"].min(),
            "test_end": split.test["match_date"].max(),
        },
        "ah": ah_metrics,
        "ou": ou_metrics,
        "calibration": {"ah": _calibration(ah_pred, ah_actual), "ou": _calibration(ou_pred, ou_actual)},
    }
    patterns = _mine_patterns(ah_test, ah_pred, ah_actual, "AH", ah_threshold) + _mine_patterns(ou_test, ou_pred, ou_actual, "OU", ou_threshold)
    artifact = {
        "feature_version": FEATURE_VERSION,
        "feature_columns": columns,
        "ah_model": ah_model,
        "ou_model": ou_model,
        "ah_threshold": ah_threshold,
        "ou_threshold": ou_threshold,
        "ah_enabled": bool((ah_metrics["untouched_test"].get("mean_profit_even_odds") or 0.0) > 0 and ah_metrics["untouched_test"].get("n", 0) >= 80),
        "ou_enabled": bool((ou_metrics["untouched_test"].get("mean_profit_even_odds") or 0.0) > 0 and ou_metrics["untouched_test"].get("n", 0) >= 80),
        "calibration": metrics["calibration"],
        "trained_at_utc": metrics["trained_at_utc"],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, output_dir / "universal_market_model.joblib")
    (output_dir / "metrics.json").write_text(json.dumps(_json_safe(metrics), ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "patterns.json").write_text(json.dumps(_json_safe({"patterns": patterns}), ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "features.json").write_text(json.dumps(columns, ensure_ascii=False, indent=2), encoding="utf-8")
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser(description="Autoentrenador universal AH/O-U con validacion temporal")
    root = Path(__file__).resolve().parents[2]
    parser.add_argument("--db", type=Path, default=root / "data" / "app_data.db")
    parser.add_argument("--output-dir", type=Path, default=root / "models" / "universal_autotrainer")
    args = parser.parse_args()
    metrics = train(args.db, args.output_dir)
    print(json.dumps(_json_safe({"quality": metrics["quality"], "split": metrics["split"], "ah": metrics["ah"]["untouched_test"], "ou": metrics["ou"]["untouched_test"]}), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
