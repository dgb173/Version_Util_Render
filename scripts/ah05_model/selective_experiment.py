#!/usr/bin/env python3
"""Selective binary AH 0.5 experiment targeting high precision with abstention.

This script deliberately excludes the previously opened final test. Two earlier
walk-forward blocks select model and gate; a later chronological block confirms
the frozen rule. It never mutates the football database.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import GradientBoostingClassifier, HistGradientBoostingClassifier
from sklearn.frozen import FrozenEstimator
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline

try:
    from scripts.ah05_model.pipeline import build_frame, feature_columns, load_records, make_preprocessor
except ModuleNotFoundError:  # Allows direct execution from the repository root.
    from pipeline import build_frame, feature_columns, load_records, make_preprocessor


def wilson(successes: int, total: int, z: float = 1.96) -> tuple[float, float]:
    if total == 0:
        return 0.0, 1.0
    p = successes / total
    denominator = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    spread = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / denominator
    return centre - spread, centre + spread


def model_factories() -> dict[str, object]:
    return {
        "logistic": LogisticRegression(max_iter=1400, C=.2, class_weight="balanced"),
        "gradient_boosting": GradientBoostingClassifier(n_estimators=100, max_depth=2, learning_rate=.03, min_samples_leaf=25, random_state=29),
        "hist_gradient": HistGradientBoostingClassifier(max_iter=130, max_leaf_nodes=12, learning_rate=.035, l2_regularization=3.0, min_samples_leaf=25, random_state=29),
    }


def add_online_team_profiles(frame: pd.DataFrame, targets: list, universe: list, half_life: int) -> pd.DataFrame:
    """Add rolling profiles built only from database matches on dates < T."""
    history = defaultdict(list)
    cursor = 0

    def key(name: str) -> str:
        return "".join(ch for ch in name.casefold() if ch.isalnum())

    def append_match(record) -> None:
        raw = record.raw_ah
        home_handicap, away_handicap = -raw, raw
        for team, venue, own, opp, handicap in (
            (record.home, 1, record.home_goals, record.away_goals, home_handicap),
            (record.away, 0, record.away_goals, record.home_goals, away_handicap),
        ):
            history[key(team)].append((record.match_date, venue, own, opp, handicap))

    def summarize(team: str, target_date, venue: int) -> dict[str, float]:
        rows = [row for row in history[key(team)][-60:] if row[0] < target_date]
        venue_rows = [row for row in rows if row[1] == venue]
        chosen = venue_rows[-25:] if len(venue_rows) >= 3 else rows[-25:]
        if not chosen:
            return {name: np.nan for name in ("n", "win", "draw", "gf", "ga", "win1", "win2p", "fav_win", "dog_nonloss", "cover", "age")}
        weights = np.asarray([.5 ** ((target_date - row[0]).days / half_life) for row in chosen])
        avg = lambda values: float(np.average(values, weights=weights))
        fav = [(row, weight) for row, weight in zip(chosen, weights) if row[4] < 0]
        dog = [(row, weight) for row, weight in zip(chosen, weights) if row[4] > 0]
        return {
            "n": float(len(chosen)), "win": avg([row[2] > row[3] for row in chosen]),
            "draw": avg([row[2] == row[3] for row in chosen]), "gf": avg([row[2] for row in chosen]),
            "ga": avg([row[3] for row in chosen]), "win1": avg([row[2] - row[3] == 1 for row in chosen]),
            "win2p": avg([row[2] - row[3] >= 2 for row in chosen]),
            "fav_win": float(np.average([row[2] > row[3] for row, _ in fav], weights=[weight for _, weight in fav])) if fav else np.nan,
            "dog_nonloss": float(np.average([row[2] >= row[3] for row, _ in dog], weights=[weight for _, weight in dog])) if dog else np.nan,
            "cover": avg([row[2] - row[3] + row[4] >= .25 for row in chosen]),
            "age": avg([(target_date - row[0]).days for row in chosen]),
        }

    output = frame.copy()
    profile_rows = []
    for target in targets:
        while cursor < len(universe) and universe[cursor].match_date < target.match_date:
            append_match(universe[cursor]); cursor += 1
        home = summarize(target.home, target.match_date, 1)
        away = summarize(target.away, target.match_date, 0)
        favourite, dog = (home, away) if target.raw_ah > 0 else (away, home)
        values = {}
        for name in home:
            values[f"online_home_{name}"] = home[name]
            values[f"online_away_{name}"] = away[name]
            values[f"online_fav_{name}"] = favourite[name]
            values[f"online_dog_{name}"] = dog[name]
            values[f"online_fav_dog_{name}_diff"] = favourite[name] - dog[name]
        profile_rows.append(values)
    profiles = pd.DataFrame(profile_rows, index=output.index)
    return pd.concat([output, profiles], axis=1)


def fit_predict(history: pd.DataFrame, future: pd.DataFrame, estimator: object, columns: list[str], numeric: list[str], categorical: list[str]) -> np.ndarray:
    calibration_start = int(len(history) * .86)
    train, calibration = history.iloc[:calibration_start], history.iloc[calibration_start:]
    model = Pipeline([("prep", make_preprocessor(numeric, categorical)), ("model", estimator)])
    model.fit(train[columns], (train.target == 0).astype(int))
    calibrated = CalibratedClassifierCV(FrozenEstimator(model), method="sigmoid", cv=None)
    calibrated.fit(calibration[columns], (calibration.target == 0).astype(int))
    return calibrated.predict_proba(future[columns])[:, 1]


def probability_metrics(target: np.ndarray, probability: np.ndarray) -> dict[str, float]:
    return {
        "log_loss": float(log_loss(target, probability)),
        "brier": float(brier_score_loss(target, probability)),
        "auc": float(roc_auc_score(target, probability)),
    }


def gate_result(target: np.ndarray, probability: np.ndarray, threshold: float) -> dict[str, float | int]:
    selected = (probability >= threshold) | (probability <= 1 - threshold)
    prediction = probability >= .5
    total = int(selected.sum())
    successes = int((prediction[selected] == target[selected]).sum()) if total else 0
    low, high = wilson(successes, total)
    return {
        "threshold": threshold, "picks": total, "hits": successes,
        "accuracy": successes / total if total else 0.0,
        "coverage": total / len(target) if len(target) else 0.0,
        "wilson_low": low, "wilson_high": high,
        "favourite_picks": int((selected & prediction).sum()),
        "underdog_nonloss_picks": int((selected & ~prediction).sum()),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/app_data.db")
    parser.add_argument("--output-dir", default="artifacts/ah05_selective")
    parser.add_argument("--half-life", type=int, default=90)
    args = parser.parse_args()
    output = Path(args.output_dir).resolve(); output.mkdir(parents=True, exist_ok=True)
    db_path = Path(args.db).resolve()
    records = load_records(db_path)
    # The last 20% is the retired test from v1 and is not touched here.
    development_records = records[:int(len(records) * .8)]
    frame = build_frame(development_records, args.half_life).sort_values(["date", "match_id"]).reset_index(drop=True)
    universe = load_records(db_path, only_ah05=False)
    frame = add_online_team_profiles(frame, development_records, universe, args.half_life)
    numeric, categorical = feature_columns(frame); columns = numeric + categorical
    n = len(frame)
    tune_boundaries = (int(n * .45), int(n * .60), int(n * .75))
    tune_slices = ((tune_boundaries[0], tune_boundaries[1]), (tune_boundaries[1], tune_boundaries[2]))
    confirmation_slice = (tune_boundaries[2], n)
    model_results = {}
    tune_predictions = {}
    for name, estimator in model_factories().items():
        chunks, targets = [], []
        for start, end in tune_slices:
            chunks.append(fit_predict(frame.iloc[:start], frame.iloc[start:end], estimator, columns, numeric, categorical))
            targets.extend((frame.iloc[start:end].target == 0).astype(int).tolist())
        probability = np.concatenate(chunks); target = np.asarray(targets, dtype=int)
        tune_predictions[name] = (target, probability)
        model_results[name] = probability_metrics(target, probability)
    selected_model = min(model_results, key=lambda name: model_results[name]["log_loss"])
    tune_target, tune_probability = tune_predictions[selected_model]
    gates = [gate_result(tune_target, tune_probability, float(threshold)) for threshold in np.arange(.55, .901, .025)]
    eligible = [gate for gate in gates if gate["picks"] >= 40 and gate["accuracy"] >= .80]
    if eligible:
        selected_gate = max(eligible, key=lambda gate: (gate["coverage"], gate["wilson_low"]))
        gate_reason = "highest coverage among tune gates with >=80% accuracy and >=40 picks"
    else:
        selected_gate = max((gate for gate in gates if gate["picks"] >= 20), key=lambda gate: (gate["wilson_low"], gate["picks"]), default=gates[0])
        gate_reason = "no tune gate met 80%; diagnostic gate only"
    confirm_start, confirm_end = confirmation_slice
    estimator = model_factories()[selected_model]
    confirm_probability = fit_predict(frame.iloc[:confirm_start], frame.iloc[confirm_start:confirm_end], estimator, columns, numeric, categorical)
    confirm_target = (frame.iloc[confirm_start:confirm_end].target == 0).astype(int).to_numpy()
    confirmation = gate_result(confirm_target, confirm_probability, float(selected_gate["threshold"]))
    confirmed = bool(eligible and confirmation["picks"] >= 30 and confirmation["accuracy"] >= .80 and confirmation["wilson_low"] >= .65)
    payload = {
        "objective": "selective accuracy >= 80% on favourite-win vs underdog-nonloss",
        "records": len(frame), "retired_test_excluded": len(records) - len(frame),
        "selected_model": selected_model, "model_tune_metrics": model_results,
        "gate_selection_reason": gate_reason, "tune_gate": selected_gate,
        "confirmation_period": {"start": frame.iloc[confirm_start].date, "end": frame.iloc[confirm_end - 1].date},
        "confirmation_probability_metrics": probability_metrics(confirm_target, confirm_probability),
        "confirmation_gate": confirmation,
        "status": "CONFIRMED_SELECTIVE_80" if confirmed else "REJECTED_80_NOT_CONFIRMED",
        "rule": "NO BET for every match outside the frozen confidence gate",
        "all_tune_gates": gates,
    }
    (output / "selective_backtest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(gates).to_csv(output / "threshold_table.csv", index=False)
    report = ["# Experimento selectivo AH ±0,5", "", f"Estado: **{payload['status']}**", "",
        f"Modelo elegido en dos folds de ajuste: **{selected_model}**.",
        f"Gate congelado: probabilidad >= {selected_gate['threshold']:.3f} para favorito o <= {1-selected_gate['threshold']:.3f} para no pérdida del dog.",
        f"Ajuste: {selected_gate['hits']}/{selected_gate['picks']} ({selected_gate['accuracy']:.1%}), cobertura {selected_gate['coverage']:.1%}, IC95% [{selected_gate['wilson_low']:.1%}, {selected_gate['wilson_high']:.1%}].",
        f"Confirmación posterior ({payload['confirmation_period']['start']} a {payload['confirmation_period']['end']}): {confirmation['hits']}/{confirmation['picks']} ({confirmation['accuracy']:.1%}), cobertura {confirmation['coverage']:.1%}, IC95% [{confirmation['wilson_low']:.1%}, {confirmation['wilson_high']:.1%}].",
        "", "Un 80% solo se acepta si sobrevive al bloque posterior. Si no, el sistema mantiene NO BET; no se baja el umbral después de mirar la confirmación."]
    (output / "REPORT.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
