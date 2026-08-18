#!/usr/bin/env python3
"""Train and audit the leakage-safe HouseMind O/U model.

This script deliberately uses chronological train/calibration/holdout segments.
The final holdout is not used to fit coefficients, calibrate probabilities, or
choose the abstention threshold.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from modules.housemind_ou import (  # noqa: E402
    NUMERIC_FEATURES,
    PROFILE,
    evaluate_match,
    extract_feature_vector,
    get_match_date,
    load_model,
    merge_match_records,
    parse_date,
    parse_score,
    safe_float,
    settle_ou_score,
)


MODEL_VERSION = "1.0.0"


def _json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _score_from_match(match: Dict[str, Any], row_score: Any = None) -> Optional[Tuple[int, int]]:
    for value in (match.get("final_score"), row_score, match.get("score")):
        parsed = parse_score(value)
        if parsed is not None:
            return parsed
    return None


def _load_training_samples(db_path: Path) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    audit: Counter = Counter()
    samples: List[Dict[str, Any]] = []
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    query = """
        SELECT match_id, bucket, state, handicap, score, match_date,
               payload_json, explorer_json
        FROM matches
        WHERE state = 'historical'
        ORDER BY match_id
    """
    try:
        for row in connection.execute(query):
            audit["rows_historical"] += 1
            explorer = _json_dict(row["explorer_json"])
            payload = _json_dict(row["payload_json"])
            row_overlay = {
                "match_id": str(row["match_id"]),
                "match_date": row["match_date"],
                "handicap": row["handicap"],
            }
            match = merge_match_records(explorer, payload, row_overlay)
            score = _score_from_match(match, row["score"])
            if score is None:
                audit["reject:no_final_score"] += 1
                continue

            feature_vector = extract_feature_vector(match, strict_temporal=True)
            quality = feature_vector["quality"]
            if not quality.get("eligible"):
                audit["reject:feature_quality"] += 1
                for reason in quality.get("reasons", []):
                    audit[f"reject_reason:{reason}"] += 1
                continue

            match_date = get_match_date(match)
            ou_line = safe_float(feature_vector["meta"].get("ou"))
            if match_date is None or ou_line is None:
                audit["reject:date_or_ou"] += 1
                continue

            total_goals = score[0] + score[1]
            settlement = settle_ou_score(total_goals, ou_line, "OVER")
            if settlement == 0:
                audit["reject:ou_push"] += 1
                continue

            samples.append(
                {
                    "match_id": str(row["match_id"]),
                    "date": match_date,
                    "home": match.get("home_name") or match.get("home_team") or "",
                    "away": match.get("away_name") or match.get("away_team") or "",
                    "league": match.get("league_name") or match.get("league") or "",
                    "score": f"{score[0]}:{score[1]}",
                    "ou_line": float(ou_line),
                    "ah_line": float(feature_vector["meta"].get("ah") or 0.0),
                    "tokens": feature_vector["tokens"],
                    "numeric": feature_vector["numeric"],
                    "label": 1.0 if settlement > 0 else 0.0,
                    "sample_weight": abs(float(settlement)),
                    "over_settlement": float(settlement),
                    "valid_contexts": int(quality.get("valid_contexts", 0)),
                }
            )
            audit["samples_usable"] += 1
            audit[f"settlement:{'positive' if settlement > 0 else 'negative'}"] += 1
            if abs(settlement) == 0.5:
                audit["settlement:half"] += 1
            if quality.get("nonpast_contexts"):
                audit["samples_with_filtered_nonpast_context"] += 1
    finally:
        connection.close()

    samples.sort(key=lambda item: (item["date"], item["match_id"]))
    audit["samples_dated_sorted"] = len(samples)
    return samples, dict(audit)


def _boundary_after_date(samples: Sequence[Dict[str, Any]], target: int) -> int:
    target = max(1, min(len(samples) - 1, target))
    boundary_date = samples[target - 1]["date"]
    while target < len(samples) and samples[target]["date"] == boundary_date:
        target += 1
    return min(target, len(samples) - 1)


def chronological_split(
    samples: Sequence[Dict[str, Any]],
    train_ratio: float = 0.60,
    calibration_ratio: float = 0.20,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    if len(samples) < 100:
        raise ValueError("HouseMind needs at least 100 temporally valid samples")
    first = _boundary_after_date(samples, int(len(samples) * train_ratio))
    second = _boundary_after_date(samples, int(len(samples) * (train_ratio + calibration_ratio)))
    if second <= first:
        second = _boundary_after_date(samples, first + max(1, len(samples) // 10))
    return list(samples[:first]), list(samples[first:second]), list(samples[second:])


def _weighted_mean(values: np.ndarray, weights: np.ndarray) -> float:
    denominator = float(np.sum(weights))
    if denominator <= 0:
        return float(np.mean(values))
    return float(np.sum(values * weights) / denominator)


def select_tokens(
    samples: Sequence[Dict[str, Any]],
    min_support: int,
    max_tokens: int,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    support: Counter = Counter()
    positives: Dict[str, float] = defaultdict(float)
    weights_by_token: Dict[str, float] = defaultdict(float)
    total_positive = 0.0
    total_weight = 0.0
    for sample in samples:
        label = float(sample["label"])
        weight = float(sample["sample_weight"])
        total_positive += label * weight
        total_weight += weight
        for token in set(sample["tokens"]):
            support[token] += 1
            positives[token] += label * weight
            weights_by_token[token] += weight

    baseline = total_positive / max(total_weight, 1e-9)
    rows: List[Tuple[float, int, str, float]] = []
    forced_prefixes = ("AH_EXACT=", "AH_FAMILY=", "OU_EXACT=", "OU_FAMILY=", "LINE_PAIR=")
    for token, count in support.items():
        required = max(6, min_support // 3) if token.startswith(forced_prefixes) else min_support
        if count < required:
            continue
        token_weight = weights_by_token[token]
        smoothed_rate = (positives[token] + 8.0 * baseline) / (token_weight + 8.0)
        effect = abs(smoothed_rate - baseline)
        score = effect * math.sqrt(count)
        if token.startswith(forced_prefixes):
            score += 0.20
        rows.append((score, count, token, smoothed_rate))

    rows.sort(key=lambda item: (-item[0], -item[1], item[2]))
    selected = rows[:max_tokens]
    audit = [
        {
            "token": token,
            "support": count,
            "smoothed_over_rate": round(rate * 100.0, 2),
            "selection_score": round(score, 5),
        }
        for score, count, token, rate in selected
    ]
    return [row[2] for row in selected], audit


def fit_transform_schema(
    samples: Sequence[Dict[str, Any]],
    min_token_support: int,
    max_tokens: int,
) -> Dict[str, Any]:
    numeric_names = list(NUMERIC_FEATURES)
    raw_numeric = np.asarray(
        [[float(sample["numeric"].get(name, 0.0)) for name in numeric_names] for sample in samples],
        dtype=np.float64,
    )
    means = np.mean(raw_numeric, axis=0)
    scales = np.std(raw_numeric, axis=0)
    scales = np.where(scales < 1e-6, 1.0, scales)
    token_names, token_audit = select_tokens(samples, min_token_support, max_tokens)
    return {
        "numeric_names": numeric_names,
        "numeric_means": means,
        "numeric_scales": scales,
        "token_names": token_names,
        "token_audit": token_audit,
    }


def transform(samples: Sequence[Dict[str, Any]], schema: Dict[str, Any]) -> np.ndarray:
    numeric_names: Sequence[str] = schema["numeric_names"]
    means = np.asarray(schema["numeric_means"], dtype=np.float64)
    scales = np.asarray(schema["numeric_scales"], dtype=np.float64)
    token_names: Sequence[str] = schema["token_names"]
    token_index = {token: index for index, token in enumerate(token_names)}
    matrix = np.zeros((len(samples), len(numeric_names) + len(token_names)), dtype=np.float64)
    for row_index, sample in enumerate(samples):
        values = np.asarray([float(sample["numeric"].get(name, 0.0)) for name in numeric_names])
        matrix[row_index, : len(numeric_names)] = np.clip((values - means) / scales, -8.0, 8.0)
        active = set(sample["tokens"])
        for token in active:
            index = token_index.get(token)
            if index is not None:
                matrix[row_index, len(numeric_names) + index] = 1.0
    return matrix


def _arrays(samples: Sequence[Dict[str, Any]]) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    labels = np.asarray([float(sample["label"]) for sample in samples], dtype=np.float64)
    weights = np.asarray([float(sample["sample_weight"]) for sample in samples], dtype=np.float64)
    settlements = np.asarray([float(sample["over_settlement"]) for sample in samples], dtype=np.float64)
    return labels, weights, settlements


def _sigmoid_array(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, -40.0, 40.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _log_loss(labels: np.ndarray, probabilities: np.ndarray, weights: np.ndarray) -> float:
    clipped = np.clip(probabilities, 1e-7, 1.0 - 1e-7)
    losses = -(labels * np.log(clipped) + (1.0 - labels) * np.log(1.0 - clipped))
    return _weighted_mean(losses, weights)


def fit_logistic(
    x_train: np.ndarray,
    y_train: np.ndarray,
    w_train: np.ndarray,
    x_validation: np.ndarray,
    y_validation: np.ndarray,
    w_validation: np.ndarray,
    *,
    seed: int,
    epochs: int,
    batch_size: int,
    learning_rate: float,
    l2: float,
) -> Dict[str, Any]:
    rng = np.random.default_rng(seed)
    dimension = x_train.shape[1]
    coefficients = np.zeros(dimension, dtype=np.float64)
    base_rate = np.clip(_weighted_mean(y_train, w_train), 1e-4, 1.0 - 1e-4)
    intercept = math.log(base_rate / (1.0 - base_rate))

    m_w = np.zeros_like(coefficients)
    v_w = np.zeros_like(coefficients)
    m_b = 0.0
    v_b = 0.0
    beta1 = 0.9
    beta2 = 0.999
    epsilon = 1e-8
    step = 0
    best_loss = float("inf")
    best_coefficients = coefficients.copy()
    best_intercept = intercept
    best_epoch = 0
    stale_epochs = 0

    for epoch in range(1, epochs + 1):
        order = rng.permutation(len(x_train))
        for start in range(0, len(order), batch_size):
            indices = order[start : start + batch_size]
            xb = x_train[indices]
            yb = y_train[indices]
            wb = w_train[indices]
            denominator = max(float(np.sum(wb)), 1e-9)
            probabilities = _sigmoid_array(xb @ coefficients + intercept)
            error = (probabilities - yb) * wb / denominator
            gradient_w = xb.T @ error + l2 * coefficients / max(1, dimension)
            gradient_b = float(np.sum(error))

            step += 1
            m_w = beta1 * m_w + (1.0 - beta1) * gradient_w
            v_w = beta2 * v_w + (1.0 - beta2) * (gradient_w * gradient_w)
            m_b = beta1 * m_b + (1.0 - beta1) * gradient_b
            v_b = beta2 * v_b + (1.0 - beta2) * (gradient_b * gradient_b)
            correction1 = 1.0 - beta1**step
            correction2 = 1.0 - beta2**step
            coefficients -= learning_rate * (m_w / correction1) / (np.sqrt(v_w / correction2) + epsilon)
            intercept -= learning_rate * (m_b / correction1) / (math.sqrt(v_b / correction2) + epsilon)

        validation_probabilities = _sigmoid_array(x_validation @ coefficients + intercept)
        validation_loss = _log_loss(y_validation, validation_probabilities, w_validation)
        if validation_loss < best_loss - 1e-5:
            best_loss = validation_loss
            best_coefficients = coefficients.copy()
            best_intercept = float(intercept)
            best_epoch = epoch
            stale_epochs = 0
        else:
            stale_epochs += 1
        if stale_epochs >= 22 and epoch >= 30:
            break

    return {
        "weights": best_coefficients,
        "intercept": best_intercept,
        "best_epoch": best_epoch,
        "validation_log_loss": best_loss,
        "epochs_run": epoch,
    }


def fit_platt(logits: np.ndarray, labels: np.ndarray, weights: np.ndarray) -> Tuple[float, float]:
    a = 1.0
    b = 0.0
    for _ in range(60):
        values = np.clip(a * logits + b, -40.0, 40.0)
        probabilities = _sigmoid_array(values)
        residual = (probabilities - labels) * weights
        variance = probabilities * (1.0 - probabilities) * weights
        gradient = np.asarray(
            [float(np.sum(residual * logits)), float(np.sum(residual))],
            dtype=np.float64,
        )
        hessian = np.asarray(
            [
                [float(np.sum(variance * logits * logits)) + 1e-3, float(np.sum(variance * logits))],
                [float(np.sum(variance * logits)), float(np.sum(variance)) + 1e-3],
            ],
            dtype=np.float64,
        )
        try:
            delta = np.linalg.solve(hessian, gradient)
        except np.linalg.LinAlgError:
            break
        a -= float(delta[0])
        b -= float(delta[1])
        a = min(5.0, max(0.05, a))
        b = min(5.0, max(-5.0, b))
        if float(np.max(np.abs(delta))) < 1e-6:
            break
    return a, b


def calibrated_probabilities(
    matrix: np.ndarray,
    fit: Dict[str, Any],
    platt_a: float,
    platt_b: float,
) -> Tuple[np.ndarray, np.ndarray]:
    logits = matrix @ fit["weights"] + float(fit["intercept"])
    raw = _sigmoid_array(logits)
    calibrated = _sigmoid_array(platt_a * logits + platt_b)
    return raw, calibrated


def _auc(labels: np.ndarray, probabilities: np.ndarray) -> Optional[float]:
    positive = int(np.sum(labels == 1.0))
    negative = int(np.sum(labels == 0.0))
    if positive == 0 or negative == 0:
        return None
    order = np.argsort(probabilities, kind="mergesort")
    sorted_probabilities = probabilities[order]
    ranks = np.empty(len(probabilities), dtype=np.float64)
    index = 0
    while index < len(order):
        end = index + 1
        while end < len(order) and sorted_probabilities[end] == sorted_probabilities[index]:
            end += 1
        average_rank = (index + 1 + end) / 2.0
        ranks[order[index:end]] = average_rank
        index = end
    rank_sum = float(np.sum(ranks[labels == 1.0]))
    return (rank_sum - positive * (positive + 1) / 2.0) / (positive * negative)


def probability_metrics(
    labels: np.ndarray,
    probabilities: np.ndarray,
    weights: np.ndarray,
) -> Dict[str, Any]:
    base_rate = _weighted_mean(labels, weights)
    baseline = np.full_like(probabilities, base_rate)
    brier = _weighted_mean((probabilities - labels) ** 2, weights)
    baseline_brier = _weighted_mean((baseline - labels) ** 2, weights)
    accuracy = _weighted_mean(((probabilities >= 0.5) == (labels >= 0.5)).astype(float), weights)
    bins = []
    for lower in np.linspace(0.0, 0.9, 10):
        upper = lower + 0.1
        mask = (probabilities >= lower) & (probabilities < upper if upper < 1.0 else probabilities <= upper)
        if not np.any(mask):
            continue
        bins.append(
            {
                "range": f"{lower:.1f}-{upper:.1f}",
                "n": int(np.sum(mask)),
                "predicted_over": round(float(np.mean(probabilities[mask])) * 100.0, 2),
                "observed_over": round(_weighted_mean(labels[mask], weights[mask]) * 100.0, 2),
            }
        )
    auc = _auc(labels, probabilities)
    return {
        "n": len(labels),
        "base_over_rate": round(base_rate * 100.0, 2),
        "log_loss": round(_log_loss(labels, probabilities, weights), 6),
        "brier": round(brier, 6),
        "baseline_brier": round(baseline_brier, 6),
        "brier_skill": round(1.0 - brier / baseline_brier, 5) if baseline_brier > 0 else None,
        "accuracy": round(accuracy * 100.0, 2),
        "auc": round(float(auc), 5) if auc is not None else None,
        "calibration_bins": bins,
    }


def _wilson_lower_bound(positive: int, total: int, z: float = 1.96) -> float:
    if total <= 0:
        return 0.0
    proportion = positive / total
    denominator = 1.0 + z * z / total
    center = proportion + z * z / (2.0 * total)
    spread = z * math.sqrt((proportion * (1.0 - proportion) + z * z / (4.0 * total)) / total)
    return (center - spread) / denominator


def selection_metrics(
    probabilities: np.ndarray,
    over_settlements: np.ndarray,
    threshold: float,
) -> Dict[str, Any]:
    selected_indices: List[int] = []
    selected_sides: List[str] = []
    selected_settlements: List[float] = []
    selected_probabilities: List[float] = []
    for index, probability in enumerate(probabilities):
        if probability >= threshold:
            selected_indices.append(index)
            selected_sides.append("OVER")
            selected_settlements.append(float(over_settlements[index]))
            selected_probabilities.append(float(probability))
        elif probability <= 1.0 - threshold:
            selected_indices.append(index)
            selected_sides.append("UNDER")
            selected_settlements.append(float(-over_settlements[index]))
            selected_probabilities.append(float(1.0 - probability))

    positive = sum(value > 0 for value in selected_settlements)
    negative = sum(value < 0 for value in selected_settlements)
    pushes = sum(value == 0 for value in selected_settlements)
    decided = positive + negative
    breakdown = Counter(
        "W" if value >= 0.99 else "HW" if value > 0 else "L" if value <= -0.99 else "HL" if value < 0 else "P"
        for value in selected_settlements
    )
    return {
        "threshold": round(float(threshold), 3),
        "selected": len(selected_indices),
        "coverage": round(len(selected_indices) * 100.0 / max(len(probabilities), 1), 2),
        "over_picks": selected_sides.count("OVER"),
        "under_picks": selected_sides.count("UNDER"),
        "positive": positive,
        "negative": negative,
        "pushes": pushes,
        "positive_rate": round(positive * 100.0 / decided, 2) if decided else None,
        "wilson_lower_95": round(_wilson_lower_bound(positive, decided) * 100.0, 2) if decided else None,
        "mean_settlement_even_odds": round(float(np.mean(selected_settlements)), 5) if selected_settlements else None,
        "mean_model_confidence": round(float(np.mean(selected_probabilities)) * 100.0, 2) if selected_probabilities else None,
        "breakdown": dict(breakdown),
    }


def choose_threshold(
    probabilities: np.ndarray,
    settlements: np.ndarray,
    minimum_selected: int,
) -> Tuple[float, bool, List[Dict[str, Any]]]:
    candidates = [round(value, 2) for value in np.arange(0.54, 0.721, 0.01)]
    rows = [selection_metrics(probabilities, settlements, threshold) for threshold in candidates]
    eligible = [
        row
        for row in rows
        if row["selected"] >= minimum_selected
        and (row["positive_rate"] or 0.0) >= 55.0
        and (row["wilson_lower_95"] or 0.0) >= 50.0
        and (row["mean_settlement_even_odds"] or 0.0) > 0.0
    ]
    pool = eligible or [row for row in rows if row["selected"] >= minimum_selected]
    if not pool:
        return 0.62, False, rows
    best = max(
        pool,
        key=lambda row: (
            float(row["wilson_lower_95"] or 0.0),
            float(row["mean_settlement_even_odds"] or -1.0),
            int(row["selected"]),
            -float(row["threshold"]),
        ),
    )
    return float(best["threshold"]), bool(eligible), rows


def _split_summary(samples: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not samples:
        return {"rows": 0, "start": None, "end": None}
    return {
        "rows": len(samples),
        "start": samples[0]["date"].isoformat(),
        "end": samples[-1]["date"].isoformat(),
        "over_positive": sum(float(sample["over_settlement"]) > 0 for sample in samples),
        "under_positive": sum(float(sample["over_settlement"]) < 0 for sample in samples),
    }


def _fit_pipeline(
    train_samples: Sequence[Dict[str, Any]],
    calibration_samples: Sequence[Dict[str, Any]],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    schema = fit_transform_schema(
        train_samples,
        min_token_support=int(args.min_token_support),
        max_tokens=int(args.max_tokens),
    )
    x_train = transform(train_samples, schema)
    x_calibration = transform(calibration_samples, schema)
    y_train, w_train, _ = _arrays(train_samples)
    y_calibration, w_calibration, _ = _arrays(calibration_samples)
    fit = fit_logistic(
        x_train,
        y_train,
        w_train,
        x_calibration,
        y_calibration,
        w_calibration,
        seed=int(args.seed),
        epochs=int(args.epochs),
        batch_size=int(args.batch_size),
        learning_rate=float(args.learning_rate),
        l2=float(args.l2),
    )
    calibration_logits = x_calibration @ fit["weights"] + float(fit["intercept"])
    platt_a, platt_b = fit_platt(calibration_logits, y_calibration, w_calibration)
    return {
        "schema": schema,
        "fit": fit,
        "platt_a": platt_a,
        "platt_b": platt_b,
    }


def _top_model_features(pipeline: Dict[str, Any], limit: int = 25) -> Dict[str, Any]:
    schema = pipeline["schema"]
    names = list(schema["numeric_names"]) + list(schema["token_names"])
    weights = pipeline["fit"]["weights"]
    rows = sorted(
        ({"feature": name, "weight": round(float(weight), 6)} for name, weight in zip(names, weights)),
        key=lambda row: abs(row["weight"]),
        reverse=True,
    )
    return {
        "toward_over": sorted((row for row in rows if row["weight"] > 0), key=lambda row: -row["weight"])[:limit],
        "toward_under": sorted((row for row in rows if row["weight"] < 0), key=lambda row: row["weight"])[:limit],
    }


def _placement_patterns(samples: Sequence[Dict[str, Any]], minimum_support: int = 35) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        ah_family = next((token.split("=", 1)[1] for token in sample["tokens"] if token.startswith("AH_FAMILY=")), "UNKNOWN")
        ou_family = next((token.split("=", 1)[1] for token in sample["tokens"] if token.startswith("OU_FAMILY=")), "UNKNOWN")
        groups[(ah_family, ou_family)].append(sample)
    rows = []
    for (ah_family, ou_family), subset in groups.items():
        if len(subset) < minimum_support:
            continue
        positive = sum(float(sample["over_settlement"]) > 0 for sample in subset)
        negative = sum(float(sample["over_settlement"]) < 0 for sample in subset)
        rows.append(
            {
                "ah_family": ah_family,
                "ou_family": ou_family,
                "n": len(subset),
                "over_positive_rate": round(positive * 100.0 / max(positive + negative, 1), 2),
                "under_positive_rate": round(negative * 100.0 / max(positive + negative, 1), 2),
                "wilson_over_lower_95": round(_wilson_lower_bound(positive, positive + negative) * 100.0, 2),
            }
        )
    rows.sort(key=lambda row: (-abs(row["over_positive_rate"] - 50.0), -row["n"], row["ah_family"], row["ou_family"]))
    return rows


def train_and_audit(db_path: Path, args: argparse.Namespace) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    samples, data_audit = _load_training_samples(db_path)
    train_samples, calibration_samples, holdout_samples = chronological_split(samples)

    audit_pipeline = _fit_pipeline(train_samples, calibration_samples, args)
    x_calibration = transform(calibration_samples, audit_pipeline["schema"])
    x_holdout = transform(holdout_samples, audit_pipeline["schema"])
    y_calibration, w_calibration, settlement_calibration = _arrays(calibration_samples)
    y_holdout, w_holdout, settlement_holdout = _arrays(holdout_samples)
    _, probability_calibration = calibrated_probabilities(
        x_calibration,
        audit_pipeline["fit"],
        audit_pipeline["platt_a"],
        audit_pipeline["platt_b"],
    )
    _, probability_holdout = calibrated_probabilities(
        x_holdout,
        audit_pipeline["fit"],
        audit_pipeline["platt_a"],
        audit_pipeline["platt_b"],
    )

    minimum_selected = max(40, int(len(calibration_samples) * 0.05))
    threshold, calibration_gate, threshold_candidates = choose_threshold(
        probability_calibration,
        settlement_calibration,
        minimum_selected,
    )
    calibration_selection = selection_metrics(probability_calibration, settlement_calibration, threshold)
    holdout_selection = selection_metrics(probability_holdout, settlement_holdout, threshold)
    calibration_probability = probability_metrics(y_calibration, probability_calibration, w_calibration)
    holdout_probability = probability_metrics(y_holdout, probability_holdout, w_holdout)

    holdout_gate = (
        holdout_selection["selected"] >= max(30, int(len(holdout_samples) * 0.03))
        and float(holdout_selection["positive_rate"] or 0.0) > 50.0
        and float(holdout_selection["mean_settlement_even_odds"] or 0.0) > 0.0
        and float(holdout_probability["brier"]) <= float(holdout_probability["baseline_brier"]) + 0.01
    )
    enabled = bool(calibration_gate and holdout_gate)

    production_cut = _boundary_after_date(samples, int(len(samples) * 0.85))
    production_train = samples[:production_cut]
    production_calibration = samples[production_cut:]
    production_pipeline = _fit_pipeline(production_train, production_calibration, args)

    schema = production_pipeline["schema"]
    fit = production_pipeline["fit"]
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    model_payload = {
        "profile": PROFILE,
        "version": MODEL_VERSION,
        "generated_at": generated_at,
        "source": {
            "database": str(db_path.resolve()),
            "historical_rows_considered": data_audit.get("rows_historical", 0),
            "safe_samples": len(samples),
            "production_train": _split_summary(production_train),
            "production_calibration": _split_summary(production_calibration),
        },
        "model": {
            "numeric_names": list(schema["numeric_names"]),
            "numeric_means": [round(float(value), 10) for value in schema["numeric_means"]],
            "numeric_scales": [round(float(value), 10) for value in schema["numeric_scales"]],
            "token_names": list(schema["token_names"]),
            "weights": [round(float(value), 10) for value in fit["weights"]],
            "intercept": round(float(fit["intercept"]), 10),
            "platt_a": round(float(production_pipeline["platt_a"]), 10),
            "platt_b": round(float(production_pipeline["platt_b"]), 10),
            "best_epoch": int(fit["best_epoch"]),
        },
        "decision": {
            "enabled": enabled,
            "threshold": threshold,
            "min_contexts": 3,
            "policy": "OVER if p>=threshold; UNDER if p<=1-threshold; otherwise NO_BET",
            "calibration_gate_passed": calibration_gate,
            "holdout_gate_passed": holdout_gate,
        },
        "audit": {
            "split": {
                "train": _split_summary(train_samples),
                "calibration": _split_summary(calibration_samples),
                "holdout": _split_summary(holdout_samples),
            },
            "calibration_probability": calibration_probability,
            "holdout_probability": holdout_probability,
            "calibration_selection": calibration_selection,
            "holdout_selection": holdout_selection,
        },
    }

    report = {
        "profile": PROFILE,
        "version": MODEL_VERSION,
        "generated_at": generated_at,
        "claim": "No model is infallible. Metrics are chronological and include abstentions.",
        "data_audit": data_audit,
        "splits": model_payload["audit"]["split"],
        "training": {
            "audit_model_best_epoch": int(audit_pipeline["fit"]["best_epoch"]),
            "audit_model_epochs_run": int(audit_pipeline["fit"]["epochs_run"]),
            "audit_tokens": len(audit_pipeline["schema"]["token_names"]),
            "production_model_best_epoch": int(production_pipeline["fit"]["best_epoch"]),
            "production_tokens": len(production_pipeline["schema"]["token_names"]),
        },
        "decision": model_payload["decision"],
        "calibration_probability": calibration_probability,
        "holdout_probability": holdout_probability,
        "calibration_selection": calibration_selection,
        "holdout_selection": holdout_selection,
        "threshold_candidates_calibration": threshold_candidates,
        "placement_patterns_holdout": _placement_patterns(holdout_samples),
        "top_model_features": _top_model_features(production_pipeline),
        "token_selection": schema["token_audit"][:120],
    }
    return model_payload, report


def _load_upcoming_matches(db_path: Path, as_of: date) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    matches: List[Dict[str, Any]] = []
    audit: Counter = Counter()
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    query = """
        SELECT match_id, handicap, score, match_date, payload_json, explorer_json
        FROM matches
        WHERE state IN ('precacheo', 'pending_results')
        ORDER BY match_id
    """
    try:
        for row in connection.execute(query):
            audit["candidate_rows"] += 1
            match = merge_match_records(
                _json_dict(row["explorer_json"]),
                _json_dict(row["payload_json"]),
                {
                    "match_id": str(row["match_id"]),
                    "match_date": row["match_date"],
                    "handicap": row["handicap"],
                },
            )
            if _score_from_match(match, row["score"]) is not None:
                audit["skip:already_finished"] += 1
                continue
            match_date = get_match_date(match)
            if match_date is None:
                audit["skip:no_date"] += 1
                continue
            if match_date < as_of:
                audit["skip:before_as_of"] += 1
                continue
            matches.append(match)
    finally:
        connection.close()
    audit["upcoming_rows"] = len(matches)
    return matches, dict(audit)


def generate_current_picks(
    db_path: Path,
    model_path: Path,
    as_of: date,
) -> Dict[str, Any]:
    payload = load_model(model_path, force_reload=True)
    upcoming, source_audit = _load_upcoming_matches(db_path, as_of)
    picks = []
    no_bet = 0
    if payload is not None:
        for match in upcoming:
            pick = evaluate_match(match, model_path=model_path)
            if pick is None:
                no_bet += 1
                continue
            match_date = get_match_date(match)
            picks.append(
                {
                    "match_id": str(match.get("match_id") or match.get("id") or ""),
                    "date": match_date.isoformat() if match_date else None,
                    "time": match.get("time") or "",
                    "league": match.get("league_name") or match.get("league") or "",
                    "home": match.get("home_name") or match.get("home_team") or "",
                    "away": match.get("away_name") or match.get("away_team") or "",
                    **pick,
                }
            )
    picks.sort(key=lambda row: (-float(row.get("probability", 0.0)), row.get("date") or "", row.get("time") or ""))
    return {
        "profile": PROFILE,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "as_of": as_of.isoformat(),
        "model_enabled": bool((payload or {}).get("decision", {}).get("enabled", False)),
        "source_audit": source_audit,
        "selected": len(picks),
        "no_bet": no_bet,
        "picks": picks,
    }


def markdown_report(report: Dict[str, Any], picks: Dict[str, Any]) -> str:
    holdout = report["holdout_selection"]
    probability = report["holdout_probability"]
    decision = report["decision"]
    lines = [
        "# HouseMind O/U v1",
        "",
        "Sistema probabilistico con validacion cronologica y abstencion. No existe garantia de acierto total.",
        "",
        "## Veredicto auditable",
        "",
        f"- Modelo habilitado: {'SI' if decision['enabled'] else 'NO'}",
        f"- Umbral de seleccion: {float(decision['threshold']) * 100:.1f}%",
        f"- Holdout intocable: {holdout['selected']} picks de {probability['n']} partidos ({holdout['coverage']}% cobertura)",
        f"- Acierto direccional holdout: {holdout['positive_rate']}%",
        f"- Limite inferior Wilson 95%: {holdout['wilson_lower_95']}%",
        f"- Brier holdout: {probability['brier']} (base {probability['baseline_brier']})",
        f"- AUC holdout: {probability['auc']}",
        f"- Liquidacion media a cuota par teorica: {holdout['mean_settlement_even_odds']}",
        "",
        "La liquidacion a cuota par es una prueba estadistica, no ROI real: la base no conserva cuotas O/U historicas completas.",
        "",
        "## Particiones temporales",
        "",
    ]
    for name, split in report["splits"].items():
        lines.append(f"- {name}: {split['rows']} partidos, {split['start']} a {split['end']}")
    lines.extend(["", "## Auditoria de datos", ""])
    for key, value in sorted(report["data_audit"].items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- {key}: {value}")

    lines.extend(["", "## Senales principales hacia Over", ""])
    for item in report["top_model_features"]["toward_over"][:15]:
        lines.append(f"- {item['feature']}: {item['weight']:+.4f}")
    lines.extend(["", "## Senales principales hacia Under", ""])
    for item in report["top_model_features"]["toward_under"][:15]:
        lines.append(f"- {item['feature']}: {item['weight']:+.4f}")

    lines.extend(["", f"## Picks desde {picks['as_of']}", ""])
    lines.append(f"Seleccionados: {picks['selected']} | NO BET: {picks['no_bet']}")
    lines.append("")
    for pick in picks["picks"][:80]:
        lines.append(
            f"- {pick['date']} {pick['time']} | {pick['home']} - {pick['away']} | "
            f"{pick['display_pick_label']} | {float(pick['probability']) * 100:.1f}%"
        )
    if not picks["picks"]:
        lines.append("- Sin picks que superen el umbral auditable.")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Train, audit and export HouseMind O/U.")
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--database", default="data/app_data.db")
    parser.add_argument("--model-output", default="models/housemind_ou_v1.json")
    parser.add_argument("--report-json", default="data/housemind/training_report.json")
    parser.add_argument("--report-md", default="data/housemind/HOUSEMIND_OU_REPORT.md")
    parser.add_argument("--picks-output", default="data/housemind/current_picks.json")
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--min-token-support", type=int, default=35)
    parser.add_argument("--max-tokens", type=int, default=360)
    parser.add_argument("--epochs", type=int, default=180)
    parser.add_argument("--batch-size", type=int, default=384)
    parser.add_argument("--learning-rate", type=float, default=0.012)
    parser.add_argument("--l2", type=float, default=0.03)
    parser.add_argument("--seed", type=int, default=20260711)
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    db_path = (project_root / args.database).resolve()
    model_path = (project_root / args.model_output).resolve()
    report_json_path = (project_root / args.report_json).resolve()
    report_md_path = (project_root / args.report_md).resolve()
    picks_path = (project_root / args.picks_output).resolve()
    as_of = parse_date(args.as_of)
    if as_of is None:
        raise SystemExit(f"Invalid --as-of date: {args.as_of}")

    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    model_payload, report = train_and_audit(db_path, args)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    report_json_path.parent.mkdir(parents=True, exist_ok=True)
    report_md_path.parent.mkdir(parents=True, exist_ok=True)
    picks_path.parent.mkdir(parents=True, exist_ok=True)

    model_path.write_text(json.dumps(model_payload, ensure_ascii=True, indent=2), encoding="utf-8")
    picks = generate_current_picks(db_path, model_path, as_of)
    report["current_picks_summary"] = {
        key: value for key, value in picks.items() if key != "picks"
    }
    report_json_path.write_text(json.dumps(report, ensure_ascii=True, indent=2), encoding="utf-8")
    picks_path.write_text(json.dumps(picks, ensure_ascii=True, indent=2), encoding="utf-8")
    report_md_path.write_text(markdown_report(report, picks), encoding="utf-8")

    holdout = report["holdout_selection"]
    print(f"[OK] model={model_path}")
    print(f"[OK] report={report_json_path}")
    print(f"[OK] picks={picks_path}")
    print(
        "[AUDIT] "
        f"enabled={model_payload['decision']['enabled']} "
        f"threshold={model_payload['decision']['threshold']:.2f} "
        f"holdout_selected={holdout['selected']} "
        f"holdout_positive_rate={holdout['positive_rate']} "
        f"holdout_wilson_lower={holdout['wilson_lower_95']} "
        f"current_picks={picks['selected']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
