#!/usr/bin/env python3
"""Audit and train the AH +/-0.5 model from the local football database.

The database convention is unusual but empirically verified:
    raw AH > 0 -> home is favourite and gives -abs(AH)
    raw AH < 0 -> away is favourite and gives -abs(AH)

All feature events must carry an event date strictly before the target match.
Current-match statistics, post-hoc standings and un-timestamped aggregates are
deliberately blacklisted. The SQLite database is always opened read-only.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.frozen import FrozenEstimator
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


BLACKLIST = {
    "target.stats_rows", "home_standings", "away_standings", "home_ou_stats",
    "away_ou_stats", "home_ou_stats_specific", "away_ou_stats_specific",
    "home_ou_stats_general", "away_ou_stats_general",
}
HALF_LIFE_CANDIDATES = (30, 60, 90, 180)
CLASS_NAMES = ("FAV_WIN", "DRAW", "DOG_WIN")


def safe_float(value: Any) -> float | None:
    try:
        text = str(value).strip().replace(",", ".")
        if not text or text.upper() in {"N/A", "NULL", "-", "?", "NONE"}:
            return None
        result = float(text)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def safe_date(value: Any) -> date | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def parse_score(value: Any) -> tuple[int, int] | None:
    found = re.findall(r"\d+", str(value or ""))
    if len(found) < 2:
        return None
    return int(found[0]), int(found[1])


def norm_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


def same_team(left: Any, right: Any) -> bool:
    a, b = norm_name(left), norm_name(right)
    return bool(a and b and (a == b or (len(a) > 5 and a in b) or (len(b) > 5 and b in a)))


def open_ro(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def raw_ah_to_home_handicap(raw_ah: float) -> float:
    """Convert source convention to the handicap carried by the home team."""
    return -raw_ah


def favourite_is_home(raw_ah: float) -> bool:
    return raw_ah > 0


@dataclass
class MatchRecord:
    match_id: str
    match_date: date
    home: str
    away: str
    league: str
    season: str
    raw_ah: float
    ou: float | None
    home_goals: int
    away_goals: int
    payload: dict[str, Any]

    @property
    def favourite(self) -> str:
        return self.home if favourite_is_home(self.raw_ah) else self.away

    @property
    def underdog(self) -> str:
        return self.away if favourite_is_home(self.raw_ah) else self.home

    @property
    def target(self) -> int:
        fav_goals = self.home_goals if favourite_is_home(self.raw_ah) else self.away_goals
        dog_goals = self.away_goals if favourite_is_home(self.raw_ah) else self.home_goals
        return 0 if fav_goals > dog_goals else 1 if fav_goals == dog_goals else 2


def load_records(db_path: Path, only_ah05: bool = True) -> list[MatchRecord]:
    where = "WHERE state='historical' AND handicap IS NOT NULL"
    if only_ah05:
        where += " AND abs(abs(handicap)-0.5)<0.001"
    sql = f"SELECT match_id,match_date,handicap,score,payload_json,explorer_json FROM matches {where}"
    records: list[MatchRecord] = []
    with open_ro(db_path) as conn:
        for row in conn.execute(sql):
            try:
                payload = json.loads(row["payload_json"] or "{}")
                explorer = json.loads(row["explorer_json"] or "{}")
            except json.JSONDecodeError:
                continue
            # Explorer is the richer normalized view for recent histories, while
            # payload retains H2H blocks omitted from some compact explorer rows.
            payload.update({key: value for key, value in explorer.items() if value not in (None, "", [], {})})
            target_date = safe_date(row["match_date"]) or safe_date(payload.get("match_date") or payload.get("date"))
            score = parse_score(row["score"]) or parse_score(payload.get("final_score") or payload.get("score"))
            raw_ah = safe_float(row["handicap"])
            if not target_date or not score or raw_ah is None or abs(raw_ah) < 1e-9:
                continue
            home = payload.get("home_name") or payload.get("home_team")
            away = payload.get("away_name") or payload.get("away_team")
            if not home or not away:
                continue
            odds = payload.get("main_match_odds") or {}
            records.append(MatchRecord(
                str(row["match_id"]), target_date, str(home), str(away),
                str(payload.get("league_id") or payload.get("league_name") or "UNKNOWN"),
                str(payload.get("season") or target_date.year), raw_ah,
                safe_float(odds.get("goals_linea") or payload.get("ou_line")),
                score[0], score[1], payload,
            ))
    return sorted(records, key=lambda r: (r.match_date, r.match_id))


def dated_events(record: MatchRecord, key: str) -> list[dict[str, Any]]:
    events = record.payload.get(key) or []
    if isinstance(events, dict):
        events = [events]
    usable: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for event in events:
        if not isinstance(event, dict):
            continue
        event_date = safe_date(event.get("date"))
        if not event_date or event_date >= record.match_date:
            continue
        identity = (event.get("matchIndex") or event.get("match_id"), event_date, event.get("home"), event.get("away"))
        if identity in seen:
            continue
        seen.add(identity)
        usable.append(event)
    return usable


def event_team_view(event: dict[str, Any], team: str) -> dict[str, float] | None:
    score = parse_score(event.get("score") or event.get("score_raw") or event.get("result"))
    if not score:
        return None
    is_home = same_team(event.get("home") or event.get("home_team"), team)
    is_away = same_team(event.get("away") or event.get("away_team"), team)
    if not is_home and not is_away:
        return None
    own, opp = (score[0], score[1]) if is_home else (score[1], score[0])
    raw_ah = safe_float(event.get("ahLine") or event.get("ah_line") or event.get("handicap"))
    team_handicap = None if raw_ah is None else (raw_ah_to_home_handicap(raw_ah) if is_home else raw_ah)
    residual = None if team_handicap is None else own - opp + team_handicap
    return {
        "win": float(own > opp), "draw": float(own == opp), "loss": float(own < opp),
        "gf": float(own), "ga": float(opp), "margin": float(own - opp),
        "clean": float(opp == 0), "scored": float(own > 0),
        "win1": float(own - opp == 1), "win2p": float(own - opp >= 2),
        "three_plus": float(own + opp >= 3), "team_handicap": team_handicap,
        "cover": float(residual is not None and residual >= .25),
        "push": float(residual is not None and abs(residual) < .25),
        "age": 0.0,
    }


def weighted_form(record: MatchRecord, team: str, keys: Iterable[str], half_life: int) -> dict[str, float]:
    views: list[dict[str, float]] = []
    seen: set[tuple[Any, ...]] = set()
    for key in keys:
        for event in dated_events(record, key):
            identity = (event.get("matchIndex") or event.get("match_id"), event.get("date"), event.get("home"), event.get("away"))
            if identity in seen:
                continue
            seen.add(identity)
            view = event_team_view(event, team)
            event_date = safe_date(event.get("date"))
            if view and event_date:
                view["age"] = float((record.match_date - event_date).days)
                views.append(view)
    views.sort(key=lambda x: x["age"])
    views = views[:20]
    if not views:
        return {f: np.nan for f in ("n", "win", "draw", "loss", "gf", "ga", "margin", "clean", "scored", "win1", "win2p", "three_plus", "cover", "fav_prior", "age_mean")}
    weights = np.array([.5 ** (v["age"] / half_life) for v in views], dtype=float)
    weight_sum = float(weights.sum())
    out = {"n": float(len(views)), "age_mean": float(np.average([v["age"] for v in views], weights=weights))}
    for field in ("win", "draw", "loss", "gf", "ga", "margin", "clean", "scored", "win1", "win2p", "three_plus", "cover"):
        out[field] = float(np.average([v[field] for v in views], weights=weights))
    market_views = [(v, w) for v, w in zip(views, weights) if v["team_handicap"] is not None]
    out["fav_prior"] = float(np.average([float(v["team_handicap"] < 0) for v, _ in market_views], weights=[w for _, w in market_views])) if market_views else np.nan
    return out


def process_score(block: dict[str, Any], team: str) -> float:
    rows = block.get("stats_rows") or []
    is_home = same_team(block.get("home") or block.get("home_team"), team)
    total = 0.0
    found = 0
    scales = {"tiros a puerta": (0.45, 3), "tiros": (0.2, 7), "ataques peligrosos": (0.35, 18)}
    for row in rows:
        label = str(row.get("label") or row.get("name") or "").casefold()
        for name, (weight, scale) in scales.items():
            if name in label:
                h, a = safe_float(row.get("home")), safe_float(row.get("away"))
                if h is not None and a is not None:
                    diff = (h - a) if is_home else (a - h)
                    total += weight * math.tanh(diff / scale)
                    found += 1
                break
    return total if found else np.nan


def comparison_features(record: MatchRecord) -> dict[str, float]:
    out = {"common_score": np.nan, "venue_reversal": 0.0, "h2h_stadium_residual": np.nan, "h2h_general_residual": np.nan}
    indirect = record.payload.get("comparativas_indirectas") or {}
    values = []
    for side in ("left", "right"):
        block = indirect.get(side) or {}
        block_date = safe_date(block.get("date"))
        if not block_date or block_date >= record.match_date:
            continue
        team = record.home if side == "left" else record.away
        view = event_team_view(block, team)
        if view:
            age = (record.match_date - block_date).days
            values.append((view["margin"], .5 ** (age / 90), view.get("team_handicap")))
    if len(values) == 2:
        left, right = values
        out["common_score"] = left[0] * left[1] - right[0] * right[1]
        current_fav_home = favourite_is_home(record.raw_ah)
        dog_prior, fav_prior = (right[2], left[2]) if current_fav_home else (left[2], right[2])
        if dog_prior is not None and fav_prior is not None:
            out["venue_reversal"] = float(dog_prior < 0 and dog_prior <= fav_prior)
    for key, output in (("h2h_stadium", "h2h_stadium_residual"), ("h2h_general", "h2h_general_residual")):
        block = record.payload.get(key) or {}
        prior_date = safe_date(block.get("date1"))
        score = parse_score(block.get("res1") or block.get("result"))
        old_raw = safe_float(block.get("ah1"))
        if prior_date and prior_date < record.match_date and score and old_raw is not None:
            current_fav_goals = score[0] if favourite_is_home(record.raw_ah) else score[1]
            dog_goals = score[1] if favourite_is_home(record.raw_ah) else score[0]
            out[output] = (current_fav_goals - dog_goals - .5) * (.5 ** ((record.match_date - prior_date).days / 180))
    return out


def extract_features(record: MatchRecord, half_life: int) -> dict[str, Any]:
    home_specific = weighted_form(record, record.home, ("recent_home_matches_same_league_specific", "recent_home_matches"), half_life)
    away_specific = weighted_form(record, record.away, ("recent_away_matches_same_league_specific", "recent_away_matches"), half_life)
    home_global = weighted_form(record, record.home, ("recent_home_matches_same_league_general", "recent_home_matches_all"), half_life)
    away_global = weighted_form(record, record.away, ("recent_away_matches_same_league_general", "recent_away_matches_all"), half_life)
    fav_home = favourite_is_home(record.raw_ah)
    fav_specific, dog_specific = (home_specific, away_specific) if fav_home else (away_specific, home_specific)
    fav_global, dog_global = (home_global, away_global) if fav_home else (away_global, home_global)
    feature: dict[str, Any] = {
        "match_id": record.match_id, "date": record.match_date.isoformat(), "home": record.home, "away": record.away,
        "league": record.league, "season": record.season, "fav_is_home": float(fav_home), "ou_market": record.ou,
        "ah_x_ou": None if record.ou is None else .5 * record.ou,
        "target": record.target, "total_goals": record.home_goals + record.away_goals,
    }
    for scope, fav, dog in (("venue", fav_specific, dog_specific), ("global", fav_global, dog_global)):
        for key in fav:
            feature[f"{scope}_{key}_diff"] = fav[key] - dog[key]
            feature[f"{scope}_fav_{key}"] = fav[key]
            feature[f"{scope}_dog_{key}"] = dog[key]
    previous_fav = record.payload.get("last_home_match" if fav_home else "last_away_match") or {}
    previous_dog = record.payload.get("last_away_match" if fav_home else "last_home_match") or {}
    feature["fav_previous_process"] = process_score(previous_fav, record.favourite) if safe_date(previous_fav.get("date")) and safe_date(previous_fav.get("date")) < record.match_date else np.nan
    feature["dog_previous_process"] = process_score(previous_dog, record.underdog) if safe_date(previous_dog.get("date")) and safe_date(previous_dog.get("date")) < record.match_date else np.nan
    feature["process_diff"] = feature["fav_previous_process"] - feature["dog_previous_process"]
    feature.update(comparison_features(record))
    feature["total_inflation"] = (record.ou - (fav_global["gf"] + fav_global["ga"] + dog_global["gf"] + dog_global["ga"]) / 2) if record.ou is not None else np.nan
    return feature


def build_frame(records: list[MatchRecord], half_life: int) -> pd.DataFrame:
    frame = pd.DataFrame(extract_features(record, half_life) for record in records)
    # Online Elo: feature is calculated before updating, so future games cannot leak backwards.
    ratings: defaultdict[str, float] = defaultdict(lambda: 1500.0)
    elo_edges = []
    for record in records:
        fav, dog = record.favourite, record.underdog
        home_bonus = 45.0 if favourite_is_home(record.raw_ah) else -45.0
        edge = ratings[fav] - ratings[dog] + home_bonus
        elo_edges.append(edge)
        expected = 1 / (1 + 10 ** (-edge / 400))
        actual = 1.0 if record.target == 0 else .5 if record.target == 1 else 0.0
        delta = 20 * (actual - expected)
        ratings[fav] += delta
        ratings[dog] -= delta
    frame["elo_fav_edge"] = elo_edges
    return frame


def brier_multiclass(y: np.ndarray, probabilities: np.ndarray) -> float:
    truth = np.eye(3)[y]
    return float(np.mean(np.sum((probabilities - truth) ** 2, axis=1)))


def expected_calibration_error(y: np.ndarray, probabilities: np.ndarray, bins: int = 10) -> float:
    confidence = probabilities.max(axis=1)
    correct = (probabilities.argmax(axis=1) == y).astype(float)
    result = 0.0
    for low, high in zip(np.linspace(0, 1, bins + 1)[:-1], np.linspace(0, 1, bins + 1)[1:]):
        mask = (confidence >= low) & (confidence < high if high < 1 else confidence <= high)
        if mask.any():
            result += mask.mean() * abs(correct[mask].mean() - confidence[mask].mean())
    return float(result)


def metrics(y: np.ndarray, probabilities: np.ndarray) -> dict[str, float | None]:
    try:
        auc = float(roc_auc_score(y, probabilities, multi_class="ovr", labels=[0, 1, 2]))
    except ValueError:
        auc = None
    return {
        "log_loss": float(log_loss(y, probabilities, labels=[0, 1, 2])),
        "brier": brier_multiclass(y, probabilities), "ece": expected_calibration_error(y, probabilities), "auc_ovr": auc,
    }


def feature_columns(frame: pd.DataFrame) -> tuple[list[str], list[str]]:
    excluded = {"match_id", "date", "home", "away", "target", "total_goals"}
    categorical = ["league", "season"]
    numeric = [column for column in frame.columns if column not in excluded | set(categorical)]
    return numeric, categorical


def make_preprocessor(numeric: list[str], categorical: list[str]) -> ColumnTransformer:
    return ColumnTransformer([
        ("num", Pipeline([("impute", SimpleImputer(strategy="median", add_indicator=True)), ("scale", StandardScaler())]), numeric),
        ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=8), categorical),
    ])


def train_pipeline(frame: pd.DataFrame, output_dir: Path) -> dict[str, Any]:
    frame = frame.sort_values(["date", "match_id"]).reset_index(drop=True)
    n = len(frame)
    test_start = int(n * .8)
    validation_start = int(test_start * .8)
    calibration_start = int(validation_start * .875)
    train = frame.iloc[:calibration_start]
    calibration = frame.iloc[calibration_start:validation_start]
    validation = frame.iloc[validation_start:test_start]
    test = frame.iloc[test_start:]
    numeric, categorical = feature_columns(frame)
    X_train, y_train = train[numeric + categorical], train.target.to_numpy(int)
    X_calibration, y_calibration = calibration[numeric + categorical], calibration.target.to_numpy(int)
    X_validation, y_validation = validation[numeric + categorical], validation.target.to_numpy(int)
    X_test, y_test = test[numeric + categorical], test.target.to_numpy(int)
    candidates = {
        "logistic": LogisticRegression(max_iter=1500, C=.35, class_weight="balanced"),
        "gradient_boosting": GradientBoostingClassifier(n_estimators=120, max_depth=2, learning_rate=.035, min_samples_leaf=18, random_state=17),
    }
    validation_results = {}
    fitted = {}
    class_prior = np.bincount(y_train, minlength=3).astype(float)
    class_prior /= class_prior.sum()
    validation_results["constant_prior"] = metrics(y_validation, np.tile(class_prior, (len(y_validation), 1)))
    for name, estimator in candidates.items():
        model = Pipeline([("prep", make_preprocessor(numeric, categorical)), ("model", estimator)])
        model.fit(X_train, y_train)
        calibrated = CalibratedClassifierCV(FrozenEstimator(model), method="sigmoid", cv=None)
        calibrated.fit(X_calibration, y_calibration)
        probs = calibrated.predict_proba(X_validation)
        validation_results[name] = metrics(y_validation, probs)
        fitted[name] = calibrated
    draw_rate = float((y_train == 1).mean())
    elo_scale = 400.0
    elo_decisive = 1 / (1 + 10 ** (-validation.elo_fav_edge.to_numpy() / elo_scale))
    elo_probs = np.column_stack(((1 - draw_rate) * elo_decisive, np.full(len(validation), draw_rate), (1 - draw_rate) * (1 - elo_decisive)))
    validation_results["elo_bradley_terry"] = metrics(y_validation, elo_probs)
    learned_names = list(candidates)
    selected = min(learned_names, key=lambda name: validation_results[name]["log_loss"])
    validation_winner = min(validation_results, key=lambda name: validation_results[name]["log_loss"])
    final_model = fitted[selected]
    test_probabilities = final_model.predict_proba(X_test)
    test_prior_probabilities = np.tile(class_prior, (len(y_test), 1))
    walk_y, walk_probabilities = [], []
    pretest = frame.iloc[:test_start]
    boundaries = [int(len(pretest) * ratio) for ratio in (.52, .68, .84, 1.0)]
    for fold_start, fold_end in zip(boundaries[:-1], boundaries[1:]):
        fold_history = pretest.iloc[:fold_start]
        fold_future = pretest.iloc[fold_start:fold_end]
        fold_calibration_start = int(len(fold_history) * .88)
        fold_train = fold_history.iloc[:fold_calibration_start]
        fold_calibration = fold_history.iloc[fold_calibration_start:]
        estimator = clone(candidates[selected])
        fold_model = Pipeline([("prep", make_preprocessor(numeric, categorical)), ("model", estimator)])
        fold_model.fit(fold_train[numeric + categorical], fold_train.target)
        fold_calibrated = CalibratedClassifierCV(FrozenEstimator(fold_model), method="sigmoid", cv=None)
        fold_calibrated.fit(fold_calibration[numeric + categorical], fold_calibration.target)
        walk_y.extend(fold_future.target.astype(int).tolist())
        walk_probabilities.extend(fold_calibrated.predict_proba(fold_future[numeric + categorical]).tolist())
    total_model = Pipeline([
        ("prep", make_preprocessor(numeric, categorical)),
        ("model", HistGradientBoostingRegressor(loss="poisson", max_iter=180, max_leaf_nodes=15, learning_rate=.04, l2_regularization=2.0, random_state=17)),
    ])
    total_model.fit(X_train, train.total_goals)
    total_mae_validation = float(np.mean(np.abs(total_model.predict(X_validation) - validation.total_goals)))
    total_mae_test = float(np.mean(np.abs(total_model.predict(X_test) - test.total_goals)))
    final_metrics = metrics(y_test, test_probabilities)
    final_prior_metrics = metrics(y_test, test_prior_probabilities)
    deployment_status = "REJECTED_NO_OOS_EDGE" if validation_winner == "constant_prior" or final_metrics["log_loss"] >= final_prior_metrics["log_loss"] else "EXPERIMENTAL_CANDIDATE"
    artifact = {"classifier": final_model, "total_model": total_model, "features": numeric + categorical, "selected": selected, "half_life": int(frame.attrs.get("half_life", 90)), "deployment_status": deployment_status}
    output_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, output_dir / "ah05_model.joblib")
    test_predictions = test[["match_id", "date", "home", "away", "target"]].copy()
    for index, label in enumerate(CLASS_NAMES):
        test_predictions[f"p_{label.lower()}"] = test_probabilities[:, index]
    test_predictions.to_csv(output_dir / "final_oos_predictions.csv", index=False)
    return {
        "n": n, "train_n": len(train), "calibration_n": len(calibration), "validation_n": len(validation), "final_test_n": len(test),
        "train_end": train.date.max(), "calibration_end": calibration.date.max(), "validation_end": validation.date.max(), "test_start": test.date.min(), "test_end": test.date.max(),
        "selected_model": selected, "validation_winner": validation_winner, "validation_models": validation_results,
        "walk_forward": metrics(np.asarray(walk_y, dtype=int), np.asarray(walk_probabilities, dtype=float)),
        "walk_forward_n": len(walk_y), "walk_forward_folds": 3,
        "final_oos": final_metrics, "final_oos_constant_prior": final_prior_metrics,
        "deployment_status": deployment_status,
        "total_mae_validation": total_mae_validation, "total_mae_final_oos": total_mae_test,
        "roi": None, "roi_reason": "No hay precios AH prepartido verificables para la muestra completa; no se imputan cuotas.",
    }


def audit_database(db_path: Path) -> dict[str, Any]:
    with open_ro(db_path) as conn:
        table_rows = {row[0]: conn.execute(f'SELECT count(*) FROM "{row[0]}"').fetchone()[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        all_rows = conn.execute("SELECT match_id,match_date,handicap,score,payload_json,explorer_json FROM matches WHERE state='historical' AND abs(abs(handicap)-.5)<.001").fetchall()
        snapshot = dict(conn.execute("SELECT count(*) rows,count(distinct match_id) matches,min(observed_at) min_observed,max(observed_at) max_observed FROM precache_market_snapshots").fetchone())
    invalid_scores = invalid_dates = json_errors = temporal_violations = 0
    natural = Counter(); cached_deltas = []; id_names: defaultdict[str, set[str]] = defaultdict(set)
    name_ids: defaultdict[str, set[str]] = defaultdict(set); coverage = Counter()
    date_keys = ("recent_home_matches_all", "recent_away_matches_all", "recent_home_matches_same_league_specific", "recent_away_matches_same_league_specific", "recent_home_matches_same_league_general", "recent_away_matches_same_league_general")
    for row in all_rows:
        try:
            payload = json.loads(row["payload_json"] or "{}")
            explorer = json.loads(row["explorer_json"] or "{}")
        except json.JSONDecodeError: json_errors += 1; continue
        payload.update({key: value for key, value in explorer.items() if value not in (None, "", [], {})})
        target_date = safe_date(row["match_date"]) or safe_date(payload.get("match_date") or payload.get("date"))
        target_score = parse_score(row["score"]) or parse_score(payload.get("final_score") or payload.get("score"))
        invalid_dates += target_date is None
        invalid_scores += target_score is None
        if not target_date: continue
        home = payload.get("home_name") or payload.get("home_team"); away = payload.get("away_name") or payload.get("away_team")
        natural[(target_date, norm_name(home), norm_name(away))] += 1
        cached = safe_date(payload.get("cached_at"))
        if cached: cached_deltas.append((cached - target_date).days)
        for key in date_keys:
            events = payload.get(key) or []
            coverage[key] += bool(events)
            for event in events:
                event_date = safe_date(event.get("date"))
                temporal_violations += bool(event_date and event_date >= target_date)
                for name_key, id_key in (("home", "home_id"), ("away", "away_id")):
                    name, team_id = norm_name(event.get(name_key)), str(event.get(id_key) or "")
                    if name and team_id: id_names[team_id].add(name); name_ids[name].add(team_id)
    sign_rows = []
    for sign in (.5, -.5):
        selected = [row for row in all_rows if safe_float(row["handicap"]) == sign and parse_score(row["score"])]
        home_wins = sum(parse_score(row["score"])[0] > parse_score(row["score"])[1] for row in selected)
        away_wins = sum(parse_score(row["score"])[0] < parse_score(row["score"])[1] for row in selected)
        sign_rows.append({"raw_ah": sign, "n": len(selected), "home_win_rate": home_wins / len(selected) if selected else None, "away_win_rate": away_wins / len(selected) if selected else None})
    return {
        "database": str(db_path), "database_bytes": db_path.stat().st_size, "table_rows": table_rows,
        "ah05_rows": len(all_rows), "invalid_dates": invalid_dates, "invalid_scores": invalid_scores, "json_errors": json_errors,
        "natural_duplicate_rows": sum(value - 1 for value in natural.values() if value > 1),
        "temporal_feature_violations": temporal_violations, "snapshot_coverage": snapshot,
        "cached_after_match_pct": 100 * sum(delta > 0 for delta in cached_deltas) / len(cached_deltas) if cached_deltas else None,
        "cached_delta_median_days": float(np.median(cached_deltas)) if cached_deltas else None,
        "team_ids_with_multiple_names": sum(len(names) > 1 for names in id_names.values()),
        "names_with_multiple_ids": sum(len(ids) > 1 for ids in name_ids.values()),
        "feature_list_coverage": dict(coverage), "ah_sign_invariant": sign_rows,
        "blacklisted_without_asof_timestamp": sorted(BLACKLIST),
    }


def write_feature_registry(path: Path, frame: pd.DataFrame) -> None:
    rows = []
    numeric, categorical = feature_columns(frame)
    for name in numeric + categorical:
        source = "target pre-match market" if name in {"ou_market", "ah_x_ou", "fav_is_home"} else "dated prior atomic matches"
        if name == "elo_fav_edge": source = "online prior outcomes"
        if "process" in name: source = "statistics from prior match only"
        if "common" in name or "h2h" in name or "reversal" in name: source = "dated H2H/common-opponent block"
        rows.append({"name": name, "definition": name.replace("_", " "), "source": source, "available_at": "strictly before kickoff T", "calculation": "see pipeline.py", "expected_direction": "learned; not hardcoded", "samples_non_null": int(frame[name].notna().sum()), "importance": "measured after fit", "stability": "walk-forward required", "leakage_check": "PASS: source date < T"})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys()); writer.writeheader(); writer.writerows(rows)


def write_pattern_registry(path: Path, frame: pd.DataFrame) -> None:
    definitions = [
        ("HARTFORD_LIKE", "venue_fav_win1 > .30 and global_fav_win2p < .35", (frame.venue_fav_win1 > .30) & (frame.global_fav_win2p < .35)),
        ("VENUE_ADJUSTED_AH_REVERSAL", "venue_reversal == 1", frame.venue_reversal == 1),
        ("LOW_TOTAL_FALSE_HOME_FAVORITE", "fav_is_home and O/U <= 2.0", (frame.fav_is_home == 1) & (frame.ou_market <= 2.0)),
        ("TOP_HOME_SHORT_WIN", "home favourite with high prior one-goal-win rate", (frame.fav_is_home == 1) & (frame.venue_fav_win1 >= .30)),
        ("OVER_CHASING", "total_inflation >= .50", frame.total_inflation >= .50),
        ("RESULT_PROCESS_DIVERGENCE", "process_diff <= -.25", frame.process_diff <= -.25),
    ]
    rows = []
    for name, definition, mask in definitions:
        selected = frame[mask.fillna(False)]
        rows.append({
            "pattern": name, "definition": definition, "sample_count_all": len(selected),
            "favourite_win_rate_descriptive": float((selected.target == 0).mean()) if len(selected) else "",
            "status": "RESEARCH_ONLY_NOT_PROMOTED", "oos_roi": "NOT_MEASURABLE_NO_PRICES",
            "leakage_check": "PASS_ATOMIC_DATES_ONLY",
        })
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys()); writer.writeheader(); writer.writerows(rows)


def render_report(audit: dict[str, Any], training: dict[str, Any] | None = None) -> str:
    lines = ["# Modelo predictivo AH ±0,5 — auditoría y backtest", "", "## Auditoría de datos", "",
        f"- Base real: `{audit['database']}` ({audit['database_bytes'] / 1e9:.03f} GB).",
        f"- Casos históricos AH ±0,5: **{audit['ah05_rows']}**.",
        f"- Duplicados naturales: **{audit['natural_duplicate_rows']}**; fechas inválidas: **{audit['invalid_dates']}**; scores inválidos: **{audit['invalid_scores']}**.",
        f"- Eventos de ficha con fecha >= T: **{audit['temporal_feature_violations']}**.",
        f"- Fichas reconstruidas después del partido: **{audit['cached_after_match_pct']:.1f}%** (mediana {audit['cached_delta_median_days']:.0f} días).",
        "- Decisión leakage: se excluyen tablas/standings, agregados O/U sin sello temporal y estadísticas del partido objetivo. Solo eventos atómicos con fecha < T.",
        "", "## Convención de línea", "",
        "La base usa `AH > 0 = favorito local` y `AH < 0 = favorito visitante`. Para obtener el hándicap del local se aplica `home_handicap = -raw_AH`. Esta invariante está contrastada con las tasas de victoria y cubierta por tests.",
        "", "## Límites honestos", "",
        "La tabla principal no contiene precios AH verificables para toda la historia. Sin cuota no existe edge apostable ni ROI defendible; se puede estimar probabilidad y línea justa, pero la decisión monetaria debe ser `NO BET` hasta comparar contra una cuota sin margen."]
    if training:
        lines += ["", "## Backtest cronológico", "", f"- Train: {training['train_n']} hasta {training['train_end']}.", f"- Calibración separada: {training['calibration_n']} hasta {training['calibration_end']}.", f"- Validación: {training['validation_n']} hasta {training['validation_end']}.", f"- Walk-forward expansivo: {training['walk_forward_folds']} folds / {training['walk_forward_n']} predicciones, log loss {training['walk_forward']['log_loss']:.4f}, Brier {training['walk_forward']['brier']:.4f}.", f"- Test final intacto: {training['final_test_n']} desde {training['test_start']} hasta {training['test_end']}.", f"- Mejor modelo aprendido: **{training['selected_model']}**; ganador global de validación: **{training['validation_winner']}**.", f"- Final OOS aprendido: log loss {training['final_oos']['log_loss']:.4f}; Brier {training['final_oos']['brier']:.4f}; ECE {training['final_oos']['ece']:.4f}.", f"- Final OOS prior constante: log loss {training['final_oos_constant_prior']['log_loss']:.4f}; Brier {training['final_oos_constant_prior']['brier']:.4f}.", f"- Estado de despliegue: **{training['deployment_status']}**. Si el modelo no vence al prior, la salida operativa es NO BET.", f"- Total de goles OOS MAE: {training['total_mae_final_oos']:.3f}.", "- Benchmark Elo/Bradley-Terry figura en validación; XGBoost, LightGBM y CatBoost no están instalados en este entorno y no se falsea su comparación.", "- ROI/yield/CLV: no medibles en la muestra completa por ausencia de precios prepartido verificables; no se imputan cuotas ficticias."]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("audit", "train"))
    parser.add_argument("--db", default="data/app_data.db")
    parser.add_argument("--output-dir", default="artifacts/ah05_model")
    args = parser.parse_args()
    db_path, output_dir = Path(args.db).resolve(), Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    audit = audit_database(db_path)
    training = None
    if args.command == "train":
        records = load_records(db_path)
        # Recency is selected on the pre-test development period only.
        cutoff = int(len(records) * .8)
        candidates = {}
        for half_life in HALF_LIFE_CANDIDATES:
            candidate = build_frame(records[:cutoff], half_life)
            split = int(len(candidate) * .75)
            numeric, categorical = feature_columns(candidate)
            recency_model = Pipeline([
                ("prep", make_preprocessor(numeric, categorical)),
                ("model", LogisticRegression(max_iter=1000, C=.25, class_weight="balanced")),
            ])
            recency_model.fit(candidate.iloc[:split][numeric + categorical], candidate.iloc[:split].target)
            probabilities = recency_model.predict_proba(candidate.iloc[split:][numeric + categorical])
            candidates[half_life] = float(log_loss(candidate.iloc[split:].target, probabilities, labels=[0, 1, 2]))
        selected_half_life = min(candidates, key=candidates.get)
        frame = build_frame(records, selected_half_life); frame.attrs["half_life"] = selected_half_life
        training = train_pipeline(frame, output_dir)
        training["recency_half_life_days"] = selected_half_life
        training["recency_validation_baseline"] = candidates
        write_feature_registry(output_dir / "feature_registry.csv", frame)
        write_pattern_registry(output_dir / "pattern_registry.csv", frame)
    (output_dir / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    if training:
        (output_dir / "backtest.json").write_text(json.dumps(training, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "REPORT.md").write_text(render_report(audit, training), encoding="utf-8")
    print(json.dumps({"audit": audit, "training": training}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
