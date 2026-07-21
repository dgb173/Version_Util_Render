"""Aprendizaje cronológico de colocación AH/O-U por liga.

La unidad de aprendizaje es el estado de una liga antes de cada partido. El
backtest avanza por jornadas: una jornada nunca puede ver resultados de esa
misma jornada ni de jornadas posteriores. El modelo es deliberadamente
descriptivo/selectivo y devuelve NO BET cuando no existe soporte suficiente.
"""

from __future__ import annotations

import json
import math
import re
import threading
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from . import sql_store


PROFILE = "league_evolution_v1"
DEFAULT_COMPANY_ID = 8
MODEL_DIR = Path(__file__).resolve().parents[2] / "data" / "league_learning"
_SCHEMA_LOCK = threading.Lock()
_SCHEMA_READY = False
_MODEL_CACHE: Dict[str, Tuple[int, Dict[str, Any]]] = {}

PATTERN_SPECS: Tuple[Tuple[str, ...], ...] = (
    ("stage", "favorite_side", "ah_family"),
    ("favorite_side", "table_relation", "ah_move"),
    ("favorite_side", "fav_venue", "dog_venue"),
    ("table_relation", "fav_trend", "ah_move"),
    ("favorite_side", "fav_trend", "dog_trend"),
    ("ah_family", "ah_move", "ou_move"),
    ("stage", "ou_family", "ou_move"),
    ("ou_family", "ou_move", "ou_placement"),
    ("favorite_side", "table_relation", "ou_placement"),
    ("stage", "favorite_side", "table_relation", "ah_move"),
    ("favorite_side", "ah_family", "fav_trend", "ah_move"),
    ("stage", "ou_family", "ou_move", "ou_placement"),
)


def _ensure_schema() -> None:
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        sql_store.ensure_bootstrap()
        with sql_store._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS league_learning_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_id TEXT NOT NULL,
                    company_id INTEGER NOT NULL,
                    train_seasons_json TEXT NOT NULL,
                    test_season TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    model_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_llr_league_created
                    ON league_learning_runs(league_id, created_at DESC);

                CREATE TABLE IF NOT EXISTS league_learning_backtest (
                    run_id INTEGER NOT NULL,
                    league_id TEXT NOT NULL,
                    season TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    round_label TEXT,
                    prediction_json TEXT NOT NULL,
                    outcome_json TEXT NOT NULL,
                    PRIMARY KEY (run_id, match_id)
                );

                CREATE TABLE IF NOT EXISTS precache_market_snapshots (
                    match_id TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    league_name TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    ah_line REAL,
                    ou_line REAL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (match_id, observed_at)
                );
                CREATE INDEX IF NOT EXISTS idx_pms_match_time
                    ON precache_market_snapshots(match_id, observed_at);
                """
            )
        _SCHEMA_READY = True


def _float(value: Any) -> Optional[float]:
    if value in (None, "", "-"):
        return None
    try:
        number = float(str(value).strip())
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> Optional[int]:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _norm(value: Any) -> str:
    return "".join(ch for ch in str(value or "").casefold() if ch.isalnum())


def _mean(values: Iterable[Optional[float]]) -> Optional[float]:
    clean = [float(value) for value in values if value is not None]
    return sum(clean) / len(clean) if clean else None


def _band(value: Optional[float], low: float, high: float, labels: Tuple[str, str, str]) -> str:
    if value is None:
        return "UNKNOWN"
    if value < low:
        return labels[0]
    if value >= high:
        return labels[2]
    return labels[1]


def _movement(delta: Optional[float]) -> str:
    if delta is None:
        return "UNKNOWN"
    if delta >= 0.24:
        return "UP"
    if delta <= -0.24:
        return "DOWN"
    return "STABLE"


def _ah_family(line: Optional[float]) -> str:
    if line is None:
        return "UNKNOWN"
    value = abs(line)
    if value < 0.01:
        return "PICKEM"
    if value <= 0.5:
        return "SMALL"
    if value <= 1.0:
        return "MEDIUM"
    if value <= 1.75:
        return "STRONG"
    return "EXTREME"


def _ou_family(line: Optional[float]) -> str:
    return _band(line, 2.75, 3.5, ("LOW", "MEDIUM", "HIGH"))


def _stage(round_value: Optional[int], played: Optional[int] = None) -> str:
    value = round_value if round_value is not None else played
    if value is None:
        return "UNKNOWN"
    if value <= 5:
        return "EARLY"
    if value <= 15:
        return "MIDDLE"
    return "LATE"


def _favorite_side(line: Optional[float]) -> str:
    if line is None or abs(line) < 0.01:
        return "PICKEM"
    return "HOME" if line > 0 else "AWAY"


def _fav_move(open_line: Optional[float], close_line: Optional[float], favorite_side: str) -> str:
    if open_line is None or close_line is None or favorite_side == "PICKEM":
        return "UNKNOWN"
    delta = close_line - open_line
    pressure = delta if favorite_side == "HOME" else -delta
    if pressure >= 0.24:
        return "TO_FAVORITE"
    if pressure <= -0.24:
        return "TO_DOG"
    return "STABLE"


def _table_relation(favorite: Dict[str, Any], dog: Dict[str, Any]) -> str:
    fav_rank, dog_rank = _int(favorite.get("rank")), _int(dog.get("rank"))
    if fav_rank is None or dog_rank is None:
        return "UNKNOWN"
    gap = dog_rank - fav_rank
    if gap >= 4:
        return "FAV_BETTER"
    if gap <= -4:
        return "FAV_WORSE"
    return "SIMILAR"


def _venue_band(node: Dict[str, Any]) -> str:
    venue = node.get("venue") if isinstance(node.get("venue"), dict) else {}
    return _band(_float(venue.get("ppg")), 1.05, 1.85, ("POOR", "AVERAGE", "STRONG"))


def _trend(values: Sequence[float]) -> str:
    recent = list(values[-3:])
    if len(recent) < 2:
        return "UNKNOWN"
    score = sum(recent) / len(recent)
    if score >= 0.35:
        return "IMPROVING"
    if score <= -0.35:
        return "DECLINING"
    return "STABLE"


def _source_line(raw: Any) -> Optional[float]:
    text = str(raw or "").strip()
    if not text:
        return None
    pieces = re.findall(r"-?\d+(?:\.\d+)?", text)
    values = [_float(piece) for piece in pieces]
    clean = [value for value in values if value is not None]
    return sum(clean) / len(clean) if clean else None


def _load_rows(league_id: str, seasons: Sequence[str], company_id: int) -> List[Dict[str, Any]]:
    _ensure_schema()
    placeholders = ",".join("?" for _ in seasons)
    params: List[Any] = [int(company_id), str(league_id), *[str(value) for value in seasons]]
    with sql_store._connect() as conn:
        raw_rows = conn.execute(
            f"""SELECT m.*,
                MAX(CASE WHEN o.market='AH' AND o.observed_at='opening' THEN o.line END) ah_open,
                MAX(CASE WHEN o.market='AH' AND o.observed_at='closing' THEN o.line END) ah_close,
                MAX(CASE WHEN o.market='OU' AND o.observed_at='opening' THEN o.line END) ou_open,
                MAX(CASE WHEN o.market='OU' AND o.observed_at='closing' THEN o.line END) ou_close
                FROM league_market_matches m
                LEFT JOIN league_market_odds o ON o.league_id=m.league_id AND o.season=m.season
                    AND o.match_id=m.match_id AND o.company_id=? AND o.phase='pre_match'
                    AND o.source_kind='summary'
                WHERE m.league_id=? AND m.season IN ({placeholders})
                GROUP BY m.season,m.match_id ORDER BY m.season,m.match_date,m.match_id""",
            params,
        ).fetchall()

    history: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    rows: List[Dict[str, Any]] = []
    for raw in raw_rows:
        item = dict(raw)
        context = json.loads(item.get("context_json") or "{}")
        source = json.loads(item.get("source_json") or "{}")
        ah_open = _float(item.get("ah_open"))
        ah_close = _float(item.get("ah_close"))
        if ah_close is None:
            ah_close = _source_line(source.get("visible_ah"))
        ou_open, ou_close = _float(item.get("ou_open")), _float(item.get("ou_close"))
        if ou_close is None:
            ou_close = _source_line(source.get("visible_ou"))
        if ou_open is None:
            ou_open = ou_close
        side = _favorite_side(ah_close)
        home = context.get("home") if isinstance(context.get("home"), dict) else {}
        away = context.get("away") if isinstance(context.get("away"), dict) else {}
        favorite, dog = (home, away) if side != "AWAY" else (away, home)
        fav_id = item.get("home_team_id") if side != "AWAY" else item.get("away_team_id")
        dog_id = item.get("away_team_id") if side != "AWAY" else item.get("home_team_id")
        fav_hist = history[(str(item["season"]), str(fav_id))]
        dog_hist = history[(str(item["season"]), str(dog_id))]
        round_number = _int(item.get("round_label"))
        goal_avg = _float(context.get("league_goal_avg_before"))
        tokens = {
            "stage": _stage(round_number),
            "favorite_side": side,
            "ah_family": _ah_family(ah_close),
            "ou_family": _ou_family(ou_close),
            "ah_move": _fav_move(ah_open, ah_close, side),
            "ou_move": _movement(ou_close - ou_open) if ou_open is not None and ou_close is not None else "UNKNOWN",
            "table_relation": _table_relation(favorite, dog),
            "fav_venue": _venue_band(favorite),
            "dog_venue": _venue_band(dog),
            "fav_trend": _trend(fav_hist),
            "dog_trend": _trend(dog_hist),
            "ou_placement": (
                "ABOVE_FORM" if ou_open is not None and goal_avg is not None and ou_open - goal_avg >= .25
                else "BELOW_FORM" if ou_open is not None and goal_avg is not None and ou_open - goal_avg <= -.25
                else "ALIGNED" if ou_open is not None and goal_avg is not None else "UNKNOWN"
            ),
        }
        home_score, away_score = _int(item.get("home_score")), _int(item.get("away_score"))
        home_residual = None
        fav_cover = over = combo = None
        if home_score is not None and away_score is not None and ah_close is not None:
            home_residual = (home_score - away_score) - ah_close
            fav_residual = home_residual if side != "AWAY" else -home_residual
            if abs(fav_residual) >= .01:
                fav_cover = fav_residual > 0
        if home_score is not None and away_score is not None and ou_close is not None:
            goal_residual = (home_score + away_score) - ou_close
            if abs(goal_residual) >= .01:
                over = goal_residual > 0
        if fav_cover is not None and over is not None:
            combo = bool(fav_cover and over)
        row = {
            "league_id": str(league_id), "season": str(item["season"]), "match_id": str(item["match_id"]),
            "round": str(item.get("round_label") or ""), "match_date": item.get("match_date"),
            "home": item.get("home_team"), "away": item.get("away_team"),
            "ah_open": ah_open, "ah_close": ah_close, "ou_open": ou_open, "ou_close": ou_close,
            "tokens": tokens, "fav_cover": fav_cover, "over": over, "combo": combo,
            "home_residual": home_residual, "home_score": home_score, "away_score": away_score,
            "context": context, "league_name": source.get("league_name") or f"Liga {league_id}",
        }
        rows.append(row)
        if home_residual is not None:
            history[(str(item["season"]), str(item.get("home_team_id")))].append(home_residual)
            history[(str(item["season"]), str(item.get("away_team_id")))].append(-home_residual)
    return rows


def _pattern_key(spec: Sequence[str], tokens: Dict[str, str]) -> str:
    return "|".join(f"{name}={tokens.get(name, 'UNKNOWN')}" for name in spec)


def _build_patterns(rows: Sequence[Dict[str, Any]], min_support: int = 10) -> Dict[str, Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for spec in PATTERN_SPECS:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[_pattern_key(spec, row["tokens"])].append(row)
        for key, sample in grouped.items():
            stats: Dict[str, Any] = {"spec": list(spec), "key": key, "matches": len(sample)}
            for label in ("fav_cover", "over", "combo"):
                usable = [row[label] for row in sample if row.get(label) is not None]
                stats[label] = {
                    "sample": len(usable),
                    "wins": sum(bool(value) for value in usable),
                    "rate": round(100 * sum(bool(value) for value in usable) / len(usable), 2) if usable else None,
                }
            if max(stats["fav_cover"]["sample"], stats["over"]["sample"]) >= min_support:
                buckets[key] = stats
    return buckets


def _baselines(rows: Sequence[Dict[str, Any]]) -> Dict[str, float]:
    out = {}
    for label in ("fav_cover", "over", "combo"):
        values = [bool(row[label]) for row in rows if row.get(label) is not None]
        out[label] = sum(values) / len(values) if values else .5
    return out


def _estimate(tokens: Dict[str, str], patterns: Dict[str, Dict[str, Any]], baselines: Dict[str, float],
              label: str, min_support: int = 10) -> Dict[str, Any]:
    matched = []
    for spec in PATTERN_SPECS:
        key = _pattern_key(spec, tokens)
        pattern = patterns.get(key)
        if not pattern:
            continue
        stat = pattern[label]
        n = int(stat.get("sample") or 0)
        if n < min_support:
            continue
        wins = int(stat.get("wins") or 0)
        prior = float(baselines.get(label, .5))
        probability = (wins + prior * 12.0) / (n + 12.0)
        weight = math.sqrt(n) * (1.0 + .18 * (len(spec) - 2))
        matched.append({"key": key, "sample": n, "rate": round(100 * probability, 2), "weight": weight})
    matched.sort(key=lambda value: (-value["weight"], -value["sample"]))
    selected = matched[:6]
    if not selected:
        return {"probability": round(100 * baselines.get(label, .5), 2), "patterns": [], "effective_sample": 0}
    probability = sum(row["rate"] * row["weight"] for row in selected) / sum(row["weight"] for row in selected)
    return {
        "probability": round(probability, 2),
        "patterns": selected,
        "effective_sample": sum(row["sample"] for row in selected),
    }


def _decision(estimate: Dict[str, Any], positive: str, negative: str) -> Dict[str, Any]:
    probability = float(estimate.get("probability") or 50.0)
    count = len(estimate.get("patterns") or [])
    sample = int(estimate.get("effective_sample") or 0)
    if count < 2 or sample < 25:
        pick = "NO BET"
    elif probability >= 58.0:
        pick = positive
    elif probability <= 42.0:
        pick = negative
    else:
        pick = "NO BET"
    distance = abs(probability - 50.0)
    confidence = "HIGH" if pick != "NO BET" and distance >= 12 and sample >= 100 else "MEDIUM" if pick != "NO BET" else "NONE"
    return {**estimate, "pick": pick, "confidence": confidence}


def _predict_tokens(tokens: Dict[str, str], train_rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    patterns = _build_patterns(train_rows)
    baselines = _baselines(train_rows)
    side = _decision(_estimate(tokens, patterns, baselines, "fav_cover"), "FAVORITE", "DOG")
    goals = _decision(_estimate(tokens, patterns, baselines, "over"), "OVER", "UNDER")
    combo = _estimate(tokens, patterns, baselines, "combo")
    return {"side": side, "goals": goals, "favorite_over_probability": combo["probability"], "baselines": baselines}


def _score_backtest(predictions: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for market, outcome_key in (("side", "fav_cover"), ("goals", "over")):
        bets = wins = 0
        by_pick: Dict[str, List[int]] = defaultdict(lambda: [0, 0])
        for row in predictions:
            node = row["prediction"][market]
            pick = node.get("pick")
            outcome = row["outcome"].get(outcome_key)
            if pick == "NO BET" or outcome is None:
                continue
            success = bool(outcome) if pick in {"FAVORITE", "OVER"} else not bool(outcome)
            bets += 1
            wins += int(success)
            by_pick[pick][0] += 1
            by_pick[pick][1] += int(success)
        result[market] = {
            "bets": bets, "wins": wins,
            "hit_rate": round(100 * wins / bets, 2) if bets else None,
            "coverage": round(100 * bets / len(predictions), 2) if predictions else 0.0,
            "by_pick": {key: {"bets": val[0], "wins": val[1], "hit_rate": round(100 * val[1] / val[0], 2)}
                        for key, val in sorted(by_pick.items())},
        }
    return result


def _team_trajectories(rows: Sequence[Dict[str, Any]], season: str) -> List[Dict[str, Any]]:
    teams: Dict[str, List[Dict[str, float]]] = defaultdict(list)
    for row in rows:
        if row["season"] != str(season) or row.get("home_residual") is None:
            continue
        ah = _float(row.get("ah_close")) or 0.0
        teams[str(row["home"])].append({"residual": float(row["home_residual"]), "valuation": ah})
        teams[str(row["away"])].append({"residual": -float(row["home_residual"]), "valuation": -ah})
    output = []
    for team, values in teams.items():
        if len(values) < 6:
            continue
        cut = max(3, len(values) // 2)
        earlier, recent = values[:cut], values[-min(5, len(values) - cut):]
        residual_delta = (_mean(x["residual"] for x in recent) or 0) - (_mean(x["residual"] for x in earlier) or 0)
        valuation_delta = (_mean(x["valuation"] for x in recent) or 0) - (_mean(x["valuation"] for x in earlier) or 0)
        if residual_delta >= .35 and valuation_delta < .20:
            status = "MEJORA_AUN_NO_PAGADA"
        elif residual_delta >= .35:
            status = "MEJORA_RECONOCIDA"
        elif residual_delta <= -.35 and valuation_delta > -.20:
            status = "POSIBLE_SOBREVALORACION"
        elif residual_delta <= -.35:
            status = "DETERIORO_RECONOCIDO"
        else:
            status = "ESTABLE"
        output.append({
            "team": team, "matches": len(values), "status": status,
            "performance_change": round(residual_delta, 3), "bookmaker_revaluation": round(valuation_delta, 3),
        })
    output.sort(key=lambda row: (-abs(row["performance_change"]), row["team"]))
    return output


def train_league(league_id: str = "381", train_seasons: Sequence[str] = ("2023", "2024"),
                 test_season: str = "2025", company_id: int = DEFAULT_COMPANY_ID) -> Dict[str, Any]:
    seasons = [*[str(value) for value in train_seasons], str(test_season)]
    rows = _load_rows(str(league_id), seasons, int(company_id))
    train_rows = [row for row in rows if row["season"] in {str(value) for value in train_seasons}]
    test_rows = [row for row in rows if row["season"] == str(test_season)]
    if not train_rows or not test_rows:
        raise RuntimeError("Faltan temporadas de entrenamiento o prueba en el Radar de Liga.")

    predictions: List[Dict[str, Any]] = []
    accumulated = list(train_rows)
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in test_rows:
        grouped[str(row.get("round") or row.get("match_date"))].append(row)
    def round_sort(value: str) -> Tuple[int, str]:
        parsed = _int(value)
        return (parsed if parsed is not None else 999, value)
    for round_label in sorted(grouped, key=round_sort):
        current = grouped[round_label]
        for row in current:
            prediction = _predict_tokens(row["tokens"], accumulated)
            predictions.append({
                "match_id": row["match_id"], "round": row["round"], "home": row["home"], "away": row["away"],
                "prediction": prediction, "outcome": {"fav_cover": row["fav_cover"], "over": row["over"], "combo": row["combo"]},
            })
        accumulated.extend(current)

    metrics = _score_backtest(predictions)
    final_patterns = _build_patterns(rows)
    league_name = next((row["league_name"] for row in rows if row.get("league_name")), f"Liga {league_id}")
    model = {
        "profile": PROFILE, "league_id": str(league_id), "league_name": league_name,
        "company_id": int(company_id), "train_seasons": [str(value) for value in train_seasons],
        "test_season": str(test_season), "trained_seasons": seasons,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "rows": len(rows), "train_rows": len(train_rows), "test_rows": len(test_rows),
        "baselines": _baselines(rows), "patterns": final_patterns, "backtest": metrics,
        "data_coverage": {
            season: {
                "matches": len([row for row in rows if row["season"] == season]),
                "ah_open_close": len([row for row in rows if row["season"] == season and row.get("ah_open") is not None]),
                "ou_open_close": len([row for row in rows if row["season"] == season and row.get("ou_open") is not None]),
            }
            for season in seasons
        },
        "trajectories": _team_trajectories(rows, str(test_season)),
        "methodology": "Walk-forward por jornadas: cada predicción usa solo temporadas anteriores y jornadas ya terminadas.",
    }
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    path = MODEL_DIR / f"league_{league_id}.json"
    path.write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    _ensure_schema()
    with sql_store._connect() as conn:
        cursor = conn.execute(
            """INSERT INTO league_learning_runs(league_id,company_id,train_seasons_json,test_season,
               profile,metrics_json,model_json,created_at) VALUES(?,?,?,?,?,?,?,?)""",
            (str(league_id), int(company_id), json.dumps(list(train_seasons)), str(test_season), PROFILE,
             json.dumps(metrics, ensure_ascii=False), json.dumps(model, ensure_ascii=False), now),
        )
        run_id = int(cursor.lastrowid)
        for row in predictions:
            conn.execute(
                """INSERT INTO league_learning_backtest(run_id,league_id,season,match_id,round_label,
                   prediction_json,outcome_json) VALUES(?,?,?,?,?,?,?)""",
                (run_id, str(league_id), str(test_season), row["match_id"], row["round"],
                 json.dumps(row["prediction"], ensure_ascii=False), json.dumps(row["outcome"], ensure_ascii=False)),
            )
    _MODEL_CACHE.pop(str(league_id), None)
    return {**model, "run_id": run_id, "model_path": str(path)}


def load_model(league_id: str = "381") -> Dict[str, Any]:
    path = MODEL_DIR / f"league_{league_id}.json"
    try:
        stamp = path.stat().st_mtime_ns
    except OSError:
        return {}
    cached = _MODEL_CACHE.get(str(league_id))
    if cached and cached[0] == stamp:
        return cached[1]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    _MODEL_CACHE[str(league_id)] = (stamp, payload)
    return payload


def get_learning_report(league_id: str = "381") -> Dict[str, Any]:
    model = load_model(str(league_id))
    if not model:
        return {"available": False, "league_id": str(league_id)}
    patterns = list((model.get("patterns") or {}).values())
    patterns.sort(
        key=lambda row: -max(abs(float((row["fav_cover"].get("rate") or 50)) - 50),
                             abs(float((row["over"].get("rate") or 50)) - 50))
    )
    return {
        "available": True, "profile": model.get("profile"), "league_id": model.get("league_id"),
        "league_name": model.get("league_name"), "generated_at": model.get("generated_at"),
        "train_seasons": model.get("train_seasons"), "test_season": model.get("test_season"),
        "rows": model.get("rows"), "backtest": model.get("backtest"),
        "data_coverage": model.get("data_coverage") or {},
        "trajectories": model.get("trajectories", [])[:20], "patterns": patterns[:20],
        "methodology": model.get("methodology"),
    }


def record_precache_snapshot(match: Dict[str, Any]) -> None:
    """Registra solo cambios reales de AH/O-U para crear historial universal."""
    if not isinstance(match, dict):
        return
    match_id = str(match.get("match_id") or match.get("id") or "").strip()
    if not match_id:
        return
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    ah = _float(odds.get("ah_linea") if odds else match.get("handicap"))
    ou = _float(odds.get("goals_linea") if odds else match.get("goals_line"))
    if ah is None and ou is None:
        return
    _ensure_schema()
    with sql_store._connect() as conn:
        previous = conn.execute(
            "SELECT ah_line,ou_line FROM precache_market_snapshots WHERE match_id=? ORDER BY observed_at DESC LIMIT 1",
            (match_id,),
        ).fetchone()
        if previous and _float(previous["ah_line"]) == ah and _float(previous["ou_line"]) == ou:
            return
        observed = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        conn.execute(
            """INSERT OR IGNORE INTO precache_market_snapshots(match_id,observed_at,league_name,home_team,away_team,
               ah_line,ou_line,payload_json) VALUES(?,?,?,?,?,?,?,?)""",
            (match_id, observed, match.get("league_name"), match.get("home_name") or match.get("home_team"),
             match.get("away_name") or match.get("away_team"), ah, ou,
             json.dumps({"source": "precacheo"}, ensure_ascii=False)),
        )


def _precache_features(match: Dict[str, Any]) -> Dict[str, str]:
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    ah = _float(odds.get("ah_linea") if odds else match.get("handicap"))
    ou = _float(odds.get("goals_linea") if odds else match.get("goals_line"))
    side = _favorite_side(ah)
    home_raw = match.get("home_standings") if isinstance(match.get("home_standings"), dict) else {}
    away_raw = match.get("away_standings") if isinstance(match.get("away_standings"), dict) else {}

    def convert(raw: Dict[str, Any]) -> Dict[str, Any]:
        played = _int(raw.get("total_pj")) or 0
        points = 3 * (_int(raw.get("total_v")) or 0) + (_int(raw.get("total_e")) or 0)
        specific = _int(raw.get("specific_pj")) or 0
        specific_points = 3 * (_int(raw.get("specific_v")) or 0) + (_int(raw.get("specific_e")) or 0)
        return {"rank": _int(raw.get("ranking")), "ppg": points / played if played else None,
                "venue": {"ppg": specific_points / specific if specific else None}}

    home, away = convert(home_raw), convert(away_raw)
    favorite, dog = (home, away) if side != "AWAY" else (away, home)
    played = max(_int(home_raw.get("total_pj")) or 0, _int(away_raw.get("total_pj")) or 0)
    with sql_store._connect() as conn:
        snapshots = conn.execute(
            "SELECT ah_line,ou_line FROM precache_market_snapshots WHERE match_id=? ORDER BY observed_at",
            (str(match.get("match_id") or match.get("id") or ""),),
        ).fetchall()
    first_ah = _float(snapshots[0]["ah_line"]) if len(snapshots) >= 2 else None
    first_ou = _float(snapshots[0]["ou_line"]) if len(snapshots) >= 2 else None
    return {
        "stage": _stage(None, played), "favorite_side": side, "ah_family": _ah_family(ah),
        "ou_family": _ou_family(ou), "ah_move": _fav_move(first_ah, ah, side),
        "ou_move": _movement(ou - first_ou) if ou is not None and first_ou is not None else "UNKNOWN",
        "table_relation": _table_relation(favorite, dog), "fav_venue": _venue_band(favorite),
        "dog_venue": _venue_band(dog), "fav_trend": "UNKNOWN", "dog_trend": "UNKNOWN",
        "ou_placement": "UNKNOWN",
    }


def predict_precache(match: Dict[str, Any]) -> Dict[str, Any]:
    league_name = str(match.get("league_name") or "")
    universal_context: Dict[str, Any] = {}
    try:
        from .clave_dicotomica import apply_key
        raw = apply_key(match)
        universal_context = {
            "ah": raw.get("ah"), "ou": raw.get("ou"),
            "prediction_tier_ah": raw.get("prediction_tier_ah"),
            "prediction_tier_ou": raw.get("prediction_tier_ou"),
            "edge_ah": raw.get("edge_AH"), "edge_ou": raw.get("edge_OU"),
            "arguments": list(raw.get("argumentos") or [])[-5:],
            "bookie_detector": raw.get("bookie_detector") or {},
        }
    except Exception:
        universal_context = {"available": False}
    candidates = []
    if MODEL_DIR.exists():
        for path in MODEL_DIR.glob("league_*.json"):
            model = load_model(path.stem.replace("league_", ""))
            if model and _norm(model.get("league_name")) == _norm(league_name):
                candidates.append(model)
    if not candidates:
        return {"available": False, "reason": "Liga todavía no entrenada", "league_name": league_name,
                "universal_context": universal_context}
    model = candidates[0]
    tokens = _precache_features(match)
    side = _decision(_estimate(tokens, model["patterns"], model["baselines"], "fav_cover"), "FAVORITE", "DOG")
    goals = _decision(_estimate(tokens, model["patterns"], model["baselines"], "over"), "OVER", "UNDER")
    return {
        "available": True, "profile": PROFILE, "league_id": model.get("league_id"),
        "league_name": model.get("league_name"), "tokens": tokens, "side": side, "goals": goals,
        "backtest": model.get("backtest"), "methodology": model.get("methodology"),
        "universal_context": universal_context,
    }


def build_learning_picks(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Publica únicamente decisiones respaldadas por el tramo fuera de muestra."""
    reading = predict_precache(match)
    if not reading.get("available"):
        return []
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    ah = _float(odds.get("ah_linea") if odds else match.get("handicap"))
    ou = _float(odds.get("goals_linea") if odds else match.get("goals_line"))
    home = str(match.get("home_name") or match.get("home_team") or "Local")
    away = str(match.get("away_name") or match.get("away_team") or "Visitante")
    backtest = reading.get("backtest") or {}
    picks: List[Dict[str, Any]] = []

    side = reading.get("side") or {}
    side_audit = backtest.get("side") or {}
    if (side.get("pick") in {"FAVORITE", "DOG"} and ah is not None
            and int(side_audit.get("bets") or 0) >= 20 and float(side_audit.get("hit_rate") or 0) >= 55.0):
        favorite_home = ah > 0
        target_home = favorite_home if side["pick"] == "FAVORITE" else not favorite_home
        team = home if target_home else away
        selected_favorite = target_home == favorite_home
        team_line = -abs(ah) if selected_favorite else abs(ah)
        probability = float(side["probability"]) if side["pick"] == "FAVORITE" else 100.0 - float(side["probability"])
        picks.append({
            "name": "Aprendizaje cronológico de liga - Handicap",
            "algorithm": "LEAGUE_EVOLUTION_V1", "type": "AH",
            "pick": "LOCAL" if target_home else "VISITA", "target": "HOME" if target_home else "AWAY",
            "match_id": match.get("match_id") or match.get("id"),
            "display_pick_label": f"{team} {team_line:+g}", "accuracy": round(probability / 100.0, 4),
            "confidence": side.get("confidence"), "prediction_tier": "LEAGUE_WALK_FORWARD",
            "audit_bets": side_audit.get("bets"), "audit_wins": side_audit.get("wins"),
            "conditions_readable": [
                f"Probabilidad contextual: {probability:.1f}%",
                f"Backtest 2025: {side_audit.get('hit_rate')}% ({side_audit.get('wins')}/{side_audit.get('bets')})",
                f"Patrones coincidentes: {len(side.get('patterns') or [])}",
            ],
            "perspective": "Evolución de liga, jornada, localía, tabla y movimiento de la casa",
            "explanation": reading.get("methodology"), "engine_version": PROFILE,
        })

    goals = reading.get("goals") or {}
    goals_audit = backtest.get("goals") or {}
    if (goals.get("pick") in {"OVER", "UNDER"} and ou is not None
            and int(goals_audit.get("bets") or 0) >= 20 and float(goals_audit.get("hit_rate") or 0) >= 55.0):
        probability = float(goals["probability"]) if goals["pick"] == "OVER" else 100.0 - float(goals["probability"])
        picks.append({
            "name": "Aprendizaje cronológico de liga - Goles",
            "algorithm": "LEAGUE_EVOLUTION_V1", "type": "OU", "pick": goals["pick"], "target": goals["pick"],
            "match_id": match.get("match_id") or match.get("id"),
            "display_pick_label": f"{goals['pick']} {ou:g}", "accuracy": round(probability / 100.0, 4),
            "confidence": goals.get("confidence"), "prediction_tier": "LEAGUE_WALK_FORWARD",
            "audit_bets": goals_audit.get("bets"), "audit_wins": goals_audit.get("wins"),
            "conditions_readable": [
                f"Probabilidad contextual: {probability:.1f}%",
                f"Backtest 2025: {goals_audit.get('hit_rate')}% ({goals_audit.get('wins')}/{goals_audit.get('bets')})",
                f"Patrones coincidentes: {len(goals.get('patterns') or [])}",
            ],
            "perspective": "Colocación O/U por jornada, localía, tabla y régimen goleador",
            "explanation": reading.get("methodology"), "engine_version": PROFILE,
        })
    return picks
