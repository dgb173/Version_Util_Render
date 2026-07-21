"""HouseMind O/U runtime and leakage-safe feature extraction.

The model is trained offline by ``scripts/housemind/train_housemind_ou.py``.
Runtime scoring only loads a compact JSON file and performs one dot product, so
it can be used from the precacheo batch endpoint without slowing page startup.
"""

from __future__ import annotations

import json
import math
import re
import threading
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_MODEL_PATH = PROJECT_ROOT / "models" / "housemind_ou_v1.json"
PROFILE = "housemind_ou_v1"

SOURCE_NAMES = (
    "prev_home",
    "prev_away",
    "h2h_stadium",
    "h2h_general",
    "h2h_col3",
    "ind_left",
    "ind_right",
)

SOURCE_NUMERIC_SUFFIXES = (
    "goal_total",
    "goal_margin",
    "ah_gap",
    "sot_total",
    "dangerous_attacks_total",
    "age_days",
)

BASE_NUMERIC_FEATURES = (
    "ah_signed",
    "ah_abs",
    "ou_line",
    "ah_ou_ratio",
    "home_rank",
    "away_rank",
    "rank_gap",
    "rank_abs_gap",
    "context_count",
    "stats_context_count",
    "context_over_balance",
    "context_goal_avg",
    "context_goal_std",
    "context_margin_avg",
    "context_sot_avg",
    "context_dangerous_attacks_avg",
    "recent_goal_avg",
    "h2h_goal_avg",
    "indirect_goal_avg",
    "context_ah_gap_avg",
    "market_delta_avg",
    "market_raise_count",
    "market_lower_count",
)

NUMERIC_FEATURES = BASE_NUMERIC_FEATURES + tuple(
    f"{source}_{suffix}"
    for source in SOURCE_NAMES
    for suffix in SOURCE_NUMERIC_SUFFIXES
)

_MODEL_CACHE_LOCK = threading.Lock()
_MODEL_CACHE: Dict[str, Any] = {
    "path": None,
    "mtime_ns": None,
    "payload": None,
}


def safe_float(value: Any) -> Optional[float]:
    """Parse decimal and split Asian lines such as ``2/2.5``."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else None

    text = str(value).strip().replace(",", ".")
    if not text or text.lower() in {"?", "n/a", "none", "null", "-"}:
        return None
    if "/" in text:
        parts = [safe_float(part) for part in text.split("/")]
        valid = [part for part in parts if part is not None]
        return sum(valid) / len(valid) if valid else None
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def parse_score(value: Any) -> Optional[Tuple[int, int]]:
    text = str(value or "").strip()
    match = re.fullmatch(r"(\d+)\s*[:\-]\s*(\d+)", text)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def parse_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        pass
    token = text.split(" ", 1)[0]
    for fmt in (
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%m-%d-%Y",
        "%d-%m-%Y",
    ):
        try:
            return datetime.strptime(token, fmt).date()
        except ValueError:
            continue
    return None


def get_match_date(match: Dict[str, Any]) -> Optional[date]:
    for key in ("match_date", "date", "start_time", "precacheo_date"):
        parsed = parse_date(match.get(key))
        if parsed is not None:
            return parsed
    return None


def get_current_lines(match: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    odds = match.get("main_match_odds")
    odds = odds if isinstance(odds, dict) else {}
    ah = safe_float(odds.get("ah_linea"))
    if ah is None:
        ah = safe_float(match.get("handicap"))
    ou = safe_float(odds.get("goals_linea"))
    if ou is None:
        ou = safe_float(match.get("goal_line") or match.get("goals_line"))
    return ah, ou


def settle_ou_score(total_goals: int, line: float, side: str = "OVER") -> float:
    """Return exact Asian settlement score in ``[-1, -.5, 0, .5, 1]``."""
    rounded = round(float(line) * 4.0) / 4.0
    floor_line = math.floor(rounded)
    fraction = round(rounded - floor_line, 2)
    if fraction == 0.25:
        split_lines = (float(floor_line), float(floor_line) + 0.5)
    elif fraction == 0.75:
        split_lines = (float(floor_line) + 0.5, float(floor_line) + 1.0)
    else:
        split_lines = (rounded,)

    values: List[float] = []
    over_side = str(side).upper() == "OVER"
    for split_line in split_lines:
        diff = float(total_goals) - split_line
        value = 1.0 if diff > 0 else -1.0 if diff < 0 else 0.0
        values.append(value if over_side else -value)
    return sum(values) / len(values)


def settlement_label(score: float) -> str:
    if score >= 0.99:
        return "W"
    if score > 0:
        return "HW"
    if score <= -0.99:
        return "L"
    if score < 0:
        return "HL"
    return "P"


def merge_match_records(*records: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge compact and rich records without replacing useful values by empties."""
    merged: Dict[str, Any] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        for key, value in record.items():
            if value is None or value == "" or value == {} or value == []:
                if key not in merged:
                    merged[key] = value
                continue
            merged[key] = value
    return merged


def _ascii_token(value: Any, max_length: int = 72) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii").upper()
    text = re.sub(r"[^A-Z0-9]+", "_", text).strip("_")
    return (text or "UNKNOWN")[:max_length]


def _line_token(value: float) -> str:
    rounded = round(float(value) * 4.0) / 4.0
    return f"{rounded:.2f}".rstrip("0").rstrip(".").replace("-", "M").replace(".", "_")


def _ah_family(value: float) -> str:
    magnitude = abs(float(value))
    if magnitude < 0.01:
        return "PICKEM"
    if magnitude <= 0.75:
        return "LOW"
    if magnitude <= 1.25:
        return "MID"
    if magnitude <= 1.75:
        return "HIGH"
    return "EXTREME"


def _ou_family(value: float) -> str:
    if value <= 2.25:
        return "LOW"
    if value <= 2.75:
        return "MID"
    if value <= 3.5:
        return "HIGH"
    return "EXTREME"


def _favorite_side(ah: float) -> str:
    # Project convention documented in PROJECT_STANDARDS.md.
    if ah > 0.01:
        return "HOME"
    if ah < -0.01:
        return "AWAY"
    return "PICKEM"


def _rank(value: Any) -> Optional[float]:
    number = safe_float(value)
    if number is None or number <= 0 or number > 500:
        return None
    return number


def _normalize_label(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return text.encode("ascii", "ignore").decode("ascii").lower().strip()


def _stats_metrics(rows: Any) -> Dict[str, Optional[float]]:
    metrics: Dict[str, Optional[float]] = {
        "shots_total": None,
        "sot_total": None,
        "attacks_total": None,
        "dangerous_attacks_total": None,
    }
    if not isinstance(rows, list):
        return metrics
    for row in rows:
        if not isinstance(row, dict):
            continue
        home = safe_float(row.get("home"))
        away = safe_float(row.get("away"))
        if home is None or away is None:
            continue
        label = _normalize_label(row.get("label"))
        total = home + away
        if "tiros a puerta" in label or "shots on target" in label or label == "sot":
            metrics["sot_total"] = total
        elif label == "tiros" or label == "shots":
            metrics["shots_total"] = total
        elif "ataques peligrosos" in label or "dangerous attacks" in label:
            metrics["dangerous_attacks_total"] = total
        elif label == "ataques" or label == "attacks":
            metrics["attacks_total"] = total
    return metrics


def _extract_numbers(value: Any) -> List[float]:
    numbers = re.findall(r"[+\-]?\d+(?:[\.,]\d+)?", str(value or ""))
    parsed = [safe_float(number) for number in numbers]
    return [number for number in parsed if number is not None]


def _movement(match: Dict[str, Any], key: str, current_ah: float) -> Optional[Dict[str, float]]:
    market = match.get("market_analysis_data")
    market = market if isinstance(market, dict) else {}
    node = market.get(key)
    node = node if isinstance(node, dict) else {}
    numbers = _extract_numbers(node.get("movement"))
    if not numbers:
        return None
    start = numbers[0]
    end = numbers[-1] if len(numbers) > 1 else current_ah
    return {
        "start": start,
        "end": end,
        "delta": abs(end) - abs(start),
        "role_flip": 1.0 if start * end < 0 else 0.0,
    }


def _source_payload(match: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
    if source in {"prev_home", "prev_away"}:
        key = "last_home_match" if source == "prev_home" else "last_away_match"
        node = match.get(key)
        if not isinstance(node, dict):
            return None
        return {
            "score": node.get("score") or node.get("result"),
            "date": node.get("date"),
            "ah": node.get("handicap_line_raw") or node.get("ah_line") or node.get("handicap"),
            "stats_rows": node.get("stats_rows"),
            "red": (safe_float(node.get("home_red")) or 0) + (safe_float(node.get("away_red")) or 0),
        }

    if source in {"h2h_stadium", "h2h_general"}:
        key = "stadium" if source == "h2h_stadium" else "general"
        raw = match.get(source)
        raw = raw if isinstance(raw, dict) else {}
        market = match.get("market_analysis_data")
        market = market if isinstance(market, dict) else {}
        market_node = market.get(key)
        market_node = market_node if isinstance(market_node, dict) else {}
        if source == "h2h_stadium":
            score = market_node.get("result") or market_node.get("score") or raw.get("res1")
            source_date = market_node.get("date") or raw.get("date1")
            ah = raw.get("ah1")
            red = (safe_float(raw.get("home_red_stadium")) or 0) + (safe_float(raw.get("away_red_stadium")) or 0)
        else:
            score = market_node.get("result") or market_node.get("score") or raw.get("res6") or raw.get("res1")
            source_date = market_node.get("date") or raw.get("date6") or raw.get("date1")
            ah = raw.get("ah6") if safe_float(raw.get("ah6")) is not None else raw.get("ah1")
            red = (safe_float(raw.get("home_red_gen")) or 0) + (safe_float(raw.get("away_red_gen")) or 0)
        movement_numbers = _extract_numbers(market_node.get("movement"))
        if safe_float(ah) is None and movement_numbers:
            ah = movement_numbers[0]
        return {
            "score": score,
            "date": source_date,
            "ah": ah,
            "stats_rows": raw.get("stats_rows"),
            "red": red,
        }

    if source == "h2h_col3":
        raw = match.get("h2h_col3")
        raw = raw if isinstance(raw, dict) else {}
        node = raw.get("col3_data")
        node = node if isinstance(node, dict) else raw
        score = node.get("score") or node.get("result")
        if parse_score(score) is None and node.get("goles_home") is not None and node.get("goles_away") is not None:
            score = f"{node.get('goles_home')}:{node.get('goles_away')}"
        return {
            "score": score,
            "date": node.get("date"),
            "ah": node.get("handicap") or node.get("ah_line"),
            "stats_rows": raw.get("stats_rows") or node.get("stats_rows"),
            "red": (safe_float(node.get("home_red")) or 0) + (safe_float(node.get("away_red")) or 0),
        }

    if source in {"ind_left", "ind_right"}:
        comparisons = match.get("comparativas_indirectas")
        comparisons = comparisons if isinstance(comparisons, dict) else {}
        node = comparisons.get("left" if source == "ind_left" else "right")
        if not isinstance(node, dict):
            return None
        return {
            "score": node.get("score") or node.get("result"),
            "date": node.get("date"),
            "ah": node.get("ah_line") or node.get("handicap_line_raw") or node.get("handicap"),
            "stats_rows": node.get("stats_rows"),
            "red": (safe_float(node.get("home_red")) or 0) + (safe_float(node.get("away_red")) or 0),
        }

    return None


def _relation(value: float, neutral_band: float = 0.12) -> str:
    if value > neutral_band:
        return "OVER"
    if value < -neutral_band:
        return "UNDER"
    return "PUSH"


def _age_band(days: int) -> str:
    if days <= 45:
        return "FRESH"
    if days <= 180:
        return "MID"
    if days <= 730:
        return "OLD"
    return "ANCIENT"


def _tempo_band(value: Optional[float], low: float, high: float) -> str:
    if value is None:
        return "UNKNOWN"
    if value <= low:
        return "LOW"
    if value >= high:
        return "HIGH"
    return "MID"


def extract_feature_vector(match: Dict[str, Any], strict_temporal: bool = True) -> Dict[str, Any]:
    """Build pre-match features only; never reads current ``score/final_score``."""
    tokens = set()
    numeric = {name: 0.0 for name in NUMERIC_FEATURES}
    reasons: List[str] = []

    match_date = get_match_date(match)
    ah, ou = get_current_lines(match)
    if match_date is None:
        reasons.append("missing_match_date")
    if ah is None:
        reasons.append("missing_ah")
    if ou is None:
        reasons.append("missing_ou")
    if reasons:
        return {
            "tokens": [],
            "numeric": numeric,
            "quality": {
                "eligible": False,
                "reasons": reasons,
                "valid_contexts": 0,
                "stats_contexts": 0,
            },
            "meta": {"match_date": match_date.isoformat() if match_date else None, "ah": ah, "ou": ou},
        }

    assert match_date is not None and ah is not None and ou is not None
    ah_family = _ah_family(ah)
    ou_family = _ou_family(ou)
    favorite_side = _favorite_side(ah)

    numeric["ah_signed"] = ah
    numeric["ah_abs"] = abs(ah)
    numeric["ou_line"] = ou
    numeric["ah_ou_ratio"] = abs(ah) / max(ou, 0.25)

    tokens.update(
        {
            f"AH_EXACT={_line_token(ah)}",
            f"AH_FAMILY={ah_family}",
            f"OU_EXACT={_line_token(ou)}",
            f"OU_FAMILY={ou_family}",
            f"FAVORITE_SIDE={favorite_side}",
            f"LINE_PAIR={ah_family}|{ou_family}",
            f"FAV_OU_PAIR={favorite_side}|{ou_family}",
            f"MONTH={match_date.month:02d}",
        }
    )

    league = match.get("league_name") or match.get("league")
    if league:
        tokens.add(f"LEAGUE={_ascii_token(league)}")

    home_standings = match.get("home_standings")
    home_standings = home_standings if isinstance(home_standings, dict) else {}
    away_standings = match.get("away_standings")
    away_standings = away_standings if isinstance(away_standings, dict) else {}
    home_rank = _rank(home_standings.get("ranking") or match.get("home_rank"))
    away_rank = _rank(away_standings.get("ranking") or match.get("away_rank"))
    if home_rank is not None and away_rank is not None:
        numeric["home_rank"] = home_rank
        numeric["away_rank"] = away_rank
        numeric["rank_gap"] = home_rank - away_rank
        numeric["rank_abs_gap"] = abs(home_rank - away_rank)
        gap = home_rank - away_rank
        tokens.add("RANK_EDGE=HOME" if gap <= -3 else "RANK_EDGE=AWAY" if gap >= 3 else "RANK_EDGE=EVEN")
    else:
        tokens.add("RANK_EDGE=UNKNOWN")

    valid_sources: List[Dict[str, Any]] = []
    invalid_future = 0
    for source in SOURCE_NAMES:
        node = _source_payload(match, source)
        if not node:
            tokens.add(f"{source.upper()}=MISSING")
            continue
        score = parse_score(node.get("score"))
        source_date = parse_date(node.get("date"))
        if score is None:
            tokens.add(f"{source.upper()}=NO_SCORE")
            continue
        if strict_temporal and source_date is None:
            tokens.add(f"{source.upper()}=NO_DATE")
            continue
        if strict_temporal and source_date is not None and source_date >= match_date:
            tokens.add(f"{source.upper()}=NONPAST")
            invalid_future += 1
            continue

        age_days = max(0, (match_date - source_date).days) if source_date else 0
        goal_total = float(score[0] + score[1])
        goal_margin = goal_total - ou
        source_ah = safe_float(node.get("ah"))
        ah_gap = abs(source_ah) - abs(ah) if source_ah is not None else 0.0
        stats = _stats_metrics(node.get("stats_rows"))
        sot_total = stats.get("sot_total")
        dangerous_total = stats.get("dangerous_attacks_total")

        numeric[f"{source}_goal_total"] = goal_total
        numeric[f"{source}_goal_margin"] = goal_margin
        numeric[f"{source}_ah_gap"] = ah_gap
        numeric[f"{source}_sot_total"] = sot_total or 0.0
        numeric[f"{source}_dangerous_attacks_total"] = dangerous_total or 0.0
        numeric[f"{source}_age_days"] = min(float(age_days), 2000.0)

        prefix = source.upper()
        relation = _relation(goal_margin)
        tokens.update(
            {
                f"{prefix}_TOTAL={relation}",
                f"{prefix}_GOALS={'0_1' if goal_total <= 1 else '2' if goal_total == 2 else '3' if goal_total == 3 else '4_PLUS'}",
                f"{prefix}_AGE={_age_band(age_days)}",
                f"{prefix}_SOT={_tempo_band(sot_total, 5, 10)}",
                f"{prefix}_DANGER={_tempo_band(dangerous_total, 70, 140)}",
                f"{prefix}_RED={'YES' if (node.get('red') or 0) > 0 else 'NO'}",
            }
        )
        if source_ah is None:
            tokens.add(f"{prefix}_AH=UNKNOWN")
        elif ah_gap >= 0.5:
            tokens.add(f"{prefix}_AH=HIST_HIGHER")
        elif ah_gap <= -0.5:
            tokens.add(f"{prefix}_AH=CURRENT_HIGHER")
        else:
            tokens.add(f"{prefix}_AH=SIMILAR")

        valid_sources.append(
            {
                "name": source,
                "goal_total": goal_total,
                "goal_margin": goal_margin,
                "ah_gap": ah_gap,
                "sot_total": sot_total,
                "dangerous_attacks_total": dangerous_total,
                "relation": relation,
            }
        )

    goals = [item["goal_total"] for item in valid_sources]
    margins = [item["goal_margin"] for item in valid_sources]
    ah_gaps = [item["ah_gap"] for item in valid_sources]
    sot_values = [item["sot_total"] for item in valid_sources if item["sot_total"] is not None]
    dangerous_values = [item["dangerous_attacks_total"] for item in valid_sources if item["dangerous_attacks_total"] is not None]
    over_count = sum(item["relation"] == "OVER" for item in valid_sources)
    under_count = sum(item["relation"] == "UNDER" for item in valid_sources)

    numeric["context_count"] = float(len(valid_sources))
    numeric["stats_context_count"] = float(len(sot_values))
    numeric["context_over_balance"] = float(over_count - under_count)
    if goals:
        goal_avg = sum(goals) / len(goals)
        numeric["context_goal_avg"] = goal_avg
        numeric["context_goal_std"] = math.sqrt(sum((value - goal_avg) ** 2 for value in goals) / len(goals))
        numeric["context_margin_avg"] = sum(margins) / len(margins)
        numeric["context_ah_gap_avg"] = sum(ah_gaps) / len(ah_gaps)
    if sot_values:
        numeric["context_sot_avg"] = sum(sot_values) / len(sot_values)
    if dangerous_values:
        numeric["context_dangerous_attacks_avg"] = sum(dangerous_values) / len(dangerous_values)

    def group_average(names: Iterable[str]) -> float:
        values = [item["goal_total"] for item in valid_sources if item["name"] in names]
        return sum(values) / len(values) if values else 0.0

    numeric["recent_goal_avg"] = group_average({"prev_home", "prev_away"})
    numeric["h2h_goal_avg"] = group_average({"h2h_stadium", "h2h_general", "h2h_col3"})
    numeric["indirect_goal_avg"] = group_average({"ind_left", "ind_right"})

    balance = over_count - under_count
    consensus = "OVER_STRONG" if balance >= 3 else "OVER_LEAN" if balance >= 1 else "UNDER_STRONG" if balance <= -3 else "UNDER_LEAN" if balance <= -1 else "MIXED"
    tokens.update(
        {
            f"CONTEXT_COUNT={'0_2' if len(valid_sources) <= 2 else '3_4' if len(valid_sources) <= 4 else '5_PLUS'}",
            f"TOTAL_CONSENSUS={consensus}",
            f"CONSENSUS_LINE={consensus}|{ou_family}",
        }
    )

    market_deltas: List[float] = []
    for key in ("stadium", "general"):
        movement = _movement(match, key, ah)
        prefix = f"MARKET_{key.upper()}"
        if movement is None:
            tokens.add(f"{prefix}=MISSING")
            continue
        delta = movement["delta"]
        market_deltas.append(delta)
        direction = "RAISE" if delta >= 0.24 else "LOWER" if delta <= -0.24 else "SAME"
        if movement["role_flip"]:
            direction = "ROLE_FLIP"
        tokens.add(f"{prefix}={direction}")
        if delta >= 0.24:
            numeric["market_raise_count"] += 1.0
        elif delta <= -0.24:
            numeric["market_lower_count"] += 1.0
    if market_deltas:
        numeric["market_delta_avg"] = sum(market_deltas) / len(market_deltas)

    if invalid_future:
        reasons.append(f"nonpast_contexts:{invalid_future}")
    if len(valid_sources) < 3:
        reasons.append("fewer_than_3_contexts")
    if len(sot_values) < 1:
        reasons.append("no_stats_context")

    eligible = len(valid_sources) >= 3 and len(sot_values) >= 1
    return {
        "tokens": sorted(tokens),
        "numeric": numeric,
        "quality": {
            "eligible": eligible,
            "reasons": reasons,
            "valid_contexts": len(valid_sources),
            "stats_contexts": len(sot_values),
            "nonpast_contexts": invalid_future,
        },
        "meta": {
            "match_date": match_date.isoformat(),
            "ah": ah,
            "ou": ou,
            "favorite_side": favorite_side,
            "league": league or "",
            "consensus": consensus,
        },
    }


def _sigmoid(value: float) -> float:
    if value >= 0:
        exp_value = math.exp(-min(value, 60.0))
        return 1.0 / (1.0 + exp_value)
    exp_value = math.exp(max(value, -60.0))
    return exp_value / (1.0 + exp_value)


def load_model(model_path: Optional[Path] = None, force_reload: bool = False) -> Optional[Dict[str, Any]]:
    path = Path(model_path or DEFAULT_MODEL_PATH).resolve()
    if not path.exists():
        return None
    try:
        mtime_ns = path.stat().st_mtime_ns
    except OSError:
        return None
    with _MODEL_CACHE_LOCK:
        if (
            not force_reload
            and _MODEL_CACHE.get("path") == str(path)
            and _MODEL_CACHE.get("mtime_ns") == mtime_ns
        ):
            return _MODEL_CACHE.get("payload")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return None
        if payload.get("profile") != PROFILE or not isinstance(payload.get("model"), dict):
            return None
        _MODEL_CACHE.update({"path": str(path), "mtime_ns": mtime_ns, "payload": payload})
        return payload


def predict_probability(
    match: Dict[str, Any],
    model_payload: Optional[Dict[str, Any]] = None,
    model_path: Optional[Path] = None,
) -> Dict[str, Any]:
    payload = model_payload or load_model(model_path)
    feature_vector = extract_feature_vector(match, strict_temporal=True)
    if payload is None:
        return {
            "action": "NO_BET",
            "reason": "model_unavailable",
            "feature_vector": feature_vector,
        }

    model = payload.get("model") or {}
    decision = payload.get("decision") or {}
    quality = feature_vector["quality"]
    min_contexts = int(decision.get("min_contexts", 3))
    if not quality.get("eligible") or int(quality.get("valid_contexts", 0)) < min_contexts:
        return {
            "action": "NO_BET",
            "reason": "insufficient_prematch_context",
            "feature_vector": feature_vector,
        }

    numeric_names: Sequence[str] = model.get("numeric_names") or []
    token_names: Sequence[str] = model.get("token_names") or []
    means: Sequence[float] = model.get("numeric_means") or []
    scales: Sequence[float] = model.get("numeric_scales") or []
    weights: Sequence[float] = model.get("weights") or []
    expected_size = len(numeric_names) + len(token_names)
    if len(weights) != expected_size or len(means) != len(numeric_names) or len(scales) != len(numeric_names):
        return {
            "action": "NO_BET",
            "reason": "invalid_model_schema",
            "feature_vector": feature_vector,
        }

    contributions: List[Tuple[str, float]] = []
    logit = float(model.get("intercept", 0.0))
    numeric_values = feature_vector["numeric"]
    for index, name in enumerate(numeric_names):
        scale = float(scales[index]) or 1.0
        standardized = (float(numeric_values.get(name, 0.0)) - float(means[index])) / scale
        contribution = standardized * float(weights[index])
        logit += contribution
        contributions.append((name, contribution))

    active_tokens = set(feature_vector["tokens"])
    offset = len(numeric_names)
    for index, name in enumerate(token_names):
        if name not in active_tokens:
            continue
        contribution = float(weights[offset + index])
        logit += contribution
        contributions.append((name, contribution))

    raw_probability = _sigmoid(logit)
    calibrated_logit = float(model.get("platt_a", 1.0)) * logit + float(model.get("platt_b", 0.0))
    probability_over = _sigmoid(calibrated_logit)
    threshold = float(decision.get("threshold", 0.60))
    enabled = bool(decision.get("enabled", False))

    if not enabled:
        action = "NO_BET"
        reason = "model_did_not_pass_holdout_gate"
    elif probability_over >= threshold:
        action = "OVER"
        reason = "probability_above_threshold"
    elif probability_over <= 1.0 - threshold:
        action = "UNDER"
        reason = "probability_below_inverse_threshold"
    else:
        action = "NO_BET"
        reason = "edge_below_threshold"

    side_probability = probability_over if action == "OVER" else 1.0 - probability_over if action == "UNDER" else max(probability_over, 1.0 - probability_over)
    contributions.sort(key=lambda item: abs(item[1]), reverse=True)
    return {
        "action": action,
        "reason": reason,
        "probability_over": probability_over,
        "probability_side": side_probability,
        "raw_probability_over": raw_probability,
        "threshold": threshold,
        "top_contributions": [
            {"feature": name, "value": round(value, 5)}
            for name, value in contributions[:6]
        ],
        "feature_vector": feature_vector,
        "model_version": payload.get("version"),
        "audit": payload.get("audit") or {},
    }


def evaluate_match(match: Dict[str, Any], model_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    """Return a frontend-compatible OU pick, or ``None`` when abstaining."""
    result = predict_probability(match, model_path=model_path)
    action = result.get("action")
    if action not in {"OVER", "UNDER"}:
        return None

    feature_vector = result["feature_vector"]
    line = float(feature_vector["meta"]["ou"])
    audit = result.get("audit") or {}
    holdout = audit.get("holdout_selection") or {}
    probability = float(result.get("probability_side", 0.5))
    display_line = f"{line:.2f}".rstrip("0").rstrip(".")
    top_features = [item["feature"] for item in result.get("top_contributions", [])[:3]]
    explanation = (
        f"Probabilidad calibrada {probability * 100:.1f}% (umbral {result.get('threshold', 0) * 100:.1f}%). "
        f"Contextos validos: {feature_vector['quality']['valid_contexts']}. "
        f"Holdout: {holdout.get('positive_rate', 'N/D')}% en {holdout.get('selected', 0)} selecciones."
    )
    if top_features:
        explanation += " Senales: " + ", ".join(top_features) + "."

    heldout_rate = safe_float(holdout.get("positive_rate"))
    heldout_accuracy = (heldout_rate / 100.0) if heldout_rate is not None else 0.0
    return {
        "name": "HouseMind O/U calibrado",
        "pick": f"{action} {display_line}",
        "display_pick_label": f"{action} {display_line}",
        "target": action,
        "type": "OU",
        "market": "OU",
        "algorithm": "HOUSEMIND_OU",
        "confidence": "HIGH" if probability >= float(result.get("threshold", 0.6)) + 0.04 else "MEDIUM",
        "probability": probability,
        "probability_over": float(result.get("probability_over", 0.5)),
        "threshold": float(result.get("threshold", 0.6)),
        "accuracy": heldout_accuracy,
        "n_samples": int(holdout.get("selected", 0) or 0),
        "n_test": int(holdout.get("selected", 0) or 0),
        "roi": 0.0,
        "selection_score": probability,
        "model_version": result.get("model_version"),
        "explanation": explanation,
        "top_contributions": result.get("top_contributions", []),
    }


def model_status(model_path: Optional[Path] = None) -> Dict[str, Any]:
    payload = load_model(model_path)
    if payload is None:
        return {"available": False, "enabled": False, "profile": PROFILE}
    decision = payload.get("decision") or {}
    return {
        "available": True,
        "enabled": bool(decision.get("enabled", False)),
        "profile": payload.get("profile"),
        "version": payload.get("version"),
        "generated_at": payload.get("generated_at"),
        "threshold": decision.get("threshold"),
        "audit": payload.get("audit") or {},
    }
