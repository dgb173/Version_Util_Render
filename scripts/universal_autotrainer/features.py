"""Extraccion prepartido para el autoentrenador universal.

Convencion del proyecto/NowGoal:
    AH positivo -> favorito local.
    AH negativo -> favorito visitante.

Todas las variables se construyen con informacion anterior al partido actual.
Los bloques historicos fechados en el futuro se descartan para evitar leakage.
"""

from __future__ import annotations

import json
import math
import re
import sqlite3
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


FEATURE_VERSION = "universal_market_v1"
MISSING = float("nan")


def safe_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip().replace(",", ".")
    if not text or text.upper() in {"-", "N/A", "NA", "NONE", "NULL", "?"} or "?" in text:
        return None
    try:
        number = float(text)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> Optional[int]:
    number = safe_float(value)
    return int(number) if number is not None else None


def parse_score(value: Any) -> Optional[Tuple[int, int]]:
    text = str(value or "").strip().replace(" - ", ":").replace("-", ":")
    if not text or "?" in text:
        return None
    match = re.fullmatch(r"\s*(\d+)\s*:\s*(\d+)\s*", text)
    return (int(match.group(1)), int(match.group(2))) if match else None


def parse_date(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.split("T", 1)[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def normalize_team(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    return "".join(ch for ch in text if ch.isalnum())


def same_team(left: Any, right: Any) -> bool:
    a, b = normalize_team(left), normalize_team(right)
    return bool(a and b and (a == b or (len(a) >= 6 and a in b) or (len(b) >= 6 and b in a)))


def _first(mapping: Dict[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None and str(value).strip() not in {"", "-"}:
            return value
    return None


def current_ah(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    return safe_float(_first(odds, ("ah_linea", "ah_line", "handicap")) or match.get("handicap"))


def current_ou(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    return safe_float(_first(odds, ("goals_linea", "goals_line", "ou_line")) or match.get("goal_line"))


def _block_line(block: Dict[str, Any]) -> Optional[float]:
    return safe_float(_first(block, ("handicap_line_raw", "ah_line", "ah", "ah1", "ah6")))


def _block_score(block: Dict[str, Any]) -> Optional[Tuple[int, int]]:
    return parse_score(_first(block, ("score", "result", "res1", "res6")))


def _block_date(block: Dict[str, Any]) -> Optional[datetime]:
    return parse_date(_first(block, ("date", "match_date", "date1", "date6")))


def _is_strictly_past(block: Dict[str, Any], match_date: Optional[datetime]) -> bool:
    if not block:
        return False
    block_date = _block_date(block)
    return match_date is None or block_date is None or block_date < match_date


def _team_role(block: Dict[str, Any], team_name: str, fallback_home: Optional[bool]) -> Optional[bool]:
    home = _first(block, ("home_team", "h2h_gen_home"))
    away = _first(block, ("away_team", "h2h_gen_away"))
    if same_team(home, team_name):
        return True
    if same_team(away, team_name):
        return False
    return fallback_home


def _stats_map(block: Dict[str, Any]) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
    output: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
    aliases = {
        "tiros": "shots",
        "shots": "shots",
        "tiros a puerta": "sot",
        "shots on target": "sot",
        "ataques": "attacks",
        "ataques peligrosos": "dangerous",
        "dangerous attacks": "dangerous",
    }
    rows = block.get("stats_rows") if isinstance(block.get("stats_rows"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label") or "").strip().lower()
        key = aliases.get(label)
        if key:
            output[key] = (safe_float(row.get("home")), safe_float(row.get("away")))
    return output


def _team_stats(block: Dict[str, Any], team_is_home: Optional[bool]) -> Dict[str, Optional[float]]:
    stats = _stats_map(block)
    result: Dict[str, Optional[float]] = {}
    if team_is_home is None:
        return result
    for key, pair in stats.items():
        team_value, opp_value = pair if team_is_home else (pair[1], pair[0])
        result[key] = team_value
        result[f"opp_{key}"] = opp_value
        result[f"d_{key}"] = (team_value - opp_value) if team_value is not None and opp_value is not None else None
        denom = (team_value or 0.0) + (opp_value or 0.0)
        result[f"share_{key}"] = team_value / denom if team_value is not None and denom > 0 else None
    return result


def _team_history_features(
    block: Dict[str, Any], team_name: str, fallback_home: Optional[bool], match_date: Optional[datetime]
) -> Dict[str, Optional[float]]:
    if not isinstance(block, dict) or not _is_strictly_past(block, match_date):
        return {}
    role = _team_role(block, team_name, fallback_home)
    score = _block_score(block)
    line = _block_line(block)
    if role is None:
        return {}
    # Fuerza positiva = el mercado consideraba mejor al equipo analizado.
    strength = line if role else (-line if line is not None else None)
    margin = None
    goals_for = None
    goals_against = None
    total = None
    if score is not None:
        goals_for, goals_against = score if role else (score[1], score[0])
        margin = goals_for - goals_against
        total = goals_for + goals_against
    residual = margin - strength if margin is not None and strength is not None else None
    stats = _team_stats(block, role)
    shots = stats.get("shots")
    return {
        "line_strength": strength,
        "margin": margin,
        "expectation_residual": residual,
        "covered": (1.0 if residual > 0.01 else (0.0 if residual < -0.01 else 0.5)) if residual is not None else None,
        "won_as_dog": 1.0 if strength is not None and strength < 0 and margin is not None and margin > 0 else 0.0,
        "failed_as_fav": 1.0 if strength is not None and strength > 0 and residual is not None and residual < 0 else 0.0,
        "goals_for": goals_for,
        "goals_against": goals_against,
        "total_goals": total,
        "conversion": goals_for / shots if goals_for is not None and shots and shots > 0 else None,
        **stats,
    }


def _add_prefixed(row: Dict[str, float], prefix: str, values: Dict[str, Optional[float]]) -> None:
    for key, value in values.items():
        row[f"{prefix}_{key}"] = float(value) if value is not None else MISSING


def _standing_features(standing: Any, prefix: str, row: Dict[str, float]) -> None:
    st = standing if isinstance(standing, dict) else {}
    for scope in ("total", "specific"):
        pj = safe_float(st.get(f"{scope}_pj"))
        wins = safe_float(st.get(f"{scope}_v"))
        draws = safe_float(st.get(f"{scope}_e"))
        losses = safe_float(st.get(f"{scope}_d"))
        gf = safe_float(st.get(f"{scope}_gf"))
        ga = safe_float(st.get(f"{scope}_gc"))
        row[f"{prefix}_{scope}_games"] = pj if pj is not None else MISSING
        for name, value in (("win_rate", wins), ("draw_rate", draws), ("loss_rate", losses), ("gf_pg", gf), ("ga_pg", ga)):
            row[f"{prefix}_{scope}_{name}"] = value / pj if value is not None and pj and pj > 0 else MISSING
        row[f"{prefix}_{scope}_nonloss_rate"] = (
            (pj - losses) / pj if losses is not None and pj and pj > 0 else MISSING
        )
    rank = safe_float(st.get("ranking"))
    row[f"{prefix}_rank"] = rank if rank is not None else MISSING


def _movement_close(movement: Any) -> Optional[float]:
    numbers = re.findall(r"[-+]?\d+(?:[\.,]\d+)?", str(movement or ""))
    return safe_float(numbers[-1]) if numbers else None


def _h2h_features(
    match: Dict[str, Any], source: str, home_name: str, away_name: str, match_date: Optional[datetime]
) -> Dict[str, Optional[float]]:
    block = match.get(f"h2h_{source}") if isinstance(match.get(f"h2h_{source}"), dict) else {}
    market = match.get("market_analysis_data") if isinstance(match.get("market_analysis_data"), dict) else {}
    market_block = market.get(source) if isinstance(market.get(source), dict) else {}
    date_value = parse_date(market_block.get("date")) or _block_date(block)
    if match_date and date_value and date_value >= match_date:
        return {}
    line = _movement_close(market_block.get("movement"))
    if line is None:
        line = safe_float(_first(block, ("ah6", "ah1")))
    past_home = block.get("h2h_gen_home") or block.get("home_team")
    orientation = 1.0
    if same_team(past_home, away_name):
        orientation = -1.0
    elif past_home and not same_team(past_home, home_name):
        orientation = 0.0
    strength_for_current_home = line * orientation if line is not None and orientation else None
    score = parse_score(market_block.get("result")) or _block_score(block)
    margin = None
    total = None
    if score is not None and orientation:
        raw_margin = score[0] - score[1]
        margin = raw_margin * orientation
        total = score[0] + score[1]
    residual = margin - strength_for_current_home if margin is not None and strength_for_current_home is not None else None
    age_days = (match_date - date_value).days if match_date and date_value else None
    return {
        "prior_home_strength": strength_for_current_home,
        "expectation_residual": residual,
        "total_goals": total,
        "age_days": age_days,
    }


def _asian_profit_from_margin(margin: int, line: float) -> float:
    """Beneficio del lado local a cuota par usando convencion AH positiva=local favorito."""
    # La apuesta local concede line goles: resultado ajustado = margen - line.
    def leg_profit(leg: float) -> float:
        adjusted = margin - leg
        return 1.0 if adjusted > 1e-9 else (-1.0 if adjusted < -1e-9 else 0.0)

    quarter = round(line * 4)
    if quarter % 2 == 0:
        return leg_profit(line)
    lower = math.floor(line * 2) / 2
    upper = math.ceil(line * 2) / 2
    return 0.5 * (leg_profit(lower) + leg_profit(upper))


def _ou_profit(total: int, line: float) -> float:
    def leg_profit(leg: float) -> float:
        return 1.0 if total > leg else (-1.0 if total < leg else 0.0)

    quarter = round(line * 4)
    if quarter % 2 == 0:
        return leg_profit(line)
    lower = math.floor(line * 2) / 2
    upper = math.ceil(line * 2) / 2
    return 0.5 * (leg_profit(lower) + leg_profit(upper))


def build_feature_row(match: Dict[str, Any], include_targets: bool = True) -> Optional[Dict[str, Any]]:
    ah = current_ah(match)
    if ah is None:
        return None
    ou = current_ou(match)
    date = parse_date(match.get("match_date") or match.get("date"))
    home_name = str(match.get("home_name") or match.get("home_team") or "")
    away_name = str(match.get("away_name") or match.get("away_team") or "")
    row: Dict[str, Any] = {
        "match_id": str(match.get("match_id") or match.get("id") or ""),
        "match_date": date.strftime("%Y-%m-%d") if date else "",
        "home_name": home_name,
        "away_name": away_name,
        "league": str(match.get("league_name") or match.get("liga") or ""),
        "feature_version": FEATURE_VERSION,
        "current_ah": ah,
        "current_ou": ou if ou is not None else MISSING,
        "ah_abs": abs(ah),
        "home_is_favorite": 1.0 if ah > 0 else 0.0,
        "away_is_favorite": 1.0 if ah < 0 else 0.0,
        "pickem": 1.0 if abs(ah) < 0.01 else 0.0,
    }
    _standing_features(match.get("home_standings"), "home", row)
    _standing_features(match.get("away_standings"), "away", row)
    for feature in ("rank", "total_win_rate", "total_draw_rate", "total_loss_rate", "specific_win_rate", "specific_draw_rate", "specific_loss_rate", "specific_nonloss_rate", "specific_gf_pg", "specific_ga_pg"):
        h, a = row.get(f"home_{feature}"), row.get(f"away_{feature}")
        row[f"gap_{feature}"] = h - a if isinstance(h, float) and isinstance(a, float) and not math.isnan(h) and not math.isnan(a) else MISSING

    prev_home = _team_history_features(match.get("last_home_match") or {}, home_name, True, date)
    prev_away = _team_history_features(match.get("last_away_match") or {}, away_name, False, date)
    _add_prefixed(row, "prev_home", prev_home)
    _add_prefixed(row, "prev_away", prev_away)

    for source in ("stadium", "general"):
        values = _h2h_features(match, source, home_name, away_name, date)
        _add_prefixed(row, f"h2h_{source}", values)
        prior = values.get("prior_home_strength")
        row[f"h2h_{source}_revaluation"] = ah - prior if prior is not None else MISSING

    indirect = match.get("comparativas_indirectas") if isinstance(match.get("comparativas_indirectas"), dict) else {}
    left = _team_history_features(indirect.get("left") or {}, home_name, None, date)
    right = _team_history_features(indirect.get("right") or {}, away_name, None, date)
    _add_prefixed(row, "ind_home", left)
    _add_prefixed(row, "ind_away", right)
    for feature in ("line_strength", "margin", "expectation_residual", "d_shots", "d_sot", "d_dangerous", "share_dangerous", "conversion", "total_goals"):
        lv, rv = left.get(feature), right.get(feature)
        row[f"ind_gap_{feature}"] = lv - rv if lv is not None and rv is not None else MISSING

    # Causas/alertas numericas que expresan los patrones conversados.
    home_specific_wr = row.get("home_specific_win_rate", MISSING)
    away_specific_nonloss = row.get("away_specific_nonloss_rate", MISSING)
    home_dom = prev_home.get("d_dangerous")
    home_shots = prev_home.get("d_shots")
    row["flag_home_result_inflation"] = 1.0 if prev_home.get("won_as_dog") == 1.0 and ((home_dom is not None and home_dom < 0) or (home_shots is not None and home_shots < 0)) else 0.0
    row["flag_hidden_resistant_away"] = 1.0 if isinstance(away_specific_nonloss, float) and not math.isnan(away_specific_nonloss) and away_specific_nonloss >= 0.65 else 0.0
    row["flag_weak_home_condition"] = 1.0 if isinstance(home_specific_wr, float) and not math.isnan(home_specific_wr) and home_specific_wr <= 0.35 else 0.0
    row["flag_common_market_home"] = 1.0 if left.get("line_strength") is not None and right.get("line_strength") is not None and left["line_strength"] - right["line_strength"] >= 0.25 else 0.0
    row["flag_common_market_away"] = 1.0 if left.get("line_strength") is not None and right.get("line_strength") is not None and left["line_strength"] - right["line_strength"] <= -0.25 else 0.0
    row["flag_ou_inflated_recent_score"] = 1.0 if ou is not None and max(prev_home.get("total_goals") or 0, prev_away.get("total_goals") or 0) >= ou + 2 else 0.0
    row["flag_local_low_production_high_ou"] = 1.0 if ou is not None and ou >= 3.0 and home_dom is not None and home_dom < 0 else 0.0

    result = parse_score(match.get("final_score") or match.get("score"))
    if include_targets and result is not None:
        hg, ag = result
        margin, total = hg - ag, hg + ag
        row["home_goals"] = hg
        row["away_goals"] = ag
        row["ah_profit_home"] = _asian_profit_from_margin(margin, ah)
        row["ou_profit_over"] = _ou_profit(total, ou) if ou is not None else MISSING
        row["total_goals_target"] = total
    return row


def load_matches_from_db(
    db_path: str | Path,
    states: Iterable[str] = ("historical",),
    require_result: bool = True,
    compact: bool = False,
) -> List[Dict[str, Any]]:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(path)
    state_values = tuple(states)
    placeholders = ",".join("?" for _ in state_values)
    sql = f"SELECT match_id, payload_json, score, match_date FROM matches WHERE state IN ({placeholders})"
    records: List[Dict[str, Any]] = []
    seen = set()
    with sqlite3.connect(str(path)) as conn:
        for match_id, payload_json, score, match_date in conn.execute(sql, state_values):
            if str(match_id) in seen:
                continue
            try:
                payload = json.loads(payload_json)
            except (TypeError, json.JSONDecodeError):
                continue
            payload.setdefault("match_id", str(match_id))
            payload.setdefault("final_score", score)
            payload.setdefault("match_date", match_date)
            if require_result and parse_score(payload.get("final_score")) is None:
                continue
            if compact:
                keep = {
                    "match_id", "match_date", "final_score", "home_name", "away_name", "league_name",
                    "main_match_odds", "home_standings", "away_standings", "last_home_match", "last_away_match",
                    "h2h_stadium", "h2h_general", "comparativas_indirectas", "market_analysis_data",
                }
                payload = {key: value for key, value in payload.items() if key in keep}
            records.append(payload)
            seen.add(str(match_id))
    return records


def feature_columns(rows: Sequence[Dict[str, Any]]) -> List[str]:
    excluded = {
        "match_id", "match_date", "home_name", "away_name", "league", "feature_version",
        "home_goals", "away_goals", "ah_profit_home", "ou_profit_over", "total_goals_target",
    }
    return sorted(
        key for key in set().union(*(row.keys() for row in rows))
        if key not in excluded and all(isinstance(row.get(key, MISSING), (int, float)) for row in rows)
    )
