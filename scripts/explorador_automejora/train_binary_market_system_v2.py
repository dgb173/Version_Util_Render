#!/usr/bin/env python3
from __future__ import annotations

import argparse
import itertools
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


PROFILE = "binary_market_system_v2_quality_filtered"
TRAINING_FILES = (
    "data_ah_0.json",
    "data_ah_0.5.json",
    "data_ah_1.5.json",
    "data_ah_2_plus.json",
    "data_minus_ah_0.5.json",
    "data_minus_ah_1.5.json",
    "data_minus_ah_2_plus.json",
)


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text or text in {"-", "?", "N/A", "None", "null"} or "?" in text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_score(value: Any) -> Optional[Tuple[int, int]]:
    text = str(value or "").strip().replace(" - ", ":").replace("-", ":")
    if not text or "?" in text:
        return None
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0].strip()), int(parts[1].strip())
    except ValueError:
        return None


def parse_date(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def fmt_num(value: Any) -> str:
    num = safe_float(value)
    if num is None:
        return "NA"
    rounded = round(num, 2)
    if abs(rounded - int(rounded)) < 1e-9:
        return str(int(rounded))
    return f"{rounded:.2f}".rstrip("0").rstrip(".")


def clean_token(text: Any) -> str:
    out = str(text or "NA").upper().strip()
    for ch in " /:-.,()[]{}":
        out = out.replace(ch, "_")
    while "__" in out:
        out = out.replace("__", "_")
    return out.strip("_") or "NA"


def same_team(a: Any, b: Any) -> bool:
    left = str(a or "").strip().lower()
    right = str(b or "").strip().lower()
    return bool(left and right and (left == right or left in right or right in left))


def current_ah(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    return safe_float(odds.get("ah_linea") if odds else match.get("handicap"))


def current_ou(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    return safe_float(odds.get("goals_linea") if odds else match.get("goals_line"))


def fav_side(ah: Optional[float]) -> str:
    if ah is None or abs(float(ah)) < 0.01:
        return "PICKEM"
    return "HOME" if float(ah) > 0 else "AWAY"


def team_is_home(home: Any, away: Any, team: Any) -> Optional[bool]:
    if same_team(home, team):
        return True
    if same_team(away, team):
        return False
    return None


def margin_for(score: Any, is_home: Optional[bool]) -> Optional[int]:
    parsed = parse_score(score)
    if parsed is None or is_home is None:
        return None
    home_goals, away_goals = parsed
    return home_goals - away_goals if is_home else away_goals - home_goals


def pressure_for(home_line: Any, is_home: Optional[bool]) -> Optional[float]:
    line = safe_float(home_line)
    if line is None or is_home is None:
        return None
    return line if is_home else -line


def residual_label(residual: Optional[float]) -> str:
    if residual is None:
        return "UNKNOWN"
    if residual >= 0.25:
        return "COVER"
    if residual <= -0.25:
        return "FAIL"
    return "PUSH"


def ou_label(residual: Optional[float]) -> str:
    if residual is None:
        return "UNKNOWN"
    if residual >= 0.25:
        return "OVER"
    if residual <= -0.25:
        return "UNDER"
    return "PUSH"


def extract_ou_line(block: Any) -> Optional[float]:
    if not isinstance(block, dict):
        return None
    return safe_float(
        block.get("goals_linea")
        or block.get("goals_line")
        or block.get("goal_line")
        or block.get("ou_line")
        or block.get("over_under_line")
        or block.get("total_line")
        or block.get("total_goals_line")
    )


def ah_family(ah: Optional[float]) -> str:
    if ah is None:
        return "AH_UNKNOWN"
    mag = abs(float(ah))
    if mag < 0.01:
        return "AH_0"
    if mag <= 0.25:
        return "AH_0_25"
    if mag <= 0.75:
        return "AH_0_5_0_75"
    if mag <= 1.25:
        return "AH_1_1_25"
    if mag <= 1.75:
        return "AH_1_5_1_75"
    return "AH_2_PLUS"


def ah_super_family(ah: Optional[float]) -> str:
    if ah is None:
        return "AHS_UNKNOWN"
    mag = abs(float(ah))
    if mag < 0.01:
        return "AHS_0"
    if mag <= 0.75:
        return "AHS_LOW"
    if mag <= 1.25:
        return "AHS_MID"
    if mag <= 1.75:
        return "AHS_HIGH"
    return "AHS_EXTREME"


def ou_family(ou: Optional[float]) -> str:
    if ou is None:
        return "OU_UNKNOWN"
    if ou <= 2.25:
        return "OU_LOW"
    if ou <= 2.75:
        return "OU_MID"
    if ou <= 3.5:
        return "OU_HIGH"
    return "OU_EXTREME"


def band_margin(value: Optional[float]) -> str:
    if value is None:
        return "M_UNKNOWN"
    if value <= -3:
        return "M_LE_NEG3"
    if value == -2:
        return "M_NEG2"
    if value == -1:
        return "M_NEG1"
    if value == 0:
        return "M_DRAW"
    if value == 1:
        return "M_POS1"
    if value == 2:
        return "M_POS2"
    return "M_GE_POS3"


def pressure_band(value: Optional[float]) -> str:
    if value is None:
        return "P_UNKNOWN"
    mag = abs(float(value))
    if mag < 0.01:
        return "P_0"
    if mag <= 0.25:
        return "P_0_25"
    if mag <= 0.75:
        return "P_0_5_0_75"
    if mag <= 1.25:
        return "P_1_1_25"
    if mag <= 1.75:
        return "P_1_5_1_75"
    return "P_2_PLUS"


def pressure_change(then_pressure: Optional[float], now_pressure: Optional[float]) -> str:
    if then_pressure is None or now_pressure is None:
        return "PRESSURE_UNKNOWN"
    if now_pressure > 0 and then_pressure > 0:
        delta = now_pressure - then_pressure
        if delta >= 1.0:
            return "PRESSURE_RAISE_AGGRESSIVE"
        if delta >= 0.25:
            return "PRESSURE_RAISE"
        if delta <= -1.0:
            return "PRESSURE_LOWER_AGGRESSIVE"
        if delta <= -0.25:
            return "PRESSURE_LOWER"
        return "PRESSURE_SAME"
    if now_pressure > 0 and then_pressure <= 0:
        if now_pressure >= 1.5:
            return "PRESSURE_NEW_FAV_HIGH"
        return "PRESSURE_NEW_FAV"
    if now_pressure <= 0 and then_pressure > 0:
        return "PRESSURE_FAV_REMOVED"
    return "PRESSURE_NO_FAV"


def total_vs_line(score: Any, ou: Optional[float]) -> str:
    parsed = parse_score(score)
    if parsed is None or ou is None:
        return "TOTAL_UNKNOWN"
    total = parsed[0] + parsed[1]
    diff = total - float(ou)
    if diff >= 0.25:
        return "TOTAL_OVER_LINE"
    if diff <= -0.25:
        return "TOTAL_UNDER_LINE"
    return "TOTAL_PUSH_LINE"


def ou_compare(prev_ou: Optional[float], current_ou: Optional[float]) -> str:
    if prev_ou is None or current_ou is None:
        return "PREV_OU_UNKNOWN"
    diff = float(prev_ou) - float(current_ou)
    if diff >= 0.25:
        return "PREV_OU_GT_CURRENT"
    if diff <= -0.25:
        return "PREV_OU_LT_CURRENT"
    return "PREV_OU_EQ_CURRENT"


def stats_edge(stats_rows: Any, is_home: Optional[bool]) -> str:
    if not isinstance(stats_rows, list) or is_home is None:
        return "STATS_NONE"
    score = 0.0
    used = 0
    for row in stats_rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label") or "").lower()
        h = safe_float(row.get("home"))
        a = safe_float(row.get("away"))
        if h is None or a is None:
            continue
        team = h if is_home else a
        opp = a if is_home else h
        diff = team - opp
        used += 1
        if "tiros a puerta" in label:
            score += 0.35 * max(-1.0, min(1.0, diff / 3.0))
        elif label == "tiros" or "shots" in label:
            score += 0.18 * max(-1.0, min(1.0, diff / 7.0))
        elif "ataques peligrosos" in label:
            score += 0.28 * max(-1.0, min(1.0, diff / 20.0))
        elif label == "ataques":
            score += 0.12 * max(-1.0, min(1.0, diff / 35.0))
    if used == 0:
        return "STATS_NONE"
    if score >= 0.30:
        return "STATS_STRONG_FOR"
    if score >= 0.12:
        return "STATS_LEAN_FOR"
    if score <= -0.30:
        return "STATS_STRONG_AGAINST"
    if score <= -0.12:
        return "STATS_LEAN_AGAINST"
    return "STATS_NEUTRAL"


def age_days(match_date: Optional[datetime], block_date: Optional[datetime]) -> Optional[int]:
    if match_date is None or block_date is None:
        return None
    return abs((match_date - block_date).days)


def freshness(age: Optional[int], max_days: int) -> str:
    if age is None:
        return "DATE_UNKNOWN"
    if age <= max_days:
        return "FRESH"
    return "OBSOLETE"


def parse_movement_start(text: Any) -> Optional[float]:
    raw = str(text or "").replace("->", " ").replace("=>", " ").replace(",", ".")
    nums: List[float] = []
    token = ""
    for ch in raw:
        if ch.isdigit() or ch in ".-+":
            token += ch
        elif token:
            num = safe_float(token)
            if num is not None:
                nums.append(num)
            token = ""
    if token:
        num = safe_float(token)
        if num is not None:
            nums.append(num)
    return nums[0] if nums else None


def load_finished(project_root: Path, include_unknown: bool = False) -> Tuple[List[Dict[str, Any]], Counter]:
    names = list(TRAINING_FILES)
    if include_unknown:
        names.append("data_unknown.json")
    data_dir = project_root / "data"
    audit: Counter = Counter()
    by_id: Dict[str, Dict[str, Any]] = {}
    for name in names:
        path = data_dir / name
        if not path.exists():
            audit[f"missing_file:{name}"] += 1
            continue
        raw = json.loads(path.read_text(encoding="utf-8"))
        rows = raw if isinstance(raw, list) else raw.get("matches", []) if isinstance(raw, dict) else []
        audit[f"loaded_file:{name}"] = len(rows)
        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                audit["skip:not_dict"] += 1
                continue
            if parse_score(row.get("final_score")) is None:
                audit["skip:no_final_score"] += 1
                continue
            if current_ah(row) is None:
                audit["skip:no_ah"] += 1
                continue
            match_id = str(row.get("match_id") or f"{name}:{idx}")
            item = dict(row)
            item["_source_file"] = name
            item["_parsed_date"] = parse_date(row.get("match_date")) or datetime.min
            old = by_id.get(match_id)
            if old is None:
                by_id[match_id] = item
                continue
            old_richness = sum(int(bool(old.get(k))) for k in ("market_analysis_data", "h2h_general", "h2h_stadium", "comparativas_indirectas"))
            new_richness = sum(int(bool(item.get(k))) for k in ("market_analysis_data", "h2h_general", "h2h_stadium", "comparativas_indirectas"))
            if new_richness > old_richness:
                by_id[match_id] = item
            audit["dedupe"] += 1
    out = list(by_id.values())
    out.sort(key=lambda r: (r.get("_parsed_date") or datetime.min, str(r.get("match_id"))))
    audit["deduped_rows"] = len(out)
    return out, audit


def favorite_names(match: Dict[str, Any], ah: Optional[float]) -> Tuple[str, str]:
    home = str(match.get("home_name") or match.get("home_team") or "")
    away = str(match.get("away_name") or match.get("away_team") or "")
    side = fav_side(ah)
    if side == "HOME":
        return home, away
    if side == "AWAY":
        return away, home
    return "", ""


def h2h_case(
    match: Dict[str, Any],
    kind: str,
    favorite: str,
    ah: Optional[float],
    ou: Optional[float],
    match_date: Optional[datetime],
    max_days: int,
) -> Optional[Dict[str, Any]]:
    market = match.get("market_analysis_data") if isinstance(match.get("market_analysis_data"), dict) else {}
    node = market.get(kind) if isinstance(market.get(kind), dict) else {}
    if kind == "stadium":
        h2h = match.get("h2h_stadium") if isinstance(match.get("h2h_stadium"), dict) else {}
        home = match.get("home_name") or match.get("home_team") or ""
        away = match.get("away_name") or match.get("away_team") or ""
        hist_line = safe_float(h2h.get("ah1"))
        score = node.get("result") or h2h.get("res1")
        date_raw = node.get("date") or h2h.get("date1")
    else:
        h2h = match.get("h2h_general") if isinstance(match.get("h2h_general"), dict) else {}
        home = h2h.get("h2h_gen_home") or ""
        away = h2h.get("h2h_gen_away") or ""
        hist_line = safe_float(h2h.get("ah6"))
        if hist_line is None:
            hist_line = safe_float(h2h.get("ah1"))
        score = node.get("result") or h2h.get("res6") or h2h.get("res1")
        date_raw = node.get("date") or h2h.get("date6") or h2h.get("date1")
    if hist_line is None:
        hist_line = parse_movement_start(node.get("movement"))
    if parse_score(score) is None:
        return None
    fav_is_home = team_is_home(home, away, favorite)
    margin = margin_for(score, fav_is_home)
    now_pressure = abs(float(ah)) if ah is not None else None
    then_pressure = pressure_for(hist_line, fav_is_home)
    residual_now = margin - now_pressure if margin is not None and now_pressure is not None else None
    residual_then = margin - abs(then_pressure) if margin is not None and then_pressure is not None else None
    date = parse_date(date_raw)
    age = age_days(match_date, date)
    return {
        "kind": kind,
        "score": score,
        "date": str(date_raw or ""),
        "age": age,
        "freshness": freshness(age, max_days),
        "hist_line": hist_line,
        "then_pressure": then_pressure,
        "now_pressure": now_pressure,
        "pressure": pressure_change(then_pressure, now_pressure),
        "margin": margin,
        "margin_band": band_margin(margin),
        "cover_now": residual_label(residual_now),
        "cover_then": residual_label(residual_then),
        "residual_now": residual_now,
        "total": total_vs_line(score, ou),
        "stats": stats_edge(h2h.get("stats_rows"), fav_is_home),
    }


def recent_case(block: Any, team: str, match_date: Optional[datetime], max_days: int) -> Optional[Dict[str, Any]]:
    if not isinstance(block, dict) or parse_score(block.get("score")) is None or not team:
        return None
    is_home = team_is_home(block.get("home_team"), block.get("away_team"), team)
    line = safe_float(block.get("handicap_line_raw") or block.get("ah") or block.get("handicap"))
    margin = margin_for(block.get("score"), is_home)
    pressure = pressure_for(line, is_home)
    residual = margin - pressure if margin is not None and pressure is not None else None
    date = parse_date(block.get("date"))
    age = age_days(match_date, date)
    total_goals = sum(parse_score(block.get("score")) or (0, 0))
    ou_line = extract_ou_line(block)
    ou_residual = total_goals - ou_line if ou_line is not None else None
    return {
        "score": block.get("score"),
        "age": age,
        "freshness": freshness(age, max_days),
        "line": line,
        "pressure": pressure,
        "margin": margin,
        "margin_band": band_margin(margin),
        "cover": residual_label(residual),
        "total_goals": total_goals,
        "ou_line": ou_line,
        "ou_own": ou_label(ou_residual),
        "stats": stats_edge(block.get("stats_rows"), is_home),
    }


def indirect_case(block: Any, team: str, match_date: Optional[datetime], max_days: int) -> Optional[Dict[str, Any]]:
    if not isinstance(block, dict) or parse_score(block.get("score")) is None or not team:
        return None
    is_home = team_is_home(block.get("home_team"), block.get("away_team"), team)
    line = safe_float(block.get("ah_line") or block.get("handicap_line_raw") or block.get("ah"))
    margin = margin_for(block.get("score"), is_home)
    pressure = pressure_for(line, is_home)
    residual = margin - pressure if margin is not None and pressure is not None else None
    date = parse_date(block.get("date"))
    age = age_days(match_date, date)
    total_goals = sum(parse_score(block.get("score")) or (0, 0))
    ou_line = extract_ou_line(block)
    ou_residual = total_goals - ou_line if ou_line is not None else None
    return {
        "score": block.get("score"),
        "age": age,
        "freshness": freshness(age, max_days),
        "line": line,
        "pressure": pressure,
        "margin": margin,
        "margin_band": band_margin(margin),
        "cover": residual_label(residual),
        "total_goals": total_goals,
        "ou_line": ou_line,
        "ou_own": ou_label(ou_residual),
        "stats": stats_edge(block.get("stats_rows"), is_home),
    }


def actual_favorite_cover(match: Dict[str, Any], ah: Optional[float]) -> Optional[bool]:
    score = parse_score(match.get("final_score"))
    side = fav_side(ah)
    if score is None or ah is None or side == "PICKEM":
        return None
    margin = score[0] - score[1] if side == "HOME" else score[1] - score[0]
    diff = float(margin) - abs(float(ah))
    if diff >= 0.25:
        return True
    if diff <= -0.25:
        return False
    return None


def actual_under(match: Dict[str, Any], ou: Optional[float]) -> Optional[bool]:
    score = parse_score(match.get("final_score"))
    if score is None or ou is None:
        return None
    diff = float(score[0] + score[1]) - float(ou)
    if diff >= 0.25:
        return False
    if diff <= -0.25:
        return True
    return None


def standing_token(match: Dict[str, Any], side: str) -> str:
    def rank(node: Any) -> Optional[int]:
        if not isinstance(node, dict):
            return None
        raw = node.get("ranking") or node.get("rank")
        try:
            return int(str(raw).strip())
        except Exception:
            return None

    if side == "PICKEM":
        return "TABLE_PICKEM"
    home_rank = rank(match.get("home_standings"))
    away_rank = rank(match.get("away_standings"))
    if home_rank is None or away_rank is None:
        return "TABLE_UNKNOWN"
    fav_rank = home_rank if side == "HOME" else away_rank
    dog_rank = away_rank if side == "HOME" else home_rank
    if fav_rank < dog_rank:
        return "TABLE_FAV_BETTER"
    if fav_rank > dog_rank:
        return "TABLE_FAV_WORSE"
    return "TABLE_EQUAL"


def add_case_features(features: Set[str], prefix: str, case: Optional[Dict[str, Any]]) -> None:
    if not case:
        features.add(f"{prefix}_MISSING")
        return
    features.add(f"{prefix}_{case['freshness']}")
    features.add(f"{prefix}_COVER_{case['cover_now'] if 'cover_now' in case else case.get('cover', 'UNKNOWN')}")
    features.add(f"{prefix}_MARGIN_{case.get('margin_band', 'M_UNKNOWN')}")
    if "pressure" in case:
        features.add(f"{prefix}_{case['pressure']}")
    if "total" in case:
        features.add(f"{prefix}_{case['total']}")
    features.add(f"{prefix}_{case.get('stats', 'STATS_NONE')}")


def build_training_row(match: Dict[str, Any], args: argparse.Namespace) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    reasons: List[str] = []
    score = parse_score(match.get("final_score"))
    ah = current_ah(match)
    ou = current_ou(match)
    if score is None:
        return None, ["no_final_score"]
    if ah is None:
        return None, ["no_current_ah"]

    match_date = match.get("_parsed_date") if isinstance(match.get("_parsed_date"), datetime) else parse_date(match.get("match_date"))
    side = fav_side(ah)
    favorite, dog = favorite_names(match, ah)
    current_pressure = abs(float(ah))

    stadium = h2h_case(match, "stadium", favorite, ah, ou, match_date, int(args.max_h2h_days)) if favorite else None
    general = h2h_case(match, "general", favorite, ah, ou, match_date, int(args.max_h2h_days)) if favorite else None
    base = stadium if stadium and stadium["freshness"] == "FRESH" else general if general and general["freshness"] == "FRESH" else stadium or general
    base_prefix = "BASE_STADIUM" if base is stadium else "BASE_GENERAL" if base is general else "BASE_NONE"

    fav_recent = None
    dog_recent = None
    if side == "HOME":
        fav_recent = recent_case(match.get("last_home_match"), favorite, match_date, int(args.max_recent_days))
        dog_recent = recent_case(match.get("last_away_match"), dog, match_date, int(args.max_recent_days))
    elif side == "AWAY":
        fav_recent = recent_case(match.get("last_away_match"), favorite, match_date, int(args.max_recent_days))
        dog_recent = recent_case(match.get("last_home_match"), dog, match_date, int(args.max_recent_days))

    ind = match.get("comparativas_indirectas") if isinstance(match.get("comparativas_indirectas"), dict) else {}
    ind_left = ind.get("left")
    ind_right = ind.get("right")
    if side == "HOME":
        fav_indirect = indirect_case(ind_left, favorite, match_date, int(args.max_indirect_days))
        dog_indirect = indirect_case(ind_right, dog, match_date, int(args.max_indirect_days))
    elif side == "AWAY":
        fav_indirect = indirect_case(ind_right, favorite, match_date, int(args.max_indirect_days))
        dog_indirect = indirect_case(ind_left, dog, match_date, int(args.max_indirect_days))
    else:
        fav_indirect = dog_indirect = None

    quality = 0
    if ou is not None:
        quality += 1
    for key, case, score_add in (
        ("stadium", stadium, 3),
        ("general", general, 2),
        ("fav_recent", fav_recent, 1),
        ("dog_recent", dog_recent, 1),
        ("fav_indirect", fav_indirect, 1),
        ("dog_indirect", dog_indirect, 1),
    ):
        if case is None:
            reasons.append(f"missing_{key}")
            continue
        if case.get("freshness") != "FRESH":
            reasons.append(f"obsolete_{key}")
            continue
        quality += score_add
        if case.get("stats") and case.get("stats") != "STATS_NONE":
            quality += 1
    if base is None:
        reasons.append("no_h2h_base")
    if side == "PICKEM":
        reasons.append("pickem_no_favorite_side")

    if quality < int(args.min_quality):
        return None, reasons + [f"quality_lt_{args.min_quality}"]

    features: Set[str] = {
        f"AH_FAMILY={ah_family(ah)}",
        f"AH_SUPER={ah_super_family(ah)}",
        f"AH_EXACT={fmt_num(abs(ah))}",
        f"FAV_SIDE={side}",
        f"OU_FAMILY={ou_family(ou)}",
        f"TABLE={standing_token(match, side)}",
        f"QUALITY={'HIGH' if quality >= 9 else 'MID'}",
        f"BASE_SOURCE={base_prefix}",
    }
    if side != "PICKEM":
        features.add("HAS_FAVORITE")
    if current_pressure >= 1.5:
        features.add("AH_HIGH_OR_MORE")
    if current_pressure >= 2.0:
        features.add("AH_EXTREME_2_PLUS")
    if ou is not None and ou >= 4.0:
        features.add("OU_4_PLUS")
    if ou is not None and ou <= 2.25:
        features.add("OU_LOW_DRAW_RISK")

    add_case_features(features, "H2H_STADIUM", stadium)
    add_case_features(features, "H2H_GENERAL", general)
    if base:
        features.add(f"BASE_COVER={base['cover_now']}")
        features.add(f"BASE_PRESSURE={base['pressure']}")
        features.add(f"BASE_STATS={base['stats']}")
        features.add(f"BASE_TOTAL={base['total']}")
        features.add(f"BASE_MARGIN={base['margin_band']}")
        if base["cover_now"] == "FAIL" and base["stats"] in {"STATS_STRONG_FOR", "STATS_LEAN_FOR"} and current_pressure >= 1.5:
            features.add("H2H_VOLUME_DOES_NOT_PAY_HIGH_LINE")
        if base["pressure"] in {"PRESSURE_RAISE_AGGRESSIVE", "PRESSURE_NEW_FAV_HIGH"} and base["cover_now"] != "COVER":
            features.add("AGGRESSIVE_PRESSURE_WITHOUT_CURRENT_COVER")

    def add_recent(prefix: str, case: Optional[Dict[str, Any]]) -> None:
        if not case:
            features.add(f"{prefix}_MISSING")
            return
        features.add(f"{prefix}_{case['freshness']}")
        features.add(f"{prefix}_COVER_{case['cover']}")
        features.add(f"{prefix}_MARGIN_{case['margin_band']}")
        features.add(f"{prefix}_{case['stats']}")
        current_total_token = total_vs_line(case["score"], ou)
        features.add(f"{prefix}_{current_total_token}")
        features.add(f"{prefix}_OU_CURRENT_{current_total_token.replace('TOTAL_', '')}")
        if case.get("ou_line") is not None:
            features.add(f"{prefix}_PREV_OU_{ou_family(case['ou_line'])}")
            features.add(f"{prefix}_{ou_compare(case['ou_line'], ou)}")
            features.add(f"{prefix}_OU_OWN_{case.get('ou_own', 'UNKNOWN')}")
        else:
            features.add(f"{prefix}_PREV_OU_UNKNOWN")
            features.add(f"{prefix}_OU_OWN_UNKNOWN")
        if case["total_goals"] >= 4:
            features.add(f"{prefix}_GOALS_4_PLUS")
        if case["total_goals"] <= 2:
            features.add(f"{prefix}_GOALS_2_MINUS")

    add_recent("FAV_RECENT", fav_recent)
    add_recent("DOG_RECENT", dog_recent)

    def add_ind(prefix: str, case: Optional[Dict[str, Any]]) -> None:
        if not case:
            features.add(f"{prefix}_MISSING")
            return
        features.add(f"{prefix}_{case['freshness']}")
        features.add(f"{prefix}_COVER_{case['cover']}")
        features.add(f"{prefix}_MARGIN_{case['margin_band']}")
        features.add(f"{prefix}_{case['stats']}")
        current_total_token = total_vs_line(case["score"], ou)
        features.add(f"{prefix}_{current_total_token}")
        features.add(f"{prefix}_OU_CURRENT_{current_total_token.replace('TOTAL_', '')}")
        if case.get("ou_line") is not None:
            features.add(f"{prefix}_PREV_OU_{ou_family(case['ou_line'])}")
            features.add(f"{prefix}_{ou_compare(case['ou_line'], ou)}")
            features.add(f"{prefix}_OU_OWN_{case.get('ou_own', 'UNKNOWN')}")
        else:
            features.add(f"{prefix}_PREV_OU_UNKNOWN")
            features.add(f"{prefix}_OU_OWN_UNKNOWN")
        if case["total_goals"] >= 4:
            features.add(f"{prefix}_GOALS_4_PLUS")
        if case["total_goals"] <= 2:
            features.add(f"{prefix}_GOALS_2_MINUS")

    add_ind("IND_FAV", fav_indirect)
    add_ind("IND_DOG", dog_indirect)

    if current_pressure >= 1.5 and fav_recent and fav_recent.get("margin") is not None:
        if fav_recent["margin"] < current_pressure:
            features.add("FAV_RECENT_SHORT_OF_HIGH_AH")
    if dog_recent and dog_recent.get("margin") is not None and dog_recent["margin"] <= -3:
        features.add("DOG_RECENT_COLLAPSE_3_PLUS")
    if fav_indirect and dog_indirect:
        if current_pressure >= 1.5 and (fav_indirect.get("margin") or 0) < 1 and (dog_indirect.get("margin") or 0) <= -3:
            features.add("INFLATION_COMMON_RIVAL")
        if fav_indirect.get("margin") is not None and fav_indirect["margin"] >= current_pressure:
            features.add("IND_FAV_VALIDATES_CURRENT_AH")
        if fav_indirect.get("margin") is not None and fav_indirect["margin"] < current_pressure:
            features.add("IND_FAV_SHORT_CURRENT_AH")
    if current_pressure >= 1.5 and ou is not None and ou >= 4:
        features.add("HIGH_AH_WITH_EXTREME_OU_VARIANCE")

    fav_cover = actual_favorite_cover(match, ah)
    under = actual_under(match, ou)
    return {
        "match_id": str(match.get("match_id") or ""),
        "date": str(match.get("match_date") or ""),
        "date_sort": (match_date or datetime.min).isoformat(),
        "source_file": match.get("_source_file"),
        "home": match.get("home_name"),
        "away": match.get("away_name"),
        "league": match.get("league_name"),
        "score": match.get("final_score"),
        "ah": ah,
        "ou": ou,
        "favorite": favorite,
        "dog": dog,
        "quality": quality,
        "features": sorted(features),
        "actual_fav_cover": fav_cover,
        "actual_under": under,
        "base_cover": base.get("cover_now") if base else "NO_H2H",
        "base_pressure": base.get("pressure") if base else "NO_H2H",
        "base_stats": base.get("stats") if base else "NO_H2H",
    }, reasons


def split_rows(rows: List[Dict[str, Any]], validation_ratio: float) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if len(rows) < 2:
        return rows, []
    ratio = max(0.1, min(0.5, validation_ratio))
    cut = int(len(rows) * (1.0 - ratio))
    cut = max(1, min(len(rows) - 1, cut))
    return rows[:cut], rows[cut:]


def candidate_keys(row: Dict[str, Any], max_combo: int) -> Set[Tuple[str, ...]]:
    feats = set(row.get("features") or [])
    anchors = {f for f in feats if f.startswith(("AH_FAMILY=", "AH_SUPER=", "AH_EXACT=", "OU_FAMILY=", "BASE_COVER=", "BASE_PRESSURE=", "BASE_STATS=", "BASE_TOTAL=", "TABLE=", "QUALITY="))}
    high_value = {
        f
        for f in feats
        if any(
            f.startswith(prefix)
            for prefix in (
                "H2H_STADIUM_COVER_",
                "H2H_GENERAL_COVER_",
                "FAV_RECENT_",
                "DOG_RECENT_",
                "IND_FAV_",
                "IND_DOG_",
            )
        )
        or f
        in {
            "INFLATION_COMMON_RIVAL",
            "H2H_VOLUME_DOES_NOT_PAY_HIGH_LINE",
            "AGGRESSIVE_PRESSURE_WITHOUT_CURRENT_COVER",
            "FAV_RECENT_SHORT_OF_HIGH_AH",
            "DOG_RECENT_COLLAPSE_3_PLUS",
            "HIGH_AH_WITH_EXTREME_OU_VARIANCE",
            "AH_HIGH_OR_MORE",
            "AH_EXTREME_2_PLUS",
            "OU_LOW_DRAW_RISK",
            "OU_4_PLUS",
        }
    }
    keys: Set[Tuple[str, ...]] = {(f,) for f in feats}
    for anchor in anchors:
        for feat in high_value:
            if feat != anchor:
                keys.add(tuple(sorted((anchor, feat))))
    base_combo = [f for f in feats if f.startswith(("BASE_COVER=", "BASE_PRESSURE=", "BASE_STATS=", "BASE_TOTAL=", "AH_FAMILY=", "OU_FAMILY="))]
    for combo in itertools.combinations(sorted(set(base_combo)), min(3, max_combo)):
        keys.add(combo)
    if max_combo >= 3:
        for a in [f for f in anchors if f.startswith(("AH_FAMILY=", "AH_SUPER=", "OU_FAMILY="))]:
            for b in [f for f in feats if f.startswith(("BASE_COVER=", "BASE_PRESSURE=", "BASE_STATS=", "BASE_TOTAL="))]:
                for c in high_value:
                    if len({a, b, c}) == 3:
                        keys.add(tuple(sorted((a, b, c))))
    if max_combo >= 4:
        for combo in itertools.combinations(sorted(set(base_combo)), 4):
            keys.add(combo)
    return keys


def outcome(row: Dict[str, Any], market: str, direction: str) -> Optional[bool]:
    if market == "side":
        cover = row.get("actual_fav_cover")
        if cover is None:
            return None
        if direction == "FAVORITE":
            return cover is True
        if direction == "DOG":
            return cover is False
    if market == "goals":
        under = row.get("actual_under")
        if under is None:
            return None
        if direction == "UNDER":
            return under is True
        if direction == "OVER":
            return under is False
    return None


def rate(rows: Sequence[Dict[str, Any]], market: str, direction: str) -> Dict[str, Any]:
    vals = [outcome(row, market, direction) for row in rows]
    valid = [v for v in vals if v is not None]
    wins = sum(1 for v in valid if v is True)
    return {
        "bets": len(valid),
        "wins": wins,
        "hit_rate": round(100.0 * wins / len(valid), 2) if valid else None,
    }


def build_index(rows: Sequence[Dict[str, Any]], max_combo: int) -> Dict[Tuple[str, ...], List[Dict[str, Any]]]:
    index: Dict[Tuple[str, ...], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for key in candidate_keys(row, max_combo):
            index[key].append(row)
    return index


def mine_rules(
    train_rows: Sequence[Dict[str, Any]],
    validation_rows: Sequence[Dict[str, Any]],
    *,
    market: str,
    min_train: int,
    min_validation: int,
    min_hit: float,
    min_lift: float,
    max_gap: float,
    max_combo: int,
) -> List[Dict[str, Any]]:
    directions = ("FAVORITE", "DOG") if market == "side" else ("UNDER", "OVER")
    train_index = build_index(train_rows, max_combo)
    validation_index = build_index(validation_rows, max_combo)
    baselines = {d: rate(validation_rows, market, d)["hit_rate"] or 0.0 for d in directions}
    rules: List[Dict[str, Any]] = []
    for key, subset_train in train_index.items():
        subset_validation = validation_index.get(key, [])
        if not subset_validation:
            continue
        for direction in directions:
            tr = rate(subset_train, market, direction)
            va = rate(subset_validation, market, direction)
            if tr["bets"] < min_train or va["bets"] < min_validation:
                continue
            if tr["hit_rate"] is None or va["hit_rate"] is None:
                continue
            if tr["hit_rate"] < min_hit or va["hit_rate"] < min_hit:
                continue
            if va["hit_rate"] < baselines[direction] + min_lift:
                continue
            gap = abs(float(tr["hit_rate"]) - float(va["hit_rate"]))
            if gap > max_gap:
                continue
            rules.append(
                {
                    "key": list(key),
                    "market": market,
                    "direction": direction,
                    "train": tr,
                    "validation": va,
                    "validation_lift": round(float(va["hit_rate"]) - baselines[direction], 2),
                    "stability_gap": round(gap, 2),
                    "examples": [
                        {
                            "match_id": row["match_id"],
                            "date": row["date"],
                            "home": row["home"],
                            "away": row["away"],
                            "score": row["score"],
                            "ah": fmt_num(row["ah"]),
                            "ou": fmt_num(row["ou"]),
                        }
                        for row in subset_validation[:6]
                    ],
                }
            )
    rules.sort(
        key=lambda r: (
            -float(r["validation"]["hit_rate"]),
            -float(r["validation_lift"]),
            -int(r["validation"]["bets"]),
            float(r["stability_gap"]),
            len(r["key"]),
            r["key"],
        )
    )
    return rules


def summarize_by_feature(rows: Sequence[Dict[str, Any]], prefix: str, market: str, direction: str) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for feat in row.get("features") or []:
            if feat.startswith(prefix):
                buckets[feat].append(row)
    out = []
    for feat, subset in buckets.items():
        r = rate(subset, market, direction)
        out.append({"feature": feat, **r})
    out.sort(key=lambda x: (-int(x["bets"]), str(x["feature"])))
    return out


def html_report(payload: Dict[str, Any]) -> str:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sistema binario real v2</title>
  <style>
    body{{margin:0;background:#f5f7fb;color:#172033;font-family:Segoe UI,Arial,sans-serif;letter-spacing:0}}
    header{{position:sticky;top:0;background:#fff;border-bottom:1px solid #d8e0ec;z-index:2}}
    .wrap{{max-width:1500px;margin:0 auto;padding:18px}}
    h1{{font-size:24px;margin:0 0 4px}} h2{{font-size:17px;margin:0 0 10px}}
    .sub{{color:#667085;font-size:13px;margin:0}}
    .stats{{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px;margin-top:14px}}
    .stat,.panel{{background:#fff;border:1px solid #d8e0ec;border-radius:8px;padding:12px}}
    .stat b{{display:block;font-size:22px}} .stat span{{font-size:12px;color:#667085}}
    .grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;align-items:start}}
    table{{width:100%;border-collapse:collapse;font-size:12px}} th,td{{border-bottom:1px solid #d8e0ec;padding:7px;text-align:left;vertical-align:top}}
    th{{background:#f8fafc;color:#334155;position:sticky;top:104px}}
    .badge{{display:inline-flex;border-radius:999px;padding:3px 7px;margin:1px;font-weight:800;font-size:11px}}
    .ok{{background:#dcfce7;color:#166534}} .warn{{background:#fef3c7;color:#92400e}} .bad{{background:#fee2e2;color:#991b1b}} .info{{background:#dbeafe;color:#1d4ed8}}
    .key{{display:flex;flex-wrap:wrap;gap:3px}} .muted{{color:#667085}} .panel{{margin-top:16px}}
    @media(max-width:900px){{.grid,.stats{{grid-template-columns:1fr}} th{{position:static}}}}
  </style>
</head>
<body>
<header><div class="wrap">
  <h1>Sistema binario real v2</h1>
  <p class="sub">Entrenado con buckets terminados del Explorador. Aplica filtros de calidad antes de aprender reglas.</p>
  <div class="stats" id="stats"></div>
</div></header>
<main class="wrap">
  <section class="panel"><h2>Filtro de calidad</h2><div id="quality"></div></section>
  <section class="grid">
    <div class="panel"><h2>Reglas AH</h2><div id="side"></div></div>
    <div class="panel"><h2>Reglas Over/Under</h2><div id="goals"></div></div>
  </section>
  <section class="panel"><h2>Resumen por AH</h2><div id="byAh"></div></section>
</main>
<script>
const payload = {data};
const esc = v => String(v ?? '').replace(/[&<>"']/g, ch => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[ch]));
const stat = (label,value) => `<div class="stat"><b>${{esc(value)}}</b><span>${{esc(label)}}</span></div>`;
document.getElementById('stats').innerHTML = [
  stat('cargados', payload.summary.loaded_rows),
  stat('usables', payload.summary.usable_rows),
  stat('descartados calidad', payload.summary.quality_rejects),
  stat('train', payload.summary.train_rows),
  stat('validacion', payload.summary.validation_rows),
  stat('reglas', (payload.rules.side.length + payload.rules.goals.length))
].join('');
document.getElementById('quality').innerHTML = `<table><thead><tr><th>Motivo</th><th>Conteo</th></tr></thead><tbody>${{Object.entries(payload.quality_audit).sort((a,b)=>b[1]-a[1]).map(([k,v])=>`<tr><td>${{esc(k)}}</td><td>${{esc(v)}}</td></tr>`).join('')}}</tbody></table>`;
function renderRules(rules) {{
  if (!rules.length) return '<p class="muted">No hay reglas que pasen soporte, hit, lift y estabilidad.</p>';
  return `<table><thead><tr><th>Direccion</th><th>Validacion</th><th>Train</th><th>Lift</th><th>Clave</th><th>Ejemplos</th></tr></thead><tbody>${{rules.map(r => `<tr>
    <td><span class="badge info">${{esc(r.direction)}}</span></td>
    <td><b>${{esc(r.validation.hit_rate)}}%</b><br><span class="muted">${{esc(r.validation.wins)}}/${{esc(r.validation.bets)}}</span></td>
    <td>${{esc(r.train.hit_rate)}}%<br><span class="muted">${{esc(r.train.wins)}}/${{esc(r.train.bets)}}</span></td>
    <td><span class="badge ok">+${{esc(r.validation_lift)}}</span><br><span class="muted">gap ${{esc(r.stability_gap)}}</span></td>
    <td><div class="key">${{r.key.map(k=>`<span class="badge warn">${{esc(k)}}</span>`).join('')}}</div></td>
    <td>${{r.examples.map(e=>`${{esc(e.date)}} · ${{esc(e.home)}}-${{esc(e.away)}} ${{esc(e.score)}}`).join('<br>')}}</td>
  </tr>`).join('')}}</tbody></table>`;
}}
document.getElementById('side').innerHTML = renderRules(payload.rules.side);
document.getElementById('goals').innerHTML = renderRules(payload.rules.goals);
document.getElementById('byAh').innerHTML = `<table><thead><tr><th>AH</th><th>Rows</th><th>Fav</th><th>Dog</th><th>Under</th><th>Over</th></tr></thead><tbody>${{payload.by_ah.map(r=>`<tr><td>${{esc(r.ah_family)}}</td><td>${{esc(r.rows)}}</td><td>${{esc(r.favorite.hit_rate)}}%</td><td>${{esc(r.dog.hit_rate)}}%</td><td>${{esc(r.under.hit_rate)}}%</td><td>${{esc(r.over.hit_rate)}}%</td></tr>`).join('')}}</tbody></table>`;
</script>
</body>
</html>
"""


def markdown_report(payload: Dict[str, Any]) -> str:
    lines = [
        "# Sistema binario real v2",
        "",
        "Entrenado con partidos terminados del Explorador y filtro de calidad previo.",
        "",
        "## Resumen",
        "",
    ]
    for key, value in payload["summary"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Top reglas AH", ""])
    for rule in payload["rules"]["side"][:20]:
        lines.append(
            f"- {rule['direction']} | val {rule['validation']['wins']}/{rule['validation']['bets']} "
            f"({rule['validation']['hit_rate']}%) | train {rule['train']['wins']}/{rule['train']['bets']} "
            f"({rule['train']['hit_rate']}%) | lift +{rule['validation_lift']} | {' + '.join(rule['key'])}"
        )
    lines.extend(["", "## Top reglas Over/Under", ""])
    for rule in payload["rules"]["goals"][:20]:
        lines.append(
            f"- {rule['direction']} | val {rule['validation']['wins']}/{rule['validation']['bets']} "
            f"({rule['validation']['hit_rate']}%) | train {rule['train']['wins']}/{rule['train']['bets']} "
            f"({rule['train']['hit_rate']}%) | lift +{rule['validation_lift']} | {' + '.join(rule['key'])}"
        )
    lines.extend(["", "## Filtro de calidad", ""])
    for key, value in sorted(payload["quality_audit"].items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def by_ah(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        feature = next((f for f in row["features"] if f.startswith("AH_FAMILY=")), "AH_FAMILY=UNKNOWN")
        buckets[feature.split("=", 1)[1]].append(row)
    out = []
    for family, subset in sorted(buckets.items()):
        out.append(
            {
                "ah_family": family,
                "rows": len(subset),
                "favorite": rate(subset, "side", "FAVORITE"),
                "dog": rate(subset, "side", "DOG"),
                "under": rate(subset, "goals", "UNDER"),
                "over": rate(subset, "goals", "OVER"),
            }
        )
    return out


def train(project_root: Path, args: argparse.Namespace) -> Dict[str, Any]:
    finished, load_audit = load_finished(project_root, bool(args.include_unknown))
    rows: List[Dict[str, Any]] = []
    quality_audit: Counter = Counter(load_audit)
    quality_rejects = 0
    for match in finished:
        row, reasons = build_training_row(match, args)
        if row is None:
            quality_rejects += 1
            for reason in reasons:
                quality_audit[f"reject:{reason}"] += 1
            continue
        rows.append(row)
        for reason in reasons:
            quality_audit[f"warning:{reason}"] += 1

    train_rows, validation_rows = split_rows(rows, float(args.validation_ratio))
    side_rules = mine_rules(
        train_rows,
        validation_rows,
        market="side",
        min_train=int(args.min_train_support),
        min_validation=int(args.min_validation_support),
        min_hit=float(args.min_hit),
        min_lift=float(args.min_lift),
        max_gap=float(args.max_gap),
        max_combo=int(args.max_combo),
    )[: int(args.max_rules)]
    goal_rules = mine_rules(
        train_rows,
        validation_rows,
        market="goals",
        min_train=int(args.min_train_support),
        min_validation=int(args.min_validation_support),
        min_hit=float(args.min_hit),
        min_lift=float(args.min_lift),
        max_gap=float(args.max_gap),
        max_combo=int(args.max_combo),
    )[: int(args.max_rules)]

    return {
        "profile": PROFILE,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "config": vars(args),
        "summary": {
            "loaded_rows": len(finished),
            "usable_rows": len(rows),
            "quality_rejects": quality_rejects,
            "train_rows": len(train_rows),
            "validation_rows": len(validation_rows),
            "validation_favorite": rate(validation_rows, "side", "FAVORITE"),
            "validation_dog": rate(validation_rows, "side", "DOG"),
            "validation_under": rate(validation_rows, "goals", "UNDER"),
            "validation_over": rate(validation_rows, "goals", "OVER"),
        },
        "quality_audit": dict(quality_audit),
        "by_ah": by_ah(validation_rows),
        "feature_audit": {
            "inflation_common_rival_side_dog": summarize_by_feature(validation_rows, "INFLATION_COMMON_RIVAL", "side", "DOG"),
            "high_ah_variance_over": summarize_by_feature(validation_rows, "HIGH_AH_WITH_EXTREME_OU_VARIANCE", "goals", "OVER"),
        },
        "rules": {
            "side": side_rules,
            "goals": goal_rules,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Entrena un sistema binario real con filtro de calidad sobre datos del Explorador.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-json", default="data/sistema_binario_real_v2.json")
    parser.add_argument("--output-html", default="sistema_binario_real_v2.html")
    parser.add_argument("--output-md", default="SISTEMA_BINARIO_REAL_V2.md")
    parser.add_argument("--validation-ratio", type=float, default=0.25)
    parser.add_argument("--min-quality", type=int, default=5)
    parser.add_argument("--max-h2h-days", type=int, default=1100)
    parser.add_argument("--max-recent-days", type=int, default=260)
    parser.add_argument("--max-indirect-days", type=int, default=900)
    parser.add_argument("--min-train-support", type=int, default=45)
    parser.add_argument("--min-validation-support", type=int, default=16)
    parser.add_argument("--min-hit", type=float, default=68.0)
    parser.add_argument("--min-lift", type=float, default=5.0)
    parser.add_argument("--max-gap", type=float, default=18.0)
    parser.add_argument("--max-combo", type=int, default=3)
    parser.add_argument("--max-rules", type=int, default=80)
    parser.add_argument("--include-unknown", action="store_true")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    payload = train(project_root, args)

    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    out_html = Path(args.output_html)
    out_html.write_text(html_report(payload), encoding="utf-8")

    out_md = Path(args.output_md)
    out_md.write_text(markdown_report(payload), encoding="utf-8")

    print(f"[OK] JSON: {out_json.resolve()}")
    print(f"[OK] HTML: {out_html.resolve()}")
    print(f"[OK] MD: {out_md.resolve()}")
    print(
        "[INFO] "
        f"loaded={payload['summary']['loaded_rows']} "
        f"usable={payload['summary']['usable_rows']} "
        f"quality_rejects={payload['summary']['quality_rejects']} "
        f"train={payload['summary']['train_rows']} "
        f"validation={payload['summary']['validation_rows']} "
        f"side_rules={len(payload['rules']['side'])} "
        f"goal_rules={len(payload['rules']['goals'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
