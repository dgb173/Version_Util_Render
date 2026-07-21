import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Tuple


STAT_KEYS = {
    "shots": ("tiros",),
    "sot": ("tiros a puerta", "tiros a puerta "),
    "attacks": ("ataques",),
    "danger": ("ataques peligrosos",),
}


def _clean_name(value: Any) -> str:
    text = str(value or "").lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"\b(fc|cf|sc|cd|afc|club)\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _name_matches(left: Any, right: Any) -> bool:
    a = _clean_name(left)
    b = _clean_name(right)
    if not a or not b:
        return False
    return a == b or a in b or b in a


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).replace(",", ".").strip()
    if not text or text in {"-", "?", "N/A", "n/a", "undefined"}:
        return None
    numbers = re.findall(r"[-+]?\d+(?:\.\d+)?", text)
    if not numbers:
        return None
    try:
        return float(numbers[-1])
    except ValueError:
        return None


def _parse_int(value: Any) -> Optional[int]:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _parse_score(score: Any) -> Optional[Tuple[int, int]]:
    if score is None:
        return None
    text = str(score).strip().replace("-", ":")
    if "?" in text or ":" not in text:
        return None
    parts = text.split(":")
    if len(parts) < 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return None


def _split_quarter_line(line: float) -> List[float]:
    frac = abs(line) % 0.5
    if abs(frac - 0.25) < 0.01:
        return [line - 0.25, line + 0.25]
    return [line]


def _asian_category(team_goals: int, opp_goals: int, line_for_team: float) -> str:
    results = []
    for line in _split_quarter_line(float(line_for_team)):
        value = (team_goals - opp_goals) + line
        if value > 0:
            results.append(1)
        elif value < 0:
            results.append(-1)
        else:
            results.append(0)
    avg = sum(results) / len(results)
    if avg > 0:
        return "COVER"
    if avg < 0:
        return "NO_COVER"
    return "PUSH"


def _team_side(match: Dict[str, Any], team_name: str) -> Optional[bool]:
    if _name_matches(team_name, match.get("home_team") or match.get("home_name")):
        return True
    if _name_matches(team_name, match.get("away_team") or match.get("away_name")):
        return False
    return None


def _opponent_name(match: Dict[str, Any], team_name: str) -> str:
    side = _team_side(match, team_name)
    if side is True:
        return str(match.get("away_team") or match.get("away_name") or "")
    if side is False:
        return str(match.get("home_team") or match.get("home_name") or "")
    return ""


def _team_goals(score: Tuple[int, int], team_is_home: bool) -> Tuple[int, int]:
    home_goals, away_goals = score
    if team_is_home:
        return home_goals, away_goals
    return away_goals, home_goals


def _cover_from_market_line(match: Dict[str, Any], team_name: str) -> str:
    score = _parse_score(match.get("score") or match.get("final_score"))
    ah_raw = _parse_float(match.get("handicap_line_raw") or match.get("handicap") or match.get("ah_line"))
    team_is_home = _team_side(match, team_name)
    if score is None or ah_raw is None or team_is_home is None:
        return "UNKNOWN"

    team_goals, opp_goals = _team_goals(score, team_is_home)
    if abs(ah_raw) < 0.01:
        line_for_team = 0.0
    else:
        home_is_fav = ah_raw > 0
        team_is_fav = (team_is_home and home_is_fav) or ((not team_is_home) and (not home_is_fav))
        line_for_team = -abs(ah_raw) if team_is_fav else abs(ah_raw)
    return _asian_category(team_goals, opp_goals, line_for_team)


def _cover_against_current_fav_line(historical_match: Dict[str, Any], fav_name: str, current_h: float) -> str:
    score = _parse_score(historical_match.get("score") or historical_match.get("res6") or historical_match.get("result"))
    fav_is_home = _team_side(historical_match, fav_name)
    if score is None or fav_is_home is None:
        return "UNKNOWN"
    fav_goals, dog_goals = _team_goals(score, fav_is_home)
    return _asian_category(fav_goals, dog_goals, -abs(current_h))


def _wdl(match: Dict[str, Any], team_name: str) -> str:
    score = _parse_score(match.get("score") or match.get("final_score"))
    team_is_home = _team_side(match, team_name)
    if score is None or team_is_home is None:
        return "UNKNOWN"
    team_goals, opp_goals = _team_goals(score, team_is_home)
    if team_goals > opp_goals:
        return "W"
    if team_goals < opp_goals:
        return "L"
    return "D"


def _stats_map(rows: Iterable[Dict[str, Any]]) -> Dict[str, Tuple[float, float]]:
    result = {}
    for row in rows or []:
        label = _clean_name(row.get("label"))
        try:
            home_val = float(str(row.get("home", 0)).replace("%", "").replace(",", "."))
            away_val = float(str(row.get("away", 0)).replace("%", "").replace(",", "."))
        except Exception:
            continue
        result[label] = (home_val, away_val)
    return result


def _stat_value(stats: Dict[str, Tuple[float, float]], key: str) -> Optional[Tuple[float, float]]:
    aliases = STAT_KEYS[key]
    for label, values in stats.items():
        if any(alias == label for alias in aliases):
            return values
    return None


def _team_stats(match: Dict[str, Any], team_name: str) -> Dict[str, float]:
    side = _team_side(match, team_name)
    if side is None:
        return {}
    stats = _stats_map(match.get("stats_rows") or [])
    out = {}
    for key in STAT_KEYS:
        values = _stat_value(stats, key)
        if values is None:
            continue
        home_val, away_val = values
        out[f"{key}_for"] = home_val if side else away_val
        out[f"{key}_against"] = away_val if side else home_val
    return out


def _stats_edge(match: Dict[str, Any], team_name: str, opponent_name: str) -> int:
    team = _team_stats(match, team_name)
    opp = _team_stats(match, opponent_name)
    edge = 0
    for key in ("shots", "sot", "danger"):
        tf = team.get(f"{key}_for")
        of = opp.get(f"{key}_for")
        if tf is None or of is None:
            continue
        if tf > of:
            edge += 1
        elif tf < of:
            edge -= 1
    return edge


def _favorite_inefficient(prev_match: Dict[str, Any], fav_name: str) -> bool:
    score = _parse_score(prev_match.get("score"))
    side = _team_side(prev_match, fav_name)
    stats = _team_stats(prev_match, fav_name)
    if score is None or side is None:
        return False
    fav_goals, opp_goals = _team_goals(score, side)
    shots_for = stats.get("shots_for")
    shots_against = stats.get("shots_against")
    sot_for = stats.get("sot_for")
    sot_against = stats.get("sot_against")
    danger_for = stats.get("danger_for")
    danger_against = stats.get("danger_against")

    shot_volume_wasted = (
        shots_for is not None and shots_against is not None and
        shots_for >= shots_against + 5 and fav_goals <= opp_goals
    )
    sot_problem = (
        sot_for is not None and sot_against is not None and
        sot_for <= sot_against and fav_goals <= opp_goals
    )
    danger_wasted = (
        danger_for is not None and danger_against is not None and
        danger_for >= danger_against * 1.5 and fav_goals <= opp_goals
    )
    return shot_volume_wasted or sot_problem or danger_wasted


def _rank_gap(match_data: Dict[str, Any], fav_is_home: bool) -> Optional[int]:
    home_rank = _parse_int((match_data.get("home_standings") or {}).get("ranking"))
    away_rank = _parse_int((match_data.get("away_standings") or {}).get("ranking"))
    if home_rank is None or away_rank is None:
        return None
    fav_rank = home_rank if fav_is_home else away_rank
    dog_rank = away_rank if fav_is_home else home_rank
    return dog_rank - fav_rank


def _h2h_general_match(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    h2h = match_data.get("h2h_general") or {}
    score = h2h.get("res6") or h2h.get("score")
    if not score or "?" in str(score):
        return None
    return {
        "home_team": h2h.get("h2h_gen_home") or h2h.get("home_team"),
        "away_team": h2h.get("h2h_gen_away") or h2h.get("away_team"),
        "score": score,
        "stats_rows": h2h.get("stats_rows") or [],
    }


def _col3_chain_supports_dog(col3: Dict[str, Any], fav_prev_opp: str, dog_prev_opp: str) -> Tuple[bool, str]:
    if not col3 or col3.get("status") != "found":
        return False, "sin Col3 espejo"
    score = _parse_score(
        f"{col3.get('goles_home')}:{col3.get('goles_away')}"
        if col3.get("goles_home") is not None and col3.get("goles_away") is not None
        else col3.get("score")
    )
    if score is None:
        return False, "Col3 sin marcador"

    col3_match = {
        "home_team": col3.get("h2h_home_team_name") or col3.get("home_team"),
        "away_team": col3.get("h2h_away_team_name") or col3.get("away_team"),
        "score": f"{score[0]}:{score[1]}",
        "handicap_line_raw": col3.get("handicap") or col3.get("ah_line"),
    }
    fav_opp_side = _team_side(col3_match, fav_prev_opp)
    dog_opp_side = _team_side(col3_match, dog_prev_opp)
    if fav_opp_side is None or dog_opp_side is None:
        return False, "Col3 no conecta los rivales previos"

    dog_opp_wdl = _wdl(col3_match, dog_prev_opp)
    fav_opp_cover = _cover_from_market_line(col3_match, fav_prev_opp)
    if dog_opp_wdl == "W" and fav_opp_cover in {"NO_COVER", "PUSH"}:
        return True, f"{dog_prev_opp} supera a {fav_prev_opp} en Col3 y {fav_prev_opp} no cubre"
    return False, "Col3 no confirma superioridad de la rama del dog"


def evaluate_match(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Patron Lexington: lectura universal desde el favorito.

    Detecta favorito inflado por posicion cuando:
    - el favorito actual no cubrio su previa;
    - el no favorito actual cubrio su previa;
    - el H2H directo no sostiene que el favorito cubra la linea actual;
    - el Col3 conecta rivales previos y favorece la cadena del no favorito.
    """
    odds = match_data.get("main_match_odds") or {}
    ah_raw = _parse_float(match_data.get("handicap") or odds.get("ah_linea"))
    if ah_raw is None or abs(ah_raw) < 0.5 or abs(ah_raw) > 1.75:
        return None

    fav_is_home = ah_raw > 0
    home_name = str(match_data.get("home_name") or match_data.get("home_team") or "")
    away_name = str(match_data.get("away_name") or match_data.get("away_team") or "")
    if not home_name or not away_name:
        return None

    fav_name = home_name if fav_is_home else away_name
    dog_name = away_name if fav_is_home else home_name
    fav_prev = match_data.get("last_home_match") if fav_is_home else match_data.get("last_away_match")
    dog_prev = match_data.get("last_away_match") if fav_is_home else match_data.get("last_home_match")
    if not isinstance(fav_prev, dict) or not isinstance(dog_prev, dict):
        return None

    conditions: List[str] = []
    score = 0

    rank_gap = _rank_gap(match_data, fav_is_home)
    if rank_gap is None or rank_gap < 6:
        return None
    score += 1
    conditions.append(f"Inflacion por tabla: favorito {fav_name} con gap ranking {rank_gap}")

    fav_prev_cover = _cover_from_market_line(fav_prev, fav_name)
    if fav_prev_cover != "NO_COVER":
        return None
    score += 2
    conditions.append(f"Favorito no cubrio su previa ({fav_prev.get('score')}, AH {fav_prev.get('handicap_line_raw')})")

    if _favorite_inefficient(fav_prev, fav_name):
        score += 1
        conditions.append("Favorito con volumen ofensivo esteril en la previa")

    dog_prev_cover = _cover_from_market_line(dog_prev, dog_name)
    if dog_prev_cover != "COVER":
        return None
    score += 2
    conditions.append(f"No favorito cubrio su previa ({dog_prev.get('score')}, AH {dog_prev.get('handicap_line_raw')})")

    if _wdl(dog_prev, dog_name) == "W":
        score += 1
        conditions.append("No favorito viene de ganar su previa")

    h2h_match = _h2h_general_match(match_data)
    if not h2h_match:
        return None
    h2h_fav_cover = _cover_against_current_fav_line(h2h_match, fav_name, abs(ah_raw))
    if h2h_fav_cover == "COVER":
        return None
    score += 2
    conditions.append(f"H2H directo no sostiene al favorito con AH {abs(ah_raw):.2f}")

    if _stats_edge(h2h_match, dog_name, fav_name) >= 1:
        score += 1
        conditions.append("H2H directo con ventaja estadistica del no favorito")

    fav_prev_opp = _opponent_name(fav_prev, fav_name)
    dog_prev_opp = _opponent_name(dog_prev, dog_name)
    col3_ok, col3_reason = _col3_chain_supports_dog(match_data.get("h2h_col3") or {}, fav_prev_opp, dog_prev_opp)
    if not col3_ok:
        return None
    score += 2
    conditions.append(col3_reason)

    confidence = min(0.78, 0.58 + score * 0.025)
    roi = max(0.0, confidence * 1.90 - 1.0)
    dog_side = "HOME" if not fav_is_home else "AWAY"
    dog_pick = "LOCAL" if dog_side == "HOME" else "VISITANTE"
    h_abs = abs(ah_raw)

    return {
        "name": "[Lexington] Favorito inflado / dog con cadena espejo",
        "pick": dog_pick,
        "target": dog_side,
        "type": "AH",
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": "LEXINGTON",
        "perspective": "Contra favorito inflado",
        "favorite": fav_name,
        "underdog": dog_name,
        "handicap": h_abs,
        "display_pick_label": f"{dog_name} +{h_abs:.2f}",
        "conditions_readable": conditions,
        "explanation": (
            f"Patron Lexington desde el favorito: {fav_name} no sostiene la linea "
            f"AH {h_abs:.2f}; {dog_name} queda protegido por +{h_abs:.2f}. "
            + " | ".join(conditions[:4])
        ),
    }


def scan_matches(matches: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    picks = []
    for match in matches:
        pick = evaluate_match(match)
        if pick:
            picks.append(pick)
    picks.sort(key=lambda item: (item.get("roi", 0), item.get("accuracy", 0)), reverse=True)
    return picks
