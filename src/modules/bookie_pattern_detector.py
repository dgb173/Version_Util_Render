"""Detector explicable de colocacion de linea y concordancia Col3.

No emite apuestas por si solo. Convierte la linea actual, los H2H, los partidos
recientes y el rombo Col3 en confirmaciones/conflictos para una prediccion que
ya haya superado las puertas de la Clave Dicotomica.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .col3_indirect_pattern import evaluate_all as evaluate_col3


def _number(value: Any) -> Optional[float]:
    if value in (None, "", "-", "?", "N/A"):
        return None
    try:
        text = str(value).strip().replace(",", ".")
        if "/" in text:
            parts = text.split("/", 1)
            return (float(parts[0]) + float(parts[1])) / 2.0
        return float(text)
    except (TypeError, ValueError):
        return None


def _score(value: Any) -> Optional[Tuple[int, int]]:
    text = str(value or "").strip().replace("-", ":")
    if "?" in text:
        return None
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return None


def _norm(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _same(left: Any, right: Any) -> bool:
    a, b = _norm(left), _norm(right)
    return bool(a and b and (a == b or a in b or b in a))


def _side(home: Any, away: Any, team: Any) -> Optional[bool]:
    if _same(home, team):
        return True
    if _same(away, team):
        return False
    return None


def _pressure(home_line: Optional[float], team_is_home: Optional[bool]) -> Optional[float]:
    if home_line is None or team_is_home is None:
        return None
    return home_line if team_is_home else -home_line


def _margin(score: Any, team_is_home: Optional[bool]) -> Optional[float]:
    parsed = _score(score)
    if parsed is None or team_is_home is None:
        return None
    return float(parsed[0] - parsed[1] if team_is_home else parsed[1] - parsed[0])


def _movement(then: Optional[float], now: float) -> str:
    if then is None:
        return "UNKNOWN"
    if now > 0 and then <= 0:
        return "NEW_FAVORITE_STATUS"
    if now <= 0 and then > 0:
        return "FAVORITE_STATUS_REMOVED"
    delta = now - then
    if delta >= 0.25:
        return "RAISE_PRESSURE"
    if delta <= -0.25:
        return "LOWER_PRESSURE"
    return "SAME_PRESSURE"


def _rank(value: Any) -> Optional[int]:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _h2h_block(match: Dict[str, Any], kind: str, favorite: str, now_pressure: float) -> Dict[str, Any]:
    market = match.get("market_analysis_data") or {}
    market_node = market.get(kind) if isinstance(market, dict) else {}
    if not isinstance(market_node, dict):
        market_node = {}

    if kind == "stadium":
        raw = match.get("h2h_stadium") or {}
        home = match.get("home_name") or match.get("home_team") or ""
        away = match.get("away_name") or match.get("away_team") or ""
        line = _number(raw.get("ah1"))
        result = market_node.get("result") or raw.get("res1")
    else:
        raw = match.get("h2h_general") or {}
        home = raw.get("h2h_gen_home") or raw.get("home_team") or ""
        away = raw.get("h2h_gen_away") or raw.get("away_team") or ""
        line = _number(raw.get("ah6"))
        if line is None:
            line = _number(raw.get("ah1"))
        result = market_node.get("result") or raw.get("res6") or raw.get("res1")

    fav_home = _side(home, away, favorite)
    then_pressure = _pressure(line, fav_home)
    fav_margin = _margin(result, fav_home)
    residual_now = fav_margin - now_pressure if fav_margin is not None else None
    return {
        "kind": kind,
        "home": home,
        "away": away,
        "score": result,
        "pressure_then": then_pressure,
        "pressure_now": now_pressure,
        "pressure_delta": now_pressure - then_pressure if then_pressure is not None else None,
        "movement": _movement(then_pressure, now_pressure),
        "residual_current_line": residual_now,
        "movement_raw": market_node.get("movement"),
    }


def _recent_block(block: Any, team: str) -> Dict[str, Any]:
    if not isinstance(block, dict):
        return {}
    home = block.get("home_team") or block.get("home") or ""
    away = block.get("away_team") or block.get("away") or ""
    team_home = _side(home, away, team)
    line = _number(block.get("handicap_line_raw") or block.get("ahLine") or block.get("ah_line"))
    pressure = _pressure(line, team_home)
    margin = _margin(block.get("score"), team_home)
    return {
        "score": block.get("score"),
        "pressure": pressure,
        "margin": margin,
        "residual": margin - pressure if margin is not None and pressure is not None else None,
    }


def _col3_direction(match: Dict[str, Any], ah: float) -> Tuple[Optional[str], Optional[str]]:
    try:
        for pick in evaluate_col3(match):
            if pick.get("type") != "AH":
                continue
            target = pick.get("target")
            if target not in {"HOME", "AWAY"}:
                continue
            direction = "FAV" if ((target == "HOME") == (ah > 0)) else "DOG"
            return direction, str(pick.get("name") or "COL3_ROMBO")
    except Exception:
        pass
    return None, None


def detect_bookie_patterns(match: Dict[str, Any], raw_ah_pick: str = "NO_BET") -> Dict[str, Any]:
    odds = match.get("main_match_odds") or {}
    ah = _number(odds.get("ah_linea"))
    if ah is None:
        ah = _number(match.get("handicap"))
    if ah is None:
        return {
            "available": False,
            "confirmation": "NO_DATA",
            "signals": [],
            "col3_direction": None,
            "col3_agrees": None,
        }

    home = match.get("home_name") or match.get("home_team") or ""
    away = match.get("away_name") or match.get("away_team") or ""
    favorite = home if ah > 0 else away if ah < 0 else home
    dog = away if ah > 0 else home if ah < 0 else away
    now_pressure = abs(ah)
    stadium = _h2h_block(match, "stadium", favorite, now_pressure)
    general = _h2h_block(match, "general", favorite, now_pressure)
    fav_prev = _recent_block(
        match.get("last_home_match") if ah >= 0 else match.get("last_away_match"),
        favorite,
    )
    dog_prev = _recent_block(
        match.get("last_away_match") if ah >= 0 else match.get("last_home_match"),
        dog,
    )

    signals: List[Dict[str, Any]] = []
    for block in (stadium, general):
        movement = block.get("movement")
        signals.append(
            {
                "id": f"{str(block['kind']).upper()}_{movement}",
                "direction": "DOG" if movement == "NEW_FAVORITE_STATUS" else "NEUTRAL",
                "movement": movement,
                "delta": block.get("pressure_delta"),
                "residual": block.get("residual_current_line"),
            }
        )

    home_rank = _rank((match.get("home_standings") or {}).get("ranking"))
    away_rank = _rank((match.get("away_standings") or {}).get("ranking"))
    fav_rank = home_rank if ah >= 0 else away_rank
    dog_rank = away_rank if ah >= 0 else home_rank
    if fav_rank is not None and dog_rank is not None and fav_rank > dog_rank:
        signals.append(
            {
                "id": "LINE_AGAINST_TABLE",
                "direction": "DOG",
                "movement": "BOOKIE_CONTRADICTS_TABLE",
                "delta": fav_rank - dog_rank,
                "residual": None,
            }
        )

    col3_direction, col3_branch = _col3_direction(match, ah)
    raw_side = "FAV" if raw_ah_pick == "FAV_CUBRE" else "DOG" if raw_ah_pick == "DOG_CUBRE" else None
    col3_agrees = (col3_direction == raw_side) if col3_direction and raw_side else None
    aligned = [signal for signal in signals if signal.get("direction") == raw_side]
    conflicts = [
        signal for signal in signals
        if signal.get("direction") in {"FAV", "DOG"} and signal.get("direction") != raw_side
    ]

    if col3_agrees and aligned:
        confirmation = "STRONG_CONFIRM"
    elif col3_agrees or len(aligned) >= 2:
        confirmation = "CONFIRM"
    elif col3_agrees is False or conflicts:
        confirmation = "CONFLICT"
    else:
        confirmation = "NEUTRAL"

    return {
        "available": True,
        "favorite": favorite,
        "dog": dog,
        "current_home_line": ah,
        "signals": signals,
        "aligned_signals": [signal["id"] for signal in aligned],
        "conflicting_signals": [signal["id"] for signal in conflicts],
        "confirmation": confirmation,
        "col3_direction": col3_direction,
        "col3_branch": col3_branch,
        "col3_agrees": col3_agrees,
        "h2h_stadium": stadium,
        "h2h_general": general,
        "favorite_previous": fav_prev,
        "dog_previous": dog_prev,
    }
