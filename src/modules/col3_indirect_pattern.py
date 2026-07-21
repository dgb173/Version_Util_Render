from typing import Any, Dict, List, Optional, Tuple

from .lexington_pattern import (
    _cover_from_market_line,
    _opponent_name,
    _parse_float,
    _parse_score,
    _team_goals,
    _team_side,
    _team_stats,
    _wdl,
)


def _current_home_line(match_data: Dict[str, Any]) -> Optional[float]:
    odds = match_data.get("main_match_odds") or {}
    return _parse_float(match_data.get("handicap") or odds.get("ah_linea"))


def _goal_line(match_data: Dict[str, Any]) -> Optional[float]:
    odds = match_data.get("main_match_odds") or {}
    return _parse_float(match_data.get("goals_line") or match_data.get("goal_line") or odds.get("goals_linea"))


def _team_name(match_data: Dict[str, Any], is_home: bool) -> str:
    if is_home:
        return str(match_data.get("home_name") or match_data.get("home_team") or "")
    return str(match_data.get("away_name") or match_data.get("away_team") or "")


def _goals_total(match: Dict[str, Any]) -> Optional[int]:
    score = _parse_score(match.get("score") or match.get("final_score"))
    if score is None:
        return None
    return score[0] + score[1]


def _recent_totals(match_data: Dict[str, Any], is_home_side: bool, limit: int = 8) -> List[int]:
    key = "recent_home_matches" if is_home_side else "recent_away_matches"
    rows = match_data.get(key) or []
    if not isinstance(rows, list):
        return []
    totals: List[int] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        total = _goals_total(row)
        if total is not None:
            totals.append(total)
    return totals


def _venue_ou_summary(match_data: Dict[str, Any], ou_line: float, is_home_side: bool) -> Dict[str, Any]:
    totals = _recent_totals(match_data, is_home_side)
    over = sum(1 for total in totals if total > ou_line)
    under = sum(1 for total in totals if total <= ou_line)
    avg = sum(totals) / len(totals) if totals else None
    return {
        "totals": totals,
        "played": len(totals),
        "over": over,
        "under": under,
        "over_rate": (over / len(totals)) if totals else None,
        "avg_total": avg,
    }


def _score_margin(match: Dict[str, Any], team_name: str) -> Optional[int]:
    score = _parse_score(match.get("score") or match.get("final_score"))
    side = _team_side(match, team_name)
    if score is None or side is None:
        return None
    team_goals, opp_goals = _team_goals(score, side)
    return team_goals - opp_goals


def _team_goals_for(match: Dict[str, Any], team_name: str) -> Optional[int]:
    score = _parse_score(match.get("score") or match.get("final_score"))
    side = _team_side(match, team_name)
    if score is None or side is None:
        return None
    team_goals, _ = _team_goals(score, side)
    return team_goals


def _stats_edge(match: Dict[str, Any], team_name: str) -> int:
    stats = _team_stats(match, team_name)
    edge = 0
    for key in ("shots", "sot", "danger"):
        team_val = stats.get(f"{key}_for")
        opp_val = stats.get(f"{key}_against")
        if team_val is None or opp_val is None:
            continue
        if team_val > opp_val:
            edge += 1
        elif team_val < opp_val:
            edge -= 1
    return edge


def _low_process(match: Dict[str, Any], team_name: str) -> bool:
    stats = _team_stats(match, team_name)
    goals_for = _team_goals_for(match, team_name)
    return (
        (goals_for is not None and goals_for <= 1)
        and (
            (stats.get("sot_for") is not None and stats["sot_for"] <= 2)
            or (stats.get("shots_for") is not None and stats["shots_for"] <= 7)
            or (stats.get("danger_for") is not None and stats["danger_for"] <= 30)
        )
    )


def _result(match: Dict[str, Any], team_name: str) -> Dict[str, Any]:
    wdl = _wdl(match, team_name)
    cover = _cover_from_market_line(match, team_name)
    margin = _score_margin(match, team_name)
    total = _goals_total(match)
    goals_for = _team_goals_for(match, team_name)
    edge = _stats_edge(match, team_name)
    score = 0.0

    if wdl == "W":
        score += 2.0
    elif wdl == "D":
        score += 0.4
    elif wdl == "L":
        score -= 2.0

    if cover == "COVER":
        score += 1.5
    elif cover == "PUSH":
        score += 0.4
    elif cover == "NO_COVER":
        score -= 1.5

    if margin is not None:
        if margin >= 2:
            score += 0.8
        elif margin <= -2:
            score -= 0.8

    if edge >= 2:
        score += 0.6
    elif edge <= -2:
        score -= 0.6

    survival_cover = cover == "COVER" and wdl != "W" and _low_process(match, team_name)
    return {
        "wdl": wdl,
        "cover": cover,
        "margin": margin,
        "total": total,
        "goals_for": goals_for,
        "edge": edge,
        "score": score,
        "survival_cover": survival_cover,
    }


def _col3_match(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    col3 = match_data.get("h2h_col3") or {}
    if not isinstance(col3, dict):
        return None
    if col3.get("status") not in (None, "", "found") and not col3.get("score"):
        return None

    score = _parse_score(
        f"{col3.get('goles_home')}:{col3.get('goles_away')}"
        if col3.get("goles_home") is not None and col3.get("goles_away") is not None
        else col3.get("score")
    )
    home = col3.get("h2h_home_team_name") or col3.get("home_team")
    away = col3.get("h2h_away_team_name") or col3.get("away_team")
    if score is None or not home or not away:
        return None

    return {
        "home_team": home,
        "away_team": away,
        "score": f"{score[0]}:{score[1]}",
        "handicap_line_raw": col3.get("handicap") or col3.get("ah_line"),
        "stats_rows": col3.get("stats_rows") or col3.get("stats") or [],
        "date": col3.get("date"),
    }


def _indirect_entries(match_data: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    indirect = match_data.get("comparativas_indirectas") or {}
    left = indirect.get("left") or {}
    right = indirect.get("right") or {}
    return left if isinstance(left, dict) else {}, right if isinstance(right, dict) else {}


def _direct_h2h_supports(match_data: Dict[str, Any], team_name: str) -> int:
    support = 0
    for key in ("h2h_stadium", "h2h_general"):
        raw = match_data.get(key) or {}
        score = raw.get("res1") or raw.get("res6") or raw.get("score")
        if not score or "?" in str(score):
            continue
        entry = {
            "home_team": raw.get("h2h_gen_home") or raw.get("home_team") or _team_name(match_data, True),
            "away_team": raw.get("h2h_gen_away") or raw.get("away_team") or _team_name(match_data, False),
            "score": score,
            "handicap_line_raw": raw.get("ah1") or raw.get("ah6") or raw.get("ah"),
            "stats_rows": raw.get("stats_rows") or [],
        }
        if _team_side(entry, team_name) is None:
            continue
        wdl = _wdl(entry, team_name)
        cover = _cover_from_market_line(entry, team_name)
        if wdl == "W" or cover == "COVER":
            support += 1
        elif wdl == "L" and cover == "NO_COVER":
            support -= 1
    return support


def _rank_gap_for_home(match_data: Dict[str, Any]) -> Optional[int]:
    def parse_rank(obj: Any) -> Optional[int]:
        try:
            return int(str((obj or {}).get("ranking")).strip())
        except Exception:
            return None

    home_rank = parse_rank(match_data.get("home_standings"))
    away_rank = parse_rank(match_data.get("away_standings"))
    if home_rank is None or away_rank is None:
        return None
    return away_rank - home_rank


def _display_ah_label(team_name: str, pick_home: bool, current_home_line: float) -> str:
    if pick_home:
        line = -abs(current_home_line) if current_home_line > 0 else abs(current_home_line)
    else:
        line = abs(current_home_line) if current_home_line > 0 else -abs(current_home_line)
    if abs(line) < 0.01:
        return f"{team_name} 0"
    return f"{team_name} {line:+.2f}"


def _ah_pick(
    match_data: Dict[str, Any],
    pick_home: bool,
    branch: str,
    confidence: float,
    conditions: List[str],
) -> Dict[str, Any]:
    current_home = _current_home_line(match_data) or 0.0
    team_name = _team_name(match_data, pick_home)
    other_name = _team_name(match_data, not pick_home)
    confidence = min(0.80, max(0.56, confidence))
    roi = max(0.0, confidence * 1.90 - 1.0)
    return {
        "name": f"[Col3 Indirect] {branch}",
        "pick": "LOCAL" if pick_home else "VISITA",
        "target": "HOME" if pick_home else "AWAY",
        "type": "AH",
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": "COL3_INDIRECT",
        "perspective": "Rombo Col3: A-X, B-Y, Y-X y las dos indirectas A-Y/B-X",
        "favorite": team_name if (pick_home == (current_home > 0)) else other_name,
        "underdog": other_name if (pick_home == (current_home > 0)) else team_name,
        "handicap": abs(current_home),
        "display_pick_label": _display_ah_label(team_name, pick_home, current_home),
        "conditions_readable": conditions,
        "explanation": (
            f"Patron Col3 indirecto ({branch}): el Col3 ordena los rivales previos y las indirectas "
            f"marcan si la linea actual es validacion o trampa. " + " | ".join(conditions[:5])
        ),
    }


def _ou_pick(match_data: Dict[str, Any], pick: str, confidence: float, conditions: List[str]) -> Optional[Dict[str, Any]]:
    ou_line = _goal_line(match_data)
    if ou_line is None:
        return None
    confidence = min(0.75, max(0.55, confidence))
    roi = max(0.0, confidence * 1.90 - 1.0)
    return {
        "name": f"[Col3 Indirect] {pick} por rombo",
        "pick": pick,
        "target": pick,
        "type": "OU",
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": "COL3_INDIRECT",
        "perspective": "Goles por ruptura/bloqueo del rombo Col3",
        "display_pick_label": f"{pick} {ou_line:.2f}",
        "conditions_readable": conditions,
        "explanation": (
            f"{pick} Col3 indirecto: la salida de goles viene de si el rombo rompe por margen "
            f"o solo protege handicap. " + " | ".join(conditions[:4])
        ),
    }


def _choose_ou(
    match_data: Dict[str, Any],
    branch: str,
    picked: Dict[str, Any],
    picked_home: bool,
    home_vs_y: Dict[str, Any],
    away_vs_x: Dict[str, Any],
    col3_y: Dict[str, Any],
    home_prev: Dict[str, Any],
    away_prev: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    ou_line = _goal_line(match_data)
    if ou_line is None:
        return None

    totals = [
        home_vs_y.get("total"),
        away_vs_x.get("total"),
        col3_y.get("total"),
        _goals_total(home_prev),
        _goals_total(away_prev),
    ]
    known_totals = [total for total in totals if total is not None]
    high_line = ou_line >= 3.25
    line_over_count = sum(1 for total in known_totals if total > ou_line)
    line_under_count = sum(1 for total in known_totals if total <= ou_line)
    extreme_count = sum(1 for total in known_totals if total >= 5)
    soft_three_count = sum(1 for total in known_totals if total >= 3)
    picked_total = picked.get("total")
    picked_goals = picked.get("goals_for") or 0
    picked_venue = _venue_ou_summary(match_data, ou_line, picked_home)
    home_venue = _venue_ou_summary(match_data, ou_line, True)
    away_venue = _venue_ou_summary(match_data, ou_line, False)
    rupture = 0
    block = 0
    reasons: List[str] = []

    if branch == "FALSO_FAV_SUPERVIVENCIA":
        block += 3
        reasons.append("favorito pequeno solo sobrevivio como dog, no valida produccion")

    if picked.get("wdl") == "W" and picked_goals >= 2:
        if picked_total is not None and picked_total > ou_line:
            rupture += 3
            reasons.append("rama elegida gana y supera la linea de gol actual")
        else:
            rupture += 1
            block += 1
            reasons.append("rama elegida gana con gol propio, pero no rompe la linea actual")
    elif picked.get("wdl") == "W" and picked_goals <= 1:
        block += 3 if high_line else 2
        reasons.append("rama elegida gana por marcador minimo")
    elif (picked_total is not None and picked_total <= 2 and picked_goals <= 1) or picked_goals == 0:
        block += 2
        reasons.append("rama elegida protege handicap con produccion corta")

    if extreme_count >= 2:
        rupture += 5
        reasons.append("dos rupturas extremas 5+ sostienen over aunque haya nodos cortos")
    elif any(total is not None and total >= 4 for total in totals):
        rupture += 2
        reasons.append("hay marcador de ruptura 4+ en el rombo")

    if line_over_count >= 2:
        rupture += 3 if high_line else 2
        reasons.append("dos nodos del rombo superan la linea O/U real")
    elif not high_line and soft_three_count >= 2:
        rupture += 2
        reasons.append("dos nodos del rombo entran en 3+ goles")

    if high_line and line_under_count >= 3:
        block += 3
        reasons.append("mayoria de nodos no supera la linea alta actual")
    elif sum(1 for total in totals if total is not None and total <= 2) >= 3:
        block += 2
        reasons.append("mayoria de nodos del rombo cae en rango corto")

    if ou_line <= 2.25:
        block += 1
    elif ou_line >= 3.5 and line_over_count <= 1:
        block += 1
        reasons.append("linea 3.5 exige 4 goles: los totales de 3 no son over")

    if high_line and picked_total is not None and picked_total <= 2 and col3_y.get("total") is not None and col3_y["total"] <= 2:
        block += 3
        reasons.append("Col3 y rama elegida son cortos: el AH no implica intercambio de goles")

    if (
        high_line
        and picked_venue["played"] >= 4
        and (picked_venue["over_rate"] or 0.0) <= 0.20
        and (picked_venue["avg_total"] is None or picked_venue["avg_total"] <= 3.0)
        and extreme_count < 2
    ):
        block += 8
        side_label = "local en casa" if picked_home else "visitante fuera"
        reasons.append(f"{side_label} no rompe la linea alta en su localia reciente")

    if (
        high_line
        and home_venue["played"] >= 4
        and away_venue["played"] >= 4
        and (home_venue["over_rate"] or 0.0) >= 0.45
        and (away_venue["over_rate"] or 0.0) >= 0.45
    ):
        rupture += 2
        reasons.append("casa/fuera recientes tambien apoyan over de linea alta")

    if home_vs_y.get("survival_cover") or away_vs_x.get("survival_cover"):
        block += 1
        reasons.append("hay cover de supervivencia, no de dominio")

    if rupture >= 4 and rupture > block:
        return _ou_pick(match_data, "OVER", 0.56 + rupture * 0.025, reasons)
    if block >= 4 and block >= rupture:
        return _ou_pick(match_data, "UNDER", 0.56 + block * 0.024, reasons)
    return None


def _evaluate_context(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    current_home = _current_home_line(match_data)
    if current_home is None or abs(current_home) > 2.25:
        return None

    home_name = _team_name(match_data, True)
    away_name = _team_name(match_data, False)
    if not home_name or not away_name:
        return None

    home_prev = match_data.get("last_home_match") or {}
    away_prev = match_data.get("last_away_match") or {}
    if not isinstance(home_prev, dict) or not isinstance(away_prev, dict):
        return None
    x_name = _opponent_name(home_prev, home_name)
    y_name = _opponent_name(away_prev, away_name)
    if not x_name or not y_name:
        return None

    col3 = _col3_match(match_data)
    if not col3 or _team_side(col3, y_name) is None or _team_side(col3, x_name) is None:
        return None

    ind_left, ind_right = _indirect_entries(match_data)
    if (
        not ind_left
        or not ind_right
        or _team_side(ind_left, home_name) is None
        or _team_side(ind_left, y_name) is None
        or _team_side(ind_right, away_name) is None
        or _team_side(ind_right, x_name) is None
    ):
        return None

    home_vs_y = _result(ind_left, home_name)
    away_vs_x = _result(ind_right, away_name)
    col3_y = _result(col3, y_name)
    col3_x = _result(col3, x_name)
    home_prev_res = _result(home_prev, home_name)
    away_prev_res = _result(away_prev, away_name)
    y_validates = col3_y["wdl"] == "W" or col3_y["cover"] in {"COVER", "PUSH"} or col3_y["score"] >= 1.2

    return {
        "current_home": current_home,
        "home_name": home_name,
        "away_name": away_name,
        "x_name": x_name,
        "y_name": y_name,
        "home_prev": home_prev,
        "away_prev": away_prev,
        "home_prev_res": home_prev_res,
        "away_prev_res": away_prev_res,
        "home_vs_y": home_vs_y,
        "away_vs_x": away_vs_x,
        "col3_y": col3_y,
        "col3_x": col3_x,
        "y_validates": y_validates,
        "home_direct": _direct_h2h_supports(match_data, home_name),
        "away_direct": _direct_h2h_supports(match_data, away_name),
        "rank_gap_home": _rank_gap_for_home(match_data),
    }


def evaluate_ah(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ctx = _evaluate_context(match_data)
    if not ctx:
        return None

    current_home = ctx["current_home"]
    home = ctx["home_name"]
    away = ctx["away_name"]
    x_name = ctx["x_name"]
    y_name = ctx["y_name"]
    home_vs_y = ctx["home_vs_y"]
    away_vs_x = ctx["away_vs_x"]
    col3_y = ctx["col3_y"]
    y_validates = ctx["y_validates"]
    home_direct = ctx["home_direct"]
    away_direct = ctx["away_direct"]
    rank_gap_home = ctx["rank_gap_home"]

    conditions: List[str] = [
        f"Col3: {y_name} vs {x_name} = {col3_y['wdl']}/{col3_y['cover']}",
        f"{home} vs {y_name}: {home_vs_y['wdl']}/{home_vs_y['cover']}",
        f"{away} vs {x_name}: {away_vs_x['wdl']}/{away_vs_x['cover']}",
    ]
    away_competes_vs_x = (
        away_vs_x["cover"] in {"COVER", "PUSH"}
        or away_vs_x["wdl"] in {"W", "D"}
        or (
            away_vs_x["margin"] is not None
            and home_vs_y["margin"] is not None
            and away_vs_x["margin"] > home_vs_y["margin"]
            and away_vs_x["score"] >= home_vs_y["score"] + 0.8
        )
        or (
            away_vs_x["margin"] is not None
            and away_vs_x["margin"] >= -1
            and away_vs_x["edge"] >= 1
        )
    )

    # Exception learned from EC de Patos: covering as a protected dog with no
    # process does not validate that team when the current market asks it to give AH.
    if (
        current_home > 0
        and current_home <= 0.75
        and y_validates
        and home_vs_y["survival_cover"]
        and home_vs_y["wdl"] != "W"
        and ctx["home_prev_res"]["wdl"] != "W"
    ):
        conditions.append("excepcion: el local solo sobrevivio como dog ante el nodo fuerte")
        confidence = 0.59
        if away_vs_x["cover"] == "NO_COVER":
            confidence -= 0.015
            conditions.append("el visitante tambien llega debil: bajar stake, pero el -AH local es falso")
        return _ah_pick(match_data, False, "FALSO_FAV_SUPERVIVENCIA", confidence, conditions)

    if (
        y_validates
        and (
            home_vs_y["cover"] == "NO_COVER"
            or home_vs_y["wdl"] == "L"
            or home_vs_y["survival_cover"]
        )
        and away_competes_vs_x
    ):
        conditions.append("el nodo fuerte del visitante castiga al local y el visitante queda menos roto contra X")
        confidence = 0.60 + max(0.0, away_vs_x["score"] - home_vs_y["score"]) * 0.018
        if home_direct < away_direct:
            confidence += 0.025
            conditions.append("H2H directo inclina tambien hacia visitante")
        return _ah_pick(match_data, False, "DOG_ROMBO", confidence, conditions)

    if (
        home_vs_y["wdl"] == "W"
        and home_vs_y["cover"] == "COVER"
        and away_vs_x["cover"] == "NO_COVER"
        and away_vs_x["wdl"] != "W"
    ):
        conditions.append("el local supera al nodo fuerte y el visitante falla contra el nodo debil")
        confidence = 0.61 + max(0.0, home_vs_y["score"] - away_vs_x["score"]) * 0.015
        if home_direct > 0:
            confidence += 0.025
            conditions.append("H2H directo confirma al local")
        if rank_gap_home is not None and rank_gap_home >= 5:
            confidence += 0.015
            conditions.append(f"ranking apoya al local ({rank_gap_home} puestos)")
        return _ah_pick(match_data, True, "FAV_ROMBO", confidence, conditions)

    return None


def evaluate_ou(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ah_pick = evaluate_ah(match_data)
    if not ah_pick:
        return None
    ctx = _evaluate_context(match_data)
    if not ctx:
        return None

    picked_home = ah_pick.get("target") == "HOME"
    picked = ctx["home_vs_y"] if picked_home else ctx["away_vs_x"]
    return _choose_ou(
        match_data,
        str(ah_pick.get("name", "")).split("]")[-1].strip() or "COL3",
        picked,
        picked_home,
        ctx["home_vs_y"],
        ctx["away_vs_x"],
        ctx["col3_y"],
        ctx["home_prev"],
        ctx["away_prev"],
    )


def evaluate_all(match_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    picks: List[Dict[str, Any]] = []
    ah_pick = evaluate_ah(match_data)
    if ah_pick:
        picks.append(ah_pick)
    ou_pick = evaluate_ou(match_data)
    if ou_pick:
        picks.append(ou_pick)
    return picks
