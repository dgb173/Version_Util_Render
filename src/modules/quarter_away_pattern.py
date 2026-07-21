from typing import Any, Dict, List, Optional, Tuple

from .lexington_pattern import (
    _cover_from_market_line,
    _opponent_name,
    _parse_float,
    _parse_int,
    _parse_score,
    _stats_edge,
    _team_goals,
    _team_side,
    _wdl,
)


ALGORITHM = "Q025_AWAY_SYSTEM"


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


def _goals_for(match: Dict[str, Any], team_name: str) -> Optional[int]:
    score = _parse_score(match.get("score") or match.get("final_score"))
    side = _team_side(match, team_name)
    if score is None or side is None:
        return None
    team_goals, _ = _team_goals(score, side)
    return team_goals


def _goals_against(match: Dict[str, Any], team_name: str) -> Optional[int]:
    score = _parse_score(match.get("score") or match.get("final_score"))
    side = _team_side(match, team_name)
    if score is None or side is None:
        return None
    _, opp_goals = _team_goals(score, side)
    return opp_goals


def _margin(match: Dict[str, Any], team_name: str) -> Optional[int]:
    score = _parse_score(match.get("score") or match.get("final_score"))
    side = _team_side(match, team_name)
    if score is None or side is None:
        return None
    team_goals, opp_goals = _team_goals(score, side)
    return team_goals - opp_goals


def _entry_result(match: Dict[str, Any], team_name: str) -> Dict[str, Any]:
    opponent = _opponent_name(match, team_name)
    wdl = _wdl(match, team_name)
    cover = _cover_from_market_line(match, team_name)
    margin = _margin(match, team_name)
    goals_for = _goals_for(match, team_name)
    total = _goals_total(match)
    edge = _stats_edge(match, team_name, opponent) if opponent else 0
    score = 0.0

    if wdl == "W":
        score += 2.0
    elif wdl == "D":
        score += 0.45
    elif wdl == "L":
        score -= 2.0

    if cover == "COVER":
        score += 1.25
    elif cover == "PUSH":
        score += 0.25
    elif cover == "NO_COVER":
        score -= 1.25

    if margin is not None:
        if margin >= 2:
            score += 0.55
        elif margin <= -2:
            score -= 0.55

    if edge >= 2:
        score += 0.45
    elif edge <= -2:
        score -= 0.45

    return {
        "wdl": wdl,
        "cover": cover,
        "margin": margin,
        "goals_for": goals_for,
        "total": total,
        "edge": edge,
        "score": score,
    }


def _h2h_entries(match_data: Dict[str, Any], home_name: str, away_name: str) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    stadium = match_data.get("h2h_stadium") or {}
    if stadium.get("res1") and "?" not in str(stadium.get("res1")):
        entries.append(
            {
                "kind": "h2h_estadio",
                "home_team": home_name,
                "away_team": away_name,
                "score": stadium.get("res1"),
                "handicap_line_raw": stadium.get("ah1"),
                "stats_rows": stadium.get("stats_rows") or [],
            }
        )
    general = match_data.get("h2h_general") or {}
    if general.get("res6") and "?" not in str(general.get("res6")):
        entries.append(
            {
                "kind": "h2h_general",
                "home_team": general.get("h2h_gen_home") or general.get("home_team") or home_name,
                "away_team": general.get("h2h_gen_away") or general.get("away_team") or away_name,
                "score": general.get("res6"),
                "handicap_line_raw": general.get("ah6"),
                "stats_rows": general.get("stats_rows") or [],
            }
        )
    return entries


def _col3_entry(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    col3 = match_data.get("h2h_col3") or {}
    if not isinstance(col3, dict) or col3.get("status") not in (None, "", "found"):
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
        "kind": "col3",
        "home_team": home,
        "away_team": away,
        "score": f"{score[0]}:{score[1]}",
        "handicap_line_raw": col3.get("handicap") or col3.get("ah_line"),
        "stats_rows": col3.get("stats_rows") or [],
    }


def _indirect_entries(match_data: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    indirect = match_data.get("comparativas_indirectas") or {}
    left = indirect.get("left") or {}
    right = indirect.get("right") or {}
    return left if isinstance(left, dict) else {}, right if isinstance(right, dict) else {}


def _rank_support(match_data: Dict[str, Any]) -> Tuple[float, List[str]]:
    def rank(is_home: bool) -> Optional[int]:
        key = "home_standings" if is_home else "away_standings"
        return _parse_int((match_data.get(key) or {}).get("ranking"))

    home_rank = rank(True)
    away_rank = rank(False)
    if home_rank is None or away_rank is None:
        return 0.0, []
    gap = home_rank - away_rank
    if gap >= 4:
        return 0.8, [f"ranking apoya al visitante ({gap} puestos)"]
    if gap <= -4:
        return -0.8, [f"ranking protege al local ({abs(gap)} puestos)"]
    return 0.0, [f"ranking casi parejo ({home_rank} vs {away_rank})"]


def _display_ah_label(team_name: str, pick_home: bool, current_home_line: float) -> str:
    line = abs(current_home_line) if pick_home else -abs(current_home_line)
    if abs(line) < 0.01:
        return f"{team_name} 0"
    return f"{team_name} {line:+.2f}"


def _compare_team_entries(
    stronger_entry: Dict[str, Any],
    stronger_team: str,
    weaker_entry: Dict[str, Any],
    weaker_team: str,
) -> float:
    stronger = _entry_result(stronger_entry, stronger_team)
    weaker = _entry_result(weaker_entry, weaker_team)
    score = 0.0

    if stronger.get("margin") is not None and weaker.get("margin") is not None:
        if stronger["margin"] > weaker["margin"]:
            score += 0.95
        elif stronger["margin"] == weaker["margin"]:
            stronger_ga = _goals_against(stronger_entry, stronger_team)
            weaker_ga = _goals_against(weaker_entry, weaker_team)
            if stronger_ga is not None and weaker_ga is not None and stronger_ga < weaker_ga:
                score += 0.75
        else:
            score -= 0.75

    if stronger.get("cover") == "COVER" and weaker.get("cover") != "COVER":
        score += 0.55
    if stronger.get("edge", 0) >= 2:
        score += 0.35
    return score


def _away_value_override(
    match_data: Dict[str, Any],
    home_name: str,
    away_name: str,
    current_home: float,
    conditions: List[str],
) -> Optional[Dict[str, Any]]:
    home_prev = match_data.get("last_home_match") or {}
    away_prev = match_data.get("last_away_match") or {}
    if not isinstance(away_prev, dict) or _team_side(away_prev, away_name) is None:
        return None

    away_prev_res = _entry_result(away_prev, away_name)
    if away_prev_res.get("wdl") != "W" or away_prev_res.get("cover") != "COVER":
        return None

    support = 1.25
    reasons = ["visitante ya gana/cubre fuera antes de aparecer -0.25"]
    left, right = _indirect_entries(match_data)

    if left and _team_side(left, home_name) is not None:
        cmp_y = _compare_team_entries(away_prev, away_name, left, home_name)
        if cmp_y >= 0.75:
            support += cmp_y
            reasons.append("mismo rival Y: visitante compite mejor que el local")
        elif cmp_y <= -0.75:
            support += cmp_y

    if right and _team_side(right, away_name) is not None and isinstance(home_prev, dict) and _team_side(home_prev, home_name) is not None:
        cmp_x = _compare_team_entries(right, away_name, home_prev, home_name)
        if cmp_x >= 0.75:
            support += cmp_x
            reasons.append("mismo rival X: visitante iguala/mejora al local")
        elif cmp_x <= -0.75:
            support += cmp_x * 0.55

        home_ga = _goals_against(home_prev, home_name)
        away_ga = _goals_against(right, away_name)
        if home_ga is not None and away_ga is not None and home_ga >= 2 and away_ga == 0:
            support += 0.70
            reasons.append("local gana pero concede; visitante limpia al mismo X")

    for entry in _h2h_entries(match_data, home_name, away_name):
        if _team_side(entry, away_name) is None:
            continue
        away_h2h = _entry_result(entry, away_name)
        if away_h2h.get("edge", 0) >= 2 and away_h2h.get("wdl") != "W":
            support += 1.05
            reasons.append(f"{entry['kind']}: marcador contra visitante pero metricas lo sostienen")
        elif away_h2h.get("wdl") == "W" and away_h2h.get("cover") == "COVER":
            support += 0.85
            reasons.append(f"{entry['kind']}: visitante ya valida el cruce directo")

    rank_delta, _ = _rank_support(match_data)
    if rank_delta >= 0.8:
        support += 0.75
        reasons.append("tabla tambien empuja al visitante")

    if support < 3.0:
        return None

    label = _display_ah_label(away_name, False, current_home)
    return _pick(
        match_data,
        "AH",
        "AWAY",
        label,
        0.60 + min(support, 5.0) * 0.018,
        conditions + ["valor visitante -0.25: " + "; ".join(reasons[:4])],
    )


def _home_capped_trap_override(
    match_data: Dict[str, Any],
    home_name: str,
    away_name: str,
    current_home: float,
    conditions: List[str],
) -> Optional[Dict[str, Any]]:
    home_prev = match_data.get("last_home_match") or {}
    away_prev = match_data.get("last_away_match") or {}
    if not isinstance(away_prev, dict) or _team_side(away_prev, away_name) is None:
        return None

    away_prev_res = _entry_result(away_prev, away_name)
    if away_prev_res.get("wdl") != "W" or away_prev_res.get("cover") != "COVER":
        return None

    left, right = _indirect_entries(match_data)
    if not left or _team_side(left, home_name) is None:
        return None

    y_from_away_prev = _opponent_name(away_prev, away_name)
    y_from_left = _opponent_name(left, home_name)
    if not y_from_away_prev or not y_from_left:
        return None
    if y_from_away_prev.strip().lower() != y_from_left.strip().lower():
        return None

    home_left_res = _entry_result(left, home_name)
    if home_left_res.get("margin") is None or home_left_res["margin"] > -3:
        return None
    if home_left_res.get("cover") != "NO_COVER":
        return None

    trap_score = 2.15
    reasons = [
        "triangulo visitante demasiado obvio para solo -0.25",
        f"local fue goleado por {y_from_left}",
        f"visitante viene de ganar/cubrir ante {y_from_away_prev}",
    ]

    stadium_supports_home = False
    for entry in _h2h_entries(match_data, home_name, away_name):
        if _team_side(entry, away_name) is None:
            continue
        away_h2h = _entry_result(entry, away_name)
        home_h2h = _entry_result(entry, home_name)
        if entry.get("kind") == "h2h_estadio" and away_h2h.get("wdl") != "W":
            trap_score += 0.75
            stadium_supports_home = True
            reasons.append("H2H estadio no confirma victoria visitante")
        if home_h2h.get("edge", 0) >= 2:
            trap_score += 0.55
            stadium_supports_home = True
            reasons.append("mismo campo protege volumen local")

    if right and isinstance(home_prev, dict) and _team_side(right, away_name) is not None and _team_side(home_prev, home_name) is not None:
        cmp_x = _compare_team_entries(right, away_name, home_prev, home_name)
        if cmp_x < 0:
            trap_score += 0.45
            reasons.append("contra X el local no queda peor que el visitante")

    rank_delta, _ = _rank_support(match_data)
    if rank_delta >= 0.8:
        trap_score += 0.35
        reasons.append("ranking tambien hacia visitante: linea sigue capada")

    if trap_score < 2.85 and not stadium_supports_home:
        return None

    label = _display_ah_label(home_name, True, current_home)
    return _pick(
        match_data,
        "AH",
        "HOME",
        label,
        0.58 + min(trap_score, 5.0) * 0.017,
        conditions + ["trampa visitante -0.25: " + "; ".join(reasons[:4])],
    )


def _pick(
    match_data: Dict[str, Any],
    pick_type: str,
    target: str,
    label: str,
    confidence: float,
    conditions: List[str],
) -> Dict[str, Any]:
    confidence = min(0.72, max(0.56, confidence))
    roi = max(0.0, confidence * 1.90 - 1.0)
    return {
        "name": f"[Q025] {label}",
        "pick": target,
        "target": target,
        "type": pick_type,
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": ALGORITHM,
        "perspective": "Sistema -0.25 visitante: residuales minimos + H2H/indirectas + produccion O/U",
        "display_pick_label": label,
        "conditions_readable": conditions,
        "explanation": "Q025: " + " | ".join(conditions[:5]),
    }


def _score_ah(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    current_home = _current_home_line(match_data)
    if current_home is None or abs(current_home + 0.25) > 0.01:
        return None

    home_name = _team_name(match_data, True)
    away_name = _team_name(match_data, False)
    if not home_name or not away_name:
        return None

    home_score = 0.0
    away_score = 0.0
    conditions: List[str] = ["linea madre: visitante -0.25, empate castiga al favorito"]

    home_prev = match_data.get("last_home_match") or {}
    away_prev = match_data.get("last_away_match") or {}
    if isinstance(home_prev, dict) and _team_side(home_prev, home_name) is not None:
        res = _entry_result(home_prev, home_name)
        home_score += res["score"] * 0.70
        conditions.append(f"prev local {home_name}: {res['wdl']}/{res['cover']}")
    if isinstance(away_prev, dict) and _team_side(away_prev, away_name) is not None:
        res = _entry_result(away_prev, away_name)
        away_score += res["score"] * 0.70
        conditions.append(f"prev visitante {away_name}: {res['wdl']}/{res['cover']}")

    for entry in _h2h_entries(match_data, home_name, away_name):
        if _team_side(entry, home_name) is not None:
            home_res = _entry_result(entry, home_name)
            away_res = _entry_result(entry, away_name)
            home_score += home_res["score"] * 0.55
            away_score += away_res["score"] * 0.75
            conditions.append(f"{entry['kind']}: local {home_res['wdl']}/{home_res['cover']} vs visitante {away_res['wdl']}/{away_res['cover']}")

    left, right = _indirect_entries(match_data)
    if left and _team_side(left, home_name) is not None:
        res = _entry_result(left, home_name)
        home_score += res["score"] * 1.05
        conditions.append(f"indirecta local: {home_name} {res['wdl']}/{res['cover']}")
    if right and _team_side(right, away_name) is not None:
        res = _entry_result(right, away_name)
        away_score += res["score"] * 1.05
        conditions.append(f"indirecta visitante: {away_name} {res['wdl']}/{res['cover']}")

    x_name = _opponent_name(home_prev, home_name) if isinstance(home_prev, dict) else ""
    y_name = _opponent_name(away_prev, away_name) if isinstance(away_prev, dict) else ""
    col3 = _col3_entry(match_data)
    if col3 and x_name and y_name and _team_side(col3, x_name) is not None and _team_side(col3, y_name) is not None:
        x_res = _entry_result(col3, x_name)
        y_res = _entry_result(col3, y_name)
        home_score += x_res["score"] * 0.45
        away_score += y_res["score"] * 0.45
        conditions.append(f"Col3: {y_name} vs {x_name} = {y_res['wdl']}/{y_res['cover']}")

    rank_delta, rank_reasons = _rank_support(match_data)
    away_score += max(0.0, rank_delta)
    home_score += max(0.0, -rank_delta)
    conditions.extend(rank_reasons)

    home_trap_pick = _home_capped_trap_override(match_data, home_name, away_name, current_home, conditions)
    if home_trap_pick:
        return home_trap_pick

    away_value_pick = _away_value_override(match_data, home_name, away_name, current_home, conditions)
    if away_value_pick:
        return away_value_pick

    edge = away_score - home_score
    if edge >= 1.40:
        label = _display_ah_label(away_name, False, current_home)
        return _pick(match_data, "AH", "AWAY", label, 0.58 + min(edge, 5.0) * 0.018, conditions)
    if edge <= -1.15:
        label = _display_ah_label(home_name, True, current_home)
        return _pick(match_data, "AH", "HOME", label, 0.57 + min(abs(edge), 5.0) * 0.017, conditions)
    return None


def _totals_context(match_data: Dict[str, Any]) -> Dict[str, Any]:
    home_name = _team_name(match_data, True)
    away_name = _team_name(match_data, False)
    left, right = _indirect_entries(match_data)
    entries: List[Tuple[str, Dict[str, Any], str]] = []

    for key, team_name, label in (
        ("last_home_match", home_name, "prev_local"),
        ("last_away_match", away_name, "prev_visitante"),
    ):
        item = match_data.get(key) or {}
        if isinstance(item, dict) and _team_side(item, team_name) is not None:
            entries.append((label, item, team_name))

    for entry in _h2h_entries(match_data, home_name, away_name):
        entries.append((entry["kind"], entry, away_name))

    if left and _team_side(left, home_name) is not None:
        entries.append(("ind_local", left, home_name))
    if right and _team_side(right, away_name) is not None:
        entries.append(("ind_visitante", right, away_name))

    col3 = _col3_entry(match_data)
    if col3:
        entries.append(("col3", col3, ""))

    return {"entries": entries, "home_name": home_name, "away_name": away_name}


def _choose_ou(match_data: Dict[str, Any], ah_pick: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    current_home = _current_home_line(match_data)
    if current_home is None or abs(current_home + 0.25) > 0.01:
        return None

    line = _goal_line(match_data)
    if line is None:
        return None

    ctx = _totals_context(match_data)
    entries = ctx["entries"]
    totals = [(label, _goals_total(entry), entry, team_name) for label, entry, team_name in entries]
    known = [(label, total, entry, team_name) for label, total, entry, team_name in totals if total is not None]
    if len(known) < 3:
        return None

    over_count = sum(1 for _, total, _, _ in known if total > line)
    under_count = sum(1 for _, total, _, _ in known if total <= line)
    current_known = [item for item in known if item[0] != "col3"]
    current_over = sum(1 for _, total, _, _ in current_known if total > line)
    current_extreme = sum(1 for _, total, _, _ in current_known if total >= 4)
    current_core_labels = {"prev_local", "prev_visitante", "ind_local", "ind_visitante"}
    current_core = [item for item in current_known if item[0] in current_core_labels]
    current_core_extreme = sum(1 for _, total, _, _ in current_core if total >= 4)
    current_indirect_extreme = sum(1 for label, total, _, _ in current_core if label.startswith("ind_") and total >= 4)
    current_concede3 = 0
    for _, _, entry, team_name in current_core:
        if not team_name:
            continue
        ga = _goals_against(entry, team_name)
        if ga is not None and ga >= 3:
            current_concede3 += 1
    col3_total = next((total for label, total, _, _ in known if label == "col3"), None)
    h2h_stadium_total = next((total for label, total, _, _ in known if label == "h2h_estadio"), None)
    reasons: List[str] = [f"linea O/U {line:.2f}: {over_count} over reales vs {under_count} under/push en nodos"]

    away_name = ctx["away_name"]
    away_low_outputs = 0
    for label, _, entry, team_name in current_known:
        if team_name == away_name and label in {"prev_visitante", "ind_visitante", "h2h_estadio", "h2h_general"}:
            gf = _goals_for(entry, away_name)
            if gf is not None and gf <= 1:
                away_low_outputs += 1

    _, right = _indirect_entries(match_data)
    away_indirect_strong = False
    if right and _team_side(right, away_name) is not None:
        right_res = _entry_result(right, away_name)
        away_gf = _goals_for(right, away_name)
        away_indirect_strong = (
            right_res.get("wdl") == "W"
            and right_res.get("cover") == "COVER"
            and away_gf is not None
            and away_gf >= 2
        )

    if h2h_stadium_total is not None and h2h_stadium_total <= 1 and line >= 2.50:
        reasons.append("H2H estadio corto: mismo campo apunta a victoria minima")
        if current_indirect_extreme == 0:
            return _pick(match_data, "OU", "UNDER", f"UNDER {line:.2f}", 0.61, reasons)

    if line >= 3.50:
        if current_concede3 >= 2 and current_over >= 2:
            reasons.append("linea 3.5 validada por colapso defensivo actual en dos nodos")
            return _pick(match_data, "OU", "OVER", f"OVER {line:.2f}", 0.61 + min(current_concede3, 3) * 0.012, reasons)
        reasons.append("linea 3.5 sin dos colapsos actuales: H2H alto no basta")
        return _pick(match_data, "OU", "UNDER", f"UNDER {line:.2f}", 0.60, reasons)

    if line >= 3.25:
        if current_core_extreme >= 2 or (current_over >= 2 and current_concede3 >= 1):
            reasons.append("linea alta validada por dos rupturas de equipos actuales")
            return _pick(match_data, "OU", "OVER", f"OVER {line:.2f}", 0.61 + min(current_over, 3) * 0.012, reasons)
        reasons.append("linea alta sin dos rupturas actuales: evitar over por ruido externo")
        return _pick(match_data, "OU", "UNDER", f"UNDER {line:.2f}", 0.59, reasons)

    if away_low_outputs >= 2 and line <= 2.50 and current_extreme == 0:
        reasons.append("visitante favorito -0.25 gana por control, no por produccion")
        return _pick(match_data, "OU", "UNDER", f"UNDER {line:.2f}", 0.60, reasons)

    if line <= 2.25:
        if (
            away_low_outputs >= 2
            and current_core_extreme == 0
            and current_concede3 == 0
            and (col3_total is None or col3_total < 5)
            and not away_indirect_strong
        ):
            reasons.append("linea baja bloqueada: visitante -0.25 produce poco y no hay colapso actual")
            return _pick(match_data, "OU", "UNDER", f"UNDER {line:.2f}", 0.60, reasons)
        if current_extreme >= 1 or over_count >= 3 or (col3_total is not None and col3_total >= 5):
            reasons.append("linea baja: una ruptura real de equipos actuales basta para over")
            return _pick(match_data, "OU", "OVER", f"OVER {line:.2f}", 0.60 + min(over_count, 4) * 0.012, reasons)
        if under_count >= over_count:
            reasons.append("linea baja pero sin produccion actual: preferencia under")
            return _pick(match_data, "OU", "UNDER", f"UNDER {line:.2f}", 0.58, reasons)

    if 2.25 < line <= 2.75:
        if current_over >= 2 or (current_extreme >= 1 and over_count >= 2):
            reasons.append("linea media: dos nodos actuales rompen la linea")
            return _pick(match_data, "OU", "OVER", f"OVER {line:.2f}", 0.60 + current_over * 0.012, reasons)
        if under_count >= over_count and away_low_outputs >= 1:
            reasons.append("linea media sin ruptura actual suficiente")
            return _pick(match_data, "OU", "UNDER", f"UNDER {line:.2f}", 0.58, reasons)

    if current_over >= 3:
        reasons.append("acumulacion de over real en equipos actuales")
        return _pick(match_data, "OU", "OVER", f"OVER {line:.2f}", 0.60, reasons)
    return None


def evaluate_ah(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return _score_ah(match_data)


def evaluate_ou(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return _choose_ou(match_data, evaluate_ah(match_data))


def evaluate_all(match_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    picks: List[Dict[str, Any]] = []
    ah = evaluate_ah(match_data)
    if ah:
        picks.append(ah)
    ou = _choose_ou(match_data, ah)
    if ou:
        picks.append(ou)
    return picks
