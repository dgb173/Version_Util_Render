from typing import Any, Dict, Iterable, List, Optional, Tuple

from .lexington_pattern import (
    _asian_category,
    _cover_from_market_line,
    _name_matches,
    _opponent_name,
    _parse_float,
    _rank_gap,
    _parse_score,
    _stats_edge,
    _team_goals,
    _team_side,
    _team_stats,
    _wdl,
)


def _current_home_line(match_data: Dict[str, Any]) -> Optional[float]:
    odds = match_data.get("main_match_odds") or {}
    return _parse_float(match_data.get("handicap") or odds.get("ah_linea"))


def _score_margin_for_team(match: Dict[str, Any], team_name: str) -> Optional[int]:
    score = _parse_score(match.get("score") or match.get("final_score"))
    side = _team_side(match, team_name)
    if score is None or side is None:
        return None
    team_goals, opp_goals = _team_goals(score, side)
    return team_goals - opp_goals


def _goals_total(match: Dict[str, Any]) -> Optional[int]:
    score = _parse_score(match.get("score") or match.get("final_score"))
    if score is None:
        return None
    return score[0] + score[1]


def _h2h_stadium_entry(match_data: Dict[str, Any], home_name: str, away_name: str) -> Optional[Dict[str, Any]]:
    h2h = match_data.get("h2h_stadium") or {}
    score = h2h.get("res1") or h2h.get("score")
    if not score or "?" in str(score):
        return None
    return {
        "home_team": home_name,
        "away_team": away_name,
        "score": score,
        "ah": _parse_float(h2h.get("ah1")),
        "stats_rows": h2h.get("stats_rows") or [],
        "date": h2h.get("date1"),
    }


def _h2h_general_entry(match_data: Dict[str, Any], home_name: str, away_name: str) -> Optional[Dict[str, Any]]:
    h2h = match_data.get("h2h_general") or {}
    score = h2h.get("res6") or h2h.get("score")
    if not score or "?" in str(score):
        return None
    return {
        "home_team": h2h.get("h2h_gen_home") or h2h.get("home_team") or home_name,
        "away_team": h2h.get("h2h_gen_away") or h2h.get("away_team") or away_name,
        "score": score,
        "ah": _parse_float(h2h.get("ah6")),
        "stats_rows": h2h.get("stats_rows") or [],
        "date": h2h.get("date6"),
    }


def _team_market_strength(entry: Dict[str, Any], team_name: str) -> Optional[float]:
    """
    Strength is positive when the team was favored in that historical match.
    The app convention stores AH positive when the row home team is favored.
    """
    ah = _parse_float(entry.get("ah"))
    side = _team_side(entry, team_name)
    if ah is None or side is None:
        return None
    return ah if side else -ah


def _home_market_shift(current_home_line: float, historical_entry: Dict[str, Any], home_name: str) -> Optional[float]:
    old_strength = _team_market_strength(historical_entry, home_name)
    if old_strength is None:
        return None
    return current_home_line - old_strength


def _col3_weakens_away_loss_reference(
    col3: Dict[str, Any],
    away_prev_opp: str,
    home_prev_opp: str,
) -> Tuple[bool, str]:
    if not col3 or col3.get("status") != "found":
        return False, "sin Col3 espejo"
    if not away_prev_opp or not home_prev_opp:
        return False, "rivales previos incompletos"

    score = _parse_score(
        f"{col3.get('goles_home')}:{col3.get('goles_away')}"
        if col3.get("goles_home") is not None and col3.get("goles_away") is not None
        else col3.get("score")
    )
    ah = _parse_float(col3.get("handicap") or col3.get("ah_line"))
    if score is None or ah is None:
        return False, "Col3 sin marcador o AH"

    col3_match = {
        "home_team": col3.get("h2h_home_team_name") or col3.get("home_team"),
        "away_team": col3.get("h2h_away_team_name") or col3.get("away_team"),
        "score": f"{score[0]}:{score[1]}",
        "handicap_line_raw": ah,
        "stats_rows": col3.get("stats_rows") or [],
    }
    if _team_side(col3_match, away_prev_opp) is None or _team_side(col3_match, home_prev_opp) is None:
        return False, "Col3 no conecta los rivales previos"

    away_loss_ref_cover = _cover_from_market_line(col3_match, away_prev_opp)
    away_loss_ref_wdl = _wdl(col3_match, away_prev_opp)
    if away_loss_ref_wdl != "W" and away_loss_ref_cover != "COVER":
        return True, f"{away_prev_opp} no valida su fuerza ante {home_prev_opp} en Col3 ({away_loss_ref_cover})"
    if away_loss_ref_cover == "NO_COVER":
        return True, f"{away_prev_opp} falla el AH de Col3 ante {home_prev_opp}"
    return False, "Col3 si valida la rama del visitante"


def _defensive_leak(match: Dict[str, Any], team_name: str) -> bool:
    stats = _team_stats(match, team_name)
    margin = _score_margin_for_team(match, team_name)
    shots_against = stats.get("shots_against")
    sot_against = stats.get("sot_against")
    danger_against = stats.get("danger_against")
    return (
        margin is not None and margin <= -1 and (
            (shots_against is not None and shots_against >= 14) or
            (sot_against is not None and sot_against >= 5) or
            (danger_against is not None and danger_against >= 45)
        )
    )


def _attacking_process(match: Dict[str, Any], team_name: str) -> bool:
    stats = _team_stats(match, team_name)
    margin = _score_margin_for_team(match, team_name)
    shots_for = stats.get("shots_for")
    sot_for = stats.get("sot_for")
    danger_for = stats.get("danger_for")
    return (
        margin is not None and margin >= 0 and (
            (shots_for is not None and shots_for >= 14) or
            (sot_for is not None and sot_for >= 5) or
            (danger_for is not None and danger_for >= 45)
        )
    )


def _low_attacking_output(match: Dict[str, Any], team_name: str) -> bool:
    stats = _team_stats(match, team_name)
    margin = _score_margin_for_team(match, team_name)
    sot_for = stats.get("sot_for")
    shots_for = stats.get("shots_for")
    return (
        margin is not None and margin <= -2 and (
            (sot_for is not None and sot_for <= 1) or
            (shots_for is not None and shots_for <= 8)
        )
    )


def _indirect_entries(match_data: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    indirect = match_data.get("comparativas_indirectas") or {}
    return indirect.get("left") or {}, indirect.get("right") or {}


def _match_from_col3(col3: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not col3 or col3.get("status") != "found":
        return None
    score = _parse_score(
        f"{col3.get('goles_home')}:{col3.get('goles_away')}"
        if col3.get("goles_home") is not None and col3.get("goles_away") is not None
        else col3.get("score")
    )
    if score is None:
        return None
    return {
        "home_team": col3.get("h2h_home_team_name") or col3.get("home_team"),
        "away_team": col3.get("h2h_away_team_name") or col3.get("away_team"),
        "score": f"{score[0]}:{score[1]}",
        "handicap_line_raw": col3.get("handicap") or col3.get("ah_line"),
        "stats_rows": col3.get("stats_rows") or [],
    }


def _poor_favorite_attack(match: Dict[str, Any], fav_name: str) -> bool:
    stats = _team_stats(match, fav_name)
    margin = _score_margin_for_team(match, fav_name)
    shots_for = stats.get("shots_for")
    sot_for = stats.get("sot_for")
    goals_for = None
    score = _parse_score(match.get("score") or match.get("final_score"))
    side = _team_side(match, fav_name)
    if score is not None and side is not None:
        goals_for, _ = _team_goals(score, side)
    return (
        margin is not None and margin <= 0 and (
            (goals_for is not None and goals_for == 0) or
            (sot_for is not None and sot_for <= 2) or
            (shots_for is not None and shots_for <= 6)
        )
    )


def _favorite_strength_shift(current_fav_line: float, historical_entry: Dict[str, Any], fav_name: str) -> Optional[float]:
    old_strength = _team_market_strength(historical_entry, fav_name)
    if old_strength is None:
        return None
    return current_fav_line - old_strength


def evaluate_ah(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Patron re-rating local.

    Detecta favoritos locales/pick'em que parecen discutibles por Col3 o stats
    de marcador, pero el mercado los revalida tras un H2H de estadio ganado y
    una degradacion real del visitante.
    """
    current_home = _current_home_line(match_data)
    if current_home is None or current_home < -0.01 or current_home > 0.50:
        return None

    home_name = str(match_data.get("home_name") or match_data.get("home_team") or "")
    away_name = str(match_data.get("away_name") or match_data.get("away_team") or "")
    if not home_name or not away_name:
        return None

    prev_home = match_data.get("last_home_match") or {}
    prev_away = match_data.get("last_away_match") or {}
    if not isinstance(prev_home, dict) or not isinstance(prev_away, dict):
        return None

    conditions: List[str] = []
    score = 0

    home_prev_cover = _cover_from_market_line(prev_home, home_name)
    if home_prev_cover != "COVER":
        return None
    home_prev_margin = _score_margin_for_team(prev_home, home_name)
    if home_prev_margin is None or home_prev_margin < 1:
        return None
    score += 2
    conditions.append(f"{home_name} gana y cubre su previa ({prev_home.get('score')})")

    if _attacking_process(prev_home, home_name):
        score += 1
        conditions.append("Proceso ofensivo local alto en la previa")

    away_prev_cover = _cover_from_market_line(prev_away, away_name)
    away_prev_margin = _score_margin_for_team(prev_away, away_name)
    if away_prev_cover == "COVER" or away_prev_margin is None or away_prev_margin > -1:
        return None
    score += 2
    conditions.append(f"{away_name} llega degradado: {prev_away.get('score')} y {away_prev_cover}")

    if _defensive_leak(prev_away, away_name):
        score += 1
        conditions.append("Visitante concede volumen/SOT altos en su previa")

    stadium = _h2h_stadium_entry(match_data, home_name, away_name)
    if not stadium:
        return None
    stadium_margin = _score_margin_for_team(stadium, home_name)
    stadium_shift = _home_market_shift(current_home, stadium, home_name)
    if stadium_margin is None or stadium_margin < 1:
        return None
    if stadium_shift is None or stadium_shift < 0.50:
        return None
    score += 3
    conditions.append(
        f"H2H estadio ganado por {home_name} y giro de linea hacia local {stadium_shift:+.2f}"
    )

    general = _h2h_general_entry(match_data, home_name, away_name)
    if general:
        general_shift = _home_market_shift(current_home, general, home_name)
        general_stats = _stats_edge(general, home_name, away_name)
        if general_shift is not None and general_shift >= 0.75:
            score += 1
            conditions.append(f"H2H general confirma re-rating hacia {home_name} ({general_shift:+.2f})")
        if general_stats >= 2:
            score += 1
            conditions.append("H2H general: marcador adverso pero stats latentes del local actual")

    home_prev_opp = _opponent_name(prev_home, home_name)
    away_prev_opp = _opponent_name(prev_away, away_name)
    col3_ok, col3_reason = _col3_weakens_away_loss_reference(
        match_data.get("h2h_col3") or {},
        away_prev_opp,
        home_prev_opp,
    )
    if not col3_ok:
        return None
    score += 2
    conditions.append(col3_reason)

    if abs(current_home) < 0.01:
        display_pick = f"{home_name} 0"
        pick_name = "[Re-rating Local] Pick'em/DNB local"
        confidence = min(0.75, 0.55 + score * 0.023)
    else:
        display_pick = f"{home_name} -{abs(current_home):.2f}"
        pick_name = "[Re-rating Local] Favorito local revalidado"
        confidence = min(0.78, 0.57 + score * 0.024)

    roi = max(0.0, confidence * 1.90 - 1.0)
    return {
        "name": pick_name,
        "pick": "LOCAL",
        "target": "HOME",
        "type": "AH",
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": "LOCAL_RERATE",
        "perspective": "Favorito/local revalidado por mercado",
        "favorite": home_name if current_home > 0 else f"{home_name} (pick'em)",
        "underdog": away_name,
        "handicap": abs(current_home),
        "display_pick_label": display_pick,
        "conditions_readable": conditions,
        "explanation": (
            f"Patron re-rating local: {home_name} pasa de discutible a revalidado por mercado. "
            + " | ".join(conditions[:5])
        ),
    }


def evaluate_high_favorite_ah(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Variante de re-rating con favorito fuerte.

    Detecta favoritos locales en AH alto donde el H2H estadio o alguna
    indirecta antigua ensucian la lectura, pero el mercado sube al favorito
    por forma reciente, H2H general y degradacion del visitante.
    """
    current_home = _current_home_line(match_data)
    if current_home is None or current_home < 1.0 or current_home > 2.0:
        return None

    home_name = str(match_data.get("home_name") or match_data.get("home_team") or "")
    away_name = str(match_data.get("away_name") or match_data.get("away_team") or "")
    if not home_name or not away_name:
        return None

    prev_home = match_data.get("last_home_match") or {}
    prev_away = match_data.get("last_away_match") or {}
    if not isinstance(prev_home, dict) or not isinstance(prev_away, dict):
        return None

    conditions: List[str] = []
    score = 0

    home_prev_cover = _cover_from_market_line(prev_home, home_name)
    home_prev_line = _parse_float(prev_home.get("handicap_line_raw"))
    home_prev_margin = _score_margin_for_team(prev_home, home_name)
    if (
        home_prev_cover != "COVER" or
        home_prev_line is None or
        abs(home_prev_line) < current_home - 0.25 or
        home_prev_margin is None or
        home_prev_margin < 3
    ):
        return None
    score += 3
    conditions.append(
        f"{home_name} ya cubrio linea dura AH {abs(home_prev_line):.2f} con goleada {prev_home.get('score')}"
    )

    if not _attacking_process(prev_home, home_name) or _stats_edge(prev_home, home_name, _opponent_name(prev_home, home_name)) < 2:
        return None
    score += 2
    conditions.append("Favorito llega con proceso ofensivo y dominio estadistico")

    away_prev_cover = _cover_from_market_line(prev_away, away_name)
    away_prev_margin = _score_margin_for_team(prev_away, away_name)
    if away_prev_cover == "COVER" or away_prev_margin is None or away_prev_margin > -2:
        return None
    score += 2
    conditions.append(f"{away_name} llega roto: {prev_away.get('score')} y {away_prev_cover}")

    if _low_attacking_output(prev_away, away_name):
        score += 1
        conditions.append("Visitante con produccion ofensiva minima en su previa")
    if _defensive_leak(prev_away, away_name):
        score += 1
        conditions.append("Visitante concede volumen/danger alto")

    general = _h2h_general_entry(match_data, home_name, away_name)
    if not general:
        return None
    general_margin = _score_margin_for_team(general, home_name)
    general_shift = _home_market_shift(current_home, general, home_name)
    if general_margin is None or general_margin < 2:
        return None
    if general_shift is None or general_shift < 0.75:
        return None
    score += 3
    conditions.append(
        f"H2H general ya muestra margen de {home_name} y re-rating de linea {general_shift:+.2f}"
    )

    if _stats_edge(general, home_name, away_name) >= 2:
        score += 1
        conditions.append("H2H general confirma superioridad estadistica del favorito")

    stadium = _h2h_stadium_entry(match_data, home_name, away_name)
    if stadium:
        stadium_shift = _home_market_shift(current_home, stadium, home_name)
        stadium_margin = _score_margin_for_team(stadium, home_name)
        if stadium_shift is not None and stadium_shift >= 0.75:
            score += 1
            if stadium_margin is not None and stadium_margin < 0:
                conditions.append("Mercado ignora derrota antigua de estadio y sube fuerte al favorito")
            else:
                conditions.append(f"H2H estadio tambien re-ratea hacia {home_name} ({stadium_shift:+.2f})")

    home_prev_opp = _opponent_name(prev_home, home_name)
    away_prev_opp = _opponent_name(prev_away, away_name)
    col3_ok, col3_reason = _col3_weakens_away_loss_reference(
        match_data.get("h2h_col3") or {},
        away_prev_opp,
        home_prev_opp,
    )
    if not col3_ok:
        return None
    score += 2
    conditions.append(col3_reason)

    confidence = min(0.81, 0.58 + score * 0.019)
    roi = max(0.0, confidence * 1.90 - 1.0)
    return {
        "name": "[Favorito Martillo] Re-rating fuerte del local",
        "pick": "LOCAL",
        "target": "HOME",
        "type": "AH",
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": "HIGH_FAV_RERATE",
        "perspective": "Favorito fuerte revalidado por mercado",
        "favorite": home_name,
        "underdog": away_name,
        "handicap": current_home,
        "display_pick_label": f"{home_name} -{current_home:.2f}",
        "conditions_readable": conditions,
        "explanation": (
            f"Patron favorito martillo: {home_name} sostiene una linea alta por forma, H2H general y re-rating. "
            + " | ".join(conditions[:5])
        ),
    }


def evaluate_ou(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ah_pick = evaluate_ah(match_data)
    if not ah_pick:
        return None

    odds = match_data.get("main_match_odds") or {}
    ou_line = _parse_float(odds.get("goals_linea"))
    if ou_line is None or ou_line > 2.75:
        return None

    home_name = str(match_data.get("home_name") or match_data.get("home_team") or "")
    away_name = str(match_data.get("away_name") or match_data.get("away_team") or "")
    prev_home = match_data.get("last_home_match") or {}
    prev_away = match_data.get("last_away_match") or {}
    stadium = _h2h_stadium_entry(match_data, home_name, away_name)
    general = _h2h_general_entry(match_data, home_name, away_name)

    conditions: List[str] = []
    score = 0

    if _goals_total(prev_home) is not None and _goals_total(prev_home) >= 3:
        score += 1
        conditions.append("Previa local ya rompe 2.5 goles")
    if _attacking_process(prev_home, home_name):
        score += 1
        conditions.append("Local llega con produccion ofensiva")
    if _defensive_leak(prev_away, away_name):
        score += 2
        conditions.append("Visitante llega concediendo demasiado")
    if stadium and _goals_total(stadium) is not None and _goals_total(stadium) >= 3:
        score += 1
        conditions.append("H2H estadio ya fue over")
    if general and _stats_edge(general, home_name, away_name) >= 2:
        score += 1
        conditions.append("H2H general under de marcador pero con SOT/tiros del local")

    if score < 4:
        return None

    confidence = min(0.72, 0.54 + score * 0.025)
    roi = max(0.0, confidence * 1.90 - 1.0)
    return {
        "name": "[Re-rating Local] Over por castigo al visitante",
        "pick": "OVER",
        "target": "OVER",
        "type": "OU",
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": "LOCAL_RERATE",
        "perspective": "Over ligado a favorito local revalidado",
        "display_pick_label": f"OVER {ou_line:.2f}",
        "conditions_readable": conditions,
        "explanation": (
            f"Over de patron re-rating local: {home_name} trae produccion y {away_name} llega degradado. "
            + " | ".join(conditions[:4])
        ),
    }


def evaluate_high_favorite_ou(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ah_pick = evaluate_high_favorite_ah(match_data)
    if not ah_pick:
        return None

    odds = match_data.get("main_match_odds") or {}
    ou_line = _parse_float(odds.get("goals_linea"))
    if ou_line is None or ou_line < 3.0 or ou_line > 4.25:
        return None

    home_name = str(match_data.get("home_name") or match_data.get("home_team") or "")
    away_name = str(match_data.get("away_name") or match_data.get("away_team") or "")
    prev_home = match_data.get("last_home_match") or {}
    prev_away = match_data.get("last_away_match") or {}
    stadium = _h2h_stadium_entry(match_data, home_name, away_name)
    general = _h2h_general_entry(match_data, home_name, away_name)
    ind_left, ind_right = _indirect_entries(match_data)
    col3 = match_data.get("h2h_col3") or {}

    conditions: List[str] = []
    score = 0

    if _goals_total(prev_home) is not None and _goals_total(prev_home) >= 4:
        score += 2
        conditions.append("Favorito viene de marcador 4+ goles")
    if _attacking_process(prev_home, home_name):
        score += 1
        conditions.append("Favorito trae volumen ofensivo")
    if _low_attacking_output(prev_away, away_name):
        score += 1
        conditions.append("Visitante llega sin pegada")
    if _defensive_leak(prev_away, away_name):
        score += 1
        conditions.append("Visitante llega concediendo demasiado")
    if general and _score_margin_for_team(general, home_name) is not None and _score_margin_for_team(general, home_name) >= 2:
        score += 1
        conditions.append("H2H general ya dio margen de 2 al favorito")
    if stadium and _goals_total(stadium) is not None and _goals_total(stadium) >= 3:
        score += 1
        conditions.append("H2H estadio previo tuvo 3+ goles")
    if ind_right and _goals_total(ind_right) is not None and _goals_total(ind_right) >= 5:
        score += 1
        conditions.append("Indirecta visitante abre escenario de goles")

    col3_score = _parse_score(
        f"{col3.get('goles_home')}:{col3.get('goles_away')}"
        if col3.get("goles_home") is not None and col3.get("goles_away") is not None
        else col3.get("score")
    )
    if col3_score and sum(col3_score) >= 5:
        score += 1
        conditions.append("Col3 espejo tambien fue 5+ goles")

    if score < 6:
        return None

    confidence = min(0.73, 0.55 + score * 0.022)
    roi = max(0.0, confidence * 1.90 - 1.0)
    return {
        "name": "[Favorito Martillo] Over por goleada del favorito",
        "pick": "OVER",
        "target": "OVER",
        "type": "OU",
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": "HIGH_FAV_RERATE",
        "perspective": "Over de favorito fuerte",
        "display_pick_label": f"OVER {ou_line:.2f}",
        "conditions_readable": conditions,
        "explanation": (
            f"Over de favorito martillo: {home_name} puede empujar el total por margen propio. "
            + " | ".join(conditions[:5])
        ),
    }


def evaluate_dog_h2h_killer_ah(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Favorito alto rechazado por doble H2H.

    El mercado infla al favorito por tabla/linea actual, pero los dos directos
    ya mostraron que el no favorito le bloquea el partido. La previa del dog
    puede ser muy mala; el filtro exige que Col3/indirecta expliquen por que
    esa derrota no pesa mas que el H2H directo.
    """
    current_home = _current_home_line(match_data)
    if current_home is None or abs(current_home) < 0.75 or abs(current_home) > 1.75:
        return None

    home_name = str(match_data.get("home_name") or match_data.get("home_team") or "")
    away_name = str(match_data.get("away_name") or match_data.get("away_team") or "")
    if not home_name or not away_name:
        return None

    fav_is_home = current_home > 0
    fav_name = home_name if fav_is_home else away_name
    dog_name = away_name if fav_is_home else home_name
    prev_fav = (match_data.get("last_home_match") if fav_is_home else match_data.get("last_away_match")) or {}
    prev_dog = (match_data.get("last_away_match") if fav_is_home else match_data.get("last_home_match")) or {}
    if not isinstance(prev_fav, dict) or not isinstance(prev_dog, dict):
        return None

    conditions: List[str] = []
    score = 0
    current_fav_line = abs(current_home)

    fav_prev_cover = _cover_from_market_line(prev_fav, fav_name)
    fav_prev_margin = _score_margin_for_team(prev_fav, fav_name)
    if fav_prev_cover == "COVER" or fav_prev_margin is None or fav_prev_margin > 0:
        return None
    if not _poor_favorite_attack(prev_fav, fav_name):
        return None
    score += 3
    conditions.append(
        f"Favorito {fav_name} viene de fallo ofensivo ({prev_fav.get('score')}, {fav_prev_cover})"
    )

    dog_prev_cover = _cover_from_market_line(prev_dog, dog_name)
    dog_prev_margin = _score_margin_for_team(prev_dog, dog_name)
    if dog_prev_cover == "COVER" or dog_prev_margin is None or dog_prev_margin > -2:
        return None
    if not _low_attacking_output(prev_dog, dog_name):
        return None
    score += 2
    conditions.append(
        f"El mercado castiga al dog por previa mala ({prev_dog.get('score')}, {dog_prev_cover})"
    )

    stadium = _h2h_stadium_entry(match_data, home_name, away_name)
    general = _h2h_general_entry(match_data, home_name, away_name)
    if not stadium or not general:
        return None

    stadium_fav_margin = _score_margin_for_team(stadium, fav_name)
    general_fav_margin = _score_margin_for_team(general, fav_name)
    if stadium_fav_margin is None or stadium_fav_margin >= 0:
        return None
    if general_fav_margin is None or general_fav_margin >= 0:
        return None

    stadium_edge = _stats_edge(stadium, fav_name, dog_name)
    general_edge = _stats_edge(general, fav_name, dog_name)
    if stadium_edge + general_edge < 3:
        return None
    score += 4
    conditions.append(
        f"Doble H2H rechaza al favorito: {fav_name} perdio ambos directos pese a edge estadistico"
    )

    stadium_shift = _favorite_strength_shift(current_fav_line, stadium, fav_name)
    general_shift = _favorite_strength_shift(current_fav_line, general, fav_name)
    valid_shifts = [shift for shift in (stadium_shift, general_shift) if shift is not None]
    if not valid_shifts or max(valid_shifts) < 0.75 or sum(valid_shifts) < 1.25:
        return None
    score += 2
    conditions.append(
        "La linea gira fuerte hacia el favorito pese a que el H2H directo ya lo bloqueo"
    )

    fav_prev_opp = _opponent_name(prev_fav, fav_name)
    dog_prev_opp = _opponent_name(prev_dog, dog_name)
    col3_match = _match_from_col3(match_data.get("h2h_col3") or {})
    if not col3_match or _team_side(col3_match, fav_prev_opp) is None or _team_side(col3_match, dog_prev_opp) is None:
        return None
    dog_prev_opp_wdl = _wdl(col3_match, dog_prev_opp)
    dog_prev_opp_cover = _cover_from_market_line(col3_match, dog_prev_opp)
    if dog_prev_opp_wdl != "W" and dog_prev_opp_cover != "COVER":
        return None
    score += 2
    conditions.append(
        f"Col3 protege la mala previa del dog: {dog_prev_opp} supera a {fav_prev_opp}"
    )

    ind_left, ind_right = _indirect_entries(match_data)
    dog_indirect = ind_right if fav_is_home else ind_left
    if not dog_indirect or _team_side(dog_indirect, dog_name) is None:
        return None
    dog_ind_cover = _cover_from_market_line(dog_indirect, dog_name)
    dog_ind_margin = _score_margin_for_team(dog_indirect, dog_name)
    if dog_ind_cover != "COVER" and (dog_ind_margin is None or dog_ind_margin < 0):
        return None
    score += 2
    conditions.append(
        f"Indirecta del dog ante rival del favorito no rompe ({dog_indirect.get('score')}, {dog_ind_cover})"
    )

    rank_gap = _rank_gap(match_data, fav_is_home)
    if rank_gap is not None and rank_gap >= 4:
        score += 1
        conditions.append(f"Riesgo de inflacion por tabla: gap ranking {rank_gap} a favor del favorito")

    confidence = min(0.80, 0.56 + score * 0.017)
    roi = max(0.0, confidence * 1.90 - 1.0)
    side_label = "LOCAL" if not fav_is_home else "VISITANTE"
    target = "HOME" if not fav_is_home else "AWAY"
    return {
        "name": "[Dog H2H Killer] Favorito alto rechazado por doble H2H",
        "pick": side_label,
        "target": target,
        "type": "AH",
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": "DOG_H2H_KILLER",
        "perspective": "Lectura desde el favorito: linea alta inflada contra un dog que ya lo bloqueo",
        "favorite": fav_name,
        "underdog": dog_name,
        "handicap": current_fav_line,
        "display_pick_label": f"{dog_name} +{current_fav_line:.2f}",
        "conditions_readable": conditions,
        "explanation": (
            f"Patron Dog H2H Killer: {fav_name} queda inflado por linea actual, "
            f"pero {dog_name} ya lo rechazo en los dos directos. "
            + " | ".join(conditions[:5])
        ),
    }


def evaluate_dog_h2h_killer_ou(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ah_pick = evaluate_dog_h2h_killer_ah(match_data)
    if not ah_pick:
        return None

    odds = match_data.get("main_match_odds") or {}
    ou_line = _parse_float(odds.get("goals_linea"))
    if ou_line is None or ou_line > 2.50:
        return None

    home_name = str(match_data.get("home_name") or match_data.get("home_team") or "")
    away_name = str(match_data.get("away_name") or match_data.get("away_team") or "")
    current_home = _current_home_line(match_data)
    if current_home is None:
        return None
    fav_is_home = current_home > 0
    fav_name = home_name if fav_is_home else away_name
    dog_name = away_name if fav_is_home else home_name
    prev_fav = (match_data.get("last_home_match") if fav_is_home else match_data.get("last_away_match")) or {}
    prev_dog = (match_data.get("last_away_match") if fav_is_home else match_data.get("last_home_match")) or {}
    stadium = _h2h_stadium_entry(match_data, home_name, away_name)
    general = _h2h_general_entry(match_data, home_name, away_name)

    conditions: List[str] = []
    score = 0
    if _poor_favorite_attack(prev_fav, fav_name):
        score += 2
        conditions.append("Favorito llega con produccion ofensiva baja")
    if _low_attacking_output(prev_dog, dog_name):
        score += 1
        conditions.append("Dog llega sin volumen ofensivo, pero protegido por AH")
    if general and _goals_total(general) is not None and _goals_total(general) <= 2:
        score += 2
        conditions.append("H2H general fue de marcador corto")
    if stadium and _score_margin_for_team(stadium, fav_name) is not None and _score_margin_for_team(stadium, fav_name) < 0:
        score += 1
        conditions.append("H2H estadio vuelve a bloquear al favorito")

    dog_indirect = (_indirect_entries(match_data)[1] if fav_is_home else _indirect_entries(match_data)[0])
    if dog_indirect and _goals_total(dog_indirect) is not None and _goals_total(dog_indirect) <= 2:
        score += 1
        conditions.append("Indirecta del dog tambien cae en rango under")

    if score < 5:
        return None

    confidence = min(0.70, 0.55 + score * 0.022)
    roi = max(0.0, confidence * 1.90 - 1.0)
    return {
        "name": "[Dog H2H Killer] Under por bloqueo del favorito",
        "pick": "UNDER",
        "target": "UNDER",
        "type": "OU",
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": "DOG_H2H_KILLER",
        "perspective": "Under ligado a favorito inflado y dog que bloquea el margen",
        "display_pick_label": f"UNDER {ou_line:.2f}",
        "conditions_readable": conditions,
        "explanation": (
            f"Under de Dog H2H Killer: {fav_name} necesita margen pero su produccion reciente y el H2H "
            f"apuntan a partido trabado contra {dog_name}. "
            + " | ".join(conditions[:4])
        ),
    }


def evaluate_match(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    return evaluate_ah(match_data) or evaluate_high_favorite_ah(match_data) or evaluate_dog_h2h_killer_ah(match_data)


def evaluate_all(match_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    picks: List[Dict[str, Any]] = []
    ah_pick = evaluate_ah(match_data)
    if ah_pick:
        picks.append(ah_pick)
    high_ah_pick = evaluate_high_favorite_ah(match_data)
    if high_ah_pick:
        picks.append(high_ah_pick)
    dog_h2h_pick = evaluate_dog_h2h_killer_ah(match_data)
    if dog_h2h_pick:
        picks.append(dog_h2h_pick)
    ou_pick = evaluate_ou(match_data)
    if ou_pick:
        picks.append(ou_pick)
    high_ou_pick = evaluate_high_favorite_ou(match_data)
    if high_ou_pick:
        picks.append(high_ou_pick)
    dog_h2h_ou_pick = evaluate_dog_h2h_killer_ou(match_data)
    if dog_h2h_ou_pick:
        picks.append(dog_h2h_ou_pick)
    return picks


def scan_matches(matches: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    picks: List[Dict[str, Any]] = []
    for match in matches:
        picks.extend(evaluate_all(match))
    picks.sort(key=lambda item: (item.get("roi", 0), item.get("accuracy", 0)), reverse=True)
    return picks
