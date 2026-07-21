from typing import Any, Dict, Iterable, List, Optional, Tuple

from .lexington_pattern import (
    _asian_category,
    _cover_from_market_line,
    _cover_against_current_fav_line,
    _name_matches,
    _opponent_name,
    _parse_float,
    _parse_int,
    _parse_score,
    _rank_gap,
    _stats_edge,
    _team_side,
    _wdl,
)


def _h2h_entries(match_data: Dict[str, Any], home_name: str, away_name: str) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    stadium = match_data.get("h2h_stadium") or {}
    if stadium.get("res1") and "?" not in str(stadium.get("res1")):
        entries.append({
            "kind": "estadio",
            "home_team": home_name,
            "away_team": away_name,
            "score": stadium.get("res1"),
            "ah": _parse_float(stadium.get("ah1")),
            "stats_rows": stadium.get("stats_rows") or [],
        })

    general = match_data.get("h2h_general") or {}
    if general.get("res6") and "?" not in str(general.get("res6")):
        entries.append({
            "kind": "general",
            "home_team": general.get("h2h_gen_home") or general.get("home_team") or home_name,
            "away_team": general.get("h2h_gen_away") or general.get("away_team") or away_name,
            "score": general.get("res6"),
            "ah": _parse_float(general.get("ah6")),
            "stats_rows": general.get("stats_rows") or [],
        })
    return entries


def _market_keeps_or_raises(current_h: float, previous_ah: Optional[float]) -> bool:
    if previous_ah is None:
        return False
    return current_h >= abs(previous_ah) - 0.01


def _col3_fails_for_dog_reference(
    col3: Dict[str, Any],
    dog_prev_opp: str,
    fav_prev_opp: str,
    current_h: float,
) -> Tuple[bool, str]:
    if not col3 or col3.get("status") != "found":
        return False, "sin Col3 espejo"
    if not dog_prev_opp or not fav_prev_opp:
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
    if _team_side(col3_match, dog_prev_opp) is None or _team_side(col3_match, fav_prev_opp) is None:
        return False, "Col3 no conecta los rivales previos"

    dog_ref_cover = _cover_from_market_line(col3_match, dog_prev_opp)
    if abs(ah) >= current_h and dog_ref_cover != "COVER":
        return True, f"{dog_prev_opp} no cubre AH {abs(ah):.2f} ante {fav_prev_opp} en Col3"
    return False, "Col3 no debilita la referencia del dog"


def _indirect_favorite_cover(match_data: Dict[str, Any], fav_name: str, dog_prev_opp: str) -> Tuple[bool, str]:
    left = (match_data.get("comparativas_indirectas") or {}).get("left") or {}
    if not left or not dog_prev_opp:
        return False, "sin indirecta local"
    if _team_side(left, fav_name) is None or not (
        _name_matches(dog_prev_opp, left.get("home_team")) or _name_matches(dog_prev_opp, left.get("away_team"))
    ):
        return False, "indirecta local no conecta al rival fuerte del dog"
    cover = _cover_from_market_line(left, fav_name)
    if cover == "COVER":
        return True, f"{fav_name} cubre indirecta ante {dog_prev_opp}"
    return False, f"{fav_name} no cubre indirecta ante {dog_prev_opp}"


def evaluate_match(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Patron favorito por proceso oculto.

    Siempre desde el favorito: el mercado mantiene una linea media aunque el H2H
    de marcador parezca negativo, porque el favorito ya cubrio una linea mas dura,
    el no favorito llega sin cover limpio, y las estadisticas/indirectas sostienen
    que el favorito tenia mas proceso que marcador.
    """
    odds = match_data.get("main_match_odds") or {}
    ah_raw = _parse_float(match_data.get("handicap") or odds.get("ah_linea"))
    if ah_raw is None or abs(ah_raw) < 0.5 or abs(ah_raw) > 1.25:
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

    current_h = abs(ah_raw)
    conditions: List[str] = []
    score = 0

    rank_gap = _rank_gap(match_data, fav_is_home)
    if rank_gap is not None:
        if rank_gap < 0 or rank_gap > 8:
            return None
        conditions.append(f"Favorito por tabla moderada, gap ranking {rank_gap}")
        score += 1

    fav_prev_cover = _cover_from_market_line(fav_prev, fav_name)
    fav_prev_ah = _parse_float(fav_prev.get("handicap_line_raw"))
    if fav_prev_cover != "COVER" or fav_prev_ah is None or abs(fav_prev_ah) < current_h:
        return None
    score += 2
    conditions.append(f"Favorito cubrio una linea mas dura previa: AH {abs(fav_prev_ah):.2f}")

    fav_prev_opp = _opponent_name(fav_prev, fav_name)
    if _stats_edge(fav_prev, fav_name, fav_prev_opp) < 2:
        return None
    score += 1
    conditions.append("Favorito confirma cover previo con ventaja estadistica")

    dog_prev_cover = _cover_from_market_line(dog_prev, dog_name)
    if dog_prev_cover == "COVER":
        return None
    score += 1
    conditions.append(f"No favorito llega sin cover limpio en su previa ({dog_prev_cover})")

    dog_prev_opp = _opponent_name(dog_prev, dog_name)
    if _stats_edge(dog_prev, dog_name, dog_prev_opp) > -2:
        return None
    score += 1
    conditions.append("No favorito fue inferior en estadisticas de su previa")

    h2h_support = []
    for entry in _h2h_entries(match_data, home_name, away_name):
        fav_cover_now = _cover_against_current_fav_line(entry, fav_name, current_h)
        fav_stats_edge = _stats_edge(entry, fav_name, dog_name)
        market_ok = _market_keeps_or_raises(current_h, entry.get("ah"))
        if fav_cover_now != "COVER" and fav_stats_edge >= 2 and market_ok:
            h2h_support.append(entry)

    if not h2h_support:
        return None
    score += 2
    strongest_h2h = h2h_support[0]
    conditions.append(
        f"H2H {strongest_h2h['kind']} no cubrio por marcador, pero el mercado mantiene/sube y las stats sostienen al favorito"
    )

    col3_ok, col3_reason = _col3_fails_for_dog_reference(
        match_data.get("h2h_col3") or {},
        dog_prev_opp,
        fav_prev_opp,
        current_h,
    )
    if not col3_ok:
        return None
    score += 1
    conditions.append(col3_reason)

    ind_ok, ind_reason = _indirect_favorite_cover(match_data, fav_name, dog_prev_opp)
    if ind_ok:
        score += 1
        conditions.append(ind_reason)

    confidence = min(0.77, 0.56 + score * 0.025)
    roi = max(0.0, confidence * 1.90 - 1.0)
    fav_side = "HOME" if fav_is_home else "AWAY"
    fav_pick = "LOCAL" if fav_is_home else "VISITANTE"
    signed_line = f"-{current_h:.2f}"

    return {
        "name": "[Favorito Proceso] Mercado sostiene al favorito",
        "pick": fav_pick,
        "target": fav_side,
        "type": "AH",
        "match_id": match_data.get("match_id") or match_data.get("id"),
        "accuracy": round(confidence, 3),
        "roi": round(roi, 3),
        "n_train": 0,
        "algorithm": "FAV_PROCESS",
        "perspective": "Favorito por proceso oculto",
        "favorite": fav_name,
        "underdog": dog_name,
        "handicap": current_h,
        "display_pick_label": f"{fav_name} {signed_line}",
        "conditions_readable": conditions,
        "explanation": (
            f"Patron favorito por proceso oculto: {fav_name} sostiene AH {signed_line}. "
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
