from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text or text == "-":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_score(value: Any) -> Optional[Tuple[int, int]]:
    if value is None:
        return None
    text = str(value).strip().replace("-", ":")
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0].strip()), int(parts[1].strip())
    except ValueError:
        return None


def _fmt_float(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    rounded = round(float(value), 2)
    if abs(rounded - int(rounded)) < 1e-9:
        return str(int(rounded))
    return f"{rounded:.2f}".rstrip("0").rstrip(".")


def _line_text_from_home_perspective(home_line: Optional[float]) -> str:
    if home_line is None:
        return "N/A"
    if abs(home_line) < 1e-9:
        return "0"
    if home_line > 0:
        return f"Home -{_fmt_float(home_line)}"
    return f"Away -{_fmt_float(abs(home_line))}"


def _parse_movement(value: Any) -> Tuple[Optional[float], Optional[float]]:
    if value is None:
        return None, None
    text = str(value).replace("->", "→")
    parts = [p.strip() for p in text.split("→")]
    if len(parts) != 2:
        return None, None
    return _safe_float(parts[0]), _safe_float(parts[1])


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _normalize_strength(value: float, scale: float = 0.75) -> float:
    return math.tanh(value / scale)


def _same_team(a: Any, b: Any) -> bool:
    return bool(str(a).strip()) and str(a).strip().lower() == str(b).strip().lower()


def _node_identity(node: Any) -> str:
    if not isinstance(node, dict):
        return ""
    keys = (
        "match1_id",
        "match6_id",
        "match_id",
        "date1",
        "date6",
        "date",
        "res1",
        "score",
        "ah1",
        "ah_line",
        "h2h_gen_home",
        "h2h_gen_away",
    )
    return "|".join(str(node.get(key, "")).strip() for key in keys)


def _same_h2h_precedent(node_a: Any, node_b: Any) -> bool:
    ident_a = _node_identity(node_a)
    ident_b = _node_identity(node_b)
    return bool(ident_a and ident_b and ident_a == ident_b)


def _sum_stats_edge(stats_rows: Any, current_team_on_home_column: bool) -> float:
    if not isinstance(stats_rows, list):
        return 0.0

    score = 0.0
    for row in stats_rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label", "")).lower().strip()
        home_val = _safe_float(row.get("home"))
        away_val = _safe_float(row.get("away"))
        if home_val is None or away_val is None:
            continue
        team_val = home_val if current_team_on_home_column else away_val
        opp_val = away_val if current_team_on_home_column else home_val
        diff = team_val - opp_val

        if "tiros a puerta" in label:
            score += 0.22 * _normalize_strength(diff, scale=3.0)
        elif "ataques peligrosos" in label:
            score += 0.18 * _normalize_strength(diff, scale=18.0)
        elif label == "tiros":
            score += 0.10 * _normalize_strength(diff, scale=7.0)
        elif label == "ataques":
            score += 0.08 * _normalize_strength(diff, scale=25.0)
    return score


@dataclass
class BlockContribution:
    block: str
    side: str
    weight: float
    raw_value: float
    contribution: float
    detail: str


def _team_line_from_perspective(raw_line: Optional[float], team_is_home: bool) -> Optional[float]:
    if raw_line is None:
        return None
    return raw_line if team_is_home else -raw_line


def _margin_for_team(score: Any, team_is_home: bool) -> Optional[int]:
    parsed = _parse_score(score)
    if parsed is None:
        return None
    hg, ag = parsed
    return hg - ag if team_is_home else ag - hg


def _add_residual_block(
    out: List[BlockContribution],
    *,
    block: str,
    team_side: str,
    residual: Optional[float],
    weight: float,
    detail: str,
) -> None:
    if residual is None:
        return
    contribution = weight * _normalize_strength(float(residual))
    out.append(
        BlockContribution(
            block=block,
            side=team_side,
            weight=weight,
            raw_value=float(residual),
            contribution=contribution,
            detail=detail,
        )
    )


def _add_direct_value_block(
    out: List[BlockContribution],
    *,
    block: str,
    side: str,
    value: float,
    weight: float,
    detail: str,
) -> None:
    contribution = weight * _clamp(value, -1.0, 1.0)
    out.append(
        BlockContribution(
            block=block,
            side=side,
            weight=weight,
            raw_value=float(value),
            contribution=contribution,
            detail=detail,
        )
    )


def _current_home_line(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds")
    if isinstance(odds, dict):
        value = _safe_float(odds.get("ah_linea"))
        if value is not None:
            return value
    return _safe_float(match.get("handicap"))


def _current_ou_line(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds")
    if isinstance(odds, dict):
        value = _safe_float(odds.get("goals_linea"))
        if value is not None:
            return value
    return _safe_float(match.get("goals_line"))


def _build_match_index(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        match_id = str(row.get("match_id") or "").strip()
        if match_id:
            out[match_id] = row
    return out


def _select_match(rows: List[Dict[str, Any]], team_query: str = "", match_id: str = "") -> Dict[str, Any]:
    if match_id:
        for row in rows:
            if str(row.get("match_id")) == str(match_id):
                return row
        raise SystemExit(f"[ERROR] match_id no encontrado: {match_id}")

    query = team_query.strip().lower()
    if not query:
        raise SystemExit("[ERROR] Debes enviar --match-id o --team-query.")

    candidates = [
        row
        for row in rows
        if query in str(row.get("home_name", "")).lower() or query in str(row.get("away_name", "")).lower()
    ]
    if not candidates:
        raise SystemExit(f"[ERROR] team-query sin candidatos: {team_query}")
    if len(candidates) == 1:
        return candidates[0]

    best = None
    best_score = -1
    for row in candidates:
        score = 0
        home = str(row.get("home_name", "")).lower()
        away = str(row.get("away_name", "")).lower()
        if query == home or query == away:
            score += 5
        if home.startswith(query) or away.startswith(query):
            score += 2
        if score > best_score:
            best = row
            best_score = score
    if best is None:
        raise SystemExit(f"[ERROR] no pude resolver el partido para: {team_query}")
    return best


def _h2h_from_market_view(
    score: Any,
    movement: Any,
    current_home_line: Optional[float],
    *,
    score_from_home_perspective: bool = True,
) -> Dict[str, Optional[float]]:
    parsed = _parse_score(score)
    hist_home_line, current_line_from_text = _parse_movement(movement)
    margin = None
    if parsed is not None:
        hg, ag = parsed
        margin = (hg - ag) if score_from_home_perspective else (ag - hg)

    residual_current = None
    movement_to_home = None
    line_now = current_home_line if current_home_line is not None else current_line_from_text
    if margin is not None and line_now is not None:
        residual_current = float(margin) - float(line_now)
    if line_now is not None and hist_home_line is not None:
        movement_to_home = float(line_now) - float(hist_home_line)
    return {
        "margin": margin,
        "hist_home_line": hist_home_line,
        "residual_current": residual_current,
        "movement_to_home": movement_to_home,
    }


def _team_block_residual(score: Any, raw_line: Optional[float], team_is_home: bool) -> Dict[str, Optional[float]]:
    margin = _margin_for_team(score, team_is_home)
    team_line = _team_line_from_perspective(raw_line, team_is_home)
    residual = None
    if margin is not None and team_line is not None:
        residual = float(margin) - float(team_line)
    return {
        "margin": margin,
        "team_line": team_line,
        "residual": residual,
    }


def _home_oriented_hist_line(
    raw_line: Optional[float],
    hist_home_team: str,
    hist_away_team: str,
    current_home_name: str,
) -> Optional[float]:
    if raw_line is None or not current_home_name:
        return None
    if _same_team(current_home_name, hist_home_team):
        return raw_line
    if _same_team(current_home_name, hist_away_team):
        return -raw_line
    return None


def _current_team_on_home_column(
    hist_home_team: str,
    hist_away_team: str,
    current_team_name: str,
) -> Optional[bool]:
    if _same_team(current_team_name, hist_home_team):
        return True
    if _same_team(current_team_name, hist_away_team):
        return False
    return None


def _draw_risk(match: Dict[str, Any], diagnostics: Dict[str, Any]) -> float:
    ah = abs(float(diagnostics["current_home_line"])) if diagnostics["current_home_line"] is not None else 0.0
    ou = diagnostics["ou_line"]
    risk = 0.16
    if ah <= 0.01:
        risk += 0.24
    elif ah <= 0.25:
        risk += 0.18
    elif ah <= 0.5:
        risk += 0.10

    if ou is not None and ou <= 2.25:
        risk += 0.12
    elif ou is not None and ou <= 2.5:
        risk += 0.08

    push_count = 0
    for block_key in ("prev_home", "prev_away", "h2h_stadium", "h2h_general"):
        block = diagnostics.get(block_key)
        if not isinstance(block, dict):
            continue
        residual = block.get("residual") if "residual" in block else block.get("residual_current")
        if residual is not None and abs(float(residual)) <= 0.10:
            push_count += 1
    risk += 0.04 * push_count

    triang = (((match.get("market_analysis_data") or {}) if isinstance(match.get("market_analysis_data"), dict) else {}).get("triangulacion"))
    if isinstance(triang, dict) and str(triang.get("etiqueta_inversa")).lower() == "true":
        risk += 0.05
    if isinstance(triang, dict) and "empate" in str(triang.get("diagnostico", "")).lower():
        risk += 0.05
    return _clamp(risk, 0.0, 0.85)


def _diag_residual(block: Any) -> Optional[float]:
    if not isinstance(block, dict):
        return None
    if block.get("residual") is not None:
        return _safe_float(block.get("residual"))
    return _safe_float(block.get("residual_current"))


def _conservative_favorite_line(
    current_home_line: float,
    diagnostics: Dict[str, Any],
) -> Optional[float]:
    ah_abs = abs(float(current_home_line))
    ou = diagnostics.get("ou_line")
    if ah_abs < 0.75 or ou is None or float(ou) > 2.75:
        return None

    if (
        diagnostics.get("repricing_jump_to_favorite")
        or diagnostics.get("underdog_same_line_fail")
        or diagnostics.get("favorite_harder_line_volume")
    ):
        return None

    if current_home_line > 0:
        fav_recent_residual = _diag_residual(diagnostics.get("prev_home"))
    else:
        fav_recent_residual = _diag_residual(diagnostics.get("prev_away"))
    if fav_recent_residual is None or fav_recent_residual > 0.25:
        return None

    h2h_bad = 0
    for block_key in ("h2h_stadium", "h2h_general"):
        residual = _diag_residual(diagnostics.get(block_key))
        if residual is not None and residual <= -0.25:
            h2h_bad += 1
    if h2h_bad < 2:
        return None

    has_indirect_support = any(
        _diag_residual(diagnostics.get(block_key)) is not None
        for block_key in ("ind_left", "ind_right")
    )
    if has_indirect_support:
        return None

    return max(0.5, ah_abs - 0.25)


def _recommended_bet(
    *,
    home_name: str,
    away_name: str,
    current_home_line: Optional[float],
    edge: float,
    draw_risk: float,
    diagnostics: Dict[str, Any],
) -> Tuple[str, str]:
    if current_home_line is None:
        return "NO BET", "Sin linea AH actual."

    if abs(current_home_line) <= 0.01:
        if edge >= 0.90:
            return f"{home_name} 0", "Linea en 0 y la suma de residuos favorece al local."
        if edge <= -0.90:
            return f"{away_name} 0", "Linea en 0 y la suma de residuos favorece al visitante."
        return "NO BET", "Pick'em con ventaja insuficiente."

    if current_home_line > 0:
        if edge >= 1.15 and draw_risk <= 0.45:
            conservative_line = _conservative_favorite_line(current_home_line, diagnostics)
            if conservative_line is not None and conservative_line < current_home_line:
                return (
                    f"{home_name} -{_fmt_float(conservative_line)}",
                    "Hay sesgo local, pero el H2H y el perfil de goles apuntan a victoria corta; mejor bajar un cuarto la exigencia.",
                )
            return f"{home_name} -{_fmt_float(current_home_line)}", "El favorito local sostiene la linea actual."
        if edge >= 0.45:
            return f"{home_name} 0", "Hay sesgo local, pero la linea corta aconseja version conservadora."
        if edge <= -0.70:
            return f"{away_name} +{_fmt_float(abs(current_home_line))}", "La linea favorece al local, pero los residuos castigan al favorito."
        return "NO BET", "La evidencia no alcanza para atacar una linea corta."

    away_line = abs(current_home_line)
    if edge <= -1.15 and draw_risk <= 0.45:
        conservative_line = _conservative_favorite_line(current_home_line, diagnostics)
        if conservative_line is not None and conservative_line < away_line:
            return (
                f"{away_name} -{_fmt_float(conservative_line)}",
                "Hay sesgo visitante, pero el H2H y el perfil de goles apuntan a victoria corta; mejor bajar un cuarto la exigencia.",
            )
        return f"{away_name} -{_fmt_float(away_line)}", "El favorito visitante sostiene la linea actual."
    if edge <= -0.45:
        return f"{away_name} 0", "Hay sesgo visitante, pero la linea corta aconseja version conservadora."
    if edge >= 0.70:
        return f"{home_name} +{_fmt_float(away_line)}", "La linea favorece al visitante, pero los residuos castigan al favorito."
    return "NO BET", "La evidencia no alcanza para atacar la linea visitante."


def analyze_match(match: Dict[str, Any]) -> Dict[str, Any]:
    home_name = str(match.get("home_name", ""))
    away_name = str(match.get("away_name", ""))
    current_home_line = _current_home_line(match)
    ou_line = _current_ou_line(match)

    contribs: List[BlockContribution] = []

    prev_home = match.get("last_home_match") if isinstance(match.get("last_home_match"), dict) else {}
    prev_away = match.get("last_away_match") if isinstance(match.get("last_away_match"), dict) else {}
    h2h_stadium = match.get("h2h_stadium") if isinstance(match.get("h2h_stadium"), dict) else {}
    h2h_general = match.get("h2h_general") if isinstance(match.get("h2h_general"), dict) else {}
    market_data = match.get("market_analysis_data") if isinstance(match.get("market_analysis_data"), dict) else {}
    market_stadium = market_data.get("stadium") if isinstance(market_data.get("stadium"), dict) else {}
    market_general = market_data.get("general") if isinstance(market_data.get("general"), dict) else {}
    ind = match.get("comparativas_indirectas") if isinstance(match.get("comparativas_indirectas"), dict) else {}
    ind_left = ind.get("left") if isinstance(ind.get("left"), dict) else {}
    ind_right = ind.get("right") if isinstance(ind.get("right"), dict) else {}
    duplicate_h2h = _same_h2h_precedent(h2h_stadium, h2h_general)

    prev_home_diag = _team_block_residual(
        prev_home.get("score"),
        _safe_float(prev_home.get("handicap_line_raw")),
        True,
    )
    prev_home_stats_edge = _sum_stats_edge(prev_home.get("stats_rows"), True)
    _add_residual_block(
        contribs,
        block="prev_home",
        team_side="HOME",
        residual=prev_home_diag.get("residual"),
        weight=1.15,
        detail=f"{home_name} previa casa residual={_fmt_float(prev_home_diag.get('residual'))}",
    )
    _add_direct_value_block(
        contribs,
        block="prev_home_stats",
        side="HOME",
        value=prev_home_stats_edge,
        weight=0.55,
        detail="Soporte estadistico del local en su previa de casa",
    )

    prev_away_diag = _team_block_residual(
        prev_away.get("score"),
        _safe_float(prev_away.get("handicap_line_raw")),
        False,
    )
    prev_away_stats_edge = _sum_stats_edge(prev_away.get("stats_rows"), False)
    _add_residual_block(
        contribs,
        block="prev_away",
        team_side="AWAY",
        residual=prev_away_diag.get("residual"),
        weight=1.15,
        detail=f"{away_name} previa fuera residual={_fmt_float(prev_away_diag.get('residual'))}",
    )
    _add_direct_value_block(
        contribs,
        block="prev_away_stats",
        side="AWAY",
        value=prev_away_stats_edge,
        weight=0.55,
        detail="Soporte estadistico del visitante en su previa fuera",
    )

    stadium_current_home_on_hist_home = _current_team_on_home_column(
        str(h2h_stadium.get("h2h_gen_home", "")),
        str(h2h_stadium.get("h2h_gen_away", "")),
        home_name,
    )
    h2h_stadium_diag = _h2h_from_market_view(
        score=market_stadium.get("result") or h2h_stadium.get("res1"),
        movement=market_stadium.get("movement") or (
            f"{h2h_stadium.get('ah1')} → {current_home_line}" if h2h_stadium.get("ah1") is not None else None
        ),
        current_home_line=current_home_line,
        score_from_home_perspective=stadium_current_home_on_hist_home is not False,
    )
    _add_residual_block(
        contribs,
        block="h2h_stadium",
        team_side="HOME",
        residual=h2h_stadium_diag.get("residual_current"),
        weight=2.40,
        detail="Precedente en este estadio reexpresado con la linea actual",
    )
    if h2h_stadium_diag.get("movement_to_home") is not None:
        _add_direct_value_block(
            contribs,
            block="h2h_stadium_movement",
            side="HOME",
            value=_normalize_strength(float(h2h_stadium_diag["movement_to_home"]), scale=0.75),
            weight=1.00,
            detail=f"Movimiento de linea estadio hacia local={_fmt_float(h2h_stadium_diag.get('movement_to_home'))}",
        )
    _add_direct_value_block(
        contribs,
        block="h2h_stadium_stats",
        side="HOME",
        value=_sum_stats_edge(h2h_stadium.get("stats_rows"), stadium_current_home_on_hist_home is not False),
        weight=0.50,
        detail="Soporte estadistico H2H estadio",
    )

    general_current_home_on_hist_home = _current_team_on_home_column(
        str(h2h_general.get("h2h_gen_home", "")),
        str(h2h_general.get("h2h_gen_away", "")),
        home_name,
    )
    h2h_general_diag = _h2h_from_market_view(
        score=market_general.get("result") or h2h_general.get("res1"),
        movement=market_general.get("movement") or (
            f"{h2h_general.get('ah1')} → {current_home_line}" if h2h_general.get("ah1") is not None else None
        ),
        current_home_line=current_home_line,
        score_from_home_perspective=general_current_home_on_hist_home is not False,
    )
    general_oriented_line = _home_oriented_hist_line(
        _safe_float(h2h_general.get("ah1")),
        str(h2h_general.get("h2h_gen_home", "")),
        str(h2h_general.get("h2h_gen_away", "")),
        home_name,
    )
    if current_home_line is not None and general_oriented_line is not None:
        h2h_general_diag["hist_home_line"] = general_oriented_line
        h2h_general_diag["movement_to_home"] = float(current_home_line) - float(general_oriented_line)
    general_residual_weight = 1.90 if not duplicate_h2h else 0.95
    general_movement_weight = 1.15 if not duplicate_h2h else 0.55
    general_stats_weight = 0.45 if not duplicate_h2h else 0.20

    _add_residual_block(
        contribs,
        block="h2h_general",
        team_side="HOME",
        residual=h2h_general_diag.get("residual_current"),
        weight=general_residual_weight,
        detail="H2H general reexpresado con la linea actual",
    )
    if h2h_general_diag.get("movement_to_home") is not None:
        _add_direct_value_block(
            contribs,
            block="h2h_general_movement",
            side="HOME",
            value=_normalize_strength(float(h2h_general_diag["movement_to_home"]), scale=0.75),
            weight=general_movement_weight,
            detail=(
                f"Movimiento de linea general hacia local={_fmt_float(h2h_general_diag.get('movement_to_home'))}"
                + (" (H2H duplicado: peso reducido)" if duplicate_h2h else "")
            ),
        )
    _add_direct_value_block(
        contribs,
        block="h2h_general_stats",
        side="HOME",
        value=_sum_stats_edge(h2h_general.get("stats_rows"), general_current_home_on_hist_home is not False),
        weight=general_stats_weight,
        detail="Soporte estadistico H2H general",
    )

    ind_left_diag = _team_block_residual(
        ind_left.get("score"),
        _safe_float(ind_left.get("ah_line")),
        str(ind_left.get("localia", "")).upper() != "A",
    )
    _add_residual_block(
        contribs,
        block="ind_left",
        team_side="HOME",
        residual=ind_left_diag.get("residual"),
        weight=1.55,
        detail=f"{home_name} vs rival comun {ind_left.get('rival_name', '-')}",
    )
    _add_direct_value_block(
        contribs,
        block="ind_left_stats",
        side="HOME",
        value=_sum_stats_edge(ind_left.get("stats_rows"), str(ind_left.get("localia", "")).upper() != "A"),
        weight=0.45,
        detail="Soporte estadistico del local contra rival comun",
    )

    ind_right_diag = _team_block_residual(
        ind_right.get("score"),
        _safe_float(ind_right.get("ah_line")),
        str(ind_right.get("localia", "")).upper() != "A",
    )
    _add_residual_block(
        contribs,
        block="ind_right",
        team_side="AWAY",
        residual=ind_right_diag.get("residual"),
        weight=1.55,
        detail=f"{away_name} vs rival comun {ind_right.get('rival_name', '-')}",
    )
    _add_direct_value_block(
        contribs,
        block="ind_right_stats",
        side="AWAY",
        value=_sum_stats_edge(ind_right.get("stats_rows"), str(ind_right.get("localia", "")).upper() != "A"),
        weight=0.45,
        detail="Soporte estadistico del visitante contra rival comun",
    )

    repricing_jump_to_favorite = False
    underdog_same_line_fail = False
    favorite_harder_line_volume = False
    if current_home_line is not None and abs(float(current_home_line)) >= 0.75:
        favorite_side = "HOME" if current_home_line > 0 else "AWAY"
        favorite_recent_diag = prev_home_diag if favorite_side == "HOME" else prev_away_diag
        favorite_recent_stats = prev_home_stats_edge if favorite_side == "HOME" else prev_away_stats_edge
        underdog_recent_diag = prev_away_diag if favorite_side == "HOME" else prev_home_diag

        movement_to_home = _safe_float(h2h_general_diag.get("movement_to_home"))
        if movement_to_home is not None:
            if (current_home_line > 0 and movement_to_home >= 1.25) or (current_home_line < 0 and movement_to_home <= -1.25):
                repricing_jump_to_favorite = True
                _add_direct_value_block(
                    contribs,
                    block="repricing_jump",
                    side=favorite_side,
                    value=_normalize_strength(abs(float(movement_to_home)), scale=1.25),
                    weight=1.25,
                    detail="Salto fuerte de mercado hacia el favorito actual frente al H2H previo.",
                )

        underdog_recent_line = _safe_float(abs(float(underdog_recent_diag.get("team_line")))) if underdog_recent_diag.get("team_line") is not None else None
        underdog_recent_residual = _diag_residual(underdog_recent_diag)
        if (
            underdog_recent_line is not None
            and underdog_recent_residual is not None
            and abs(float(underdog_recent_line) - abs(float(current_home_line))) <= 0.25
            and float(underdog_recent_residual) <= -0.75
        ):
            underdog_same_line_fail = True
            _add_direct_value_block(
                contribs,
                block="underdog_same_line_fail",
                side=favorite_side,
                value=_normalize_strength(abs(float(underdog_recent_residual)), scale=0.75),
                weight=1.15,
                detail="El no favorito ya fallo una linea igual o muy parecida a la actual.",
            )

        favorite_recent_line = _safe_float(abs(float(favorite_recent_diag.get("team_line")))) if favorite_recent_diag.get("team_line") is not None else None
        if (
            favorite_recent_line is not None
            and favorite_recent_stats is not None
            and float(favorite_recent_line) - abs(float(current_home_line)) >= 1.50
            and float(favorite_recent_stats) >= 0.35
        ):
            favorite_harder_line_volume = True
            _add_direct_value_block(
                contribs,
                block="favorite_harder_line_volume",
                side=favorite_side,
                value=0.85,
                weight=1.05,
                detail="El favorito ya compitio una linea mucho mas dura y dejo dominio estadistico reutilizable.",
            )

    home_score = sum(c.contribution for c in contribs if c.side == "HOME")
    away_score = sum(c.contribution for c in contribs if c.side == "AWAY")
    edge = home_score - away_score

    diagnostics = {
        "current_home_line": current_home_line,
        "ou_line": ou_line,
        "prev_home": prev_home_diag,
        "prev_away": prev_away_diag,
        "h2h_stadium": h2h_stadium_diag,
        "h2h_general": h2h_general_diag,
        "ind_left": ind_left_diag,
        "ind_right": ind_right_diag,
        "duplicate_h2h": duplicate_h2h,
        "prev_home_stats_edge": prev_home_stats_edge,
        "prev_away_stats_edge": prev_away_stats_edge,
        "repricing_jump_to_favorite": repricing_jump_to_favorite,
        "underdog_same_line_fail": underdog_same_line_fail,
        "favorite_harder_line_volume": favorite_harder_line_volume,
    }
    draw_risk = _draw_risk(match, diagnostics)
    recommended_bet, bet_reason = _recommended_bet(
        home_name=home_name,
        away_name=away_name,
        current_home_line=current_home_line,
        edge=edge,
        draw_risk=draw_risk,
        diagnostics=diagnostics,
    )

    confidence = "BAJA"
    abs_edge = abs(edge)
    if abs_edge >= 2.8 and draw_risk <= 0.38:
        confidence = "ALTA"
    elif abs_edge >= 1.4:
        confidence = "MEDIA"

    summary = (
        f"Linea actual {_line_text_from_home_perspective(current_home_line)} | "
        f"score_home={home_score:.2f} score_away={away_score:.2f} edge={edge:.2f}. "
        f"DrawRisk={draw_risk*100.0:.1f}%. Pick={recommended_bet}."
    )

    top = sorted(contribs, key=lambda c: abs(c.contribution), reverse=True)
    return {
        "match": {
            "match_id": str(match.get("match_id", "")),
            "home": home_name,
            "away": away_name,
            "league": match.get("league_name"),
            "kickoff": f"{match.get('match_date', '')} {match.get('time', '')}".strip(),
            "ah_home_line": current_home_line,
            "ah_text": _line_text_from_home_perspective(current_home_line),
            "ou_line": ou_line,
        },
        "scores": {
            "home_score": round(home_score, 3),
            "away_score": round(away_score, 3),
            "edge_home_minus_away": round(edge, 3),
            "draw_risk": round(draw_risk, 4),
        },
        "pick": {
            "recommended_bet": recommended_bet,
            "confidence": confidence,
            "reason": bet_reason,
        },
        "blocks": [
            {
                "block": c.block,
                "side": c.side,
                "weight": round(c.weight, 2),
                "raw_value": round(c.raw_value, 3),
                "contribution": round(c.contribution, 3),
                "detail": c.detail,
            }
            for c in top
        ],
        "diagnostics": {
            "prev_home": prev_home_diag,
            "prev_away": prev_away_diag,
            "h2h_stadium": h2h_stadium_diag,
            "h2h_general": h2h_general_diag,
            "ind_left": ind_left_diag,
            "ind_right": ind_right_diag,
        },
        "summary_text": summary,
    }


def _to_markdown(payload: Dict[str, Any]) -> str:
    match = payload["match"]
    scores = payload["scores"]
    pick = payload["pick"]

    lines = [
        f"# Handicap Math: {match['home']} vs {match['away']}",
        "",
        f"- Match ID: {match['match_id']}",
        f"- Liga: {match.get('league', '-')}",
        f"- Hora detectada: {match.get('kickoff', '-')}",
        f"- AH actual: {match['ah_text']}",
        f"- OU actual: {_fmt_float(match.get('ou_line'))}",
        "",
        "## Veredicto",
        "",
        f"- Pick: **{pick['recommended_bet']}**",
        f"- Confianza: **{pick['confidence']}**",
        f"- Motivo: {pick['reason']}",
        f"- Edge Home-Away: **{scores['edge_home_minus_away']}**",
        f"- Riesgo de empate: **{round(scores['draw_risk'] * 100.0, 1)}%**",
        "",
        "## Bloques Mas Influyentes",
        "",
        "| Bloque | Lado | Valor | Contribucion | Lectura |",
        "|---|---|---:|---:|---|",
    ]
    for row in payload["blocks"][:10]:
        lines.append(
            f"| {row['block']} | {row['side']} | {row['raw_value']} | {row['contribution']} | {row['detail']} |"
        )

    diag = payload["diagnostics"]
    lines.extend(
        [
            "",
            "## Residuales",
            "",
            f"- Prev Home residual: { _fmt_float(diag['prev_home'].get('residual')) }",
            f"- Prev Away residual: { _fmt_float(diag['prev_away'].get('residual')) }",
            f"- H2H estadio residual con linea actual: { _fmt_float(diag['h2h_stadium'].get('residual_current')) }",
            f"- H2H general residual con linea actual: { _fmt_float(diag['h2h_general'].get('residual_current')) }",
            f"- Indirecta local residual: { _fmt_float(diag['ind_left'].get('residual')) }",
            f"- Indirecta visitante residual: { _fmt_float(diag['ind_right'].get('residual')) }",
            "",
            "## Lectura Corta",
            "",
            payload["summary_text"],
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Razonamiento matematico de handicap a partir de precacheo.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--team-query", default="")
    parser.add_argument("--match-id", default="")
    parser.add_argument("--output-md", default="")
    parser.add_argument("--output-json", default="")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    precache_path = project_root / "data" / "data_precacheo.json"
    if not precache_path.exists():
        raise SystemExit(f"[ERROR] No existe {precache_path}")

    rows = json.loads(precache_path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise SystemExit("[ERROR] data_precacheo.json no es una lista.")

    match = _select_match(rows, team_query=args.team_query, match_id=args.match_id)
    payload = analyze_match(match)
    md = _to_markdown(payload)

    if args.output_json:
        out_json = Path(args.output_json)
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] JSON: {out_json}")
    if args.output_md:
        out_md = Path(args.output_md)
        out_md.write_text(md, encoding="utf-8")
        print(f"[OK] Markdown: {out_md}")

    print(
        "[INFO] "
        f"target={payload['match']['home']} vs {payload['match']['away']} "
        f"pick={payload['pick']['recommended_bet']} "
        f"edge={payload['scores']['edge_home_minus_away']} "
        f"draw_risk={round(payload['scores']['draw_risk'] * 100.0, 1)}%"
    )


if __name__ == "__main__":
    main()
