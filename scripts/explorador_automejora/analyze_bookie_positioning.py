#!/usr/bin/env python3
from __future__ import annotations

import argparse
from copy import deepcopy
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


PROFILE = "bookie_positioning_v1"


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text or text in {"-", "?", "N/A"}:
        return None
    if "/" in text:
        parts = text.split("/")
        if len(parts) == 2:
            left = _safe_float(parts[0])
            right = _safe_float(parts[1])
            if left is not None and right is not None:
                return (left + right) / 2.0
    try:
        return float(text)
    except ValueError:
        return None


def _parse_score(value: Any) -> Optional[Tuple[int, int]]:
    if value is None:
        return None
    text = str(value).strip().replace("-", ":")
    if "?" in text:
        return None
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0].strip()), int(parts[1].strip())
    except ValueError:
        return None


def _fmt(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    rounded = round(float(value), 2)
    if abs(rounded - int(rounded)) < 1e-9:
        return str(int(rounded))
    return f"{rounded:.2f}".rstrip("0").rstrip(".")


def _plain(value: Any) -> str:
    return str(value if value is not None else "").replace("→", "->")


def _same_team(left: Any, right: Any) -> bool:
    return bool(str(left).strip()) and str(left).strip().lower() == str(right).strip().lower()


def _team_on_home_column(home_team: Any, away_team: Any, team_name: str) -> Optional[bool]:
    if _same_team(home_team, team_name):
        return True
    if _same_team(away_team, team_name):
        return False
    return None


def _current_home_line(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    value = _safe_float(odds.get("ah_linea"))
    if value is not None:
        return value
    return _safe_float(match.get("handicap"))


def _current_ou_line(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    value = _safe_float(odds.get("goals_linea"))
    if value is not None:
        return value
    return _safe_float(match.get("goals_line"))


def _favorite_side(current_home_line: Optional[float]) -> str:
    if current_home_line is None or abs(float(current_home_line)) < 1e-9:
        return "PICKEM"
    return "HOME" if float(current_home_line) > 0 else "AWAY"


def _team_pressure_from_home_line(home_line: Optional[float], team_is_home: Optional[bool]) -> Optional[float]:
    if home_line is None or team_is_home is None:
        return None
    return float(home_line) if team_is_home else -float(home_line)


def _margin_for_team(score: Any, team_is_home: Optional[bool]) -> Optional[int]:
    parsed = _parse_score(score)
    if parsed is None or team_is_home is None:
        return None
    home_goals, away_goals = parsed
    return home_goals - away_goals if team_is_home else away_goals - home_goals


def _total_goals(score: Any) -> Optional[int]:
    parsed = _parse_score(score)
    if parsed is None:
        return None
    return parsed[0] + parsed[1]


def _normalize(value: float, scale: float) -> float:
    if scale <= 0:
        return 0.0
    return math.tanh(value / scale)


def _stat_value(row: Dict[str, Any], home_col: bool) -> Tuple[Optional[float], Optional[float]]:
    home_val = _safe_float(row.get("home"))
    away_val = _safe_float(row.get("away"))
    if home_val is None or away_val is None:
        return None, None
    team = home_val if home_col else away_val
    opponent = away_val if home_col else home_val
    return team, opponent


def _stats_edge(stats_rows: Any, team_on_home_column: Optional[bool]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "shots_diff": None,
        "sot_diff": None,
        "attacks_diff": None,
        "danger_diff": None,
        "score": 0.0,
        "verdict": "NO_STATS",
    }
    if team_on_home_column is None or not isinstance(stats_rows, list):
        return out

    score = 0.0
    for row in stats_rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label", "")).strip().lower()
        team_val, opp_val = _stat_value(row, team_on_home_column)
        if team_val is None or opp_val is None:
            continue
        diff = float(team_val) - float(opp_val)
        if "tiros a puerta" in label:
            out["sot_diff"] = diff
            score += 0.35 * _normalize(diff, 3.0)
        elif label == "tiros" or "shots" in label:
            out["shots_diff"] = diff
            score += 0.18 * _normalize(diff, 7.0)
        elif "ataques peligrosos" in label:
            out["danger_diff"] = diff
            score += 0.28 * _normalize(diff, 20.0)
        elif label == "ataques":
            out["attacks_diff"] = diff
            score += 0.12 * _normalize(diff, 35.0)

    out["score"] = round(score, 3)
    if score >= 0.30:
        out["verdict"] = "STRONG_FOR_TEAM"
    elif score >= 0.12:
        out["verdict"] = "LEAN_FOR_TEAM"
    elif score <= -0.30:
        out["verdict"] = "STRONG_AGAINST_TEAM"
    elif score <= -0.12:
        out["verdict"] = "LEAN_AGAINST_TEAM"
    else:
        out["verdict"] = "NEUTRAL"
    return out


def _residual_label(residual: Optional[float]) -> str:
    if residual is None:
        return "UNKNOWN"
    if residual >= 0.25:
        return "COVER"
    if residual <= -0.25:
        return "FAIL"
    return "PUSH_ZONE"


def _pressure_change_label(delta: Optional[float], then_pressure: Optional[float], now_pressure: Optional[float]) -> str:
    if delta is None or then_pressure is None or now_pressure is None:
        return "UNKNOWN"
    if float(now_pressure) > 0 and float(then_pressure) > 0:
        if float(delta) <= -0.25:
            return "LOWER_PRESSURE_KEEP_FAVORITE"
        if float(delta) >= 0.25:
            return "RAISE_PRESSURE_KEEP_FAVORITE"
        return "SAME_PRESSURE_KEEP_FAVORITE"
    if float(now_pressure) > 0 and float(then_pressure) <= 0:
        return "NEW_FAVORITE_STATUS"
    if float(now_pressure) <= 0 and float(then_pressure) > 0:
        return "FAVORITE_STATUS_REMOVED"
    return "NO_FAVORITE_PRESSURE"


def _rank(value: Any) -> Optional[int]:
    try:
        text = str(value).strip()
        if not text or text == "N/A":
            return None
        return int(float(text))
    except ValueError:
        return None


def _market_frame(match: Dict[str, Any]) -> Dict[str, Any]:
    current_home_line = _current_home_line(match)
    ou_line = _current_ou_line(match)
    fav_side = _favorite_side(current_home_line)
    home = str(match.get("home_name", "Local"))
    away = str(match.get("away_name", "Visitante"))
    favorite = home if fav_side == "HOME" else away if fav_side == "AWAY" else "Pick'em"
    dog = away if fav_side == "HOME" else home if fav_side == "AWAY" else "Pick'em"

    home_rank = _rank((match.get("home_standings") or {}).get("ranking"))
    away_rank = _rank((match.get("away_standings") or {}).get("ranking"))
    fav_rank = home_rank if fav_side == "HOME" else away_rank if fav_side == "AWAY" else None
    dog_rank = away_rank if fav_side == "HOME" else home_rank if fav_side == "AWAY" else None

    if fav_rank is None or dog_rank is None or fav_side == "PICKEM":
        table_positioning = "TABLE_UNKNOWN"
    elif fav_rank < dog_rank:
        table_positioning = "TABLE_ALIGNED"
    elif fav_rank > dog_rank:
        table_positioning = "LINE_AGAINST_TABLE"
    else:
        table_positioning = "TABLE_EQUAL"

    return {
        "home_line": current_home_line,
        "ou_line": ou_line,
        "fav_side": fav_side,
        "favorite": favorite,
        "non_favorite": dog,
        "home_rank": home_rank,
        "away_rank": away_rank,
        "favorite_rank": fav_rank,
        "non_favorite_rank": dog_rank,
        "table_positioning": table_positioning,
    }


def _h2h_case(match: Dict[str, Any], key: str, frame: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    home = str(match.get("home_name", ""))
    away = str(match.get("away_name", ""))
    current_home_line = frame.get("home_line")
    current_fav = str(frame.get("favorite", ""))
    current_fav_pressure = abs(float(current_home_line)) if current_home_line is not None else None

    market = match.get("market_analysis_data") if isinstance(match.get("market_analysis_data"), dict) else {}
    market_node = market.get(key) if isinstance(market.get(key), dict) else {}

    if key == "stadium":
        h2h = match.get("h2h_stadium") if isinstance(match.get("h2h_stadium"), dict) else {}
        hist_home = home
        hist_away = away
        hist_line = _safe_float(h2h.get("ah1"))
        score = market_node.get("result") or h2h.get("res1")
        date = market_node.get("date") or h2h.get("date1")
        stats_rows = h2h.get("stats_rows")
    elif key == "general":
        h2h = match.get("h2h_general") if isinstance(match.get("h2h_general"), dict) else {}
        hist_home = str(h2h.get("h2h_gen_home") or "")
        hist_away = str(h2h.get("h2h_gen_away") or "")
        hist_line = _safe_float(h2h.get("ah6") if h2h.get("ah6") is not None else h2h.get("ah1"))
        score = market_node.get("result") or h2h.get("res6") or h2h.get("res1")
        date = market_node.get("date") or h2h.get("date6") or h2h.get("date1")
        stats_rows = h2h.get("stats_rows")
    else:
        return None

    if not score or str(score) == "?:?":
        return None

    fav_on_home = _team_on_home_column(hist_home, hist_away, current_fav)
    fav_margin = _margin_for_team(score, fav_on_home)
    hist_fav_pressure = _team_pressure_from_home_line(hist_line, fav_on_home)
    residual_hist_line = (
        float(fav_margin) - float(hist_fav_pressure)
        if fav_margin is not None and hist_fav_pressure is not None
        else None
    )
    residual_current_line = (
        float(fav_margin) - float(current_fav_pressure)
        if fav_margin is not None and current_fav_pressure is not None
        else None
    )
    favorite_pressure_delta = (
        float(current_fav_pressure) - float(hist_fav_pressure)
        if current_fav_pressure is not None and hist_fav_pressure is not None
        else None
    )
    total = _total_goals(score)
    ou_line = frame.get("ou_line")
    ou_read = "UNKNOWN"
    if total is not None and ou_line is not None:
        if total > float(ou_line):
            ou_read = "OVER_CURRENT_LINE"
        elif total < float(ou_line):
            ou_read = "UNDER_CURRENT_LINE"
        else:
            ou_read = "PUSH_CURRENT_LINE"

    return {
        "key": key,
        "date": date,
        "home": hist_home,
        "away": hist_away,
        "score": score,
        "hist_home_line": hist_line,
        "movement_raw": market_node.get("movement"),
        "current_favorite": current_fav,
        "current_fav_on_home_col": fav_on_home,
        "current_fav_margin": fav_margin,
        "current_fav_pressure_now": current_fav_pressure,
        "current_fav_pressure_then": hist_fav_pressure,
        "favorite_pressure_delta": favorite_pressure_delta,
        "pressure_change_label": _pressure_change_label(
            favorite_pressure_delta,
            hist_fav_pressure,
            current_fav_pressure,
        ),
        "residual_hist_line": residual_hist_line,
        "residual_current_line": residual_current_line,
        "cover_hist_line": _residual_label(residual_hist_line),
        "cover_current_line": _residual_label(residual_current_line),
        "total_goals": total,
        "ou_read": ou_read,
        "stats_edge_for_current_fav": _stats_edge(stats_rows, fav_on_home),
    }


def _team_recent_case(block: Dict[str, Any], team_name: str, raw_line_key: str) -> Dict[str, Any]:
    home = str(block.get("home_team") or "")
    away = str(block.get("away_team") or "")
    team_on_home = _team_on_home_column(home, away, team_name)
    raw_line = _safe_float(block.get(raw_line_key))
    margin = _margin_for_team(block.get("score"), team_on_home)
    pressure = _team_pressure_from_home_line(raw_line, team_on_home)
    residual = float(margin) - float(pressure) if margin is not None and pressure is not None else None
    return {
        "home": home,
        "away": away,
        "score": block.get("score"),
        "date": block.get("date"),
        "team": team_name,
        "team_on_home_col": team_on_home,
        "home_line": raw_line,
        "team_pressure": pressure,
        "team_margin": margin,
        "residual": residual,
        "cover_label": _residual_label(residual),
        "total_goals": _total_goals(block.get("score")),
        "stats_edge": _stats_edge(block.get("stats_rows"), team_on_home),
    }


def _recent_cases(match: Dict[str, Any]) -> Dict[str, Any]:
    home = str(match.get("home_name", ""))
    away = str(match.get("away_name", ""))
    prev_home = match.get("last_home_match") if isinstance(match.get("last_home_match"), dict) else {}
    prev_away = match.get("last_away_match") if isinstance(match.get("last_away_match"), dict) else {}
    return {
        "prev_home": _team_recent_case(prev_home, home, "handicap_line_raw") if prev_home else None,
        "prev_away": _team_recent_case(prev_away, away, "handicap_line_raw") if prev_away else None,
    }


def _indirect_cases(match: Dict[str, Any]) -> Dict[str, Any]:
    home = str(match.get("home_name", ""))
    away = str(match.get("away_name", ""))
    ind = match.get("comparativas_indirectas") if isinstance(match.get("comparativas_indirectas"), dict) else {}
    left = ind.get("left") if isinstance(ind.get("left"), dict) else {}
    right = ind.get("right") if isinstance(ind.get("right"), dict) else {}

    out: Dict[str, Any] = {"left": None, "right": None, "col3": None}
    if left and left.get("score"):
        out["left"] = _team_recent_case(left, home, "ah_line")
    if right and right.get("score"):
        out["right"] = _team_recent_case(right, away, "ah_line")

    col3 = match.get("h2h_col3") if isinstance(match.get("h2h_col3"), dict) else {}
    if col3 and col3.get("status") == "found":
        score = f"{col3.get('goles_home')}:{col3.get('goles_away')}"
        home_team = str(col3.get("h2h_home_team_name") or "")
        away_team = str(col3.get("h2h_away_team_name") or "")
        out["col3"] = {
            "home": home_team,
            "away": away_team,
            "score": score,
            "date": col3.get("date"),
            "home_line": _safe_float(col3.get("handicap")),
            "total_goals": _total_goals(score),
            "stats_home_edge": _stats_edge(col3.get("stats_rows"), True),
        }
    return out


def _add_label(labels: List[Dict[str, Any]], label_id: str, title: str, weight: float, evidence: str) -> None:
    labels.append(
        {
            "id": label_id,
            "title": title,
            "weight": round(float(weight), 2),
            "evidence": evidence,
        }
    )


def _choose_base_h2h(h2h: Dict[str, Any]) -> Tuple[str, str]:
    stadium = h2h.get("stadium")
    general = h2h.get("general")
    if general and general.get("date"):
        return "general", "Se usa el H2H general mas reciente como mapa base del posicionamiento."
    if stadium:
        return "stadium", "Se usa el H2H de estadio porque es el precedente directo disponible."
    return "none", "No hay H2H util para fijar mapa base."


def _build_labels(
    frame: Dict[str, Any],
    h2h: Dict[str, Any],
    recent: Dict[str, Any],
    indirect: Dict[str, Any],
) -> List[Dict[str, Any]]:
    labels: List[Dict[str, Any]] = []

    if frame.get("table_positioning") == "LINE_AGAINST_TABLE":
        _add_label(
            labels,
            "LINEA_CONTRA_TABLA",
            "La casa contradice la clasificacion",
            1.10,
            f"{frame.get('favorite')} es favorito con ranking {frame.get('favorite_rank')} frente a {frame.get('non_favorite')} ranking {frame.get('non_favorite_rank')}.",
        )
    elif frame.get("table_positioning") == "TABLE_ALIGNED":
        _add_label(
            labels,
            "LINEA_ALINEADA_CON_TABLA",
            "La tabla no explica la trampa por si sola",
            0.45,
            f"El favorito de mercado tambien esta mejor clasificado: {frame.get('favorite_rank')} vs {frame.get('non_favorite_rank')}.",
        )

    h2h_blocks = [b for b in (h2h.get("stadium"), h2h.get("general")) if isinstance(b, dict)]
    failed_h2h = [b for b in h2h_blocks if b.get("cover_current_line") == "FAIL"]
    if failed_h2h:
        _add_label(
            labels,
            "H2H_CASTIGA_LINEA_ACTUAL",
            "El historial directo no cubre la exigencia actual",
            1.40,
            f"{len(failed_h2h)} precedente(s) H2H dejan residual negativo para {frame.get('favorite')} con la linea de hoy.",
        )

    kept_fav_after_fail = [
        b
        for b in h2h_blocks
        if b.get("cover_current_line") == "FAIL"
        and b.get("current_fav_pressure_now") is not None
        and float(b.get("current_fav_pressure_now")) > 0.0
    ]
    if kept_fav_after_fail:
        _add_label(
            labels,
            "FAVORITO_MANTENIDO_TRAS_FALLO_H2H",
            "La casa mantiene favorito al equipo que fallo el H2H",
            1.60,
            "El marcador directo no justificaba al favorito, pero la linea actual no le retira el estatus.",
        )

    raised_after_fail = [
        b
        for b in h2h_blocks
        if b.get("cover_current_line") == "FAIL"
        and b.get("favorite_pressure_delta") is not None
        and float(b.get("favorite_pressure_delta")) > 0.10
    ]
    if raised_after_fail:
        _add_label(
            labels,
            "REPRICE_AGRESIVO_CONTRA_RESULTADO",
            "La casa sube exigencia pese al fallo directo",
            1.50,
            "En al menos un precedente la exigencia sube hacia el favorito actual aunque el H2H no le daba el marcador.",
        )

    volume_amnesty = [
        b
        for b in h2h_blocks
        if b.get("cover_current_line") == "FAIL"
        and (b.get("stats_edge_for_current_fav") or {}).get("score", 0) >= 0.30
    ]
    if volume_amnesty:
        _add_label(
            labels,
            "VOLUMEN_PERDONA_RESULTADO",
            "El mercado puede estar corrigiendo marcador con volumen",
            1.55,
            "Hay H2H donde el favorito actual no gano/cubrio, pero la produccion estadistica fue claramente suya.",
        )

    stadium = h2h.get("stadium")
    general = h2h.get("general")
    if isinstance(stadium, dict) and isinstance(general, dict):
        st_score = (stadium.get("stats_edge_for_current_fav") or {}).get("score", 0)
        gen_score = (general.get("stats_edge_for_current_fav") or {}).get("score", 0)
        if st_score <= -0.12 and gen_score >= 0.30:
            _add_label(
                labels,
                "H2H_PARTIDO_DIVIDIDO",
                "Estadio y H2H general cuentan historias opuestas",
                1.30,
                "En el estadio el favorito actual fue peor, pero en el H2H general tuvo volumen fuerte.",
            )

    ou_line = frame.get("ou_line")
    if ou_line is not None and float(ou_line) <= 2.25:
        _add_label(
            labels,
            "OU_CAPADO",
            "La casa limita el techo de goles",
            1.20,
            f"O/U actual {_fmt(ou_line)}: el mercado no esta vendiendo partido roto.",
        )
        h2h_under = [b for b in h2h_blocks if b.get("ou_read") in {"UNDER_CURRENT_LINE", "PUSH_CURRENT_LINE"}]
        if h2h_under:
            _add_label(
                labels,
                "MEMORIA_H2H_UNDER",
                "El total se apoya en memoria directa fria",
                1.25,
                f"{len(h2h_under)} H2H quedan por debajo o en push respecto al O/U actual.",
            )

    prev_away = recent.get("prev_away")
    if isinstance(prev_away, dict) and prev_away.get("total_goals") is not None and ou_line is not None:
        if int(prev_away["total_goals"]) >= 4 and float(ou_line) <= 2.25:
            _add_label(
                labels,
                "GOLEADA_NO_PERSEGUIDA",
                "La casa no persigue la ultima goleada",
                1.35,
                f"{frame.get('favorite')} viene de partido de {prev_away['total_goals']} goles, pero el total sigue en {_fmt(ou_line)}.",
            )

    fav_recent = recent.get("prev_home") if frame.get("fav_side") == "HOME" else recent.get("prev_away")
    if isinstance(fav_recent, dict):
        stats_score = (fav_recent.get("stats_edge") or {}).get("score", 0)
        residual = fav_recent.get("residual")
        if residual is not None and float(residual) >= 1.0 and stats_score <= -0.12:
            _add_label(
                labels,
                "RESULTADO_MEJOR_QUE_PROCESO",
                "El favorito llega con resultado fuerte pero volumen discutible",
                1.15,
                "La previa cubre de sobra, pero las estadisticas no sostienen una superioridad limpia.",
            )

    ind_right = indirect.get("right")
    if frame.get("fav_side") == "AWAY" and isinstance(ind_right, dict):
        stats_score = (ind_right.get("stats_edge") or {}).get("score", 0)
        if stats_score <= -0.30:
            _add_label(
                labels,
                "INDIRECTA_DEBILITA_FAVORITO",
                "La indirecta contradice al favorito actual",
                1.25,
                f"{frame.get('favorite')} aparece dominado en la indirecta contra {ind_right.get('home') or ind_right.get('away')}.",
            )

    col3 = indirect.get("col3")
    if isinstance(col3, dict) and col3.get("total_goals") is not None and ou_line is not None:
        if int(col3["total_goals"]) <= float(ou_line):
            _add_label(
                labels,
                "COL3_ENFRIA_TOTAL",
                "El espejo Col3 tambien enfria el total",
                0.80,
                f"El espejo {col3.get('home')} vs {col3.get('away')} termina {col3.get('score')}.",
            )

    return sorted(labels, key=lambda item: item["weight"], reverse=True)


def _build_narrative(
    frame: Dict[str, Any],
    h2h: Dict[str, Any],
    recent: Dict[str, Any],
    indirect: Dict[str, Any],
    labels: List[Dict[str, Any]],
) -> Dict[str, Any]:
    base_key, base_reason = _choose_base_h2h(h2h)
    base = h2h.get(base_key) if base_key != "none" else None
    why_yes: List[str] = []
    why_no: List[str] = []
    ou_notes: List[str] = []
    training_rule: List[str] = []

    if isinstance(base, dict):
        move = base.get("movement_to_current_fav")
        pressure_delta = base.get("favorite_pressure_delta")
        residual = base.get("residual_current_line")
        stats_score = (base.get("stats_edge_for_current_fav") or {}).get("score", 0)
        why_yes.append(
            f"Mapa base {base_key}: {base.get('home')} {base.get('score')} {base.get('away')}; "
            f"cambio de presion del favorito={_fmt(pressure_delta)}, residual con linea actual={_fmt(residual)}."
        )
        if stats_score >= 0.30:
            why_yes.append(
                "Aunque el marcador no cubra, el volumen del H2H base favorece al favorito actual; esa es la correccion contraintuitiva posible."
            )
        elif stats_score <= -0.12:
            why_no.append(
                "El H2H base no solo falla por marcador: tambien deja volumen contrario al favorito actual."
            )

    if frame.get("table_positioning") == "TABLE_ALIGNED":
        why_yes.append("La clasificacion acompana al favorito de mercado, asi que la linea no depende solo de memoria H2H.")
    elif frame.get("table_positioning") == "LINE_AGAINST_TABLE":
        why_yes.append("La linea va contra tabla: si la casa insiste, esta comprando matchup, localia o memoria especifica.")

    fav_recent = recent.get("prev_home") if frame.get("fav_side") == "HOME" else recent.get("prev_away")
    if isinstance(fav_recent, dict):
        if fav_recent.get("cover_label") == "COVER":
            why_yes.append("La previa del favorito cubre la expectativa de mercado, pero debe validarse por volumen.")
        if (fav_recent.get("stats_edge") or {}).get("score", 0) <= -0.12:
            why_no.append("La previa del favorito puede ser eficiente, no dominante: resultado por encima del proceso.")

    for block_key in ("stadium", "general"):
        block = h2h.get(block_key)
        if isinstance(block, dict) and block.get("cover_current_line") == "FAIL":
            why_no.append(
                f"H2H {block_key} no cubre la linea actual para {frame.get('favorite')}: residual {_fmt(block.get('residual_current_line'))}."
            )

    ind_right = indirect.get("right")
    if isinstance(ind_right, dict) and (ind_right.get("stats_edge") or {}).get("score", 0) <= -0.30:
        why_no.append("La indirecta del visitante muestra resistencia en marcador, pero con dominio estadistico recibido.")

    ou_line = frame.get("ou_line")
    if ou_line is not None and float(ou_line) <= 2.25:
        ou_notes.append(f"O/U {_fmt(ou_line)} obliga a leer margen corto, empate y push; no valida automaticamente un guion de over.")
    h2h_totals = [
        b.get("total_goals")
        for b in (h2h.get("stadium"), h2h.get("general"))
        if isinstance(b, dict) and b.get("total_goals") is not None
    ]
    if h2h_totals and ou_line is not None:
        ou_notes.append(f"Totales H2H contra linea actual: {', '.join(str(x) for x in h2h_totals)} vs OU {_fmt(ou_line)}.")

    label_ids = {item["id"] for item in labels}
    if "GOLEADA_NO_PERSEGUIDA" in label_ids:
        training_rule.append(
            "Si hay goleada reciente pero OU sigue en 2/2.25, marcar GOLEADA_NO_PERSEGUIDA: la casa separa resultado reciente de total esperado."
        )
    if "VOLUMEN_PERDONA_RESULTADO" in label_ids:
        training_rule.append(
            "Si el H2H no cubre pero el volumen fue del favorito actual, marcar VOLUMEN_PERDONA_RESULTADO: posible correccion del marcador."
        )
    if "FAVORITO_MANTENIDO_TRAS_FALLO_H2H" in label_ids:
        training_rule.append(
            "Si un equipo falla el H2H pero sigue favorito, no leerlo como pick: leerlo como posicionamiento que necesita soporte en volumen, tabla o indirectas."
        )
    if "REPRICE_AGRESIVO_CONTRA_RESULTADO" in label_ids:
        training_rule.append(
            "Si ademas la exigencia sube tras un H2H fallido, marcar repricing agresivo: la casa esta forzando una lectura contraria al marcador."
        )
    if "OU_CAPADO" in label_ids:
        training_rule.append(
            "Con AH corto y OU capado, el sistema debe narrar proteccion de empate/margen corto antes que recomendacion de handicap."
        )

    return {
        "base_map": {
            "key": base_key,
            "reason": base_reason,
            "case": base,
        },
        "why_bookie_can_place_it": why_yes,
        "why_it_is_counterintuitive": why_no,
        "ou_positioning": ou_notes,
        "trainable_rules": training_rule,
    }


def analyze_positioning(match: Dict[str, Any]) -> Dict[str, Any]:
    frame = _market_frame(match)
    h2h = {
        "stadium": _h2h_case(match, "stadium", frame),
        "general": _h2h_case(match, "general", frame),
    }
    recent = _recent_cases(match)
    indirect = _indirect_cases(match)
    labels = _build_labels(frame, h2h, recent, indirect)
    narrative = _build_narrative(frame, h2h, recent, indirect, labels)

    return {
        "profile": PROFILE,
        "match": {
            "match_id": str(match.get("match_id", "")),
            "home": match.get("home_name"),
            "away": match.get("away_name"),
            "league": match.get("league_name"),
            "date": match.get("match_date"),
            "time": match.get("time"),
        },
        "market_frame": frame,
        "h2h": h2h,
        "recent": recent,
        "indirect": indirect,
        "labels": labels,
        "narrative": narrative,
        "note": "Sistema descriptivo de posicionamiento. No emite pick ni recomendacion de apuesta.",
    }


def _apply_score_overrides(
    match: Dict[str, Any],
    *,
    h2h_general_score: str = "",
    h2h_stadium_score: str = "",
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    cloned = deepcopy(match)
    applied: Dict[str, str] = {}

    def _set_market_result(node_key: str, score: str) -> None:
        market = cloned.setdefault("market_analysis_data", {})
        if isinstance(market, dict):
            node = market.setdefault(node_key, {})
            if isinstance(node, dict):
                node["result"] = score

    general_score = str(h2h_general_score or "").strip()
    if general_score:
        _set_market_result("general", general_score)
        h2h_general = cloned.setdefault("h2h_general", {})
        if isinstance(h2h_general, dict):
            h2h_general["res6"] = general_score
            h2h_general["res6_raw"] = general_score.replace(":", "-")
        applied["h2h_general_score"] = general_score

    stadium_score = str(h2h_stadium_score or "").strip()
    if stadium_score:
        _set_market_result("stadium", stadium_score)
        h2h_stadium = cloned.setdefault("h2h_stadium", {})
        if isinstance(h2h_stadium, dict):
            h2h_stadium["res1"] = stadium_score
            h2h_stadium["res1_raw"] = stadium_score.replace(":", "-")
        applied["h2h_stadium_score"] = stadium_score

    return cloned, applied


def _select_match(rows: List[Dict[str, Any]], match_id: str, team_query: str) -> Dict[str, Any]:
    if match_id:
        for row in rows:
            if str(row.get("match_id")) == str(match_id):
                return row
        raise SystemExit(f"[ERROR] match_id no encontrado: {match_id}")

    query = team_query.strip().lower()
    if not query:
        raise SystemExit("[ERROR] Usa --match-id o --team-query.")
    candidates = [
        row
        for row in rows
        if query in str(row.get("home_name", "")).lower()
        or query in str(row.get("away_name", "")).lower()
    ]
    if not candidates:
        raise SystemExit(f"[ERROR] Sin candidatos para: {team_query}")
    return candidates[0]


def _render_markdown(payload: Dict[str, Any]) -> str:
    match = payload["match"]
    frame = payload["market_frame"]
    narrative = payload["narrative"]
    base = narrative["base_map"]

    lines = [
        f"# Bookie Positioning: {match.get('home')} vs {match.get('away')}",
        "",
        f"- Match ID: {match.get('match_id')}",
        f"- Liga: {match.get('league')}",
        f"- Fecha: {match.get('date')} {match.get('time') or ''}".strip(),
        f"- AH actual: {_fmt(frame.get('home_line'))} | Favorito: {frame.get('favorite')}",
        f"- O/U actual: {_fmt(frame.get('ou_line'))}",
        f"- Lectura tabla: {frame.get('table_positioning')} ({frame.get('favorite_rank')} vs {frame.get('non_favorite_rank')})",
    ]
    if payload.get("overrides"):
        override_txt = ", ".join(f"{key}={value}" for key, value in payload["overrides"].items())
        lines.append(f"- Overrides manuales: {override_txt}")
    lines.extend(
        [
            "",
            "## Escenario Base",
            "",
            f"- Bloque: **{base.get('key')}**",
            f"- Motivo: {base.get('reason')}",
        ]
    )

    case = base.get("case")
    if isinstance(case, dict):
        lines.extend(
            [
                f"- Partido base: {case.get('home')} {case.get('score')} {case.get('away')} ({case.get('date')})",
                f"- Presion del favorito entonces: {_fmt(case.get('current_fav_pressure_then'))}",
                f"- Presion del favorito ahora: {_fmt(case.get('current_fav_pressure_now'))}",
                f"- Movimiento bruto H2H: {_plain(case.get('movement_raw') or 'N/A')}",
                f"- Cambio de presion del favorito actual: {_fmt(case.get('favorite_pressure_delta'))}",
                f"- Lectura del cambio: {case.get('pressure_change_label')}",
                f"- Residual con linea actual: {_fmt(case.get('residual_current_line'))} ({case.get('cover_current_line')})",
                f"- Stats edge favorito actual: {case.get('stats_edge_for_current_fav', {}).get('verdict')} ({case.get('stats_edge_for_current_fav', {}).get('score')})",
            ]
        )

    lines.extend(["", "## Etiquetas Detectadas", ""])
    if payload["labels"]:
        for item in payload["labels"]:
            lines.append(f"- **{item['id']}** ({item['weight']}): {item['title']}. {item['evidence']}")
    else:
        lines.append("- Sin etiqueta fuerte: mercado sin anomalia clara con los datos disponibles.")

    lines.extend(["", "## Por Que Si", ""])
    if narrative["why_bookie_can_place_it"]:
        lines.extend(f"- {text}" for text in narrative["why_bookie_can_place_it"])
    else:
        lines.append("- No hay soporte claro para explicar la colocacion desde los bloques disponibles.")

    lines.extend(["", "## Por Que No", ""])
    if narrative["why_it_is_counterintuitive"]:
        lines.extend(f"- {text}" for text in narrative["why_it_is_counterintuitive"])
    else:
        lines.append("- No aparecen frenos fuertes; revisar si faltan indirectas o H2H.")

    lines.extend(["", "## O/U En El Mapa", ""])
    if narrative["ou_positioning"]:
        lines.extend(f"- {text}" for text in narrative["ou_positioning"])
    else:
        lines.append("- El total no aporta una senal estructural clara.")

    lines.extend(["", "## Reglas Entrenables", ""])
    if narrative["trainable_rules"]:
        lines.extend(f"- {text}" for text in narrative["trainable_rules"])
    else:
        lines.append("- Caso util como observacion, pero sin regla nueva automatica.")

    lines.extend(["", "_Salida descriptiva: no contiene pick ni recomendacion de apuesta._", ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Detecta patrones de posicionamiento de bookies sin emitir picks.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--match-id", default="")
    parser.add_argument("--team-query", default="")
    parser.add_argument("--output-md", default="")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--override-h2h-general-score", default="")
    parser.add_argument("--override-h2h-stadium-score", default="")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    precache_path = project_root / "data" / "data_precacheo.json"
    if not precache_path.exists():
        raise SystemExit(f"[ERROR] No existe {precache_path}")

    rows = json.loads(precache_path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise SystemExit("[ERROR] data_precacheo.json no es una lista.")
    match = _select_match(rows, args.match_id, args.team_query)
    match, overrides = _apply_score_overrides(
        match,
        h2h_general_score=args.override_h2h_general_score,
        h2h_stadium_score=args.override_h2h_stadium_score,
    )
    payload = analyze_positioning(match)
    payload["overrides"] = overrides
    markdown = _render_markdown(payload)

    if args.output_json:
        out_json = Path(args.output_json)
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] JSON: {out_json}")
    if args.output_md:
        out_md = Path(args.output_md)
        out_md.write_text(markdown, encoding="utf-8")
        print(f"[OK] Markdown: {out_md}")

    print(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
