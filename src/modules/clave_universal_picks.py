"""Adaptador unico de la Clave Dicotomica para la columna Picks.

La UI y el endpoint batch no deben mezclar motores con criterios incompatibles.
Este modulo publica exclusivamente predicciones que hayan superado las puertas
de produccion de ``clave_dicotomica.apply_key``. Las lecturas OBSERVATION y
NO_BET permanecen en el contrato detallado de la clave, pero no se convierten
en apuestas.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .clave_dicotomica import apply_key, parse_ah
from .universal_market_v3_picks import build_market_v3_picks
from .league_evolution_learning import build_learning_picks


ALGORITHM = "CLAVE_DICOTOMICA_UNIVERSAL"


def _same_team(left: Any, right: Any) -> bool:
    def normalized(value: Any) -> str:
        return "".join(ch for ch in str(value or "").lower() if ch.isalnum())

    a = normalized(left)
    b = normalized(right)
    return bool(a and b and (a == b or a in b or b in a))


def _team_target(match: Dict[str, Any], team_name: str) -> Optional[str]:
    home = match.get("home_name") or match.get("home_team") or ""
    away = match.get("away_name") or match.get("away_team") or ""
    if _same_team(team_name, home):
        return "HOME"
    if _same_team(team_name, away):
        return "AWAY"
    return None


def _display_team_line(match: Dict[str, Any], target: str) -> str:
    odds = match.get("main_match_odds") or {}
    raw = parse_ah(odds.get("ah_linea"))
    if raw is None:
        raw = parse_ah(match.get("handicap"))
    magnitude = abs(raw or 0.0)
    home_favorite = bool(raw is not None and raw > 0)
    selected_is_favorite = (target == "HOME") == home_favorite if magnitude else False
    team_line = -magnitude if selected_is_favorite else magnitude
    if magnitude < 0.01:
        return "0"
    return f"{team_line:+.2f}"


def _confidence_probability(value: str, *, market: str) -> float:
    """Valor conservador solo para ordenar; no se presenta como garantia."""
    if value == "LOW":
        return 0.54
    if market == "OU":
        return 0.60 if value == "HIGH" else 0.57
    return 0.62 if value == "HIGH" else 0.58


def build_universal_picks(match: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Devuelve como maximo un pick AH y uno O/U, ambos de produccion."""
    result = apply_key(match)
    match_id = match.get("match_id") or match.get("id")
    picks: List[Dict[str, Any]] = []

    ah_pick = result.get("ah")
    ah_tier = result.get("prediction_tier_ah")
    if ah_tier in {"PRODUCTION", "PRODUCTION_EXPANSION"} and ah_pick in {"FAV_CUBRE", "DOG_CUBRE"}:
        selected_team = result.get("fav") if ah_pick == "FAV_CUBRE" else result.get("dog")
        target = _team_target(match, str(selected_team or ""))
        if target:
            confidence = str(result.get("confidence_ah") or "MEDIUM")
            probability = _confidence_probability(confidence, market="AH")
            line = _display_team_line(match, target)
            rules = list(result.get("production_ah_rules") or [])
            bookie = result.get("bookie_detector") or {}
            rules.extend(bookie.get("aligned_signals") or [])
            picks.append(
                {
                    "name": "Clave Dicotomica Universal - Handicap",
                    "algorithm": ALGORITHM,
                    "type": "AH",
                    "pick": "LOCAL" if target == "HOME" else "VISITA",
                    "target": target,
                    "match_id": match_id,
                    "display_pick_label": f"{selected_team} {line}",
                    "accuracy": probability,
                    "roi": round(probability * 1.90 - 1.0, 3),
                    "confidence": confidence,
                    "prediction_tier": ah_tier,
                    "is_expansion": ah_tier == "PRODUCTION_EXPANSION",
                    "expansion_rule": result.get("expansion_ah_rule"),
                    "edge": result.get("edge_AH", 0),
                    "quality": result.get("quality") or {},
                    "conditions_readable": rules,
                    "perspective": "Linea asiatica, residuales H2H, previos, indirectas y Col3",
                    "explanation": " | ".join((result.get("argumentos") or [])[-3:]),
                    "engine_version": result.get("engine_version"),
                    "bookie_confirmation": result.get("bookie_confirmation"),
                    "col3_direction": bookie.get("col3_direction"),
                    "col3_agrees": bookie.get("col3_agrees"),
                }
            )

    ou_pick = result.get("ou")
    if result.get("prediction_tier_ou") == "PRODUCTION" and ou_pick in {"OVER", "UNDER"}:
        confidence = str(result.get("confidence_ou") or "MEDIUM")
        probability = _confidence_probability(confidence, market="OU")
        odds = match.get("main_match_odds") or {}
        ou_line = parse_ah(odds.get("goals_linea"))
        line_label = f" {ou_line:g}" if ou_line is not None else ""
        rules = list(result.get("production_ou_rules") or [])
        picks.append(
            {
                "name": "Clave Dicotomica Universal - Goles",
                "algorithm": ALGORITHM,
                "type": "OU",
                "pick": ou_pick,
                "target": ou_pick,
                "match_id": match_id,
                "display_pick_label": f"{ou_pick}{line_label}",
                "accuracy": probability,
                "roi": round(probability * 1.90 - 1.0, 3),
                "confidence": confidence,
                "edge": result.get("edge_OU", 0),
                "quality": result.get("quality") or {},
                "conditions_readable": rules,
                "perspective": "Linea O/U contrastada con H2H, previos, indirectas y Col3",
                "explanation": " | ".join((result.get("argumentos") or [])[-3:]),
                "engine_version": result.get("engine_version"),
            }
        )

    picks.extend(build_market_v3_picks(match))
    picks.extend(build_learning_picks(match))
    return picks
