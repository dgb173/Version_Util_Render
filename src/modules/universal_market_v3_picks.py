"""Publica en precacheo los picks auditados del Motor Universal v3.

El entrenador genera ``data/universal_market_v3.json``. Este adaptador es
deliberadamente de solo lectura: nunca reentrena durante una petición web y
rechaza predicciones obsoletas si equipos o líneas ya no coinciden.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ALGORITHM = "UNIVERSAL_MARKET_V3_AUDITED"
DISPLAY_NAME = "Motor Universal v3 - Expectativas, Casa/Fuera y Rival Comun"
FLAT_ODDS = 1.90

_CACHE: Dict[str, Any] = {"path": None, "mtime": None, "payload": None, "index": {}}


def _float(value: Any) -> Optional[float]:
    try:
        return float(value) if value not in (None, "", "-") else None
    except (TypeError, ValueError):
        return None


def _normalized(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _model_path() -> Path:
    configured = os.getenv("UNIVERSAL_MARKET_V3_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[2] / "data" / "universal_market_v3.json"


def _load_model() -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    path = _model_path()
    try:
        mtime = path.stat().st_mtime_ns
    except OSError:
        return {}, {}
    if _CACHE["path"] == str(path) and _CACHE["mtime"] == mtime:
        return _CACHE["payload"] or {}, _CACHE["index"] or {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}, {}
    if payload.get("profile") != "universal_market_v3":
        return {}, {}
    index = {
        str(row.get("match_id")): row
        for row in payload.get("predictions", [])
        if isinstance(row, dict) and row.get("match_id") is not None
    }
    _CACHE.update({"path": str(path), "mtime": mtime, "payload": payload, "index": index})
    return payload, index


def _current_lines(match: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    ah = _float(odds.get("ah_linea"))
    if ah is None:
        ah = _float(match.get("handicap"))
    ou = _float(odds.get("goals_linea"))
    if ou is None:
        ou = _float(match.get("goals_line") or match.get("ou"))
    return ah, ou


def _same_market(match: Dict[str, Any], prediction: Dict[str, Any]) -> bool:
    home = match.get("home_name") or match.get("home_team")
    away = match.get("away_name") or match.get("away_team")
    if _normalized(home) != _normalized(prediction.get("home")):
        return False
    if _normalized(away) != _normalized(prediction.get("away")):
        return False
    ah, ou = _current_lines(match)
    pred_ah = _float(prediction.get("ah"))
    pred_ou = _float(prediction.get("ou"))
    if ah is None or pred_ah is None or abs(ah - pred_ah) > 0.01:
        return False
    if ou is not None and pred_ou is not None and abs(ou - pred_ou) > 0.01:
        return False
    return True


def _audit_metrics(node: Dict[str, Any]) -> Tuple[float, int, int, float]:
    audit = node.get("audit_tier") if isinstance(node.get("audit_tier"), dict) else {}
    bets = int(audit.get("bets") or 0)
    wins = int(audit.get("wins") or 0)
    hit_rate = float(audit.get("hit_rate") or 0.0)
    probability = hit_rate / 100.0
    roi = probability * FLAT_ODDS - 1.0
    return probability, bets, wins, roi


def _team_pick(match: Dict[str, Any], prediction: Dict[str, Any], node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    direction = node.get("pick")
    if direction not in {"FAVORITE", "DOG"}:
        return None
    ah, _ = _current_lines(match)
    if ah is None or abs(ah) < 0.01:
        return None
    favorite_target = "HOME" if ah > 0 else "AWAY"
    target = favorite_target if direction == "FAVORITE" else ("AWAY" if favorite_target == "HOME" else "HOME")
    home = match.get("home_name") or match.get("home_team") or "Local"
    away = match.get("away_name") or match.get("away_team") or "Visitante"
    team = home if target == "HOME" else away
    selected_is_favorite = target == favorite_target
    team_line = -abs(ah) if selected_is_favorite else abs(ah)
    line_label = "0" if abs(team_line) < 0.01 else f"{team_line:+.2f}"
    probability, bets, wins, roi = _audit_metrics(node)
    if bets < 20 or probability < 0.55:
        return None
    return {
        "name": f"{DISPLAY_NAME} - Handicap Asiatico",
        "algorithm": ALGORITHM,
        "type": "AH",
        "pick": "LOCAL" if target == "HOME" else "VISITA",
        "target": target,
        "match_id": match.get("match_id") or match.get("id"),
        "display_pick_label": f"{team} {line_label}",
        "accuracy": round(probability, 4),
        "roi": round(roi, 4),
        "roi_basis": f"ROI estimado con cuota plana {FLAT_ODDS:.2f}; no es ROI de cuotas historicas ejecutadas",
        "confidence": node.get("confidence") or "AUDITED",
        "prediction_tier": "AUDITED_PRODUCTION",
        "audit_bets": bets,
        "audit_wins": wins,
        "conditions_readable": [
            f"Acierto auditado: {probability * 100:.2f}% ({wins}/{bets})",
            f"ROI estimado @{FLAT_ODDS:.2f}: {roi * 100:+.2f}%",
            f"Reglas coincidentes: {int(node.get('supporting_rules') or 0)}",
        ],
        "perspective": "Linea AH, residuales, rival comun, posicion y rendimiento especifico casa/fuera",
        "explanation": "Pick aceptado por discovery, validacion, confirmacion y veto de auditoria temporal.",
        "engine_version": "universal_market_v3",
    }


def _goals_pick(match: Dict[str, Any], node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    direction = node.get("pick")
    if direction not in {"OVER", "UNDER"}:
        return None
    _, ou = _current_lines(match)
    if ou is None:
        return None
    probability, bets, wins, roi = _audit_metrics(node)
    if bets < 20 or probability < 0.55:
        return None
    return {
        "name": f"{DISPLAY_NAME} - Goles",
        "algorithm": ALGORITHM,
        "type": "OU",
        "pick": direction,
        "target": direction,
        "match_id": match.get("match_id") or match.get("id"),
        "display_pick_label": f"{direction} {ou:g}",
        "accuracy": round(probability, 4),
        "roi": round(roi, 4),
        "roi_basis": f"ROI estimado con cuota plana {FLAT_ODDS:.2f}; no es ROI de cuotas historicas ejecutadas",
        "confidence": node.get("confidence") or "AUDITED",
        "prediction_tier": "AUDITED_PRODUCTION",
        "audit_bets": bets,
        "audit_wins": wins,
        "conditions_readable": [
            f"Acierto auditado: {probability * 100:.2f}% ({wins}/{bets})",
            f"ROI estimado @{FLAT_ODDS:.2f}: {roi * 100:+.2f}%",
            f"Reglas coincidentes: {int(node.get('supporting_rules') or 0)}",
        ],
        "perspective": "Causa del O/U: ataque favorito, debilidad rival, ambos ataques o inflacion reciente",
        "explanation": "Pick aceptado por discovery, validacion, confirmacion y veto de auditoria temporal.",
        "engine_version": "universal_market_v3",
    }


def build_market_v3_picks(match: Dict[str, Any], *, payload: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Devuelve como maximo un AH y un O/U v3 que hayan pasado auditoria."""
    if payload is None:
        payload, index = _load_model()
    else:
        index = {str(row.get("match_id")): row for row in payload.get("predictions", []) if isinstance(row, dict)}
    if payload.get("profile") != "universal_market_v3":
        return []
    match_id = str(match.get("match_id") or match.get("id") or "")
    prediction = index.get(match_id)
    if not prediction or not _same_market(match, prediction):
        return []
    picks = []
    side = _team_pick(match, prediction, prediction.get("side") or {})
    goals = _goals_pick(match, prediction.get("goals") or {})
    if side:
        picks.append(side)
    if goals:
        picks.append(goals)
    return picks
