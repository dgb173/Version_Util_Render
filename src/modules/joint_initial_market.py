"""Lectura conjunta de las líneas AH y O/U iniciales, sin inventar precios ni xG."""

from __future__ import annotations

from typing import Any, Dict
import re
import math
from datetime import datetime


def _number(value: Any):
    try:
        if value is None or str(value).strip() in {"", "-", "N/A", "?"}:
            return None
        text = str(value).strip().replace(",", ".")
        if "/" in text:
            a, b = text.split("/", 1)
            result = (float(a) + float(b)) / 2
        else:
            result = float(text)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _date(value):
    text = str(value or '').strip()[:10]
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%d-%m-%Y'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def _recent_goal_residuals(match: Dict[str, Any], limit: int = 10) -> Dict[str, Any]:
    """Goles reales menos línea O/U histórica; no equivale a goles menos xG."""
    current = ((match.get("pre_match_context") or {}).get("current") or {})
    match_date = _date(match.get('match_date') or current.get('date'))
    output = {}
    for role, key in (("home", "home_matches"), ("away", "away_matches")):
        values = []
        valid_rows = []
        seen = set()
        for row in (current.get(key) or []):
            row_date = _date(row.get('date'))
            if not match_date or not row_date or row_date >= match_date:
                continue
            identity = row.get('matchIndex') or row.get('match_id') or (row.get('date'), row.get('home'), row.get('away'))
            if identity in seen:
                continue
            seen.add(identity)
            valid_rows.append(row)
        valid_rows.sort(key=lambda row: _date(row.get('date')) or datetime.min.date(), reverse=True)
        for row in valid_rows[:limit]:
            score = re.fullmatch(r"(\d+)\s*[:-]\s*(\d+)", str(row.get("score") or row.get("score_raw") or ""))
            line = _number(row.get("ouLine"))
            if line is None:
                line = _number(row.get("over_under_line_raw"))
            if score and line is not None:
                values.append(int(score.group(1)) + int(score.group(2)) - line)
        output[role] = {
            "sample": len(values),
            "mean_goals_minus_initial_ou": round(sum(values) / len(values), 3) if values else None,
        }
    return output


def classify_joint_initial_market(
    match: Dict[str, Any], *, edge_ah: float = 0, edge_ou: float = 0,
    quality_eligible: bool = False,
) -> Dict[str, Any]:
    """Clasifica ambos mercados siempre que existan líneas y marca imposibilidades lógicas.

    La dirección forzada es descriptiva: nunca representa una apuesta rentable sin
    cuotas iniciales de ambos lados y validación fuera de muestra.
    """
    odds = match.get("main_match_odds") or {}
    stage = str(odds.get('odds_stage') or 'unverified').lower()
    if stage not in {'initial', 'unverified'}:
        return {'available': False, 'reason': 'El método requiere líneas iniciales, no posteriores'}
    ah = _number(odds.get("ah_linea"))
    ou = _number(odds.get("goals_linea"))
    if ah is None or ou is None or abs(ah) > 20 or not 0 < ou <= 30 or any(abs(v * 4 - round(v * 4)) > 1e-8 for v in (ah, ou)):
        return {"available": False, "reason": "Faltan líneas AH/O/U iniciales"}

    ah_side = "FAV_CUBRE" if edge_ah >= 0 else "DOG_CUBRE"
    ou_side = "OVER" if edge_ou >= 0 else "UNDER"
    # AH=0 carece de favorito; la etiqueta usa el local como referencia.
    ah_reference = "HOME" if ah == 0 else "HOME" if ah > 0 else "AWAY"
    max_goals = max(16, int(abs(ah) + ou + 6))

    def compatible(side_ah: str, side_ou: str) -> bool:
        for home_goals in range(max_goals + 1):
            for away_goals in range(max_goals + 1):
                fav_margin = (home_goals - away_goals) if ah >= 0 else (away_goals - home_goals)
                ah_ok = fav_margin > abs(ah) if side_ah == "FAV_CUBRE" else fav_margin < abs(ah)
                total = home_goals + away_goals
                ou_ok = total > ou if side_ou == "OVER" else total < ou
                if ah_ok and ou_ok:
                    return True
        return False

    initial_compatible = compatible(ah_side, ou_side)
    original_ah, original_ou = ah_side, ou_side
    if not initial_compatible:
        # Se conserva la dirección AH y se corrige O/U solo como clasificación
        # obligatoria. Una hipótesis deportiva no debe contener dos sucesos
        # mutuamente imposibles.
        alternative_ou = "UNDER" if ou_side == "OVER" else "OVER"
        if compatible(ah_side, alternative_ou):
            ou_side = alternative_ou
        else:
            ah_side = "DOG_CUBRE" if ah_side == "FAV_CUBRE" else "FAV_CUBRE"

    return {
        "available": True,
        "ah_initial": ah,
        "ou_initial": ou,
        "initial_provenance": stage,
        "ah_reference": ah_reference,
        "forced_ah": ah_side,
        "forced_ou": ou_side,
        "original_ah": original_ah,
        "original_ou": original_ou,
        "forced_tie_ah": edge_ah == 0,
        "forced_tie_ou": edge_ou == 0,
        "initial_joint_compatible": initial_compatible,
        "joint_status": "COMPATIBLE" if initial_compatible else "LOGICAL_CONFLICT_CORRECTED",
        "reliability": "LOW" if not initial_compatible or not quality_eligible or stage != 'initial' else "UNVALIDATED",
        "xg_status": "NOT_AVAILABLE",
        "historical_ou_residuals": _recent_goal_residuals(match),
        "roi_status": "NOT_CALCULABLE_WITHOUT_RECORDED_INITIAL_PRICES",
    }
