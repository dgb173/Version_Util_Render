"""Validación compartida de marcadores finales con o sin hándicap."""

import re


_FINAL_SCORE_RE = re.compile(r"^\d+\s*-\s*\d+$")
_MISSING_HANDICAP_VALUES = {"", "-", "n/a", "null", "none"}


def validate_finished_result(score_text, handicap_raw):
    """Devuelve marcador/AH validados o ``None`` si no hay resultado final."""
    score = str(score_text or "").strip()
    if not _FINAL_SCORE_RE.fullmatch(score):
        return None

    handicap = str(handicap_raw or "").strip()
    result_only = handicap.lower() in _MISSING_HANDICAP_VALUES
    return {
        "score": score,
        "handicap": "N/A" if result_only else handicap,
        "result_only": result_only,
    }
