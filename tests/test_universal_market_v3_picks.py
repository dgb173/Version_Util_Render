import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from modules.universal_market_v3_picks import ALGORITHM, build_market_v3_picks  # noqa: E402


def _match():
    return {
        "match_id": "42",
        "home_name": "Equipo Local",
        "away_name": "Equipo Visitante",
        "main_match_odds": {"ah_linea": "0.5", "goals_linea": "3.25"},
    }


def _payload():
    return {
        "profile": "universal_market_v3",
        "predictions": [{
            "match_id": "42", "home": "Equipo Local", "away": "Equipo Visitante", "ah": 0.5, "ou": 3.25,
            "side": {"pick": "DOG", "confidence": "LOW", "supporting_rules": 2, "audit_tier": {"bets": 130, "wins": 73, "hit_rate": 56.15}},
            "goals": {"pick": "UNDER", "confidence": "LOW", "supporting_rules": 1, "audit_tier": {"bets": 123, "wins": 76, "hit_rate": 61.79}},
        }],
    }


def test_publishes_concrete_audited_ah_and_ou_with_estimated_roi():
    picks = build_market_v3_picks(_match(), payload=_payload())
    assert len(picks) == 2
    assert all(p["algorithm"] == ALGORITHM for p in picks)
    ah = next(p for p in picks if p["type"] == "AH")
    ou = next(p for p in picks if p["type"] == "OU")
    assert ah["display_pick_label"] == "Equipo Visitante +0.50"
    assert ah["roi"] == 0.0668
    assert ou["display_pick_label"] == "UNDER 3.25"
    assert ou["roi"] == 0.174
    assert "cuota plana 1.90" in ou["roi_basis"]


def test_rejects_stale_prediction_when_market_line_changed():
    match = _match()
    match["main_match_odds"]["ah_linea"] = "0.75"
    assert build_market_v3_picks(match, payload=_payload()) == []


def test_rejects_unvalidated_or_small_audit_sample():
    payload = _payload()
    payload["predictions"][0]["side"]["audit_tier"] = {"bets": 10, "wins": 8, "hit_rate": 80.0}
    payload["predictions"][0]["goals"] = {"pick": "NO BET"}
    assert build_market_v3_picks(_match(), payload=payload) == []
