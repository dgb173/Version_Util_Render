from argparse import Namespace
from pathlib import Path

from scripts.explorador_automejora.train_universal_market_v3 import (
    audit_gate,
    enrich_features,
    temporal_split,
    venue_metrics,
)


def test_venue_metrics_capture_home_weakness_and_away_resilience():
    home = venue_metrics({"specific_pj": "5", "specific_v": "2", "specific_e": "0", "specific_d": "3", "specific_gf": "11", "specific_gc": "14"})
    away = venue_metrics({"specific_pj": "6", "specific_v": "0", "specific_e": "4", "specific_d": "2", "specific_gf": "9", "specific_gc": "12"})
    assert home["win_rate"] == 0.4
    assert away["nonloss_rate"] == 4 / 6


def test_temporal_split_keeps_order_and_audit_untouched():
    rows = [{"n": i} for i in range(100)]
    parts = temporal_split(rows)
    assert [len(parts[k]) for k in ("discovery", "validation", "confirmation", "audit")] == [50, 20, 15, 15]
    assert parts["discovery"][0]["n"] == 0
    assert parts["audit"][0]["n"] == 85


def test_grotta_style_context_adds_inflation_flags():
    class V2:
        @staticmethod
        def fav_side(_):
            return "HOME"

    match = {
        "home_standings": {"ranking": "8", "specific_pj": "5", "specific_v": "2", "specific_e": "0", "specific_d": "3", "specific_gf": "6", "specific_gc": "7"},
        "away_standings": {"ranking": "10", "specific_pj": "6", "specific_v": "0", "specific_e": "4", "specific_d": "2", "specific_gf": "7", "specific_gc": "8"},
    }
    row = {"ah": 0.5, "ou": 3.25, "features": ["FAV_RECENT_COVER_COVER", "FAV_RECENT_STATS_STRONG_AGAINST", "DOG_RECENT_GOALS_4_PLUS"]}
    enrich_features(match, row, V2())
    features = set(row["features"])
    assert "FAVORITE_WEAK_IN_VENUE" in features
    assert "DOG_RESILIENT_IN_VENUE" in features
    assert "RESULT_INFLATED_FAVORITE" in features
    assert "OU_INFLATED_BY_RECENT_SCORE" in features


def test_audit_gate_rejects_a_confidence_tier_that_did_not_validate():
    pred = {"pick": "OVER", "market_pick": "OVER 3.25", "confidence": "HIGH"}
    audit = {"by_confidence": {"HIGH": {"bets": 80, "wins": 40, "hit_rate": 50.0}}}
    gated = audit_gate(pred, audit)
    assert gated["pick"] == "NO BET"
    assert gated["confidence"] == "REJECTED_BY_AUDIT"
