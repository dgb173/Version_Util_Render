import copy
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from modules.housemind_ou import (  # noqa: E402
    PROFILE,
    extract_feature_vector,
    predict_probability,
    safe_float,
    settle_ou_score,
)


def _stats(sot_home=5, sot_away=4, danger_home=62, danger_away=51):
    return [
        {"label": "Tiros", "home": "12", "away": "10"},
        {"label": "Tiros a Puerta", "home": str(sot_home), "away": str(sot_away)},
        {"label": "Ataques", "home": "101", "away": "94"},
        {
            "label": "Ataques Peligrosos",
            "home": str(danger_home),
            "away": str(danger_away),
        },
    ]


def _match():
    return {
        "match_id": "test-1",
        "match_date": "2026-07-11",
        "home_name": "Home",
        "away_name": "Away",
        "league_name": "Test League",
        "final_score": "9:9",
        "main_match_odds": {"ah_linea": "0.5", "goals_linea": "2.5"},
        "home_standings": {"ranking": "3"},
        "away_standings": {"ranking": "8"},
        "last_home_match": {
            "date": "2026-07-01",
            "score": "2:1",
            "handicap_line_raw": "0.25",
            "stats_rows": _stats(),
        },
        "last_away_match": {
            "date": "2026-06-30",
            "score": "1:0",
            "handicap_line_raw": "-0.5",
            "stats_rows": _stats(3, 4, 48, 55),
        },
        "h2h_stadium": {
            "date1": "2026-01-10",
            "res1": "1:1",
            "ah1": "0.5",
            "stats_rows": _stats(4, 3, 50, 45),
        },
        "market_analysis_data": {
            "stadium": {"date": "2026-01-10", "result": "1:1", "movement": "0.25 -> 0.5"}
        },
    }


class HouseMindFeatureTests(unittest.TestCase):
    def test_safe_float_understands_split_asian_line(self):
        self.assertEqual(safe_float("2/2.5"), 2.25)

    def test_quarter_line_settlement_is_exact(self):
        self.assertEqual(settle_ou_score(2, 2.25, "OVER"), -0.5)
        self.assertEqual(settle_ou_score(2, 2.25, "UNDER"), 0.5)
        self.assertEqual(settle_ou_score(3, 2.75, "OVER"), 0.5)
        self.assertEqual(settle_ou_score(3, 3.0, "OVER"), 0.0)

    def test_current_final_score_never_changes_features(self):
        first = _match()
        second = copy.deepcopy(first)
        second["final_score"] = "0:0"
        second["score"] = "7:2"
        first_vector = extract_feature_vector(first)
        second_vector = extract_feature_vector(second)
        self.assertTrue(first_vector["quality"]["eligible"])
        self.assertEqual(first_vector["tokens"], second_vector["tokens"])
        self.assertEqual(first_vector["numeric"], second_vector["numeric"])

    def test_same_day_or_future_context_is_filtered(self):
        match = _match()
        match["market_analysis_data"]["stadium"]["date"] = "2026-07-11"
        match["h2h_stadium"]["date1"] = "2026-07-11"
        vector = extract_feature_vector(match)
        self.assertIn("H2H_STADIUM=NONPAST", vector["tokens"])
        self.assertEqual(vector["quality"]["nonpast_contexts"], 1)
        self.assertEqual(vector["quality"]["valid_contexts"], 2)
        self.assertFalse(vector["quality"]["eligible"])

    def test_runtime_applies_calibrated_abstention_policy(self):
        match = _match()
        model = {
            "profile": PROFILE,
            "version": "test",
            "model": {
                "numeric_names": ["ou_line"],
                "numeric_means": [2.5],
                "numeric_scales": [1.0],
                "token_names": ["OU_FAMILY=MID"],
                "weights": [0.0, 1.0],
                "intercept": 0.0,
                "platt_a": 1.0,
                "platt_b": 0.0,
            },
            "decision": {"enabled": True, "threshold": 0.65, "min_contexts": 3},
            "audit": {},
        }
        result = predict_probability(match, model_payload=model)
        self.assertEqual(result["action"], "OVER")
        self.assertGreater(result["probability_over"], 0.70)

        model["decision"]["enabled"] = False
        disabled = predict_probability(match, model_payload=model)
        self.assertEqual(disabled["action"], "NO_BET")
        self.assertEqual(disabled["reason"], "model_did_not_pass_holdout_gate")


if __name__ == "__main__":
    unittest.main()
