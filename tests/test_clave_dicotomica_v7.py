import copy
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from modules.clave_dicotomica import apply_key  # noqa: E402


def _stats(home=10, away=8, sot_home=5, sot_away=3, danger_home=60, danger_away=45):
    return [
        {"label": "Tiros", "home": str(home), "away": str(away)},
        {"label": "Tiros a Puerta", "home": str(sot_home), "away": str(sot_away)},
        {"label": "Ataques", "home": "100", "away": "90"},
        {
            "label": "Ataques Peligrosos",
            "home": str(danger_home),
            "away": str(danger_away),
        },
    ]


def _match():
    return {
        "match_id": "v7-test",
        "match_date": "2026-07-20",
        "home_name": "Home",
        "away_name": "Away",
        "final_score": "9:9",
        "main_match_odds": {"ah_linea": "0.5", "goals_linea": "2.5"},
        "h2h_general": {
            "res6": "1:0",
            "date6": "2026-04-01",
            "ah6": "0.5",
            "h2h_gen_home": "Home",
            "h2h_gen_away": "Away",
            "stats_rows": _stats(),
        },
        "h2h_stadium": {
            "res1": "2:0",
            "date1": "2026-02-01",
            "ah1": "0.5",
            "stats_rows": _stats(),
        },
        "last_home_match": {
            "date": "2026-07-01",
            "score": "2:0",
            "handicap_line_raw": "0.5",
            "stats_rows": _stats(),
        },
        "last_away_match": {
            "date": "2026-07-02",
            "score": "0:1",
            "handicap_line_raw": "-0.25",
            "stats_rows": _stats(8, 10, 3, 5, 45, 60),
        },
        "comparativas_indirectas": {
            "left": {
                "date": "2026-06-20",
                "score": "0:1",
                "localia": "H",
                "ah_line": "0.25",
                "stats_rows": _stats(9, 11, 3, 5, 42, 58),
            },
            "right": {
                "date": "2026-06-22",
                "score": "0:2",
                "localia": "A",
                "ah_line": "-1.0",
                "stats_rows": _stats(7, 12, 2, 6, 38, 64),
            },
        },
        "h2h_col3": {
            "col3_data": {
                "date": "2026-03-10",
                "score": "1:1",
                "ah": "0.25",
            }
        },
        "home_standings": {
            "ranking": "3",
            "wins": "8",
            "played": "15",
            "draws": "3",
            "home_played": "8",
            "home_draws": "2",
        },
        "away_standings": {
            "ranking": "7",
            "wins": "5",
            "played": "15",
            "draws": "4",
            "away_played": "7",
            "away_draws": "2",
        },
    }


class ClaveDicotomicaV7Tests(unittest.TestCase):
    def test_current_result_never_changes_prediction(self):
        first = _match()
        second = copy.deepcopy(first)
        second["final_score"] = "0:0"
        second["score"] = "7:2"
        self.assertEqual(apply_key(first), apply_key(second))

    def test_canonical_stadium_and_nested_col3_are_read(self):
        result = apply_key(_match())
        self.assertEqual(result["engine_version"], "V7.0")
        self.assertEqual(result["stadium_RH"], 1.5)
        self.assertGreaterEqual(result["quality"]["evidence_blocks"], 7)
        self.assertTrue(result["quality"]["eligible"])

    def test_indirect_ah_line_fields_activate_inverted_quality(self):
        result = apply_key(_match())
        self.assertTrue(result["u2_calidad_invertida"])
        self.assertIn("CALIDAD_RELATIVA_INVERTIDA", result["flags"])

    def test_favorite_flip_generates_new_favorite_pressure(self):
        match = _match()
        match["h2h_general"].update({"res6": "0:1", "ah6": "-0.5"})
        result = apply_key(match)
        self.assertEqual(result["pressure"], "PRESSURE_NEW_FAV")

    def test_missing_h2h_names_uses_line_orientation(self):
        match = _match()
        match["h2h_general"].pop("h2h_gen_home")
        match["h2h_general"].pop("h2h_gen_away")
        result = apply_key(match)
        self.assertEqual(result["pressure"], "PRESSURE_SAME")

    def test_nonpast_context_blocks_publication(self):
        match = _match()
        match["comparativas_indirectas"]["left"]["date"] = match["match_date"]
        result = apply_key(match)
        self.assertFalse(result["quality"]["eligible"])
        self.assertIn("ind_fav", result["quality"]["nonpast_contexts"])
        self.assertEqual(result["ah"], "NO_BET")

    def test_guard_keeps_weak_raw_pick_as_observation(self):
        match = _match()
        match["h2h_stadium"]["res1"] = "0:1"
        result = apply_key(match)
        self.assertEqual(result["raw_ah"], "FAV_CUBRE")
        self.assertEqual(result["ah"], "NO_BET")
        self.assertEqual(result["prediction_tier_ah"], "OBSERVATION")
        self.assertIn("edge menor de 3.5 sin micro-regla promovida", result["ah_gate_reasons"])

    def test_missing_odds_returns_complete_v7_contract(self):
        result = apply_key({})
        self.assertEqual(result["engine_version"], "V7.0")
        self.assertEqual(result["raw_ah"], "NO_BET")
        self.assertEqual(result["prediction_tier_ah"], "NO_BET")
        self.assertFalse(result["quality"]["eligible"])

    def test_unvalidated_exact_handicap_is_observation_only(self):
        match = _match()
        match["main_match_odds"]["ah_linea"] = "0.75"
        match["h2h_stadium"]["res1"] = "4:0"
        result = apply_key(match)
        self.assertEqual(result["raw_ah"], "FAV_CUBRE")
        self.assertFalse(result["validated_ah_line"])
        self.assertEqual(result["ah"], "NO_BET")
        self.assertIn(
            "linea AH sin validacion cronologica suficiente",
            result["ah_gate_reasons"],
        )

    def test_validated_expansion_promotes_home_favorite_half_goal(self):
        match = _match()
        match["h2h_stadium"]["res1"] = "4:0"
        result = apply_key(match)
        self.assertFalse(result["validated_ah_line"])
        self.assertTrue(result["validated_ah_expansion"])
        self.assertEqual(result["raw_ah"], "FAV_CUBRE")
        self.assertEqual(result["ah"], "FAV_CUBRE")
        self.assertIn("AH -0.50", result["ah_label"])
        self.assertEqual(result["prediction_tier_ah"], "PRODUCTION_EXPANSION")
        self.assertEqual(result["bookie_confirmation"], "NEUTRAL")
        self.assertTrue(result["expansion_ah_rule"].startswith("EXP_AH_01"))
        self.assertEqual(
            result["core_ah_gate_reasons"],
            ["linea AH sin validacion cronologica suficiente"],
        )

    def test_validated_exact_handicap_can_reach_production(self):
        match = _match()
        match["main_match_odds"]["ah_linea"] = "1.5"
        match["h2h_stadium"]["res1"] = "4:0"
        result = apply_key(match)
        self.assertTrue(result["validated_ah_line"])
        self.assertEqual(result["prediction_tier_ah"], "PRODUCTION")
        self.assertIn(result["ah"], {"FAV_CUBRE", "DOG_CUBRE"})


if __name__ == "__main__":
    unittest.main()
