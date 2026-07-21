import copy
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from modules.clave_universal_picks import ALGORITHM, build_universal_picks  # noqa: E402
from tests.test_clave_dicotomica_v7 import _match  # noqa: E402


class ClaveUniversalPicksTests(unittest.TestCase):
    def test_only_universal_algorithm_is_emitted(self):
        match = _match()
        match["main_match_odds"]["ah_linea"] = "1.5"
        match["h2h_stadium"]["res1"] = "4:0"
        picks = build_universal_picks(match)
        self.assertTrue(picks)
        self.assertTrue(all(pick["algorithm"] == ALGORITHM for pick in picks))
        self.assertLessEqual(sum(pick["type"] == "AH" for pick in picks), 1)
        self.assertLessEqual(sum(pick["type"] == "OU" for pick in picks), 1)

    def test_observations_are_not_published_as_bets(self):
        match = _match()
        match["h2h_stadium"]["res1"] = "0:1"
        self.assertEqual(build_universal_picks(match), [])

    def test_validated_expansion_is_published_and_marked(self):
        match = _match()
        match["h2h_stadium"]["res1"] = "4:0"
        picks = build_universal_picks(match)
        ah_pick = next(pick for pick in picks if pick["type"] == "AH")
        self.assertTrue(ah_pick["is_expansion"])
        self.assertEqual(ah_pick["prediction_tier"], "PRODUCTION_EXPANSION")
        self.assertTrue(ah_pick["display_pick_label"].endswith("-0.50"))
        self.assertTrue(ah_pick["expansion_rule"].startswith("EXP_AH_01"))

    def test_current_result_cannot_change_published_picks(self):
        first = _match()
        first["h2h_stadium"]["res1"] = "4:0"
        second = copy.deepcopy(first)
        first["final_score"] = "8:0"
        second["final_score"] = "0:8"
        self.assertEqual(build_universal_picks(first), build_universal_picks(second))


if __name__ == "__main__":
    unittest.main()
