import copy
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from modules.bookie_pattern_detector import detect_bookie_patterns  # noqa: E402
from tests.test_clave_dicotomica_v7 import _match  # noqa: E402


class BookiePatternDetectorTests(unittest.TestCase):
    def test_detects_new_favorite_status_from_h2h(self):
        match = _match()
        match["main_match_odds"]["ah_linea"] = "1.25"
        match["h2h_general"]["ah6"] = "-0.25"
        result = detect_bookie_patterns(match, "DOG_CUBRE")
        ids = {signal["id"] for signal in result["signals"]}
        self.assertIn("GENERAL_NEW_FAVORITE_STATUS", ids)
        self.assertIn("GENERAL_NEW_FAVORITE_STATUS", result["aligned_signals"])

    def test_current_result_is_never_used(self):
        first = _match()
        second = copy.deepcopy(first)
        first["final_score"] = "8:0"
        first["score"] = "8:0"
        second["final_score"] = "0:8"
        second["score"] = "0:8"
        self.assertEqual(
            detect_bookie_patterns(first, "DOG_CUBRE"),
            detect_bookie_patterns(second, "DOG_CUBRE"),
        )

    def test_line_against_table_points_to_dog(self):
        match = _match()
        match["home_standings"]["ranking"] = "10"
        match["away_standings"]["ranking"] = "2"
        result = detect_bookie_patterns(match, "DOG_CUBRE")
        signal = next(s for s in result["signals"] if s["id"] == "LINE_AGAINST_TABLE")
        self.assertEqual(signal["direction"], "DOG")


if __name__ == "__main__":
    unittest.main()
