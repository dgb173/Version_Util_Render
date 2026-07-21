import unittest

from scripts.finished_result_validation import validate_finished_result


class FinishedMatchesWithoutHandicapTests(unittest.TestCase):
    def test_finished_result_is_kept_when_handicap_is_missing(self):
        verified = validate_finished_result("2 - 0", "N/A")

        self.assertEqual(verified["score"], "2 - 0")
        self.assertEqual(verified["handicap"], "N/A")
        self.assertIs(verified["result_only"], True)

    def test_missing_handicap_never_bypasses_result_validation(self):
        self.assertIsNone(validate_finished_result("Pendiente", "N/A"))
        self.assertIsNone(validate_finished_result("?:?", ""))

    def test_normal_handicap_path_is_preserved(self):
        verified = validate_finished_result("1-2", " 0.5 ")

        self.assertEqual(verified["handicap"], "0.5")
        self.assertIs(verified["result_only"], False)


if __name__ == "__main__":
    unittest.main()
