import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from modules import rival_handicap_samples as rhs


def _row(score, ah):
    return {
        "score_raw": score,
        "score": score.replace("-", ":"),
        "ahLine_raw": str(ah),
        "ahLine": str(ah),
    }


def test_home_ah_result_supports_quarter_settlements():
    assert rhs._home_ah_result(_row("2-1", "0.75"))["code"] == "HALF_WIN"
    assert rhs._home_ah_result(_row("1-1", "0.25"))["code"] == "HALF_LOSS"
    assert rhs._home_ah_result(_row("1-0", "0"))["code"] == "COVER"
    assert rhs._home_ah_result(_row("0-0", "0"))["code"] == "PUSH"


def test_exact_filter_is_numeric_and_not_similar():
    rows = [_row("2-1", "0.75"), _row("1-1", "0.25"), _row("1-0", "0/0.5")]
    exact = [row for row in rows if abs(rhs._market_line(row) - 0.25) <= 1e-9]
    assert len(exact) == 2
    assert rows[0] not in exact


def test_comparison_status_uses_col3_wdl_order():
    assert rhs._comparison_status({"rank": 2}, {"rank": 1}) == "MEJORA"
    assert rhs._comparison_status({"rank": 1}, {"rank": 1}) == "IGUALA"
    assert rhs._comparison_status({"rank": 0}, {"rank": 2}) == "EMPEORA"

