from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from modules import nowgoal_fetcher


def _row(match_id="3074436", handicap=None, country_id=66, goal_line=None):
    values = [None] * 37
    values[0] = int(match_id)
    values[1] = 58
    values[4] = "Local"
    values[5] = "Visitante"
    values[6] = "2099-09-08 13:00:00"
    values[8] = 0
    values[9] = 0
    values[10] = 0
    values[21] = handicap
    values[23] = country_id
    values[25] = goal_line
    encoded = ",".join("null" if value is None else repr(value) for value in values)
    return f"A[0]=[{encoded}];"


def test_country_id_is_never_parsed_as_goal_line():
    matches = nowgoal_fetcher.parse_matches_from_bf_content(
        _row(handicap=0.25, country_id=66),
        status_filter="upcoming",
        require_handicap=True,
        require_goal_line=False,
    )
    assert len(matches) == 1
    assert matches[0]["handicap"] == "0.25"
    assert matches[0]["goal_line"] == "N/A"


def test_both_market_lines_are_required_when_requested():
    assert nowgoal_fetcher.parse_matches_from_bf_content(
        _row(handicap=0.25, country_id=66),
        status_filter="upcoming",
        require_handicap=True,
        require_goal_line=True,
    ) == []

    matches = nowgoal_fetcher.parse_matches_from_bf_content(
        _row(handicap=0.25, country_id=66, goal_line=2.5),
        status_filter="upcoming",
        require_handicap=True,
        require_goal_line=True,
    )
    assert [(row["handicap"], row["goal_line"]) for row in matches] == [("0.25", "2.5")]


def test_market_line_ranges_reject_random_identifiers():
    assert not nowgoal_fetcher._is_valid_numeric_handicap(64)
    assert not nowgoal_fetcher._is_valid_numeric_goal_line(66)
    assert nowgoal_fetcher._is_valid_numeric_handicap(-0.75)
    assert nowgoal_fetcher._is_valid_numeric_goal_line(3.25)


def test_upcoming_rows_are_enriched_with_all_tab_xml_markets():
    matches = nowgoal_fetcher.parse_matches_from_bf_content(
        _row(match_id="3088105", handicap=None, goal_line=None),
        status_filter="upcoming",
        odds_by_match={"3088105": {"handicap": "0.5", "goal_line": "4"}},
        require_handicap=True,
        require_goal_line=True,
    )

    assert [(row["id"], row["handicap"], row["goal_line"]) for row in matches] == [
        ("3088105", "0.5", "4")
    ]


def test_split_asian_lines_are_normalized_instead_of_dropped():
    matches = nowgoal_fetcher.parse_matches_from_bf_content(
        _row(match_id="9", handicap=None, goal_line=None),
        status_filter="upcoming",
        odds_by_match={"9": {"handicap": "0/0.5", "goal_line": "2.5/3"}},
        require_handicap=True,
        require_goal_line=True,
    )

    assert [(row["handicap"], row["goal_line"]) for row in matches] == [("0.25", "2.75")]
