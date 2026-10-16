from src.modules.joint_initial_market import classify_joint_initial_market
import pytest
from src.modules.asian_settlement import settlement


def test_favorite_minus_2_5_and_under_2_5_cannot_both_cover():
    match = {"main_match_odds": {"ah_linea": "2.5", "goals_linea": "2.5"}}
    result = classify_joint_initial_market(match, edge_ah=2, edge_ou=-2)
    assert result["initial_joint_compatible"] is False
    assert result["joint_status"] == "LOGICAL_CONFLICT_CORRECTED"
    assert (result["forced_ah"], result["forced_ou"]) == ("FAV_CUBRE", "OVER")


def test_favorite_minus_1_5_and_under_2_5_are_possible_together():
    match = {"main_match_odds": {"ah_linea": "1.5", "goals_linea": "2.5"}}
    result = classify_joint_initial_market(match, edge_ah=2, edge_ou=-2)
    assert result["initial_joint_compatible"] is True  # 2-0
    assert result["forced_ou"] == "UNDER"


def test_missing_lines_are_not_fabricated():
    assert not classify_joint_initial_market({"main_match_odds": {"ah_linea": "0.5"}})["available"]


def test_explicit_later_lines_are_not_used_as_initials():
    result = classify_joint_initial_market({'main_match_odds': {'ah_linea':.5,'goals_linea':2.5,'odds_stage':'closing'}})
    assert result['available'] is False


def test_historical_goal_residual_uses_ou_line_not_xg():
    match = {
        "match_date": "2026-10-12",
        "main_match_odds": {"ah_linea": "0.5", "goals_linea": "2.5"},
        "pre_match_context": {"current": {
            "home_matches": [{"date": "2026-10-10", "score": "2:1", "ouLine": "2.5"}],
            "away_matches": [{"date": "2026-10-10", "score": "0:1", "ouLine": "2.0"}],
        }},
    }
    result = classify_joint_initial_market(match)
    assert result["historical_ou_residuals"]["home"]["mean_goals_minus_initial_ou"] == 0.5
    assert result["historical_ou_residuals"]["away"]["mean_goals_minus_initial_ou"] == -1.0
    assert result["xg_status"] == "NOT_AVAILABLE"


@pytest.mark.parametrize('line', ['nan', 'inf', '0.3', '1e308'])
def test_invalid_market_line_does_not_create_a_prediction(line):
    assert classify_joint_initial_market({'main_match_odds': {'ah_linea': line, 'goals_linea': '2.5'}})['available'] is False


def test_recent_context_excludes_future_and_duplicate_matches():
    match = {'match_date': '2026-10-12', 'main_match_odds': {'ah_linea': '0.5', 'goals_linea': '2.5'},
             'pre_match_context': {'current': {'home_matches': [
                 {'matchIndex':'1','date':'2026-10-10','score':'2:1','ouLine':'2.5'},
                 {'matchIndex':'1','date':'2026-10-10','score':'2:1','ouLine':'2.5'},
                 {'matchIndex':'2','date':'2026-10-13','score':'9:0','ouLine':'2.5'},
             ]}}}
    result = classify_joint_initial_market(match)
    assert result['historical_ou_residuals']['home']['sample'] == 1
    assert result['historical_ou_residuals']['home']['mean_goals_minus_initial_ou'] == .5


def test_joint_compatibility_matches_exact_asian_payoffs_across_lines():
    for ah in (0, .25, .5, .75, 1.5, 2.25, 2.5, 3.5):
        for ou in (.5, 1.25, 2, 2.25, 2.5, 2.75, 3.5, 5.5):
            for edge_ah in (-1, 1):
                for edge_ou in (-1, 1):
                    result = classify_joint_initial_market({'main_match_odds': {'ah_linea':ah,'goals_linea':ou}}, edge_ah=edge_ah, edge_ou=edge_ou)
                    feasible = any(
                        settlement(h-a, ah) * edge_ah > 0 and settlement(h+a, ou) * edge_ou > 0
                        for h in range(8) for a in range(8)
                    )
                    assert result['initial_joint_compatible'] == feasible


def test_away_favorite_is_symmetric_to_home_favorite():
    for line in (.25, .5, 1.25, 2.5):
        home = classify_joint_initial_market({'main_match_odds': {'ah_linea':line,'goals_linea':2.5}},edge_ah=1,edge_ou=-1)
        away = classify_joint_initial_market({'main_match_odds': {'ah_linea':-line,'goals_linea':2.5}},edge_ah=1,edge_ou=-1)
        assert (home['forced_ah'],home['forced_ou']) == (away['forced_ah'],away['forced_ou'])
        assert away['ah_reference'] == 'AWAY'
