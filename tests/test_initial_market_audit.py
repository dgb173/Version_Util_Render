import copy
from collections import Counter
from datetime import date

import pytest
import numpy as np

from src.modules.asian_settlement import settlement, unit_profit
from src.modules.sql_store import _compact_main_match_odds
from scripts.clave_dicotomica.audit_forced_initial import build_features, initial_decimal, sanitized_engine_match, probabilities, predictions, mean


@pytest.mark.parametrize('margin,line,expected', [(2, 1.75, .5), (2, 2.25, -.5),
    (2, 2, 0), (0, -.25, .5), (0, .25, -.5), (3, 2.5, 1)])
def test_exact_asian_quarter_settlement(margin, line, expected):
    assert settlement(margin, line) == expected


def test_half_win_and_half_loss_use_exact_price():
    assert unit_profit(.5, 1.9) == pytest.approx(.45)
    assert unit_profit(-.5, 1.9) == -.5
    assert unit_profit(0, 1.9) == 0


@pytest.mark.parametrize('line', [float('nan'), float('inf'), .3])
def test_invalid_lines_are_rejected(line):
    with pytest.raises(ValueError):
        settlement(2, line)


def test_unknown_price_format_cannot_be_used_for_real_roi():
    assert initial_decimal({'odds_stage': 'initial', 'ah_home_cuota': '1.03'}, 'ah_home_cuota') is None
    assert initial_decimal({'odds_stage': 'initial', 'price_format': 'hongkong', 'ah_home_cuota': '1.03'}, 'ah_home_cuota') == pytest.approx(2.03)


def test_compaction_preserves_initial_prices_and_format():
    raw = {'ah_linea': '0.5', 'goals_linea': '2.5', 'ah_home_cuota': '1.03',
           'price_format': 'unverified', 'odds_stage': 'initial'}
    assert _compact_main_match_odds(raw) == raw


def _row(mid, day, result):
    return {'id':mid, 'date':day, 'ah':.5, 'ou':2.5, 'score':result, 'prices':[None]*4,
            'match':{'match_id':mid, 'home_name':'Home', 'away_name':'Away', 'league_id':'1',
                     'match_date':day.isoformat(), 'main_match_odds':{'ah_linea':'.5','goals_linea':'2.5'}}}


def test_same_day_results_are_never_visible_to_features():
    rows = [_row('1', date(2026,1,1), (9,0)), _row('2', date(2026,1,1), (0,1)),
            _row('3', date(2026,1,2), (2,0))]
    changed = copy.deepcopy(rows)
    changed[0]['score'] = (0,9)
    a = build_features(rows, Counter()); b = build_features(changed, Counter())
    assert a[1]['features']['form_home_sample'] == b[1]['features']['form_home_sample'] == 0
    assert a[2]['features']['form_home_margin'] != b[2]['features']['form_home_margin']


def test_future_context_and_unverified_standings_are_removed():
    match = {'last_home_match':{'date':'2026-01-02','score':'9:0'},
             'home_standings':{'ranking':'1'}, 'market_analysis_data':{'general':{'movement':'0 -> 2'}}}
    cleaned = sanitized_engine_match(match,date(2026,1,1),Counter())
    assert cleaned['last_home_match'] == {}
    assert 'home_standings' not in cleaned
    assert 'market_analysis_data' not in cleaned
    assert match['last_home_match']['score'] == '9:0'


def test_probability_distribution_respects_exact_line_settlement_support():
    class UniformModel:
        classes_ = np.arange(5)
        def __getitem__(self,key):
            return self
        def predict_proba(self,x):
            return np.full((len(x),5),.2)
    p = probabilities(UniformModel(), np.zeros((3,1)), lines=np.array([2.5,2.25,2.75]),total_goals=True)
    assert list(p[0]) == [.5,0,0,0,.5]
    assert p[1,2] == p[1,3] == 0  # Over 2.25: no push ni media ganancia.
    assert p[2,1] == p[2,2] == 0  # Over 2.75: no media pérdida ni push.
    assert np.allclose(p.sum(axis=1),1)


def test_over_quarter_goal_cannot_lose_a_full_unit_at_zero_goals():
    class UniformModel:
        classes_ = np.arange(5)
        def __getitem__(self,key):
            return self
        def predict_proba(self,x):
            return np.full((len(x),5),.2)
    p = probabilities(UniformModel(),np.zeros((1,1)),lines=np.array([.25]),total_goals=True)
    assert p[0,0] == 0
    assert p[0,1] == .5


class FixedProbabilityModel:
    classes_ = np.arange(5)
    def __init__(self, win):
        self.win = win
    def __getitem__(self,key):
        return self
    def predict_proba(self,x):
        return np.tile([1-self.win,0,0,0,self.win],(len(x),1))


def test_forced_direction_compares_initial_prices_and_expected_profit():
    picks,_ = predictions([FixedProbabilityModel(.55)]*2,[1,1],np.zeros((1,1)),
        np.array([[.5,2.5]]),np.array([[1.5,2.5,1.9,2.0]]))
    assert list(picks[0]) == [False,True]  # 55% no hace rentable favorito a 1.50.


def test_coherent_choice_does_not_blindly_preserve_weaker_ah_direction():
    picks,_ = predictions([FixedProbabilityModel(.55),FixedProbabilityModel(.2)],[1,1],
        np.zeros((1,1)),np.array([[2.5,2.5]]),np.full((1,4),2.))
    assert list(picks[0]) == [False,False]  # No favorito + Under; la señal O/U domina.


def test_missing_observation_does_not_erase_observed_mean():
    assert mean([None,float('nan'),2,4]) == 3


def test_full_goal_history_is_used_without_inventing_unverified_ah():
    event = _row('1',date(2026,1,1),(2,0))
    event['ah'] = event['ou'] = None
    target = _row('2',date(2026,1,2),(1,0))
    result = build_features([target],Counter(),[event])[0]
    assert result['features']['form_home_sample'] == 1
    assert result['features']['form_home_margin'] == 2
    assert result['features']['form_home_cover_sample'] == 0
