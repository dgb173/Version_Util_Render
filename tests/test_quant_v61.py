import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest
from flask import Flask

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from modules import quant_research as q
from modules.quant_api import append_audit, create_blueprint


@pytest.mark.parametrize('margin,line,price,expected', [
    (0, .25, 1.9, .45), (0, -.25, 1.9, -.5),
    (1, -.75, 1.9, .45), (1, -1, 1.9, 0),
    (-3, 2.75, 1.9, -.5), (3, -2.75, 1.9, .45),
    (0, 0, 2., 0), (-1, .5, 1.9, -1), (2, -.5, 1.9, .9),
])
def test_asian_contract(margin, line, price, expected):
    assert q.settlement(margin, line, price) == pytest.approx(expected)


def test_joint_distribution_and_exact_ev():
    p = q.matrix(1.6, 1.1, -.08)
    independent = q.matrix(1.6, 1.1)
    assert p.sum() == pytest.approx(1)
    assert p.min() >= 0
    assert p[2, 2] == pytest.approx(independent[2, 2])
    assert sum(q.outcome_probs(p)) == pytest.approx(1)
    for market, side, line in [('AH', 'home', -.75), ('AH', 'away', .75),
                                ('OU', 'over', -2.75), ('OU', 'under', 2.75)]:
        got = q.value_market(p, market, side, line, 1.9)
        expected = 0
        for x in range(p.shape[0]):
            for y in range(p.shape[1]):
                margin = x - y if market == 'AH' else x + y
                if side in ('away', 'under'):
                    margin = -margin
                expected += p[x, y] * q.settlement(margin, line, 1.9)
        assert got['ev'] == pytest.approx(expected)
        assert sum(got['settlements'].values()) == pytest.approx(1)
        assert q.value_market(p, market, side, line, got['fair_odds'])['ev'] == pytest.approx(0)


def fixture():
    ids = [q.identity('League', team) for team in ('Home', 'Away')]
    model = {'training_end': '2026-10-24', 'mu': 0., 'home_advantage': .3, 'rho': 0.,
             'league': {'League': 0.}, 'attack': dict.fromkeys(ids, 0.), 'defence': dict.fromkeys(ids, 0.),
             'evidence': dict.fromkeys(ids, {'n': 20, 'effective_n': 15.})}
    payload = {'home_name': 'Home', 'away_name': 'Away', 'league_name': 'League', 'match_date': '10/25/2026',
               'start_time': '2026-10-25T20:00:00+01:00', 'main_match_odds': {
                   'ah_linea': .25, 'goals_linea': 2.75, 'price_format': 'hongkong',
                   'ah_home_cuota': .9, 'ah_away_cuota': .9, 'goals_over_cuota': .9, 'goals_under_cuota': .9,
                   'captured_at': '2026-10-25T08:00:00Z'}}
    return model, payload, datetime(2026, 10, 25, 10, tzinfo=timezone.utc)


def test_prices_signs_and_missing_stats():
    model, payload, now = fixture()
    result = q.predict(payload, model, now)
    assert result['markets'][0]['line'] == -.25  # positive source AH -> home gives
    assert result['markets'][0]['odds_decimal'] == 1.9
    assert all(v is None for v in result['stats_signals'].values())
    assert result['protocol_approved'] is False
    payload['main_match_odds'].pop('price_format')
    result = q.predict(payload, model, now)
    assert not result['picks']
    assert all(m['ev'] is None for m in result['markets'])


@pytest.mark.parametrize('change,expected', [
    ({'match_date': '2026-10-24'}, 'ABSTAIN_MODELO'),
    ({'home_name': 'Unseen'}, 'PRONOSTICO_MODELO'),
    ({'final_score': '0:0'}, 'ERROR_DATOS'),
    ({'start_time': '2026-10-25T09:00:00Z'}, 'ABSTAIN_INCIERTO'),
    ({'start_time': None}, 'ABSTAIN_INCIERTO'),
    ({'match_date': None}, 'ERROR_DATOS'),
])
def test_prevents_hindsight_live_and_missing_data(change, expected):
    model, payload, now = fixture()
    payload.update(change)
    got = q.predict(payload, model, now)
    assert got['status'] == expected
    assert not got['picks']
    if expected == 'PRONOSTICO_MODELO':
        assert got['selections']
        assert got['risk'] == 'ALTO'


def test_unverified_capture_cannot_issue_candidate():
    model, payload, now = fixture()
    payload['main_match_odds']['captured_at'] = '2026-10-26T00:00:00Z'
    assert not q.predict(payload, model, now)['picks']


def test_legacy_spain_time_and_ambiguous_dst():
    model, payload, now = fixture()
    payload.pop('start_time')
    payload['time'] = '20:00'
    result = q.predict(payload, model, now)
    assert result['kickoff_at'] == '2026-10-25T20:00:00+01:00'
    payload['time'] = '02:30'
    assert 'kickoff_at' not in q.predict(payload, model, now)


def test_append_only_and_routes(tmp_path, monkeypatch):
    model, payload, now = fixture()
    db = tmp_path / 'audit.sqlite'
    result = q.predict(payload, model, now)
    rid = append_audit('123', payload, result, db)
    assert rid == append_audit('123', payload, result, db)
    with sqlite3.connect(db) as conn:
        assert conn.execute('select count(*) from predictions').fetchone()[0] == 1
        assert json.loads(conn.execute('select packet from predictions').fetchone()[0])['prediction'] == result
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("update predictions set event_id='other'")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute('delete from predictions')
    monkeypatch.setattr(q, 'load_model', lambda: model)
    app = Flask(__name__)
    app.register_blueprint(create_blueprint(lambda mid: payload if mid == '123' else None))
    client = app.test_client()
    assert client.get('/api/quant/status').json['trained']
    assert client.get('/api/quant/predict/123').json['success']
    assert client.get('/api/quant/predict/missing').status_code == 404
    response = client.post('/api/quant/bulk', json={'match_ids': ['123', 'missing']})
    assert response.status_code == 200
    assert 'score_matrix' not in response.json['predictions']['123']
    assert response.json['predictions']['missing']['status'] == 'ERROR_DATOS'
    assert client.post('/api/quant/bulk', json={'match_ids': []}).status_code == 400
    assert client.post('/api/quant/bulk', json={'match_ids': ['123'] * 51}).status_code == 400


def test_regularized_fit_is_finite_and_learns_home_advantage():
    from datetime import date, timedelta
    rng = np.random.default_rng(61)
    rows = [{'date': date(2026, 1, 1) + timedelta(days=i // 5),
             'home': str(i % 5), 'away': str((i + 1) % 5), 'league': 'L',
             'hg': int(rng.poisson(2.2)), 'ag': int(rng.poisson(.8))} for i in range(400)]
    fitted = q.fit(rows)
    h, a = q.intensities(fitted, '0', '1', 'L')
    assert h > a
    assert 0 < h < 8 and 0 < a < 8
    assert fitted['training_end'] == rows[-1]['date'].isoformat()
    assert q.matrix(h, a).sum() == pytest.approx(1)
