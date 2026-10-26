import sys
from datetime import date
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from modules.quant_features import feature_packet, feature_rows, fit_form, form_intensities, enrich_prior_stats
from modules.quant_research import predict


def payload():
    event = {'match_id': 'old', 'date': '2026-10-01', 'home_team': 'Rival', 'away_team': 'Home', 'score': '1:2',
             'stats_rows': [{'label': 'Tiros', 'home': '20', 'away': '0'},
                            {'label': 'Tiros a Puerta', 'home': '6', 'away': '0'},
                            {'label': 'Ataques', 'home': '100', 'away': '70'},
                            {'label': 'Ataques Peligrosos', 'home': '60', 'away': '30'}]}
    return {'match_id': 'today', 'home_name': 'Home', 'away_name': 'Away', 'match_date': '2026-10-25',
            'last_home_match': event,
            'comparativas_indirectas': {'left': event},
            'recent_home_matches': [dict(event, stats_rows=[])],
            'stats_rows': [{'label': 'Tiros', 'home': 9999, 'away': 9999}]}


def test_telemetry_orients_deduplicates_and_preserves_zero():
    p = payload()
    packet = feature_packet(p)
    assert packet['home']['gf'] == 2
    assert packet['home']['shots_for'] == 0
    assert packet['home']['shots_against'] == 20
    assert packet['home']['dangerous_attacks_for'] == 30
    assert packet['home']['counts']['shots_for'] == 1
    assert packet['away']['shots_for'] is None
    assert packet['away']['counts']['shots_for'] == 0


def test_target_stats_and_future_events_never_enter_features():
    p = payload()
    original = feature_packet(p)
    p['stats_rows'] = [{'label': 'Tiros', 'home': 12, 'away': 4}]
    future = dict(p['last_home_match'], date='2026-10-26', match_id='future')
    p['recent_home_matches'].extend([future, dict(future, date='2026-10-25')])
    assert feature_packet(p) == original
    p['last_home_match']['match_id'] = 'today'
    p.pop('recent_home_matches'); p.pop('comparativas_indirectas')
    assert feature_packet(p)['home']['counts']['gf'] == 0


def test_telemetry_learns_predictive_effect_without_zero_imputation():
    rng = np.random.default_rng(61)
    raw, targets = [], []
    for _ in range(300):
        p = payload()
        danger = float(rng.uniform(5, 80))
        p['last_home_match']['stats_rows'][-1]['away'] = danger
        packet = feature_packet(p)
        raw.extend(feature_rows(packet))
        targets.extend((int(rng.poisson(.3 + danger / 25)), int(rng.poisson(1.))))
    model = fit_form(raw, targets, np.ones(len(targets)), 1, telemetry=True)
    p = payload(); low = feature_packet(p)
    p['last_home_match']['stats_rows'][-1]['away'] = 80
    high = feature_packet(p)
    assert form_intensities(model, high)[0] > form_intensities(model, low)[0]
    assert np.isfinite(model['beta']).all()
    assert all(v is not None for v in model['means'])


def test_full_forecast_uses_trained_stats_and_returns_concrete_selections():
    from datetime import datetime, timezone
    from modules.quant_features import feature_names
    names = feature_names(True)
    n = len(names)
    beta = [0.] * (1 + 2 * n)
    beta[1 + names.index('own_dangerous_attacks_for')] = .2
    telemetry_model = {'means': [0.] * n, 'scales': [1.] * n, 'beta': beta,
                       'telemetry': True, 'blend_weight': 1., 'training_end': '2026-10-24'}
    model = {'training_end': '2026-10-24', 'mu': 0., 'home_advantage': .2, 'league': {},
             'attack': {}, 'defence': {}, 'evidence': {}, 'rho': -.025, 'telemetry_model': telemetry_model}
    p = payload()
    p.update(start_time='2026-10-25T22:00:00+01:00', league_name='L',
             main_match_odds={'ah_linea': 0, 'goals_linea': 2.5})
    now = datetime(2026, 10, 25, 12, tzinfo=timezone.utc)
    a = predict(p, model, now)
    assert a['telemetry']['used_in_prediction']
    assert set(a['selections']) == {'AH', 'OU'}
    assert not a['picks']  # Unpriced forecasts remain explicit, not economic approvals.
    assert a['risk'] == 'ALTO'
    p['last_home_match']['stats_rows'][-1]['away'] = 80
    b = predict(p, model, now)
    assert b['lambdas']['home'] > a['lambdas']['home']
    assert b['dixon_coles_rho'] == 0


def test_closed_cards_resolve_real_prior_stats_without_mutating_snapshot():
    p = payload()
    prior = {'match_id': 'old', 'home_name': 'Rival', 'away_name': 'Home', 'match_date': '10/01/2026',
             'final_score': '1:2', 'stats_rows': list(p['last_home_match']['stats_rows'])}
    p['last_home_match']['stats_rows'] = []
    p.pop('comparativas_indirectas'); p.pop('recent_home_matches')
    enriched = enrich_prior_stats(p, lambda mid: prior)
    assert not p['last_home_match']['stats_rows']
    assert feature_packet(enriched)['home']['dangerous_attacks_for'] == 30
    assert enriched['quant_stats_enrichment'][0]['event_id'] == 'old'
    prior['match_date'] = '10/25/2026'
    assert not enrich_prior_stats(p, lambda mid: prior)['last_home_match']['stats_rows']
    prior['match_date'] = '10/01/2026'
    prior['home_name'] = 'Other Team'
    assert not enrich_prior_stats(p, lambda mid: prior)['last_home_match']['stats_rows']
