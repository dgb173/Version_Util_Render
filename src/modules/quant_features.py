"""Dated pre-match form and telemetry; never read target-match stats_rows."""
import math
import copy
import unicodedata

import numpy as np

from .quant_research import number, parse_date, score

STAT_NAMES = {'tiros': 'shots', 'tiros a puerta': 'on_target', 'ataques': 'attacks',
              'ataques peligrosos': 'dangerous_attacks'}
STAT_KEYS = tuple(STAT_NAMES.values())
SOURCE_KEYS = ('last_home_match', 'last_away_match', 'h2h_stadium', 'h2h_general',
               'h2h_col3', 'recent_home_matches', 'recent_away_matches',
               'recent_home_matches_all', 'recent_away_matches_all',
               'recent_home_matches_same_league_specific', 'recent_away_matches_same_league_specific',
               'recent_home_matches_same_league_general', 'recent_away_matches_same_league_general')


def enrich_prior_stats(payload, lookup):
    """Resolve closed/deferred cards from completed matches already in the DB.

    No network, no target match telemetry, and no reliance on expanded DOM.
    Return a copied snapshot so the exact evidence can be sealed in the audit.
    """
    day = parse_date(payload.get('match_date') or payload.get('date'))
    if day is None:
        return payload
    snapshot = copy.deepcopy(payload)
    indirect = snapshot.get('comparativas_indirectas') or {}
    cards = [snapshot.get(k) for k in ('last_home_match', 'last_away_match')]
    if isinstance(indirect, dict):
        cards.extend((indirect.get('left'), indirect.get('right')))
    evidence = []
    for event in cards:
        if not isinstance(event, dict):
            continue
        prior_day = parse_date(event.get('date') or event.get('match_date'))
        mid = event.get('match_id') or event.get('matchIndex')
        if not mid or str(mid) == str(payload.get('match_id')) or not prior_day or prior_day >= day:
            continue
        rows = event.get('stats_rows') or []
        present = {norm(r.get('label') or r.get('name')) for r in rows if isinstance(r, dict)
                   and number(r.get('home')) is not None and number(r.get('away')) is not None}
        if all(k in present for k in STAT_NAMES):
            continue
        prior = lookup(str(mid))
        if not isinstance(prior, dict) or not score(prior.get('final_score') or prior.get('score')):
            continue
        stored_day = parse_date(prior.get('match_date') or prior.get('date'))
        if stored_day != prior_day or stored_day >= day:
            continue
        expected = (norm(event.get('home_team') or event.get('home')), norm(event.get('away_team') or event.get('away')))
        actual = (norm(prior.get('home_name') or prior.get('home_team')), norm(prior.get('away_name') or prior.get('away_team')))
        if expected != actual:
            continue
        extra = [r for r in prior.get('stats_rows') or [] if isinstance(r, dict)
                 and norm(r.get('label') or r.get('name')) in STAT_NAMES
                 and norm(r.get('label') or r.get('name')) not in present
                 and number(r.get('home')) is not None and number(r.get('away')) is not None]
        if extra:
            event['stats_rows'] = list(rows) + copy.deepcopy(extra)
            evidence.append({'event_id': str(mid), 'date': stored_day.isoformat(), 'fields_added': len(extra),
                             'source': 'completed_match_in_local_database'})
    snapshot['quant_stats_enrichment'] = evidence
    return snapshot


def norm(value):
    return ''.join(c for c in unicodedata.normalize('NFKD', str(value or '').strip().casefold())
                   if not unicodedata.combining(c))


def historical_events(payload, decision_day):
    sources = [payload.get(k) for k in SOURCE_KEYS]
    indirect = payload.get('comparativas_indirectas') or {}
    if not isinstance(indirect, dict):
        indirect = {}
    sources.extend([indirect.get('left'), indirect.get('right')])
    events = {}
    for source in sources:
        for event in source if isinstance(source, list) else [source]:
            if not isinstance(event, dict):
                continue
            day = parse_date(event.get('date') or event.get('match_date'))
            home = event.get('home_team') or event.get('home') or event.get('h2h_home_team_name')
            away = event.get('away_team') or event.get('away') or event.get('h2h_away_team_name')
            if not day or day >= decision_day or not home or not away:
                continue
            key = (day, norm(home), norm(away))
            eid = str(event.get('match_id') or event.get('matchIndex') or '')
            if eid and eid == str(payload.get('match_id') or payload.get('id') or ''):
                continue
            goals = score(event.get('score') or event.get('score_raw') or event.get('result'))
            if goals is None and event.get('goles_home') is not None:
                goals = score(f"{event.get('goles_home')}:{event.get('goles_away')}")
            if goals is not None and max(goals) > 20:
                goals = None
            stats = {}
            for row in event.get('stats_rows') or []:
                if not isinstance(row, dict):
                    continue
                label = norm(row.get('label') or row.get('name'))
                if label not in STAT_NAMES:
                    continue
                h, a = number(row.get('home')), number(row.get('away'))
                maximum = 100 if label.startswith('tiros') else 1000
                if h is not None and a is not None and 0 <= h <= maximum and 0 <= a <= maximum:
                    stats[STAT_NAMES[label]] = (h, a)
            if key not in events:
                events[key] = {'id': eid, 'day': day, 'home': norm(home), 'away': norm(away),
                               'goals': goals, 'stats': stats}
            else:
                events[key]['stats'].update(stats)
                if events[key]['goals'] is None:
                    events[key]['goals'] = goals
    return list(events.values())


def form(payload, team, day, half_life=180, events=None):
    values = {k: [] for k in ('gf', 'ga', *(s + suffix for s in STAT_KEYS for suffix in ('_for', '_against')))}
    evidence = []
    for event in historical_events(payload, day) if events is None else events:
        if norm(team) not in (event['home'], event['away']):
            continue
        home = norm(team) == event['home']
        w = 2 ** (-(day - event['day']).days / half_life)
        if event['goals'] is not None:
            own, opp = event['goals'] if home else event['goals'][::-1]
            values['gf'].append((own, w))
            values['ga'].append((opp, w))
        for key, pair in event['stats'].items():
            own, opp = pair if home else pair[::-1]
            values[key + '_for'].append((own, w))
            values[key + '_against'].append((opp, w))
        if event['goals'] is not None or event['stats']:
            evidence.append({'event_id': event['id'], 'date': event['day'].isoformat(),
                             'is_home': home, 'has_stats': bool(event['stats'])})
    summary = {k: sum(v * w for v, w in vs) / sum(w for _, w in vs) if vs else None
               for k, vs in values.items()}
    summary['counts'] = {k: len(vs) for k, vs in values.items()}
    summary['events'] = evidence
    return summary


def feature_packet(payload, day=None, half_life=180):
    day = day or parse_date(payload.get('match_date') or payload.get('date'))
    if day is None:
        return {'home': {}, 'away': {}}
    events = historical_events(payload, day)
    return {side: form(payload, payload.get(side + '_name') or payload.get(side + '_team'), day, half_life, events)
            for side in ('home', 'away')}


def feature_names(telemetry=True):
    keys = ['gf', 'ga'] + ([s + suffix for s in STAT_KEYS for suffix in ('_for', '_against')] if telemetry else [])
    return ['is_home', 'own_matches', 'opponent_matches'] + [who + '_' + k for who in ('own', 'opponent') for k in keys]


def feature_rows(packet, telemetry=True, neutral=False):
    names = feature_names(telemetry)
    rows = []
    for side in ('home', 'away'):
        own, opp = packet[side], packet['away' if side == 'home' else 'home']
        row = [float(side == 'home' and not neutral), math.log1p(own.get('counts', {}).get('gf', 0)),
               math.log1p(opp.get('counts', {}).get('gf', 0))]
        for name in names[3:]:
            whom, key = name.split('_', 1)
            value = (own if whom == 'own' else opp).get(key)
            row.append(math.log1p(value) if value is not None else np.nan)
        rows.append(row)
    return rows


def design(raw, means, scales):
    missing = ~np.isfinite(raw)
    standardized = (np.where(missing, means, raw) - means) / scales
    return np.column_stack((np.ones(len(raw)), np.clip(standardized, -5, 5), missing.astype(float)))


def fit_form(raw, targets, weights, ridge, telemetry=True):
    raw = np.array(raw, dtype=float)
    finite = np.isfinite(raw)
    means = np.nansum(raw, axis=0) / np.maximum(1, finite.sum(axis=0))
    centered = np.where(finite, raw - means, 0.)
    scales = np.maximum(.1, np.sqrt((centered ** 2).sum(axis=0) / np.maximum(1, finite.sum(axis=0))))
    x = design(raw, means, scales)
    y, weights = np.asarray(targets, dtype=float), np.asarray(weights, dtype=float)
    beta = np.zeros(x.shape[1]); beta[0] = math.log(max(.1, float(np.average(y, weights=weights))))
    penalty = np.full(x.shape[1], float(ridge)); penalty[0] = .01

    def objective(b):
        eta = np.clip(x @ b, -8, 5)
        return float(np.sum(weights * (np.exp(eta) - y * eta)) + .5 * np.sum(penalty * b ** 2))

    for _ in range(25):
        eta = np.clip(x @ beta, -8, 5)
        mu = np.exp(eta)
        gradient = x.T @ (weights * (mu - y)) + penalty * beta
        hessian = x.T @ (x * (weights * mu)[:, None]) + np.diag(penalty)
        step = np.linalg.solve(hessian, gradient)
        rate, previous = 1., objective(beta)
        while rate > 1e-5 and objective(beta - rate * step) > previous:
            rate /= 2
        beta -= rate * step
        if np.max(np.abs(rate * step)) < 1e-6:
            break
    return {'means': means.tolist(), 'scales': scales.tolist(), 'beta': beta.tolist(),
            'ridge': ridge, 'telemetry': telemetry, 'feature_names': feature_names(telemetry)}


def form_intensities(model, packet, neutral=False):
    raw = np.array(feature_rows(packet, model['telemetry'], neutral), dtype=float)
    x = design(raw, np.array(model['means']), np.array(model['scales']))
    return tuple(np.exp(np.clip(x @ np.array(model['beta']), math.log(.1), math.log(8))).tolist())
