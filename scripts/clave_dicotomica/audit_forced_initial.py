"""Auditoría cronológica de clasificación obligatoria AH/O/U, solo líneas iniciales.

Los escenarios de cuota constante son simulaciones, nunca ROI observado.
No se seleccionan reglas ni variables usando el tramo final.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import json
import math
from pathlib import Path
import sqlite3
import sys
import unicodedata

import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import log_loss
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src'))
from modules.asian_settlement import settlement, unit_profit
from modules.clave_dicotomica import apply_key
from modules.joint_initial_market import classify_joint_initial_market

STATES = np.array([-1., -.5, 0., .5, 1.])


def number(value):
    try:
        text = str(value).strip().replace(',', '.')
        val = sum(float(p) for p in text.split('/')) / len(text.split('/'))
        return val if math.isfinite(val) else None
    except (ValueError, TypeError):
        return None


def date(value):
    text = str(value or '').strip()[:10]
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%d-%m-%Y'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def score(value):
    import re
    found = re.fullmatch(r'(\d+)\s*[:-]\s*(\d+)', str(value or '').strip())
    return tuple(map(int, found.groups())) if found else None


def team(value):
    return ''.join(c for c in unicodedata.normalize('NFKC', str(value or '')).casefold() if c.isalnum())


def mean(values):
    values = [x for x in values if x is not None and np.isfinite(x)]
    return sum(values) / len(values) if values else np.nan


def initial_decimal(odds, key):
    price = number(odds.get(key))
    if price is None or odds.get('odds_stage') != 'initial':
        return None
    fmt = str(odds.get('ah_price_format' if key.startswith('ah_') else 'ou_price_format') or odds.get('price_format') or '').lower()
    if fmt == 'hongkong':
        price += 1
    elif fmt != 'decimal':
        return None
    return price if price > 1 else None


def load_rows(db_path, quote_lookup=None):
    audit = Counter()
    rows = []
    keep = ('home_name', 'away_name', 'home_team', 'away_team', 'match_date', 'date', 'start_time',
            'league_id', 'league_name', 'league', 'main_match_odds', 'last_home_match', 'last_away_match',
            'h2h_general', 'h2h_stadium', 'h2h_col3', 'comparativas_indirectas', 'market_analysis_data',
            'home_standings', 'away_standings', 'pre_match_context', 'context_scraped_at', 'cached_at',
            'precacheo_date', 'is_neutral_venue')
    with sqlite3.connect('file:' + db_path.as_posix() + '?mode=ro', uri=True) as connection:
        query = "SELECT match_id, match_date, score, payload_json, explorer_json FROM matches WHERE state='historical'"
        parameters = []
        if quote_lookup:
            audit['archive_historical_rows'] = connection.execute("SELECT count(*) FROM matches WHERE state='historical'").fetchone()[0]
            parameters = list(quote_lookup)
            query += ' AND match_id IN (' + ','.join('?' for _ in parameters) + ')'
        for mid, raw_date, raw_score, payload, explorer in connection.execute(query,parameters):
            audit['historical_rows'] += 1
            match = {}
            for raw in (payload, explorer):
                obj = json.loads(raw or '{}')
                match.update({k: obj[k] for k in keep if k in obj and obj[k] not in (None, '', {}, [])})
                if score(obj.get('final_score')) is not None:
                    raw_score = obj['final_score']
            match['match_id'] = str(mid)
            if quote_lookup and str(mid) in quote_lookup:
                match['main_match_odds'] = quote_lookup[str(mid)]['main_match_odds']
            # Evitar retener HTML y cientos de filas de contexto por partido.
            context = match.get('pre_match_context') or {}
            current = context.get('current') or {}
            match['pre_match_context'] = {'current': {
                'date': current.get('date'),
                **{key: [{k: node[k] for k in ('date','home','away','score','score_raw','ouLine','matchIndex') if k in node}
                          for node in (current.get(key) or [])[:20]] for key in ('home_matches','away_matches')},
            }}
            day = date(match.get('match_date')) or date(match.get('date')) or date(raw_date)
            result = score(raw_score)
            odds = match.get('main_match_odds') or {}
            if odds.get('odds_stage') not in (None, 'initial', 'unverified'):
                audit['rejected_explicit_noninitial_stage'] += 1
                continue
            ah, ou = number(odds.get('ah_linea')), number(odds.get('goals_linea'))
            if day is None:
                audit['rejected_no_match_date'] += 1
                continue
            if result is None:
                audit['rejected_no_score'] += 1
                continue
            if ah is None or ou is None or abs(ah) > 20 or not 0 < ou <= 30 or any(abs(x * 4 - round(x * 4)) > 1e-8 for x in (ah, ou)):
                audit['rejected_no_valid_initial_lines'] += 1
                continue
            if not team(match.get('home_name') or match.get('home_team')) or not team(match.get('away_name') or match.get('away_team')):
                audit['rejected_no_teams'] += 1
                continue
            match['match_date'] = day.isoformat()
            match['home_name'] = match.get('home_name') or match.get('home_team')
            match['away_name'] = match.get('away_name') or match.get('away_team')
            timestamp = date(match.get('context_scraped_at') or match.get('cached_at') or match.get('precacheo_date'))
            if timestamp is None:
                audit['snapshot_time_unknown'] += 1
            elif timestamp >= day:
                audit['snapshot_not_demonstrably_prematch'] += 1
            else:
                audit['snapshot_previous_day_or_earlier'] += 1
            prices = [initial_decimal(odds, key) for key in ('ah_home_cuota', 'ah_away_cuota', 'goals_over_cuota', 'goals_under_cuota')]
            audit['raw_price_rows'] += int(any(number(odds.get(k)) is not None for k in ('ah_home_cuota', 'ah_away_cuota', 'goals_over_cuota', 'goals_under_cuota')))
            audit['verified_initial_price_rows'] += int(all(p is not None for p in prices))
            rows.append({'id': str(mid), 'date': day, 'match': match, 'home':match['home_name'], 'away':match['away_name'], 'ah': ah, 'ou': ou, 'score': result, 'prices': prices})
    rows.sort(key=lambda row: (row['date'], row['id']))
    audit['usable_rows'] = len(rows)
    return rows, dict(audit)


def form_features(entries, pressure, prefix, features):
    recent = entries[-10:]
    features[prefix + '_sample'] = len(recent)
    for label in ('margin', 'gf', 'ga', 'total', 'ah_residual', 'ou_residual', 'rival_strength'):
        features[prefix + '_' + label] = mean([r[label] for r in recent])
    features[prefix + '_win_rate'] = mean([int(r['margin'] > 0) for r in recent])
    features[prefix + '_draw_rate'] = mean([int(r['margin'] == 0) for r in recent])
    features[prefix + '_cover_mean'] = mean([r['ah_settlement'] for r in recent])
    features[prefix + '_cover_sample'] = sum(r['ah_settlement'] is not None for r in recent)
    features[prefix + '_margin_at_current_line'] = mean([r['margin'] - pressure for r in recent])


def sanitized_engine_match(match, day, audit):
    import copy
    cleaned = copy.deepcopy(match)
    # Las clasificaciones guardadas al recuperar un partido histórico no prueban
    # cuál era la tabla previa. Se excluyen del experimento principal.
    cleaned.pop('home_standings', None)
    cleaned.pop('away_standings', None)
    cleaned.pop('market_analysis_data', None)
    for key in ('last_home_match', 'last_away_match', 'h2h_general', 'h2h_stadium'):
        node = cleaned.get(key) or {}
        d = date(node.get('date') or node.get('date6') or node.get('date1'))
        if not d or d >= day:
            cleaned[key] = {}
            audit['excluded_context_' + key] += int(bool(node))
    comp = cleaned.get('comparativas_indirectas') or {}
    for key in ('left', 'right'):
        node = comp.get(key) or {}
        d = date(node.get('date'))
        if not d or d >= day:
            comp[key] = {}
    col3 = cleaned.get('h2h_col3') or {}
    node = col3.get('col3_data') or col3
    d = date(node.get('date'))
    if not d or d >= day:
        cleaned['h2h_col3'] = {}
    return cleaned


def load_goal_history(db_path):
    events=[]
    with sqlite3.connect('file:'+db_path.as_posix()+'?mode=ro',uri=True) as connection:
        payload = 'COALESCE(explorer_json,payload_json)'
        fields = [f"json_extract({payload}, '$.{key}')" for key in ('home_name','away_name','league_id','league_name','match_date','final_score')]
        query = "SELECT match_id,match_date,score," + ','.join(fields) + " FROM matches WHERE state='historical'"
        for mid,raw_date,raw_score,home,away,lid,lname,rich_date,rich_score in connection.execute(query):
            day,result = date(rich_date) or date(raw_date),score(rich_score) or score(raw_score)
            if day and result and home and away:
                events.append({'id':str(mid),'date':day,'score':result,'ah':None,'ou':None,
                    'match':{'home_name':home,'away_name':away,'league_id':lid,'league_name':lname}})
    return events


def build_features(rows, audit, events=None):
    history = defaultdict(list)
    direct = defaultdict(list)
    by_day = defaultdict(list)
    target_ids = {r['id'] for r in rows}
    combined = {r['id']:r for r in (events or [])}
    combined.update({r['id']:r for r in rows})
    for row in sorted(combined.values(),key=lambda r:(r['date'],r['id'])):
        by_day[row['date']].append(row)
    for day, daily in by_day.items():
        strengths = {}
        for event in daily:
            m=event['match']; league=str(m.get('league_id') or m.get('league_name') or m.get('league') or 'UNKNOWN')
            event['league']=league
            for identity in (team(m['home_name']),team(m['away_name'])):
                strengths[(league,identity)] = mean([r['margin'] for r in history[(league,identity)][-10:]])
        for row in daily:
            if row['id'] not in target_ids:
                continue
            match, ah, ou = row['match'], row['ah'], row['ou']
            league = str(match.get('league_id') or match.get('league_name') or match.get('league') or 'UNKNOWN')
            home, away = team(match['home_name']), team(match['away_name'])
            features = {'market_ah': ah, 'market_abs_ah': abs(ah), 'market_ou': ou,
                        'market_ah_x_ou': abs(ah) * ou, 'market_pickem': int(ah == 0),
                        'market_neutral': int(bool(match.get('is_neutral_venue')))}
            if all(price is not None for price in row['prices']):
                home_price,away_price,over_price,under_price = row['prices']
                features.update({'market_favorite_price':home_price if ah>=0 else away_price,
                    'market_dog_price':away_price if ah>=0 else home_price,
                    'market_over_price':over_price,'market_under_price':under_price})
            for name, identity, pressure, venue in (('home', home, ah, 'H'), ('away', away, -ah, 'A')):
                entries = history[(league, identity)]
                form_features(entries, pressure, 'form_' + name, features)
                local_entries = [r for r in entries if r['venue'] == venue]
                form_features(local_entries, pressure, 'field_' + name, features)
                same_line = [r for r in entries if r['pressure'] is not None and abs(r['pressure'] - pressure) < .01][-15:]
                features['recurrent_' + name + '_sample'] = len(same_line)
                features['recurrent_' + name + '_settlement'] = mean([r['ah_settlement'] for r in same_line])
                features['recurrent_' + name + '_margin'] = mean([r['margin'] for r in same_line])
                features['goals_' + name + '_recent_total'] = mean([r['total'] for r in entries[-5:]])
                features['goals_' + name + '_same_ou_sample'] = sum(r['ou'] is not None and abs(r['ou'] - ou) < .01 for r in entries[-20:])
                features['goals_' + name + '_same_ou_residual'] = mean([r['ou_residual'] for r in entries[-20:] if r['ou'] is not None and abs(r['ou'] - ou) < .01])
                other = away if name == 'home' else home
                other_strength = mean([r['margin'] for r in history[(league, other)][-10:]])
                similar = [r for r in entries[-30:] if np.isfinite(other_strength) and np.isfinite(r['rival_strength']) and abs(r['rival_strength'] - other_strength) < .75]
                features['rival_' + name + '_sample'] = len(similar)
                features['rival_' + name + '_margin'] = mean([r['margin'] for r in similar])
            pair = direct[(league, tuple(sorted((home, away))))][-5:]
            features['h2h_sample'] = len(pair)
            features['h2h_margin'] = mean([r['margin'] if r['home'] == home else -r['margin'] for r in pair])
            features['h2h_total'] = mean([r['total'] for r in pair])
            stadium = [r for r in pair if r['home'] == home]
            features['h2h_stadium_margin'] = mean([r['margin'] for r in stadium])
            features['h2h_stadium_total'] = mean([r['total'] for r in stadium])
            home_common = {r['rival']: r for r in history[(league, home)][-30:]}
            away_common = {r['rival']: r for r in history[(league, away)][-30:]}
            common = sorted(set(home_common) & set(away_common))
            features['indirect_sample'] = len(common)
            features['indirect_margin_gap'] = mean([home_common[r]['margin'] - away_common[r]['margin'] for r in common])
            cleaned = sanitized_engine_match(match, day, audit)
            for key, prefix in (('last_home_match', 'prev_home'), ('last_away_match', 'prev_away'), ('h2h_general', 'context_general'), ('h2h_stadium', 'context_stadium')):
                node = cleaned.get(key) or {}
                previous_score = score(node.get('score') or node.get('res6') or node.get('res1'))
                features[prefix + '_total'] = sum(previous_score) if previous_score else np.nan
                features[prefix + '_abs_margin'] = abs(previous_score[0] - previous_score[1]) if previous_score else np.nan
                for stat in node.get('stats_rows') or []:
                    label = team(stat.get('label'))
                    label = next((v for v in ('tirosapuerta', 'ataquespeligrosos', 'tiros', 'corners') if v == label), None)
                    if label:
                        h, a = number(stat.get('home')), number(stat.get('away'))
                        if h is not None and a is not None:
                            features['stats_' + prefix + '_' + label + '_total'] = h + a
            col3 = cleaned.get('h2h_col3') or {}
            col3 = col3.get('col3_data') or col3
            s = score(col3.get('score') or col3.get('result'))
            if s is None and col3.get('goles_home') is not None and col3.get('goles_away') is not None:
                a, b = number(col3.get('goles_home')), number(col3.get('goles_away'))
                s = (a, b) if a is not None and b is not None else None
            features['col3_total'] = sum(s) if s else np.nan
            features['col3_abs_margin'] = abs(s[0] - s[1]) if s else np.nan
            # Es descriptivo para comparar el motor existente. No aporta variables
            # al modelo que se entrena debajo.
            verdict = apply_key(cleaned)
            forced = classify_joint_initial_market(match, edge_ah=verdict.get('edge_AH', 0), edge_ou=verdict.get('edge_OU', 0))
            row['legacy_forced'] = (forced['forced_ah'] == 'FAV_CUBRE', forced['forced_ou'] == 'OVER')
            row['legacy_uncorrected'] = (verdict.get('edge_AH', 0) >= 0, verdict.get('edge_OU', 0) >= 0)
            row['legacy_published'] = (verdict.get('ah'), verdict.get('ou'))
            audit['legacy_forced_tie_ah'] += int(verdict.get('edge_AH', 0) == 0)
            audit['legacy_forced_tie_ou'] += int(verdict.get('edge_OU', 0) == 0)
            row['features'] = features
            row['league'] = league
            margin = row['score'][0] - row['score'][1]
            row['targets'] = (settlement(margin if ah >= 0 else -margin, abs(ah)), settlement(sum(row['score']), ou))
            row['rival_strengths'] = (features['form_away_margin'], features['form_home_margin'])
        # Ningún partido puede ver resultados de su mismo día.
        for row in daily:
            match, ah, ou, result = row['match'], row['ah'], row['ou'], row['score']
            home, away, league = team(match['home_name']), team(match['away_name']), row['league']
            margin, total = result[0] - result[1], sum(result)
            for identity, rival, venue, gf, ga, pressure, strength in (
                (home, away, 'H', result[0], result[1], ah, strengths[(league,away)]),
                (away, home, 'A', result[1], result[0], -ah if ah is not None else None, strengths[(league,home)])):
                history[(league, identity)].append({'venue': venue, 'margin': gf - ga, 'gf': gf, 'ga': ga,
                    'total': total, 'pressure': pressure, 'ou': ou, 'ah_residual': gf - ga - pressure if pressure is not None else None,
                    'ou_residual': total - ou if ou is not None else None, 'ah_settlement': settlement(gf - ga, pressure) if pressure is not None else None,
                    'rival': rival, 'rival_strength': strength})
            direct[(league, tuple(sorted((home, away))))].append({'home': home, 'margin': margin, 'total': total})
    return rows


def metrics(rows, picks):
    result = {}
    for market, index in (('AH', 0), ('OU', 1)):
        values = np.array([r['targets'][index] * (1 if p[index] else -1) for r, p in zip(rows, picks)])
        gains, losses = float(values[values > 0].sum()), float(-values[values < 0].sum())
        decided = int(np.count_nonzero(values))
        actual = []
        for r, p, settled in zip(rows, picks, values):
            if index == 0:
                home_pick = p[0] == (r['ah'] >= 0)
                price = r['prices'][0 if home_pick else 1]
            else:
                price = r['prices'][2 if p[1] else 3]
            if price is not None:
                actual.append(unit_profit(float(settled), price))
        result[market] = {'predictions': len(rows), 'wins_or_halfwins': int(sum(values > 0)),
            'losses_or_halflosses': int(sum(values < 0)), 'pushes': int(sum(values == 0)),
            'decided_hit_rate': round(100 * sum(values > 0) / decided, 3) if decided else None,
            'break_even_constant_decimal': round(1 + losses / gains, 4) if gains else None,
            'scenario_roi_percent': {str(price): round(100 * sum(unit_profit(float(v), price) for v in values) / len(values), 3)
                                     for price in (1.8, 1.9, 1.95, 2.0)},
            'real_priced_predictions': len(actual), 'real_net_units': round(sum(actual), 5) if actual else None,
            'real_roi_percent': round(100 * sum(actual) / len(actual), 4) if actual else None}
    return result


def probabilities(model, x, temperature=1, lines=None, total_goals=False):
    raw = model.predict_proba(x)
    aligned = np.full((len(x), 5), 1e-12)
    for k, cls in enumerate(model[-1].classes_):
        aligned[:, int(cls)] = raw[:, k]
    logits = np.log(np.clip(aligned,1e-12,1)) / temperature
    logits -= logits.max(axis=1, keepdims=True)
    adjusted = np.exp(logits)
    if lines is not None:
        allowed = np.zeros_like(adjusted)
        for line in np.unique(lines):
            # Los goles y márgenes son enteros: las liquidaciones imposibles
            # para esta línea reciben probabilidad cero por identidad matemática.
            support = np.zeros(5)
            start = 0 if total_goals else math.floor(line) - 2
            for outcome in range(start, math.ceil(line) + 3):
                value = settlement(outcome, float(line))
                support[int(np.where(STATES == value)[0][0])] = 1
            allowed[lines == line] = support
        adjusted *= allowed
    return adjusted / adjusted.sum(axis=1, keepdims=True)


def fit_models(x, targets, train, valid, columns, market_lines, c=None, temperature=None, algorithm='linear'):
    models, temperatures, losses = [], [], []
    for market in range(2):
        best = None
        grid = (7,15) if algorithm=='boosting' else (.03,.1,1.)
        for penalty in ((c[market],) if c else grid):
            if algorithm=='boosting':
                model = make_pipeline(SimpleImputer(strategy='median',add_indicator=True),
                    HistGradientBoostingClassifier(max_leaf_nodes=penalty,min_samples_leaf=30,
                        l2_regularization=10,max_iter=120,learning_rate=.05,early_stopping=False,random_state=20261012))
            else:
                model = make_pipeline(SimpleImputer(strategy='median', add_indicator=True), StandardScaler(),
                                      LogisticRegression(C=penalty, max_iter=800, solver='lbfgs'))
            model.fit(x[train][:, columns], targets[train, market])
            for temp in ((temperature[market],) if temperature else (1., 1.5, 2.)):
                p = probabilities(model, x[valid][:, columns], temp, market_lines[valid, market], total_goals=market == 1)
                loss = log_loss(targets[valid, market], p, labels=list(range(5)))
                if best is None or loss < best[0]:
                    best = (loss, model, temp, penalty)
        losses.append(float(best[0])); models.append(best[1]); temperatures.append(best[2])
    return models, temperatures, losses


def expected_returns(probability, prices):
    gain = probability[:,STATES>0] @ STATES[STATES>0]
    loss = -(probability[:,STATES<0] @ STATES[STATES<0])
    return np.column_stack((gain * (prices[:,0]-1)-loss, loss * (prices[:,1]-1)-gain))


def predictions(models, temps, x, market_lines, prices=None):
    ps = [probabilities(m, x, t, market_lines[:,j], total_goals=j == 1) for j,(m, t) in enumerate(zip(models, temps))]
    return decisions_from_probabilities(ps,market_lines,prices), ps


def decisions_from_probabilities(ps, market_lines, prices=None):
    if prices is None:
        prices = np.full((len(market_lines),4),2.)
    returns = [expected_returns(ps[j],prices[:,j*2:j*2+2]) for j in range(2)]
    options = []
    max_under_goals = np.ceil(market_lines[:,1])-1
    for ah_side,ou_side in ((0,0),(0,1),(1,0),(1,1)):
        utility = returns[0][:,ah_side] + returns[1][:,ou_side]
        if ou_side == 1:
            minimum = np.floor(market_lines[:,0])+1 if ah_side == 0 else np.where(market_lines[:,0]>0,0,1)
            utility = np.where(minimum<=max_under_goals,utility,-np.inf)
        options.append(utility)
    choice = np.argmax(np.column_stack(options),axis=1)
    return np.column_stack((choice<2,choice%2==0))


def run(db_path, output, quote_cache=None):
    quotes = json.loads(quote_cache.read_text(encoding='utf-8')).get('quotes', {}) if quote_cache else None
    rows, audit = load_rows(db_path,quotes)
    if quote_cache:
        priced_rows = []
        for row in rows:
            quote = quotes.get(row['id'])
            if not quote:
                continue
            odds = quote['main_match_odds']
            row['ah'], row['ou'] = number(odds['ah_linea']), number(odds['goals_linea'])
            row['match']['main_match_odds'] = odds
            row['prices'] = [initial_decimal(odds,k) for k in ('ah_home_cuota','ah_away_cuota','goals_over_cuota','goals_under_cuota')]
            if all(p is not None for p in row['prices']):
                priced_rows.append(row)
        audit['archived_lines_not_certified'] = len(rows)
        audit['recovered_verified_initial_prices'] = len(priced_rows)
        audit['usable_rows'] = len(priced_rows)
        rows = priced_rows
    print(f'[DATA] {len(rows)} filas válidas; reconstruyendo variables previas', flush=True)
    counters = Counter(audit)
    events=load_goal_history(db_path) if quote_cache else None
    counters['prior_result_history_rows'] = len(events or rows)
    rows = build_features(rows, counters,events)
    output.mkdir(parents=True, exist_ok=True)
    prepared = [{k: v for k,v in r.items() if k in ('id','date','home','away','ah','ou','targets','features','league','prices','legacy_forced','legacy_uncorrected','legacy_published')} for r in rows]
    for r in prepared:
        r['date'] = str(r['date'])
        r['features'] = {k: (v if np.isfinite(v) else None) for k,v in r['features'].items()}
    (output/'prepared_features.json').write_text(json.dumps(prepared,ensure_ascii=False,allow_nan=False),encoding='utf-8')
    for r in rows:
        r.pop('match', None)
    del prepared
    dates = sorted(set(r['date'] for r in rows))
    if len(dates) < 10:
        raise ValueError('Histórico insuficiente para tres períodos cronológicos')
    train_end, valid_end = dates[int(len(dates) * .6)], dates[int(len(dates) * .8)]
    train = np.array([r['date'] < train_end for r in rows]); valid = np.array([train_end <= r['date'] < valid_end for r in rows])
    test = np.array([r['date'] >= valid_end for r in rows]); test_rows = [r for r, use in zip(rows, test) if use]
    names = sorted({k for r in rows for k in r['features']})
    x = np.array([[r['features'].get(k, np.nan) for k in names] for r in rows])
    targets = np.array([[int(np.where(STATES == y)[0][0]) for y in r['targets']] for r in rows])
    market_lines = np.array([[abs(r['ah']),r['ou']] for r in rows])
    market_prices = np.array([[(r['prices'][0] if r['ah']>=0 else r['prices'][1]) or 2.,
        (r['prices'][1] if r['ah']>=0 else r['prices'][0]) or 2.,r['prices'][2] or 2.,r['prices'][3] or 2.] for r in rows])
    groups = sorted(set(k.split('_')[0] for k in names))
    configs = {'all_variables': list(range(len(names))), 'initial_lines_only': [i for i, k in enumerate(names) if k.startswith('market_')]}
    for group in groups:
        if group != 'market':
            configs['without_' + group] = [i for i, k in enumerate(names) if not k.startswith(group + '_')]
    configs['boosted_all_variables'] = configs['all_variables']
    configs['boosted_initial_market'] = configs['initial_lines_only']
    fits, val_reports = {}, {}
    for name, cols in configs.items():
        cols = [i for i in cols if np.any(np.isfinite(x[train,i]))]
        models, temps, losses = fit_models(x, targets, train, valid, cols, market_lines,
                                         algorithm='boosting' if name.startswith('boosted_') else 'linear')
        fits[name] = (models, temps, cols)
        picks, _ = predictions(models, temps, x[valid][:, cols], market_lines[valid],market_prices[valid])
        val_reports[name] = {'log_loss_mean': sum(losses) / 2, 'log_loss_by_market': losses,
            'features': len(cols), 'temperature': temps,
            'parameters': [m[-1].get_params() for m in models], 'metrics': metrics([r for r, use in zip(rows, valid) if use], picks)}
        print(f'[VALIDATION] {name}: log_loss={sum(losses)/2:.5f}', flush=True)
    selected = min(val_reports, key=lambda name: val_reports[name]['log_loss_mean'])
    selected_by_market = [min(val_reports,key=lambda name:val_reports[name]['log_loss_by_market'][j]) for j in range(2)]
    final = {}
    predictions_by_config = {}
    for name, (models, temps, cols) in fits.items():
        picks, ps = predictions(models, temps, x[test][:, cols], market_lines[test],market_prices[test])
        predictions_by_config[name] = picks
        final[name] = {'metrics': metrics(test_rows, picks),
            'log_loss_by_market': [log_loss(targets[test, j], p, labels=list(range(5))) for j, p in enumerate(ps)]}
        final[name]['forced_negative_estimated_ev_percent'] = {
            label:round(100 * np.mean(expected_returns(ps[j],market_prices[test,j*2:j*2+2])[np.arange(len(picks)),np.where(picks[:,j],0,1)]<0),2)
            for j,label in enumerate(('AH','OU'))}
    for name, extractor in (('legacy_forced_corrected', 'legacy_forced'), ('legacy_forced_original', 'legacy_uncorrected')):
        final[name] = {'metrics': metrics(test_rows, [r[extractor] for r in test_rows])}
    for name, direction in (('always_favorite_over', (True, True)), ('always_dog_under', (False, False))):
        final[name] = {'metrics': metrics(test_rows, [direction] * len(test_rows))}
    if selected_by_market[0] != selected_by_market[1]:
        ps = [probabilities(fits[name][0][j],x[test][:,fits[name][2]],fits[name][1][j],market_lines[test,j],total_goals=j==1)
              for j,name in enumerate(selected_by_market)]
        selected = 'marketwise_selection'
        picks = decisions_from_probabilities(ps,market_lines[test],market_prices[test])
        predictions_by_config[selected] = picks
        final[selected] = {'metrics':metrics(test_rows,picks),
            'log_loss_by_market':[log_loss(targets[test,j],p,labels=list(range(5))) for j,p in enumerate(ps)]}
    chosen = predictions_by_config[selected]
    day_metrics = []
    for day in sorted(set(r['date'] for r in test_rows)):
        mask = [i for i, r in enumerate(test_rows) if r['date'] == day]
        day_metrics.append([[sum(unit_profit(r['targets'][j] * (1 if chosen[i,j] else -1), 1.9) for i in mask for r in [test_rows[i]]), len(mask)] for j in range(2)])
    rng = np.random.default_rng(20261012)
    boot = np.array(day_metrics)
    intervals = {}
    for j, market in enumerate(('AH', 'OU')):
        sims = []
        for _ in range(1000):
            sample = boot[rng.integers(0, len(boot), len(boot)), j]
            sims.append(100 * sample[:,0].sum() / sample[:,1].sum())
        intervals[market] = list(np.round(np.percentile(sims, [2.5,97.5]), 3))
    real_intervals = None
    combined = None
    if all(all(price is not None for price in r['prices']) for r in test_rows):
        real_daily = []
        for day in sorted(set(r['date'] for r in test_rows)):
            ids = [i for i,r in enumerate(test_rows) if r['date']==day]
            sums = [0.,0.]
            for i in ids:
                r,p = test_rows[i],chosen[i]
                home_pick = bool(p[0]) == (r['ah']>=0)
                prices = (r['prices'][0 if home_pick else 1],r['prices'][2 if p[1] else 3])
                for j in range(2):
                    sums[j] += unit_profit(r['targets'][j]*(1 if p[j] else -1),prices[j])
            real_daily.append([*sums,len(ids)])
        values = np.array(real_daily)
        real_intervals = {}
        for j,label in enumerate(('AH','OU','COMBINED')):
            samples = []
            for _ in range(1000):
                # Bloques de 7 fechas para conservar parte de la dependencia.
                starts = rng.integers(0,len(values),math.ceil(len(values)/7))
                ids = np.concatenate([(np.arange(s,s+7)%len(values)) for s in starts])[:len(values)]
                sampled = values[ids]
                profit = sampled[:,j].sum() if j<2 else sampled[:,:2].sum()
                stakes = sampled[:,2].sum() * (1 if j<2 else 2)
                samples.append(100*profit/stakes)
            real_intervals[label] = list(np.round(np.percentile(samples,[2.5,97.5]),3))
        cumulative = np.r_[0,np.cumsum(values[:,:2].sum(axis=1))]
        combined = {'net_units':round(float(cumulative[-1]),4),'staked_units':2*len(test_rows),
            'roi_percent':round(float(cumulative[-1])*100/(2*len(test_rows)),4),
            'max_drawdown_daily_units':round(float(np.max(np.maximum.accumulate(cumulative)-cumulative)),4)}
    breakdown = {}
    for group_name, getter in (('exact_initial_ah', lambda r: str(r['ah'])), ('league', lambda r:r['league'])):
        grouped = defaultdict(list)
        for i, r in enumerate(test_rows):
            grouped[getter(r)].append(i)
        breakdown[group_name] = {k:metrics([test_rows[i] for i in ids], chosen[ids]) for k,ids in grouped.items() if len(ids)>=30}
    fold_reports = []
    test_dates = sorted(set(r['date'] for r in test_rows))
    for fold_days in np.array_split(test_dates, 4):
        days = set(fold_days)
        ids = [i for i,r in enumerate(test_rows) if r['date'] in days]
        if ids:
            fold_reports.append({'start':str(min(days)), 'end':str(max(days)), 'metrics':metrics([test_rows[i] for i in ids], chosen[ids])})
    report = {'generated_at':datetime.now().astimezone().isoformat(), 'data_audit':dict(counters),
        'methodology': {'split':'60/20/20 por días únicos; mismo día nunca participa en antecedentes',
            'selection':'variables, C y temperatura elegidos solo en validación; prueba final descriptiva y sin reajustes',
            'probability_support':'Se anulan estados de liquidación imposibles para cada línea exacta',
            'decision':'Mayor retorno estimado de una combinación compatible; se clasifica incluso con EV estimado negativo',
            'forced_coverage':'100% de partidos con fecha, equipos, marcador y ambas líneas válidas',
            'limits':['Sin garantía de snapshots inmutables previos; precios iniciales mayormente ausentes',
                      'Clasificación oficial excluida por falta de fecha previa verificable',
                      'Sin xG, lesiones, motivación, flujo de dinero ni movimientos',
                      'AH/OU disponibles etiquetados iniciales por el scraper; archivo legado sin metadatos suficientes',
                      'Histórico de equipos reconstruido desde archivo parcial; no representa calendario completo',
                      'ROI a cuotas constantes es hipotético, no observado',
                      'Los períodos históricos ya pudieron intervenir en reglas existentes; se requiere seguimiento prospectivo']},
        'periods': {name:{'rows':int(mask.sum()), 'start':str(min(r['date'] for r,use in zip(rows,mask) if use)), 'end':str(max(r['date'] for r,use in zip(rows,mask) if use))} for name,mask in (('train',train),('validation',valid),('test',test))},
        'feature_names':names, 'feature_nonmissing':{k:int(np.isfinite(x[:,i]).sum()) for i,k in enumerate(names)},
        'validation':val_reports, 'selected_on_validation':selected, 'selected_by_market':dict(zip(('AH','OU'),selected_by_market)), 'test':final,
        'selected_scenario_1_90_daily_bootstrap_95':intervals, 'selected_test_folds':fold_reports, 'selected_breakdown':breakdown}
    report['selected_real_roi_block_bootstrap_95'] = real_intervals
    report['selected_combined_real_priced_backtest'] = combined
    if quote_cache:
        report['methodology']['limits'][0] = 'Snapshots previos no certificados; aperturas y precios recuperados retrospectivamente desde el proveedor'
        report['methodology']['limits'][3] = 'AH y O/U de apertura de SoccerAjax; selección fija de Bet365 y fallback Sbobet, precios nativos HK convertidos a decimal'
    output.mkdir(parents=True, exist_ok=True)
    (output/'audit.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
    import csv
    with (output/'test_predictions.csv').open('w',encoding='utf-8',newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['match_id','date','home','away','ah_initial','ou_initial','ah_pick','ou_pick','ah_settlement','ou_settlement'])
        for r,p in zip(test_rows,chosen):
            writer.writerow((r['id'],r['date'],r['home'],r['away'],r['ah'],r['ou'],'FAV_CUBRE' if p[0] else 'DOG_CUBRE','OVER' if p[1] else 'UNDER',r['targets'][0]*(1 if p[0] else -1),r['targets'][1]*(1 if p[1] else -1)))
    labels = {'all_variables':'Todas las variables', 'initial_lines_only':'Solo líneas iniciales',
        'without_col3':'Sin Col3','without_context':'Sin H2H recuperado','without_field':'Sin factor campo',
        'without_form':'Sin forma ganadora','without_goals':'Sin historial de goles','without_h2h':'Sin H2H reconstruido',
        'without_indirect':'Sin rivales comunes','without_prev':'Sin previas','without_recurrent':'Sin AH recurrente',
        'without_rival':'Sin nivel de rivales','without_stats':'Sin tiros, ataques y córners',
        'legacy_forced_corrected':'Motor actual: combinación corregida','legacy_forced_original':'Motor actual: direcciones originales',
        'always_favorite_over':'Referencia: favorito + over','always_dog_under':'Referencia: no favorito + under'}
    labels.update({'boosted_all_variables':'Árboles: todas las variables','boosted_initial_market':'Árboles: mercado inicial',
                   'marketwise_selection':'Selección independiente AH y O/U'})
    if quote_cache: labels['initial_lines_only']='Mercado inicial: líneas y cuotas'
    lines = ['# Auditoría de clasificación obligatoria AH/O/U inicial', '', f'Selección en validación: **{labels.get(selected,selected)}**.',
        f"Prueba final: **{len(test_rows)} partidos**, de {report['periods']['test']['start']} a {report['periods']['test']['end']}.",
        '', '| Modelo | AH decidido | AH escenario cuota 1.90 | O/U decidido | O/U escenario cuota 1.90 |', '|---|---:|---:|---:|---:|']
    for name,node in final.items():
        a,o=node['metrics']['AH'],node['metrics']['OU']
        lines.append(f"| {labels.get(name,name)} | {a['decided_hit_rate']}% | {a['scenario_roi_percent']['1.9']}% | {o['decided_hit_rate']}% | {o['scenario_roi_percent']['1.9']}% |")
    lines += ['', '**Las columnas de cuota 1.90 son simulaciones. No demuestran rentabilidad real.**', '', '## Cobertura y límites', '', '```json', json.dumps(dict(counters),indent=2), '```', '']
    if combined:
        selected_metrics=final[selected]['metrics']
        lines += ['## Backtest a cuotas iniciales recuperadas', '',
            f"- AH: {selected_metrics['AH']['real_net_units']} unidades; ROI {selected_metrics['AH']['real_roi_percent']}%.",
            f"- O/U: {selected_metrics['OU']['real_net_units']} unidades; ROI {selected_metrics['OU']['real_roi_percent']}%.",
            f"- Ambos mercados, una unidad por mercado y partido: {combined['net_units']} unidades; ROI {combined['roi_percent']}%.",
            f"- Descenso máximo del capital al cierre de cada fecha: {combined['max_drawdown_daily_units']} unidades.",
            f"- Intervalos exploratorios 95% con bloques de fechas: {real_intervals}.",
            '', 'Son precios de apertura publicados por el proveedor, recuperados retrospectivamente; no son apuestas ejecutadas.', '']
    lines += ['- '+limit for limit in report['methodology']['limits']]
    (output/'INFORME.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print('[DONE] '+str(output),flush=True)
    print(json.dumps({'selected':selected,'test':final[selected]},indent=2),flush=True)
    return report


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--database',default=str(ROOT/'data/app_data.db'))
    parser.add_argument('--output',default=str(ROOT/'informes/2026-10-12_auditoria_metodo'))
    parser.add_argument('--quote-cache')
    args=parser.parse_args()
    from threadpoolctl import threadpool_limits
    with threadpool_limits(limits=1):
        run(Path(args.database).resolve(),Path(args.output).resolve(),Path(args.quote_cache).resolve() if args.quote_cache else None)
