"""Train dated form + shots/attacks features, with temporal ablation and blend."""
import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'
import argparse
import hashlib
import json
import math
import sqlite3
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'src')]
from modules.quant_research import fit, intensities, matrix, outcome_probs, parse_date, score
from modules.quant_features import feature_packet, feature_rows, fit_form, form_intensities
from scripts.train_quant_v61 import atomic_json


def load(db, as_of):
    records, seen, rejected = [], set(), Counter()
    with sqlite3.connect(f'{db.resolve().as_uri()}?mode=ro', uri=True) as conn:
        query = "SELECT match_id,match_date,score,payload_json,explorer_json FROM matches WHERE state='historical' AND match_date < ? ORDER BY match_date,match_id"
        for mid, raw_day, raw_score, raw_payload, raw_explorer in conn.execute(query, (as_of.isoformat(),)):
            p = json.loads(raw_payload or '{}')
            if not p.get('home_name') or not p.get('away_name'):
                extra = json.loads(raw_explorer or '{}')
                p = {**p, **{k: v for k, v in extra.items() if v is not None and k not in p}}
            day, goals = parse_date(raw_day), score(raw_score)
            home, away, league = p.get('home_name') or p.get('home_team'), p.get('away_name') or p.get('away_team'), p.get('league_name')
            if not day or not goals or not home or not away or not league or max(goals) > 20 or p.get('is_neutral_venue'):
                rejected['identity_score_venue'] += 1
                continue
            key = (day, str(home).strip().casefold(), str(away).strip().casefold(), str(league).strip().casefold())
            if key in seen:
                rejected['duplicate_event'] += 1
                continue
            seen.add(key)
            packet = feature_packet(p, day)
            for side in ('home', 'away'):
                packet[side].pop('events', None)
            records.append({'id': str(mid), 'date': day, 'home': str(home).strip(), 'away': str(away).strip(),
                            'league': str(league).strip(), 'hg': goals[0], 'ag': goals[1], 'packet': packet})
            if len(records) % 5000 == 0:
                print(f'Extraídos {len(records)} partidos con variables anteriores al encuentro', flush=True)
    return records, dict(rejected)


def fit_rows(rows, ridge, telemetry):
    end = max(r['date'] for r in rows)
    raw, y, weights = [], [], []
    for r in rows:
        raw.extend(feature_rows(r['packet'], telemetry))
        y.extend((r['hg'], r['ag']))
        weights.extend([2 ** (-(end - r['date']).days / 180)] * 2)
    return fit_form(raw, y, weights, ridge, telemetry)


def predictions(model, rows):
    return [form_intensities(model, r['packet']) for r in rows]


def evaluate(rows, predictions):
    ll, brier, scorell, correct = [], [], [], []
    for r, (lam, nu) in zip(rows, predictions):
        p = matrix(float(np.clip(lam, .1, 8)), float(np.clip(nu, .1, 8)))
        probs = np.array(outcome_probs(p))
        target = 0 if r['hg'] > r['ag'] else 1 if r['hg'] == r['ag'] else 2
        ll.append(-math.log(max(1e-15, probs[target])))
        scorell.append(-math.log(max(1e-15, p[r['hg'], r['ag']])))
        brier.append(float(((probs - np.eye(3)[target]) ** 2).sum()))
        correct.append(int(probs.argmax() == target))
    return {'n': len(rows), 'log_loss_1x2': float(np.mean(ll)), 'score_log_loss': float(np.mean(scorell)),
            'brier_1x2': float(np.mean(brier)), 'accuracy_1x2': float(np.mean(correct))}


def blend(base, form, weight):
    # Geometric averaging preserves the log-linear score intensity scale.
    return np.exp((1 - weight) * np.log(np.maximum(base, .1)) + weight * np.log(np.maximum(form, .1)))


def train(db, as_of, output):
    rows, rejected = load(db, as_of)
    days = sorted({r['date'] for r in rows})
    if len(days) < 30 or len(rows) < 500:
        raise ValueError('Muestra insuficiente')
    vstart, tstart = days[int(.70 * len(days))], days[int(.85 * len(days))]
    development = [r for r in rows if r['date'] < vstart]
    validation = [r for r in rows if vstart <= r['date'] < tstart]
    test = [r for r in rows if r['date'] >= tstart]
    experiments, winners = [], {}
    for telemetry in (False, True):
        fitted = []
        for ridge in (1, 10, 100, 1000):
            model = fit_rows(development, ridge, telemetry)
            measures = evaluate(validation, predictions(model, validation))
            experiment = {'telemetry': telemetry, 'ridge': ridge, **measures}
            experiments.append(experiment); fitted.append((experiment, model))
        winners[telemetry] = min(fitted, key=lambda pair: pair[0]['score_log_loss'])
    best_config, best_model = winners[True]
    base_dev = fit(development, 180, 10)
    base_val = np.array([intensities(base_dev, r['home'], r['away'], r['league']) for r in validation])
    form_val = np.array(predictions(best_model, validation))
    blends = [{'weight': w, **evaluate(validation, blend(base_val, form_val, w))} for w in (0., .25, .5, .75, 1.)]
    best_blend = min(blends, key=lambda e: e['score_log_loss'])
    before_test = development + validation
    test_stats = fit_rows(before_test, best_config['ridge'], True)
    test_no_stats = fit_rows(before_test, winners[False][0]['ridge'], False)
    base_test = fit(before_test, 180, 10)
    bpred = np.array([intensities(base_test, r['home'], r['away'], r['league']) for r in test])
    spred = np.array(predictions(test_stats, test))
    coverage = {}
    for stat in ('shots', 'on_target', 'attacks', 'dangerous_attacks'):
        coverage[stat] = sum(all(r['packet'][side].get(stat + '_for') is not None for side in ('home', 'away')) for r in rows)
    report = {'as_of_exclusive': as_of.isoformat(), 'n': len(rows), 'rejected': rejected, 'coverage_both_teams': coverage,
              'split': {'train': len(development), 'validation': len(validation), 'test': len(test),
                        'validation_start': vstart.isoformat(), 'test_start': tstart.isoformat()},
              'experiments': experiments, 'blend_validation': blends, 'selected_blend': best_blend['weight'],
              'heldout_with_stats': evaluate(test, spred),
              'heldout_without_stats': evaluate(test, predictions(test_no_stats, test)),
              'heldout_team_baseline': evaluate(test, bpred),
              'heldout_blend': evaluate(test, blend(bpred, spred, best_blend['weight'])),
              'protocol_approved': False,
              'limits': ['Archivo retrospectivo: no acredita disponibilidad prospectiva de estadísticas.',
                         'Solo variables de eventos fechados antes del objetivo; no se usan sus stats_rows.',
                         'La ausencia se imputa a la media del entrenamiento más un indicador, nunca a cero.',
                         'Selecciones forzadas describen el modelo; no prueban ventaja económica.',
                         'Prueba ya observada: las nuevas comparaciones son investigación, requieren nueva prueba prospectiva.']}
    augmentation = fit_rows(rows, best_config['ridge'], True)
    augmentation.update(blend_weight=best_blend['weight'], training_end=max(r['date'] for r in rows).isoformat(),
                        training_count=len(rows), half_life=180, validation=report)
    augmentation['model_id'] = hashlib.sha256(json.dumps(augmentation, sort_keys=True).encode()).hexdigest()
    model = json.loads(output.read_text(encoding='utf-8'))
    model['telemetry_model'] = augmentation
    model['version'] = '6.1 RESEARCH + STATS'
    model['model_id'] = hashlib.sha256(json.dumps({k: v for k, v in model.items() if k != 'model_id'}, sort_keys=True).encode()).hexdigest()
    atomic_json(output, model)
    atomic_json(ROOT / 'artifacts/quant_v61/telemetry_training_report.json', report)
    print(json.dumps({k: v for k, v in report.items() if k not in ('experiments', 'blend_validation')}, indent=2), flush=True)
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, default=ROOT / 'data/app_data.db')
    parser.add_argument('--as-of', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, default=ROOT / 'models/quant_v61.json')
    args = parser.parse_args()
    train(args.db, args.as_of, args.output)
