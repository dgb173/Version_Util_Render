"""Reproducible chronological training on existing SQLite results, read-only.

Usage: python scripts/train_quant_v61.py --as-of 2026-10-25
The final 15% of distinct dates never participates in parameter selection.
Historical archive ingestion times do not prove prospective availability;
all metrics and candidates remain RESEARCH, never protocol approved.
"""
from __future__ import annotations

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
sys.path.insert(0, str(ROOT / 'src'))
from modules.quant_research import fit, intensities, matrix, outcome_probs, parse_date, score


def load_records(db, as_of):
    records, rejected = [], Counter()
    seen = set()
    sql = """SELECT match_id,match_date,score,
        COALESCE(json_extract(payload_json,'$.home_name'),json_extract(explorer_json,'$.home_name')),
        COALESCE(json_extract(payload_json,'$.away_name'),json_extract(explorer_json,'$.away_name')),
        COALESCE(json_extract(payload_json,'$.league_name'),json_extract(explorer_json,'$.league_name')),
        json_extract(payload_json,'$.is_neutral_venue')
        FROM matches WHERE state='historical' ORDER BY match_date,match_id"""
    with sqlite3.connect(f'{db.resolve().as_uri()}?mode=ro', uri=True) as conn:
        for mid, raw_day, raw_score, home, away, league, neutral in conn.execute(sql):
            day, goals = parse_date(raw_day), score(raw_score)
            if not day or not goals or not home or not away or not league or home == away:
                rejected['missing_identity_date_or_score'] += 1
                continue
            if day >= as_of:
                rejected['on_or_after_as_of'] += 1
                continue
            if max(goals) > 20:
                rejected['score_out_of_range'] += 1
                continue
            # This baseline estimates ordinary home advantage. Neutral and
            # ambiguous venue records cannot be silently mixed with home games.
            if neutral:
                rejected['neutral_venue'] += 1
                continue
            event = (day, str(league).strip().casefold(), str(home).strip().casefold(), str(away).strip().casefold())
            if event in seen:
                rejected['duplicate_event'] += 1
                continue
            seen.add(event)
            records.append({'id': str(mid), 'date': day, 'home': str(home).strip(), 'away': str(away).strip(),
                            'league': str(league).strip(), 'hg': goals[0], 'ag': goals[1]})
    return records, dict(rejected)


def metrics(model, records):
    ll, brier, score_ll, hits, unknown, unsupported = [], [], [], [], 0, 0
    for r in records:
        lam, nu = intensities(model, r['home'], r['away'], r['league'])
        try:
            p = matrix(lam, nu, model.get('rho', 0))
        except ValueError:
            unsupported += 1
            continue
        probs = np.array(outcome_probs(p))
        target = 0 if r['hg'] > r['ag'] else 1 if r['hg'] == r['ag'] else 2
        ll.append(-math.log(max(probs[target], 1e-15)))
        brier.append(float(((probs - np.eye(3)[target]) ** 2).sum()))
        score_ll.append(-math.log(max(float(p[r['hg'], r['ag']]), 1e-15)))
        hits.append(int(probs.argmax() == target))
        from modules.quant_research import identity
        unknown += any(identity(r['league'], r[t]) not in model['attack'] for t in ('home', 'away'))
    if not ll:
        raise ValueError('No hay encuentros evaluables')
    return {'n': len(ll), 'log_loss_1x2': float(np.mean(ll)), 'brier_1x2': float(np.mean(brier)),
            'score_log_loss': float(np.mean(score_ll)), 'accuracy_1x2': float(np.mean(hits)),
            'unknown_team_matches': unknown, 'unsupported_intensity_matches': unsupported}


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
    temp.replace(path)


def train(db, as_of, output, report_dir):
    records, rejected = load_records(db, as_of)
    days = sorted({r['date'] for r in records})
    if len(records) < 500 or len(days) < 30:
        raise ValueError('Se necesitan al menos 500 resultados y 30 fechas distintas')
    dev_end, test_start = days[int(len(days) * .70)], days[int(len(days) * .85)]
    train_rows = [r for r in records if r['date'] < dev_end]
    valid_rows = [r for r in records if dev_end <= r['date'] < test_start]
    test_rows = [r for r in records if r['date'] >= test_start]
    if min(map(len, (train_rows, valid_rows, test_rows))) < 50:
        raise ValueError('Partición temporal insuficiente')
    experiments = []
    for half_life in (90, 180, 365):
        for ridge in (10, 30):
            model = fit(train_rows, half_life, ridge)
            for rho in (0., -.025, -.05):
                model['rho'] = rho
                result = metrics(model, valid_rows)
                experiments.append({'half_life': half_life, 'ridge': ridge, 'rho': rho, **result})
    # Select by validation only; prefer support over invalid Dixon-Coles fits.
    best = min(experiments, key=lambda e: (e['unsupported_intensity_matches'], e['score_log_loss']))
    pretest = [r for r in records if r['date'] < test_start]
    held_model = fit(pretest, best['half_life'], best['ridge'])
    held_model['rho'] = best['rho']
    test_metrics = metrics(held_model, test_rows)
    # Global Poisson reference trained on the same pretest period, no team data.
    reference = {'mu': math.log(np.mean([r['ag'] for r in pretest])),
                 'home_advantage': math.log(np.mean([r['hg'] for r in pretest]) / np.mean([r['ag'] for r in pretest])),
                 'attack': {}, 'defence': {}, 'league': {}, 'rho': 0.}
    baseline = metrics(reference, test_rows)
    # Walk forward with frozen hyperparameters: each window may only learn
    # results strictly before its first date, including past test windows.
    windows = []
    test_days = sorted({r['date'] for r in test_rows})
    for block in np.array_split(np.array(test_days, dtype=object), 3):
        if len(block):
            rows = [r for r in test_rows if block[0] <= r['date'] <= block[-1]]
            past = [r for r in records if r['date'] < block[0]]
            rolling = fit(past, best['half_life'], best['ridge'])
            rolling['rho'] = best['rho']
            windows.append({'from': block[0].isoformat(), 'to': block[-1].isoformat(),
                            'training_end': rolling['training_end'], **metrics(rolling, rows)})
    total = sum(w['n'] for w in windows)
    rolling_metrics = {key: sum(w[key] * w['n'] for w in windows) / total
                       for key in ('log_loss_1x2', 'brier_1x2', 'score_log_loss', 'accuracy_1x2')}
    rolling_metrics.update(n=total, unknown_team_matches=sum(w['unknown_team_matches'] for w in windows),
                           unsupported_intensity_matches=sum(w['unsupported_intensity_matches'] for w in windows))
    digest_rows = '\n'.join(json.dumps({**r, 'date': r['date'].isoformat()}, sort_keys=True) for r in records)
    report = {'version': '6.1 RESEARCH', 'as_of_exclusive': as_of.isoformat(), 'accepted': len(records),
              'rejected': rejected, 'dataset_sha256': hashlib.sha256(digest_rows.encode()).hexdigest(),
              'split': {'training': len(train_rows), 'validation': len(valid_rows), 'test': len(test_rows),
                        'validation_start': dev_end.isoformat(), 'test_start': test_start.isoformat()},
              'selected': best, 'experiments': experiments, 'heldout': test_metrics,
              'baseline_heldout': baseline, 'walk_forward': rolling_metrics, 'heldout_windows': windows,
              'beats_baseline_logloss': test_metrics['log_loss_1x2'] < baseline['log_loss_1x2'],
              'roi': None, 'protocol_approved': False,
              'limitations': ['Archivo retrospectivo: no demuestra disponibilidad prospectiva de cada resultado.',
                              'No se usan estadísticas del partido objetivo ni tablas recalculadas después.',
                              'Sin evaluación de rentabilidad con cuotas históricas verificadas.',
                              'Parámetros elegidos antes de prueba; ventanas walk-forward reajustan solo con el pasado.',
                              'No hay intervalos de incertidumbre ni calibración prospectiva; solo simulación.',
                              'Fechas sin hora: se excluye todo el día de decisión.']}
    final = fit(records, best['half_life'], best['ridge'])
    final.update(rho=best['rho'], validation={'heldout': test_metrics, 'baseline': baseline, 'walk_forward': rolling_metrics,
                 'beats_baseline_logloss': report['beats_baseline_logloss']}, dataset_sha256=report['dataset_sha256'])
    final['model_id'] = hashlib.sha256(json.dumps(final, sort_keys=True).encode()).hexdigest()
    report['model_id'] = final['model_id']
    atomic_json(output, final)
    atomic_json(report_dir / 'training_report.json', report)
    text = f'''# Modelo v6.1 RESEARCH

Resultados existentes aceptados: {len(records)}. Cierre exclusivo: {as_of}.
Entrenamiento / validación / prueba: {len(train_rows)} / {len(valid_rows)} / {len(test_rows)}.
Inicio de prueba sin ajuste de parámetros: {test_start}.

| Métrica fuera de muestra | Modelo | Poisson global |
|---|---:|---:|
| Log loss 1X2 (menor mejor) | {test_metrics['log_loss_1x2']:.6f} | {baseline['log_loss_1x2']:.6f} |
| Brier 1X2 (menor mejor) | {test_metrics['brier_1x2']:.6f} | {baseline['brier_1x2']:.6f} |
| Acierto 1X2 | {test_metrics['accuracy_1x2']:.2%} | {baseline['accuracy_1x2']:.2%} |

Selección interna: semivida {best['half_life']} días, regularización {best['ridge']}, rho {best['rho']}.
El artefacto final se reajusta con todos los resultados anteriores al cierre.
Sus métricas no se confunden con las del modelo congelado evaluado en prueba.

Estado: investigación, sin aprobación de apuestas. ROI no medido.

Walk-forward (tres ventanas, parámetros fijos, reajuste solo con el pasado):
log loss {rolling_metrics['log_loss_1x2']:.6f}, Brier {rolling_metrics['brier_1x2']:.6f},
acierto {rolling_metrics['accuracy_1x2']:.2%}. Los resultados de arriba corresponden
al modelo congelado previo a toda la prueba y permiten una comparación conservadora.

''' + '\n'.join('- ' + s for s in report['limitations'])
    (report_dir / 'README.md').write_text(text, encoding='utf-8')
    print(json.dumps({k: report[k] for k in ('accepted', 'split', 'selected', 'heldout', 'baseline_heldout')}, indent=2))
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, default=ROOT / 'data/app_data.db')
    parser.add_argument('--as-of', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, default=ROOT / 'models/quant_v61.json')
    parser.add_argument('--report-dir', type=Path, default=ROOT / 'artifacts/quant_v61')
    args = parser.parse_args()
    train(args.db, args.as_of, args.output, args.report_dir)
