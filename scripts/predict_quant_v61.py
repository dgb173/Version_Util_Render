"""Export current pre-cache forecasts using the trained v6.1 research model."""
import argparse
import json
import sqlite3
import sys
from functools import lru_cache
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src'))
from modules.quant_research import load_model, parse_date, predict
from modules.quant_features import enrich_prior_stats
from scripts.train_quant_v61 import atomic_json


def export(db, day, output):
    model = load_model()
    if model is None:
        raise ValueError('Entrenar primero con train_quant_v61.py')
    now = datetime.now(timezone.utc)
    forecasts = []
    with sqlite3.connect(f'{db.resolve().as_uri()}?mode=ro', uri=True) as conn:
        @lru_cache(maxsize=1000)
        def lookup(mid):
            found = conn.execute('SELECT payload_json FROM matches WHERE match_id=?', (str(mid),)).fetchone()
            return json.loads(found[0]) if found else None
        for mid, raw in conn.execute("SELECT match_id,payload_json FROM matches WHERE state='precacheo'"):
            payload = json.loads(raw)
            if parse_date(payload.get('match_date') or payload.get('date')) != day:
                continue
            result = predict(payload, model, now)
            if result.get('probabilities_1x2'):
                payload = enrich_prior_stats(payload, lookup)
                result = predict(payload, model, now)
            result.pop('score_matrix', None)
            forecasts.append({'event_id': mid, 'home': payload.get('home_name'), 'away': payload.get('away_name'),
                              'start_time': payload.get('start_time'), **result})
    counts = dict(Counter(p['status'] for p in forecasts))
    # Coverage-first ordering, not a calibrated estimate of betting profit.
    def priority(f):
        quality = f.get('data_quality', {})
        return (f.get('risk') == 'MEDIO', min(quality.get('minimum_danger_stats_matches', 0), 3),
                min(quality.get('minimum_form_matches', 0), 10),
                sum(s['prob_profit'] for s in f.get('selections', {}).values()))
    top = sorted([f for f in forecasts if f.get('selections') and not any(
        marker in (str(f.get('home', '')) + ' ' + str(f.get('away', ''))).casefold()
        for marker in ('u19', 'u20', 'u21', 'u23', 'reserves'))], key=priority, reverse=True)[:10]
    atomic_json(output, {'decision_at': now.isoformat(), 'date': day.isoformat(), 'counts': counts,
                         'top_picks': top, 'ranking': 'cobertura de datos primero; no rentabilidad calibrada',
                         'forecasts': forecasts})
    text = [f'# Pronósticos v6.1 RESEARCH · {day}', '',
            f'Cálculo UTC: {now.isoformat()}. Solo simulación; no hay pronósticos infalibles.', '',
            '| Inicio | Partido | Estado | Candidatos |', '|---|---|---|---|']
    for f in sorted(forecasts, key=lambda r: r.get('start_time') or ''):
        def describe(p):
            ev = f"{p['ev']:.1%}" if p['ev'] is not None else 'N/D'
            label = {'home': f['home'], 'away': f['away'], 'over': 'Over', 'under': 'Under'}[p['side']]
            line = abs(p['line']) if p['market'] == 'OU' else p['line']
            return f"{p['market']} {label} {line:+g}, P(beneficio) {p['prob_profit']:.1%}, EV {ev}, riesgo {p.get('risk', f.get('risk', 'N/D'))}"
        picks = '; '.join(describe(p) for p in f.get('selections', f['picks']).values()) or 'Sin pronóstico prepartido válido'
        name = f"{f['home']} – {f['away']}".replace('|', '/')
        text.append(f"| {f.get('kickoff_at') or f.get('start_time') or 'N/D'} | {name} | {f['status']} | {picks} |")
    output.with_suffix('.md').write_text('\n'.join(text), encoding='utf-8')
    shortlist = [f'# Picks concretos · {day}', '', f'Cálculo UTC: {now.isoformat()}.', '',
                 'Selecciones del modelo. P(beneficio) incluye medias ganancias; no son garantías.',
                 'Ordenadas por cobertura de datos, con cuotas del snapshot local.', '',
                 '| Inicio (España) | Partido | Pick AH | Pick goles | Estadísticas en modelo | Riesgo |', '|---|---|---|---|---|---|']
    for f in top:
        def cell(market):
            p = f['selections'].get(market)
            if not p:
                return 'Línea no disponible'
            who = {'home': f['home'], 'away': f['away'], 'over': 'Over', 'under': 'Under'}[p['side']]
            line = abs(p['line']) if market == 'OU' else p['line']
            odds = f"{p['odds_decimal']:.2f}" if p['odds_decimal'] else 'N/D'
            return f"{who} {line:+g} · P(beneficio) {p['prob_profit']:.1%} · cuota {odds}"
        name = f"{f['home']} – {f['away']}".replace('|', '/')
        start = f.get('kickoff_at') or f.get('start_time') or 'N/D'
        stats = 'Sí' if f.get('telemetry', {}).get('used_in_prediction') else 'No'
        shortlist.append(f"| {start} | {name} | {cell('AH')} | {cell('OU')} | {stats} | {f.get('risk', 'N/D')} |")
    (output.parent / 'current_picks.md').write_text('\n'.join(shortlist), encoding='utf-8')
    print(json.dumps({'counts': counts, 'top_picks': len(top), 'output': str(output)}, ensure_ascii=False))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, default=ROOT / 'data/app_data.db')
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--output', type=Path, default=ROOT / 'artifacts/quant_v61/current_forecasts.json')
    args = parser.parse_args()
    export(args.db, args.date, args.output)
