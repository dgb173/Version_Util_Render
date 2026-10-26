"""v6.1 research: score distribution and exact Asian settlement.

Only dated, completed match scores are features. Prices require an explicit
format; absence of a price never becomes an invented 1.90 quote.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = ROOT / 'models' / 'quant_v61.json'
_cache = (None, None)


def number(value):
    try:
        result = float(str(value).replace(',', '.'))
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def parse_date(value):
    text = str(value or '').strip()
    for fmt in ('%Y-%m-%d', '%m/%d/%Y'):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            pass
    return None


def score(value):
    match = re.fullmatch(r'\s*(\d{1,2})\s*[:-]\s*(\d{1,2})\s*', str(value or ''))
    return tuple(map(int, match.groups())) if match else None


def quarter(value):
    value = number(value)
    if value is None or abs(value * 4 - round(value * 4)) > 1e-7:
        raise ValueError('Línea ausente o no expresada en cuartos de gol')
    return round(value * 4)


def settlement(margin, line, odds):
    """Net units, with line carried by selected side (or signed total)."""
    odds = number(odds)
    if odds is None or odds <= 1:
        raise ValueError('Se requiere cuota decimal > 1')
    q = quarter(line)
    parts = (q - 1, q + 1) if q % 2 else (q,)
    values = [odds - 1 if 4 * margin + part > 0 else
              -1.0 if 4 * margin + part < 0 else 0.0 for part in parts]
    return sum(values) / len(parts)


def matrix(lam, nu, rho=0.0):
    if not (0 < lam <= 8 and 0 < nu <= 8):
        raise ValueError('Intensidad fuera del soporte validado (0,8]')
    x = np.arange(36)
    hp = np.array([math.exp(-lam) * lam ** int(i) / math.factorial(int(i)) for i in x])
    ap = np.array([math.exp(-nu) * nu ** int(i) / math.factorial(int(i)) for i in x])
    p = np.outer(hp, ap)
    factors = (1 - lam * nu * rho, 1 + lam * rho, 1 + nu * rho, 1 - rho)
    if min(factors) <= 0:
        raise ValueError('Corrección Dixon-Coles no positiva')
    for cell, factor in zip(((0, 0), (0, 1), (1, 0), (1, 1)), factors):
        p[cell] *= factor
    return p / p.sum()


def outcome_probs(p):
    return [float(np.tril(p, -1).sum()), float(np.trace(p)), float(np.triu(p, 1).sum())]


def value_market(p, market, side, line, odds=None):
    q = quarter(line)
    x, y = np.indices(p.shape)
    margin = x - y if market == 'AH' else x + y
    if side in ('away', 'under'):
        margin = -margin
    parts = (q - 1, q + 1) if q % 2 else (q,)
    # Gross winning/losing fractions, independent of the offered price.
    w = sum((4 * margin + part > 0).astype(float) for part in parts) / len(parts)
    loss = sum((4 * margin + part < 0).astype(float) for part in parts) / len(parts)
    masses = {
        'win': float(p[w == 1].sum()),
        'half_win': float(p[(w == .5) & (loss == 0)].sum()),
        'push': float(p[(w == 0) & (loss == 0)].sum()),
        'half_loss': float(p[(loss == .5) & (w == 0)].sum()),
        'loss': float(p[loss == 1].sum()),
    }
    a, b = float((p * w).sum()), float((p * loss).sum())
    return {'market': market, 'side': side, 'line': line, 'odds_decimal': odds,
            'fair_odds': 1 + b / a if a > 1e-12 else None,
            'ev': a * (odds - 1) - b if odds is not None else None,
            'prob_profit': masses['win'] + masses['half_win'], 'settlements': masses}


def decimal_price(odds, key, market):
    price = number(odds.get(key))
    fmt = odds.get('ah_price_format' if market == 'AH' else 'ou_price_format') or odds.get('price_format')
    if price is None:
        return None
    if fmt == 'hongkong' and price > 0:
        return price + 1
    if fmt == 'decimal' and price > 1:
        return price
    return None


def identity(league, team):
    return str(league).strip().casefold() + '|' + str(team).strip().casefold()


def fit(records, half_life=180, ridge=20, iterations=180):
    """Penalized Poisson attack/defence MLE with continuous time decay.

    A damped diagonal Newton step is a descent step with factor 1/4 for
    the four nonzero entries per observation. Ridge identifies parameters.
    """
    teams = sorted({identity(r['league'], r[k]) for r in records for k in ('home', 'away')})
    leagues = sorted({r['league'] for r in records})
    ti, li = {t: i for i, t in enumerate(teams)}, {t: i for i, t in enumerate(leagues)}
    n, m = len(teams), len(leagues)
    h = np.array([ti[identity(r['league'], r['home'])] for r in records])
    a = np.array([ti[identity(r['league'], r['away'])] for r in records])
    l = np.array([li[r['league']] for r in records])
    end = max(r['date'] for r in records)
    weights = np.array([2 ** (-(end - r['date']).days / half_life) for r in records])
    y = np.array([r['hg'] for r in records] + [r['ag'] for r in records], dtype=float)
    weights = np.tile(weights, 2)
    attack, defence, league = np.zeros(n), np.zeros(n), np.zeros(m)
    mu, advantage = math.log(max(.1, y.mean())), 0.0
    atk, de, lg = np.concatenate((h, a)), np.concatenate((a, h)), np.tile(l, 2)
    home = np.concatenate((np.ones(len(h)), np.zeros(len(h))))
    for _ in range(iterations):
        eta = mu + home * advantage + attack[atk] + defence[de] + league[lg]
        pred = np.exp(np.clip(eta, -5, math.log(8)))
        residual, curvature = weights * (pred - y), weights * pred
        mu -= .2 * residual.sum() / max(curvature.sum(), 1e-9)
        advantage -= .2 * (residual * home).sum() / max((curvature * home).sum(), 1e-9)
        for params, idx, count in ((attack, atk, n), (defence, de, n), (league, lg, m)):
            grad = np.bincount(idx, weights=residual, minlength=count) + ridge * params
            diag = np.bincount(idx, weights=curvature, minlength=count) + ridge
            params -= .2 * grad / diag
    evidence = {}
    for t, idx in ti.items():
        mask = (atk == idx)
        w = weights[mask]
        evidence[t] = {'n': int(mask.sum()), 'effective_n': float(w.sum() ** 2 / (w @ w))}
    return {'version': '6.1 RESEARCH', 'half_life': half_life, 'ridge': ridge, 'rho': 0.,
            'mu': mu, 'home_advantage': advantage,
            'attack': dict(zip(teams, attack.tolist())), 'defence': dict(zip(teams, defence.tolist())),
            'league': dict(zip(leagues, league.tolist())), 'evidence': evidence,
            'training_end': end.isoformat(), 'training_count': len(records)}


def intensities(model, home, away, league, neutral=False):
    h, a = identity(league, home), identity(league, away)
    common = model['mu'] + model['league'].get(league, 0.)
    lam = math.exp(common + (0 if neutral else model['home_advantage']) +
                   model['attack'].get(h, 0.) + model['defence'].get(a, 0.))
    nu = math.exp(common + model['attack'].get(a, 0.) + model['defence'].get(h, 0.))
    return lam, nu


def load_model(path=MODEL_PATH):
    global _cache
    if not path.exists():
        return None
    stamp = (str(path), path.stat().st_mtime_ns)
    if _cache[0] != stamp:
        _cache = (stamp, json.loads(path.read_text(encoding='utf-8')))
    return _cache[1]


def predict(payload, model=None, decision_at=None):
    model = model if model is not None else load_model()
    now = decision_at or datetime.now(timezone.utc)
    result = {'version': '6.1 RESEARCH', 'status': 'ABSTAIN_MODELO', 'alerts': [],
              'markets': [], 'picks': {}, 'selections': {}, 'decision_at': now.isoformat(),
              'protocol_approved': False, 'stats_signals': {'b5': None, 'g5': None, 'g6': None}}
    if not model:
        result['alerts'].append('Modelo sin entrenar: ejecutar scripts/train_quant_v61.py')
        return result
    result['training_end'] = model['training_end']
    result['version'] = model.get('version', result['version'])
    result['validation'] = model.get('validation')
    result['model_id'] = model.get('model_id') or model.get('dataset_sha256')
    home, away = payload.get('home_name') or payload.get('home_team'), payload.get('away_name') or payload.get('away_team')
    league = str(payload.get('league_name') or '')
    day = parse_date(payload.get('match_date') or payload.get('date'))
    if not home or not away or not day:
        result.update(status='ERROR_DATOS', alerts=['Faltan equipos o fecha válida'])
        return result
    training_end = max(model['training_end'], model.get('telemetry_model', {}).get('training_end', model['training_end']))
    if day <= parse_date(training_end):
        result['alerts'].append('Partido anterior al cierre de entrenamiento: requiere modelo histórico separado')
        return result
    if (day - parse_date(model['training_end'])).days > 7:
        result['alerts'].append('Modelo desactualizado: volver a entrenar con resultados recientes')
        return result
    if score(payload.get('final_score') or payload.get('score')):
        result.update(status='ERROR_DATOS', alerts=['Partido con marcador observado: no es un pronóstico previo'])
        return result
    kickoff = None
    try:
        kickoff = datetime.fromisoformat(str(payload.get('start_time', '')).replace('Z', '+00:00'))
        if kickoff.tzinfo is None:
            kickoff = None
    except ValueError:
        pass
    # Legacy cache stores the scraper's Spain-local date and HH:MM separately
    # (extract_match_datetime_spain). Reconstruct that documented convention.
    if kickoff is None and re.fullmatch(r'\d{1,2}:\d{2}:?', str(payload.get('time', ''))):
        try:
            hour, minute = map(int, str(payload['time']).rstrip(':').split(':'))
            legacy = datetime(day.year, day.month, day.day, hour, minute, tzinfo=ZoneInfo('Europe/Madrid'))
            # Do not guess the fold during the DST clock change.
            if legacy.replace(fold=0).utcoffset() == legacy.replace(fold=1).utcoffset():
                kickoff = legacy
                result['alerts'].append('Inicio reconstruido del formato legado: fecha y hora de España')
        except ValueError:
            pass
    if kickoff is None or kickoff <= now:
        result.update(status='ABSTAIN_INCIERTO', alerts=[
            'Inicio con zona horaria no verificado o partido ya iniciado; modelo exclusivamente prepartido'])
        return result
    result['kickoff_at'] = kickoff.isoformat()
    evidence = [model['evidence'].get(identity(league, t), {'n': 0, 'effective_n': 0}) for t in (home, away)]
    result['evidence'] = dict(zip(('home', 'away'), evidence))
    sufficient_archive = min(e['effective_n'] for e in evidence) >= 8
    if not sufficient_archive:
        result['alerts'].append('Historial del archivo escaso: el pronóstico tiene mayor incertidumbre')
    from .quant_features import feature_packet, form_intensities
    packet = feature_packet(payload, day)
    result['telemetry'] = {'home': packet['home'], 'away': packet['away'], 'used_in_prediction': False,
                           'source': 'modelo_equipos', 'enrichment': payload.get('quant_stats_enrichment', [])}
    observed_stats = any(packet[s].get(key + '_for') is not None
                         for s in ('home', 'away') for key in ('shots', 'on_target', 'attacks', 'dangerous_attacks'))
    result['telemetry']['observed_stats_available'] = observed_stats
    lam, nu = intensities(model, home, away, league, payload.get('is_neutral_venue') is True)
    augmentation = model.get('telemetry_model')
    form_count = min(packet[s].get('counts', {}).get('gf', 0) for s in ('home', 'away'))
    sufficient_form = form_count >= 3
    if augmentation:
        fl, fn = form_intensities(augmentation, packet, payload.get('is_neutral_venue') is True)
        # Keep the mixture selected on the validation period for every team;
        # sparse teams are labelled as uncertain instead of changing the rule.
        weight = augmentation['blend_weight']
        lam, nu = (math.exp((1 - weight) * math.log(max(.1, b)) + weight * math.log(f))
                   for b, f in ((lam, fl), (nu, fn)))
        result['telemetry'].update(used_in_prediction=weight > 0 and observed_stats, uses_telemetry_model=True,
                                   source='mezcla_equipos_y_stats' if weight < 1 else 'forma_y_stats',
                                   blend_weight=weight, validation=augmentation.get('validation'))
    else:
        result['alerts'].append('Tiros y ataques visibles como evidencia; falta entrenar su modelo para usarlos numéricamente')
    stats_count = min(packet[s].get('counts', {}).get('dangerous_attacks_for', 0) for s in ('home', 'away'))
    result['risk'] = 'MEDIO' if (sufficient_archive and sufficient_form) or (augmentation and form_count >= 5 and stats_count >= 2) else 'ALTO'
    result['data_quality'] = {'archive_sufficient': sufficient_archive, 'form_sufficient': sufficient_form,
                              'minimum_form_matches': form_count, 'minimum_danger_stats_matches': stats_count}
    try:
        # Telemetry model validation uses independent Poisson (rho=0).
        rho = 0. if augmentation else model.get('rho', 0)
        p = matrix(lam, nu, rho)
    except ValueError as exc:
        result.update(status='ABSTAIN_MODELO', alerts=[str(exc)])
        return result
    result.update(lambdas={'home': lam, 'away': nu}, probabilities_1x2=dict(zip(('home', 'draw', 'away'), outcome_probs(p))))
    result['selection_1x2'] = max(result['probabilities_1x2'], key=result['probabilities_1x2'].get)
    result['dixon_coles_rho'] = rho
    result['score_matrix'] = p.tolist()
    odds = payload.get('main_match_odds') or {}
    ah, ou = number(odds.get('ah_linea')), number(odds.get('goals_linea'))
    result['favorite_side'] = ('home' if ah > 0 else 'away') if ah is not None else None
    specs = [('AH', 'home', -ah if ah is not None else None, 'ah_home_cuota'),
             ('AH', 'away', ah, 'ah_away_cuota'), ('OU', 'over', -ou if ou is not None else None, 'goals_over_cuota'),
             ('OU', 'under', ou, 'goals_under_cuota')]
    for market, side, line, key in specs:
        if line is None:
            continue
        try:
            if market == 'OU' and ou < 0:
                raise ValueError('Línea de goles negativa')
            result['markets'].append(value_market(p, market, side, line, decimal_price(odds, key, market)))
        except ValueError as exc:
            result['alerts'].append(str(exc))
    if any(m['odds_decimal'] is None for m in result['markets']) or len(result['markets']) < 4:
        result['alerts'].append('Faltan líneas, cuotas o formato explícito: EV no disponible en esos mercados')
    # Prices are useful for simulation only unless their timestamps precede both
    # the decision and a verified kickoff. No automatic approval of money bets.
    result['alerts'].append('Modelo de investigación; sin rentabilidad prospectiva demostrada ni aprobación de apuestas')
    result['status'] = 'ABSTAIN_INCIERTO'
    captured = None
    try:
        captured = datetime.fromisoformat(str(odds.get('captured_at', '')).replace('Z', '+00:00'))
        if captured.tzinfo is None or captured > now or captured >= kickoff:
            captured = None
    except ValueError:
        pass
    if captured is None:
        result['alerts'].append('Cuotas sin sello temporal previo verificable: la selección no acredita valor negociable')
    validated = model.get('validation', {}).get('beats_baseline_logloss', False)
    if not validated:
        result['alerts'].append('Sin mejora validada frente al Poisson global: no se emiten candidatos')
    for market in ('AH', 'OU'):
        available = [m for m in result['markets'] if m['market'] == market]
        if available:
            priced = all(m['ev'] is not None for m in available)
            selected = max(available, key=lambda m: m['ev'] if priced else m['prob_profit'])
            selected = {**selected, 'criterion': 'EV_estimado' if priced else 'probabilidad_de_beneficio',
                        'label': 'Pronóstico del modelo; ventaja no demostrada', 'risk': result['risk']}
            selected['reasons'] = [f'Goles esperados: local {lam:.2f}, visitante {nu:.2f}, total {lam + nu:.2f}.',
                                   f'Historial reciente mínimo entre ambos equipos: {form_count} partidos con marcador.']
            if result['telemetry']['used_in_prediction']:
                for field, label in (('shots_for', 'Tiros a favor'), ('dangerous_attacks_for', 'Ataques peligrosos a favor')):
                    hv, av = packet['home'].get(field), packet['away'].get(field)
                    if hv is not None and av is not None:
                        selected['reasons'].append(f'{label}, media histórica ponderada: local {hv:.1f}, visitante {av:.1f}.')
            result['selections'][market] = selected
        options = [m for m in result['markets'] if m['market'] == market and m['ev'] is not None and m['ev'] > .03]
        if options and captured is not None and validated and (sufficient_archive or (augmentation and sufficient_form)):
            result['picks'][market] = max(options, key=lambda m: m['ev'])
    if result['selections']:
        result['status'] = 'PRONOSTICO_MODELO'
    if result['picks']:
        result['status'] = 'CANDIDATO_SIMULACION'
    canonical = json.dumps({'snapshot': payload, 'model_id': result['model_id'], 'model_end': model['training_end'], 'decision': result['decision_at']}, sort_keys=True, ensure_ascii=False)
    result['packet_sha256'] = hashlib.sha256(canonical.encode()).hexdigest()
    return result
