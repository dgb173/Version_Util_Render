"""Descriptive pre-match pattern taxonomy. No fitted probabilities or betting claims.

Source AH convention: positive means home concedes. All team views convert it
to the signed handicap applied to that team. Target outcomes never enter features.
"""
from collections import Counter
from datetime import datetime
from statistics import median
import math
import re
import unicodedata

VERSION = '1.0'
LABELS = {1: 'Gana completa', .5: 'Media ganancia', 0: 'Devolución', -.5: 'Media pérdida', -1: 'Pierde completa'}


def number(value):
    try:
        if value is None or isinstance(value, bool) or str(value).strip() == '':
            return None
        text = str(value).strip().replace('−', '-').replace(',', '.')
        if '/' in text:
            parts = [float(x) for x in text.split('/')]
            return sum(parts) / len(parts) if len(parts) == 2 else None
        n = float(text)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def first(obj, *keys):
    return next((obj[k] for k in keys if obj.get(k) not in (None, '', '-', 'N/A')), None)


def norm(s):
    return ''.join(c for c in unicodedata.normalize('NFKD', str(s or '').strip().casefold()) if not unicodedata.combining(c))


def day(value):
    s = str(value or '').strip()
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00')).date()
    except ValueError:
        for fmt in ('%d/%m/%Y', '%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d'):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                pass
    return None


def score(value):
    m = re.fullmatch(r'\s*(\d{1,2})\s*[:-]\s*(\d{1,2})\s*', str(value or ''))
    return tuple(map(int, m.groups())) if m else None


def settlement(value, line):
    """Return stake fraction won/lost (not monetary ROI); split quarter lines."""
    if line is None or value is None or abs(line * 4 - round(line * 4)) > 1e-6:
        return None
    parts = [line] if round(line * 4) % 2 == 0 else [line - .25, line + .25]
    return sum((value + p > 0) - (value + p < 0) for p in parts) / len(parts)


def counts(values):
    c = Counter(v for v in values if v is not None)
    return {'n': sum(c.values()), 'win': c[1], 'half_win': c[.5], 'push': c[0], 'half_loss': c[-.5], 'loss': c[-1]}


def direction(values, minimum=3):
    known = [v for v in values if v is not None]
    if len(known) < minimum:
        return 'ND'
    pos, neg = sum(v > 0 for v in known), sum(v < 0 for v in known)
    return 'FAVOR' if pos > neg else 'CONTRA' if neg > pos else 'MIXTO'


def event(raw, cutoff, target_id):
    if not isinstance(raw, dict):
        return None
    eid = str(first(raw, 'match_id', 'matchIndex', 'id') or '')
    date = day(first(raw, 'date', 'match_date', 'start_time'))
    # Same-day rows lack reliable kickoff ordering; exclude conservatively.
    if not cutoff or not date or date >= cutoff or (eid and eid == target_id):
        return None
    h = first(raw, 'home_team', 'home', 'home_name', 'h2h_home_team_name')
    a = first(raw, 'away_team', 'away', 'away_name', 'h2h_away_team_name')
    goals = score(first(raw, 'score', 'score_raw', 'result', 'final_score'))
    if goals is None and raw.get('goles_home') is not None:
        goals = score(f"{raw.get('goles_home')}:{raw.get('goles_away')}")
    if not h or not a or goals is None:
        return None
    ah = number(first(raw, 'ahLine', 'handicap_line_raw', 'ah_line', 'handicap', 'ah'))
    ou = number(first(raw, 'ouLine', 'over_under_line_raw', 'goal_line', 'goals_linea', 'ou_line'))
    stats = {}
    for r in raw.get('stats_rows') or []:
        if not isinstance(r, dict):
            continue
        name = norm(first(r, 'label', 'name'))
        key = {'tiros a puerta': 'sot', 'tiros': 'shots', 'ataques peligrosos': 'danger', 'ataques': 'attacks', 'corners': 'corners'}.get(name)
        x, y = number(r.get('home')), number(r.get('away'))
        if key and x is not None and y is not None and x >= 0 and y >= 0:
            stats[key] = [x, y]
    reds = [number(raw.get('home_red')), number(raw.get('away_red'))]
    return {'id': eid, 'date': date.isoformat(), 'home': str(h), 'away': str(a), 'goals': list(goals),
            'score': f'{goals[0]}:{goals[1]}', 'ah': ah, 'ou': ou, 'stats': stats,
            'reds': reds, 'league': str(first(raw, 'league_id_hist', 'league_id') or ''),
            'different_league': bool(raw.get('is_different_league')), 'age_days': (cutoff-date).days}


def side(e, team):
    if norm(team) == norm(e['home']):
        return 0
    if norm(team) == norm(e['away']):
        return 1
    return None


def view(e, team, target_line, goal_line):
    i = side(e, team)
    if i is None:
        return None
    own, opp = e['goals'][i], e['goals'][1-i]
    line = (-e['ah'] if i == 0 else e['ah']) if e['ah'] is not None else None
    total = own + opp
    return {**e, 'team': team, 'venue': 'CASA' if i == 0 else 'FUERA', 'opponent': e['away' if i == 0 else 'home'],
            'gf': own, 'ga': opp, 'margin': own-opp, 'wdl': 'V' if own > opp else 'D' if own < opp else 'E',
            'own_line': line, 'residual': own-opp+line if line is not None else None,
            'actual_ah': settlement(own-opp, line), 'target_ah': settlement(own-opp, target_line),
            'over_actual': settlement(total, -e['ou']) if e['ou'] is not None else None,
            'over_target': settlement(total, -goal_line) if goal_line is not None else None,
            'team_stats': {k: [v[i], v[1-i]] for k,v in e['stats'].items()}}


def profile(rows, team, venue, target_line, goal_line, league):
    values = [view(e, team, target_line, goal_line) for e in rows if side(e, team) is not None]
    values.sort(key=lambda e: e['date'], reverse=True)
    recent = values[:5]
    same_venue = [e for e in values if e['venue'] == venue][:5]
    # Comparable means same venue, same known competition, same role, +/-0.25.
    comparable = [e for e in values if e['venue'] == venue and league and e['league'] == league
                  and target_line is not None and e['own_line'] is not None
                  and (e['own_line'] < 0) == (target_line < 0) and abs(e['own_line']-target_line) <= .25][:10]
    habitual = [e['own_line'] for e in same_venue if e['own_line'] is not None]
    med = median(habitual) if habitual else None
    telemetry = {}
    for k in ('sot', 'shots', 'danger', 'attacks', 'corners'):
        pairs = [e['team_stats'][k] for e in recent if k in e['team_stats']]
        telemetry[k] = {'n': len(pairs), 'for': round(sum(p[0] for p in pairs)/len(pairs), 2) if pairs else None,
                        'against': round(sum(p[1] for p in pairs)/len(pairs), 2) if pairs else None}
    return {'team': team, 'n': len(recent), 'wins': sum(e['margin'] > 0 for e in recent),
            'draws': sum(e['margin'] == 0 for e in recent), 'losses': sum(e['margin'] < 0 for e in recent),
            'wide_wins': sum(e['margin'] >= 2 for e in recent), 'wide_losses': sum(e['margin'] <= -2 for e in recent),
            'actual': counts(e['actual_ah'] for e in recent), 'target': counts(e['target_ah'] for e in recent),
            'over_actual': counts(e['over_actual'] for e in recent), 'over_target': counts(e['over_target'] for e in recent),
            'rows': recent, 'same_venue': same_venue, 'comparable': comparable, 'habitual': med, 'habitual_n': len(habitual),
            'line_change': round(target_line-med, 2) if med is not None and target_line is not None else None,
            'telemetry': telemetry, 'available_n': len(values)}


def analyze(payload):
    p = payload
    ctx = (p.get('pre_match_context') or {}).get('current') or {}
    odds = p.get('main_match_odds') or {}
    home = first(p, 'home_name', 'home_team', 'home') or ctx.get('home_name') or ''
    away = first(p, 'away_name', 'away_team', 'away') or ctx.get('away_name') or ''
    mid = str(p.get('match_id') or '')
    cutoff = day(first(p, 'start_time_utc', 'start_time', 'match_date', 'date'))
    ah = number(first(p, 'handicap', 'ah_real'))
    if ah is None:
        ah = number(odds.get('ah_linea'))
    ou = number(first(p, 'goal_line', 'ou_line'))
    if ou is None:
        ou = number(odds.get('goals_linea'))
    favorite = home if ah is not None and ah > 0 else away if ah is not None and ah < 0 else None
    underdog = away if favorite == home else home if favorite else None
    line = -abs(ah) if favorite else None
    league = str(p.get('league_id') or ctx.get('league_id') or '')
    issues = []
    if not cutoff:
        issues.append('Falta fecha del partido: no se incorporan antecedentes sin corte temporal.')
    if not favorite:
        issues.append('AH 0 o ausente: no se asigna favorito automáticamente. Se muestran local y visitante.')
    if ou is None:
        issues.append('Falta la línea O/U; no se calcula cobertura de goles actual.')
    if p.get('is_neutral_venue'):
        issues.append('Sede neutral: la comparación casa/fuera requiere revisión.')
    pool = {}
    def add(raw):
        e = event(raw, cutoff, mid)
        if not e:
            return None
        key = '|'.join((e['date'], norm(e['home']), norm(e['away'])))
        if e['id']:
            conflict = next((x for x in pool.values() if x['id'] == e['id'] and (x['date'],norm(x['home']),norm(x['away'])) != (e['date'],norm(e['home']),norm(e['away']))), None)
            if conflict:
                issues.append(f'Antecedente {e["id"]}: fuentes con equipos o marcador contradictorios.')
                return None
        old = pool.get(key)
        if old:
            if (norm(old['home']), norm(old['away']), old['goals']) != (norm(e['home']), norm(e['away']), e['goals']):
                issues.append(f'Antecedente {key}: fuentes con equipos o marcador contradictorios.')
                return old
            for k in ('ah', 'ou', 'league'):
                if old[k] in (None, ''):
                    old[k] = e[k]
            old['stats'].update(e['stats'])
            return old
        pool[key] = e
        return e
    for s in ('home', 'away'):
        for suffix in ('', '_all', '_same_league_specific', '_same_league_general'):
            for r in p.get(f'recent_{s}_matches{suffix}') or []:
                add(r)
        for r in ctx.get(s+'_matches') or []:
            add(r)
        add(p.get('last_'+s+'_match'))
    # H2H and bridges are separate evidence; they do not get an extra vote in form.
    direct = []
    for kind in ('h2h_stadium', 'h2h_general'):
        block = p.get(kind) or {}
        for raw in block.get('all_matches') or []:
            e = event(raw, cutoff, mid)
            if e and {norm(e['home']), norm(e['away'])} == {norm(home), norm(away)}:
                direct.append(e)
        for suffix in ('1', '6'):
            h, a = (home, away) if suffix == '1' else (block.get('h2h_gen_home'), block.get('h2h_gen_away'))
            e = event({'home': h, 'away': a, 'score': block.get('res'+suffix), 'ah': block.get('ah'+suffix),
                       'date': block.get('date'+suffix), 'match_id': block.get('match'+suffix+'_id'),
                       'stats_rows': block.get('stats_rows') or []}, cutoff, mid)
            if e:
                direct.append(e)
    for e in pool.values():
        if {norm(e['home']), norm(e['away'])} == {norm(home), norm(away)}:
            direct.append(e)
    unique = {}
    for e in direct:
        key = '|'.join((e['date'], norm(e['home']), norm(e['away'])))
        if key not in unique:
            unique[key] = e
    direct = sorted(unique.values(), key=lambda e: e['date'], reverse=True)
    subject = favorite or home
    def h2h_model(e):
        v = view(e, subject, line, ou)
        v['requirement_change'] = round(v['own_line']-line, 2) if v['own_line'] is not None and line is not None else None
        v['ou_change'] = round(ou-e['ou'], 2) if ou is not None and e['ou'] is not None else None
        return v
    h2h = [h2h_model(e) for e in direct[:10] if side(e, subject) is not None]
    stadium = next((h2h_model(e) for e in direct if norm(e['home']) == norm(home)), None)
    for e in pool.values():
        e['different_league'] = e['different_league'] or bool(league and e['league'] and e['league'] != league)
    prof_home = profile(list(pool.values()), home, 'CASA', line if favorite == home else -line if favorite else None, ou, league)
    prof_away = profile(list(pool.values()), away, 'FUERA', line if favorite == away else -line if favorite else None, ou, league)
    fav = prof_home if favorite == home else prof_away if favorite else None
    dog = prof_away if favorite == home else prof_home if favorite else None
    # Col3 connects the previous opponents. Explicit identities prevent inversions.
    previous = {s: event(p.get('last_'+s+'_match'), cutoff, mid) for s in ('home', 'away')}
    rivals = {}
    for s, team in (('home', home), ('away', away)):
        e = previous[s]
        i = side(e, team) if e else None
        rivals[s] = e['away' if i == 0 else 'home'] if i is not None else None
    c = event(p.get('h2h_col3') or p.get('h2h_col3_general'), cutoff, mid)
    col3 = {'status': 'ND', 'row': None, 'reason': 'Col3 o rivales previos no disponibles.', 'signal': 'ND'}
    fav_side = 'home' if favorite == home else 'away'
    dog_side = 'away' if fav_side == 'home' else 'home'
    if c and favorite and all(rivals.values()) and {norm(c['home']), norm(c['away'])} == {norm(x) for x in rivals.values()} and norm(rivals['home']) != norm(rivals['away']):
        mirror = rivals[dog_side]
        row = view(c, mirror, line, ou)
        own_prev = view(previous[fav_side], favorite, line, ou)
        residual_delta = own_prev['residual'] - row['residual'] if own_prev['residual'] is not None and row['residual'] is not None else None
        col3 = {'status': 'OK', 'row': row, 'favorite_previous': own_prev,
                'mirror': mirror, 'common_opponent': rivals[fav_side],
                'orientation': 'DIRECTA' if side(c, mirror) == (0 if fav_side == 'home' else 1) else 'INVERSA',
                'residual_delta': residual_delta,
                'line_gap': round(line-row['own_line'], 2) if row['own_line'] is not None else None,
                'signal': 'FAVOR' if residual_delta is not None and residual_delta > 0 else 'CONTRA' if residual_delta is not None and residual_delta < 0 else 'MIXTO' if residual_delta == 0 else 'ND',
                'reason': 'Compara al favorito y al rival espejo contra el mismo oponente. Es evidencia indirecta, no una equivalencia de fuerza.'}
    elif c:
        col3['reason'] = 'Col3 encontrado, pero no se puede verificar el enlace con ambos rivales previos.'
    indirect = []
    for key, team, other_side in (('left', home, 'away'), ('right', away, 'home')):
        e = event((p.get('comparativas_indirectas') or {}).get(key), cutoff, mid)
        other = previous[other_side]
        other_team = away if team == home else home
        a = view(e, team, line if favorite == team else -line if favorite else None, ou) if e else None
        b = view(other, other_team, line if favorite == other_team else -line if favorite else None, ou) if other else None
        common = bool(a and b and norm(a['opponent']) == norm(b['opponent']))
        delta = None
        if common and a['residual'] is not None and b['residual'] is not None and favorite:
            delta = (a['residual']-b['residual']) * (1 if team == favorite else -1)
        indirect.append({'side': key, 'row': a, 'reference': b, 'verified': common, 'residual_delta': delta,
                         'signal': 'FAVOR' if delta is not None and delta > 0 else 'CONTRA' if delta is not None and delta < 0 else 'MIXTO' if delta == 0 else 'ND'})
    signals = []
    def signal(key, label, value, reason):
        signals.append({'key': key, 'label': label, 'value': value, 'reason': reason})
    signal('form', 'Margen del favorito', direction([e['target_ah'] for e in fav['rows']]) if fav else 'ND', 'Últimos cinco resultados reexpresados en la línea actual; rivales distintos.')
    signal('resistance', 'Resistencia del rival', direction([-e['target_ah'] for e in dog['rows'] if e['target_ah'] is not None]) if dog else 'ND', 'La cobertura del rival se expresa desde el lado del favorito.')
    comparable = direction([e['actual_ah'] for e in fav['comparable']]) if fav else 'ND'
    signal('comparable', 'Hándicaps comparables', comparable, 'Misma sede, competición y papel; diferencia máxima de 0,25. Mínimo tres casos.')
    last = h2h[0] if h2h else None
    hv = last['target_ah'] if last else None
    signal('h2h', 'Último enfrentamiento', 'FAVOR' if hv is not None and hv > 0 else 'CONTRA' if hv is not None and hv < 0 else 'MIXTO' if hv == 0 else 'ND', 'Un único antecedente; no se cuenta dos veces si coincide con el del estadio.')
    signal('col3', 'Col3 y hándicaps', col3['signal'], col3['reason'])
    for r in indirect:
        signal('ind_'+r['side'], 'Indirecta '+('local' if r['side']=='left' else 'visitante'), r['signal'], 'Diferencia de rendimientos frente a su línea ante un rival común verificado.')
    sot = fav['telemetry']['sot'] if fav else {'n': 0}
    production = 'ND'
    if sot['n'] >= 3:
        production = 'FAVOR' if sot['for'] > sot['against'] else 'CONTRA' if sot['for'] < sot['against'] else 'MIXTO'
    signal('production', 'Producción del favorito', production, 'Diferencia media de tiros a puerta; mínimo tres partidos con estadísticas. Confirma o contradice, sin equivaler a goles.')
    # Primary classification requires form on both sides and comparable handicap support.
    primary = [s['value'] for s in signals[:3]]
    all_known = [s['value'] for s in signals if s['value'] != 'ND']
    if not favorite or 'ND' in primary or any('contradictorios' in s for s in issues):
        category = 'DATOS_INSUFICIENTES'
    elif 'FAVOR' in all_known and 'CONTRA' in all_known:
        category = 'EVIDENCIA_ENFRENTADA'
    elif all(v == 'FAVOR' for v in primary):
        category = 'APOYO_AL_FAVORITO'
    elif all(v == 'CONTRA' for v in primary):
        category = 'LINEA_NO_RESPALDADA'
    else:
        category = 'MARGEN_INCIERTO'
    ou_home = direction([e['over_target'] for e in prof_home['rows']])
    ou_away = direction([e['over_target'] for e in prof_away['rows']])
    ou_type = 'ND' if 'ND' in (ou_home, ou_away) else 'OVER' if ou_home == ou_away == 'FAVOR' else 'UNDER' if ou_home == ou_away == 'CONTRA' else 'MIXTO'
    # A taxonomy key, never a fitted prediction. Exact AH/OU retained.
    movement = 'ND' if not last or last['requirement_change'] is None else 'SUBE' if last['requirement_change'] > 0 else 'BAJA' if last['requirement_change'] < 0 else 'IGUAL'
    def bucket_change(n):
        return 'ND' if n is None else 'SUBE' if n > 0 else 'BAJA' if n < 0 else 'IGUAL'
    def previous_code(s, team):
        e = previous[s]
        v = view(e, team, line if team == favorite else -line if line is not None else None, ou) if e else None
        return f'{v["wdl"]}/{v["own_line"]}/{v["actual_ah"]}' if v else 'ND'
    fingerprint = '|'.join([f'UA{VERSION}', f'AH={abs(ah) if ah is not None else "ND"}', f'OU={ou}', f'SEDE={fav_side if favorite else "NEUTRO"}',
                            f'FORMA={signals[0]["value"]}', f'RIVAL={signals[1]["value"]}', f'COMP={comparable}',
                            f'H2H={last["wdl"] if last else "ND"}/{movement}', f'COL3={col3["signal"]}/{col3.get("orientation","ND")}',
                            f'IND={indirect[0]["signal"]}/{indirect[1]["signal"]}', f'GOLES={ou_type}',
                            f'PREV_F={previous_code(fav_side, favorite) if favorite else "ND"}',
                            f'PREV_R={previous_code(dog_side, underdog) if underdog else "ND"}',
                            f'HAB={bucket_change(-fav["line_change"] if fav and fav["line_change"] is not None else None)}',
                            f'TIROS={production}', f'OU_H2H={bucket_change(last.get("ou_change") if last else None)}',
                            f'COL3_AH={c["ah"] if c else "ND"}', f'COL3_OU={c["ou"] if c else "ND"}'])
    for s in signals:
        if s['value'] == 'ND':
            issues.append(s['label']+': evidencia insuficiente o no verificable.')
    for prof in (prof_home, prof_away):
        if prof['n'] < 5:
            issues.append(f'{prof["team"]}: solo {prof["n"]} antecedentes válidos.')
        if prof['telemetry']['sot']['n'] < prof['n']:
            issues.append(f'{prof["team"]}: tiros a puerta disponibles en {prof["telemetry"]["sot"]["n"]}/{prof["n"]}.')
    flags = [e for e in pool.values() if any(x is not None and x > 0 for x in e['reds'])]
    if flags:
        issues.append(f'{len(flags)} antecedentes con expulsiones: consultar las filas antes de compararlos.')
    return {'version': VERSION, 'id': mid, 'home': home, 'away': away, 'date': cutoff.isoformat() if cutoff else None,
            'favorite': favorite, 'favorite_side': fav_side if favorite else None, 'line': line, 'ou': ou,
            'market': {'stage': odds.get('odds_stage'), 'captured_at': odds.get('captured_at'),
                       'intramatch_movement': None, 'note': 'El cambio H2H compara partidos distintos. Apertura→cierre no disponible como serie verificada.'},
            'profiles': {'home': prof_home, 'away': prof_away}, 'h2h': h2h, 'h2h_stadium': stadium,
            'context': {s: {'standings': p.get(s+'_standings') or {},
                            'ou_general': p.get(s+'_ou_stats_general') or {},
                            'ou_venue': p.get(s+'_ou_stats_specific') or {},
                            'rest_days': (cutoff-day(prof['rows'][0]['date'])).days if cutoff and prof['rows'] else None}
                        for s, prof in (('home', prof_home), ('away', prof_away))},
            'col3': col3, 'indirect': indirect, 'signals': signals, 'category': category, 'ou_category': ou_type,
            'fingerprint': fingerprint, 'issues': list(dict.fromkeys(issues)),
            'rules': 'Categorías descriptivas sin calibrar. Mayoría de resultados positivos/negativos con mínimo tres casos; se muestran las medias liquidaciones por separado. No son probabilidades. Antecedentes sin fecha o del mismo día/posteriores se excluyen. El resultado final del partido analizado no interviene.'}
