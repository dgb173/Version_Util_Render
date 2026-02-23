"""
SMART PATTERN DETECTOR
Analiza directamente los JSON, detecta patrones ganadores, 
y se autorregula basándose en resultados reales.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
from collections import defaultdict
from datetime import datetime


def load_all_matches(data_dir):
    """Carga todos los partidos de los JSON."""
    all_matches = []
    
    json_files = [
        'data_ah_0.5.json', 'data_ah_0.json', 'data_minus_ah_0.5.json',
        'data_ah_1.5.json', 'data_minus_ah_1.5.json',
        'data_ah_2_plus.json', 'data_minus_ah_2_plus.json',
    ]
    
    for fname in json_files:
        fpath = data_dir / fname
        if fpath.exists():
            print(f"  Cargando {fname}...", end='', flush=True)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_matches.extend(data)
                    print(f" {len(data)} partidos")
            except:
                print(" error")
    
    return all_matches


def parse_score(score_str):
    """Parsea resultado."""
    if not score_str or score_str in ['-', '?:?', '??', '']:
        return None, None
    score_str = str(score_str).replace('-', ':')
    parts = score_str.split(':')
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except:
        return None, None


def get_actual_winner(home_g, away_g, ah_line):
    """
    Determina quién ganó la apuesta AH.
    
    Analyzes data directly:
    - AH positive: HOME is favorite, gives handicap (subtracted)
    - AH negative: AWAY is favorite, home receives handicap (subtracted negative = added)
    """
    home_adjusted = home_g - ah_line
    
    if home_adjusted > away_g + 0.25:
        return 'HOME'
    elif home_adjusted > away_g:
        return 'HOME_HALF'
    elif home_adjusted == away_g:
        return 'PUSH'
    elif home_adjusted >= away_g - 0.25:
        return 'AWAY_HALF'
    else:
        return 'AWAY'


def extract_pattern_features(match):
    """Extrae características del partido para detectar patrones."""
    
    # Score
    score = match.get('final_score', '')
    home_g, away_g = parse_score(score)
    if home_g is None:
        return None
    
    # AH
    odds = match.get('main_match_odds', {})
    ah_str = odds.get('ah_linea')
    if not ah_str or ah_str == '-':
        return None
    try:
        ah = float(ah_str)
    except:
        return None
    
    # Resultado real
    winner = get_actual_winner(home_g, away_g, ah)
    
    # Features para patrones
    features = {
        'ah_line': ah,
        'ah_zone': categorize_ah(ah),
        'winner': winner,
        'is_home_win': winner in ['HOME', 'HOME_HALF'],
        'is_away_win': winner in ['AWAY', 'AWAY_HALF'],
    }
    
    # H2H Stadium
    h2h = match.get('h2h_stadium', {})
    h2h_res = h2h.get('res1')
    if h2h_res and h2h_res not in ['?:?', '-']:
        h, a = parse_score(h2h_res)
        if h is not None:
            features['h2h_stadium_wdl'] = 'W' if h > a else ('L' if h < a else 'D')
        else:
            features['h2h_stadium_wdl'] = None
    else:
        features['h2h_stadium_wdl'] = None
    
    # H2H General
    h2h_gen = match.get('h2h_general', {})
    h2h_gen_res = h2h_gen.get('res1')
    if h2h_gen_res and h2h_gen_res not in ['?:?', '-']:
        h, a = parse_score(h2h_gen_res)
        if h is not None:
            features['h2h_general_wdl'] = 'W' if h > a else ('L' if h < a else 'D')
        else:
            features['h2h_general_wdl'] = None
    else:
        features['h2h_general_wdl'] = None
    
    # Prev Home
    prev_home = match.get('last_home_match') or {}
    prev_h_score = prev_home.get('score')
    if prev_h_score:
        h, a = parse_score(prev_h_score)
        if h is not None:
            features['prev_home_wdl'] = 'W' if h > a else ('L' if h < a else 'D')
        else:
            features['prev_home_wdl'] = None
    else:
        features['prev_home_wdl'] = None
    
    # Prev Away
    prev_away = match.get('last_away_match') or {}
    prev_a_score = prev_away.get('score')
    if prev_a_score:
        h, a = parse_score(prev_a_score)
        if h is not None:
            features['prev_away_wdl'] = 'W' if a > h else ('L' if a < h else 'D')
        else:
            features['prev_away_wdl'] = None
    else:
        features['prev_away_wdl'] = None
    
    return features


def categorize_ah(ah):
    """Categoriza el AH."""
    if ah <= -1.5:
        return 'AWAY_BIG_FAV'
    elif ah <= -0.5:
        return 'AWAY_FAV'
    elif ah < 0:
        return 'AWAY_SLIGHT'
    elif ah == 0:
        return 'PICK_EM'
    elif ah < 0.5:
        return 'HOME_SLIGHT'
    elif ah < 1.5:
        return 'HOME_FAV'
    else:
        return 'HOME_BIG_FAV'


def find_patterns(matches_features, min_samples=25, min_win_rate=0.65):
    """Encuentra patrones ganadores."""
    
    # Agrupar por combinaciones de features
    pattern_stats = defaultdict(lambda: {'total': 0, 'home_wins': 0, 'away_wins': 0})
    
    # Patrones simples por zona AH (SOLO ZONA)
    for f in matches_features:
        if f is None:
            continue
        
        zone = f['ah_zone']
        
        # Pattern 1: Solo Zona
        key = ('zone', zone)
        pattern_stats[key]['total'] += 1
        if f['is_home_win']:
            pattern_stats[key]['home_wins'] += 1
        elif f['is_away_win']:
            pattern_stats[key]['away_wins'] += 1
        
        # Patrones con H2H
        if f.get('h2h_stadium_wdl'):
            key = ('zone_h2h', zone, f['h2h_stadium_wdl'])
            pattern_stats[key]['total'] += 1
            if f['is_home_win']:
                pattern_stats[key]['home_wins'] += 1
            elif f['is_away_win']:
                pattern_stats[key]['away_wins'] += 1
        
        # Patrones con prev_home
        if f.get('prev_home_wdl'):
            key = ('zone_prev_home', zone, f['prev_home_wdl'])
            pattern_stats[key]['total'] += 1
            if f['is_home_win']:
                pattern_stats[key]['home_wins'] += 1
            elif f['is_away_win']:
                pattern_stats[key]['away_wins'] += 1
        
        # Patrones con prev_away
        if f.get('prev_away_wdl'):
            key = ('zone_prev_away', zone, f['prev_away_wdl'])
            pattern_stats[key]['total'] += 1
            if f['is_home_win']:
                pattern_stats[key]['home_wins'] += 1
            elif f['is_away_win']:
                pattern_stats[key]['away_wins'] += 1
        
        # Combinaciones dobles
        if f.get('prev_home_wdl') and f.get('prev_away_wdl'):
            key = ('zone_both_prev', zone, f['prev_home_wdl'], f['prev_away_wdl'])
            pattern_stats[key]['total'] += 1
            if f['is_home_win']:
                pattern_stats[key]['home_wins'] += 1
            elif f['is_away_win']:
                pattern_stats[key]['away_wins'] += 1
    
    # Filtrar patrones ganadores
    winning_patterns = []
    
    for pattern, stats in pattern_stats.items():
        if stats['total'] < min_samples:
            continue
        
        home_rate = stats['home_wins'] / stats['total']
        away_rate = stats['away_wins'] / stats['total']
        
        if home_rate >= min_win_rate:
            winning_patterns.append({
                'pattern': pattern,
                'bet': 'HOME',
                'samples': stats['total'],
                'wins': stats['home_wins'],
                'win_rate': home_rate
            })
        elif away_rate >= min_win_rate:
            winning_patterns.append({
                'pattern': pattern,
                'bet': 'AWAY',
                'samples': stats['total'],
                'wins': stats['away_wins'],
                'win_rate': away_rate
            })
    
    # Ordenar por win_rate
    winning_patterns.sort(key=lambda x: x['win_rate'], reverse=True)
    
    return winning_patterns


def create_rules_from_patterns(patterns):
    """Convierte patrones en reglas aplicables."""
    rules = []
    
    for i, p in enumerate(patterns):
        pattern_type = p['pattern'][0]
        
        conditions = []
        
        if pattern_type == 'zone':
            conditions.append({'feature': 'ah_zone', 'op': '==', 'value': p['pattern'][1]})
        
        elif pattern_type == 'zone_h2h':
            conditions.append({'feature': 'ah_zone', 'op': '==', 'value': p['pattern'][1]})
            conditions.append({'feature': 'h2h_stadium_wdl', 'op': '==', 'value': p['pattern'][2]})
        
        elif pattern_type == 'zone_prev_home':
            conditions.append({'feature': 'ah_zone', 'op': '==', 'value': p['pattern'][1]})
            conditions.append({'feature': 'prev_home_wdl', 'op': '==', 'value': p['pattern'][2]})
        
        elif pattern_type == 'zone_prev_away':
            conditions.append({'feature': 'ah_zone', 'op': '==', 'value': p['pattern'][1]})
            conditions.append({'feature': 'prev_away_wdl', 'op': '==', 'value': p['pattern'][2]})
        
        elif pattern_type == 'zone_both_prev':
            conditions.append({'feature': 'ah_zone', 'op': '==', 'value': p['pattern'][1]})
            conditions.append({'feature': 'prev_home_wdl', 'op': '==', 'value': p['pattern'][2]})
            conditions.append({'feature': 'prev_away_wdl', 'op': '==', 'value': p['pattern'][3]})
        
        rules.append({
            'name': f"SMART_{int(p['win_rate']*100)}%_{p['bet'][0]}_{i+1}",
            'market': 'AH',
            'bet_side': p['bet'],
            'conditions': conditions,
            'metrics': {
                'samples': p['samples'],
                'wins': p['wins'],
                'win_rate': p['win_rate']
            }
        })
    
    return rules


def main():
    print("=" * 70)
    print("SMART PATTERN DETECTOR")
    print("Analiza datos reales y detecta patrones ganadores")
    print("=" * 70)
    
    base_path = Path(__file__).parent.parent
    data_dir = base_path / 'data'
    
    # Cargar datos
    print("\n[1/4] Cargando partidos...")
    matches = load_all_matches(data_dir)
    print(f"Total partidos: {len(matches)}")
    
    # Extraer features
    print("\n[2/4] Extrayendo features...")
    features = []
    for m in matches:
        f = extract_pattern_features(m)
        if f:
            features.append(f)
    print(f"Partidos con features: {len(features)}")
    
    # Verificar distribución de ganadores
    print("\n[3/4] Analizando distribución...")
    home_wins = sum(1 for f in features if f['is_home_win'])
    away_wins = sum(1 for f in features if f['is_away_win'])
    pushes = len(features) - home_wins - away_wins
    print(f"  HOME wins: {home_wins} ({home_wins/len(features)*100:.1f}%)")
    print(f"  AWAY wins: {away_wins} ({away_wins/len(features)*100:.1f}%)")
    print(f"  PUSH: {pushes} ({pushes/len(features)*100:.1f}%)")
    
    # Encontrar patrones
    print("\n[4/4] Buscando patrones ganadores...")
    
    # Intentar diferentes umbrales (más relajados)
    for min_wr in [0.60, 0.58, 0.55]:
        for min_samp in [30, 25, 20, 15]:
            patterns = find_patterns(features, min_samples=min_samp, min_win_rate=min_wr)
            
            if len(patterns) >= 20:  # Buscamos al menos 20 reglas
                print(f"\nEncontrados {len(patterns)} patrones con WR>={min_wr*100:.0f}%, samples>={min_samp}")
                break
        if len(patterns) >= 20:
            break
    
    if not patterns:
        print("No se encontraron patrones significativos")
        return
    
    # Convertir a reglas
    rules = create_rules_from_patterns(patterns)
    
    # Guardar
    output = base_path / 'models' / 'top_rules.json'
    with open(output, 'w', encoding='utf-8') as f:
        json.dump({
            'version': '5.0-smart',
            'generated': datetime.now().isoformat(),
            'total_rules': len(rules),
            'rules': rules
        }, f, indent=2, ensure_ascii=False)
    
    # Resultados
    print("\n" + "=" * 70)
    print("PATRONES ENCONTRADOS")
    print("=" * 70)
    
    for r in rules[:15]:
        conds = ' + '.join([f"{c['feature']}={c['value']}" for c in r['conditions']])
        print(f"  {r['name']:25} | {conds}")
        print(f"    -> Bet {r['bet_side']}: {r['metrics']['win_rate']*100:.1f}% ({r['metrics']['samples']} samples)")
    
    print(f"\nGuardado en: {output}")


if __name__ == '__main__':
    main()
