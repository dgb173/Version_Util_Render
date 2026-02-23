"""
=== MINADOR DE REGLAS V3 - AMPLIADO ===
- Combinaciones de 1, 2, 3 y 4 filtros
- Apuestas tanto a FAVORITO como NO FAVORITO
- ROI mínimo 25%
- Objetivo: 200 reglas
"""

import json
import itertools
from pathlib import Path

# Configuración
DATA_DIR = Path('data')
OUTPUT_RULES = DATA_DIR / 'mined_rules.json'
MIN_SAMPLES = 15  # 15 muestras mínimas
MIN_ROI = 0.15  # 15% ROI mínimo
TARGET_RULES = 150
ODDS = 1.85









# --- Funciones de Cálculo ---

def parse_score(score_str):
    if not score_str: return None, None
    s = str(score_str).replace(':', '-').replace(' ', '')
    if '-' not in s or '?' in s: return None, None
    try:
        parts = s.split('-')
        return int(parts[0]), int(parts[1])
    except:
        return None, None

def calculate_ah_result(home_goals, away_goals, ah_line):
    """
    Calcula resultado desde perspectiva del FAVORITO y del NO FAVORITO.
    Retorna: (fav_result, dog_result)
    Valores: 1 (WIN), 0.5 (HALF WIN), 0 (PUSH), -0.5 (HALF LOSS), -1 (LOSS)
    """
    is_home_fav = ah_line >= 0
    
    if is_home_fav:
        fav_diff = home_goals - away_goals
        handicap = ah_line
    else:
        fav_diff = away_goals - home_goals
        handicap = abs(ah_line)
    
    # El favorito DA goles, necesita ganar por MÁS del hándicap
    adjusted = fav_diff - handicap
    
    # Manejar líneas de cuartos
    if abs(handicap % 0.5) == 0.25:
        line1 = handicap - 0.25
        line2 = handicap + 0.25
        adj1 = fav_diff - line1
        adj2 = fav_diff - line2
        r1 = 1 if adj1 > 0 else (-1 if adj1 < 0 else 0)
        r2 = 1 if adj2 > 0 else (-1 if adj2 < 0 else 0)
        fav_result = (r1 + r2) / 2
    else:
        if adjusted > 0:
            fav_result = 1
        elif adjusted < 0:
            fav_result = -1
        else:
            fav_result = 0
    
    # El resultado del DOG es exactamente opuesto
    dog_result = -fav_result
    
    return fav_result, dog_result

def calculate_roi(results, odds=ODDS):
    if not results: return 0
    total = 0
    for r in results:
        if r == 1: total += (odds - 1)
        elif r == 0.5: total += (odds - 1) / 2
        elif r == -0.5: total -= 0.5
        elif r == -1: total -= 1
    return total / len(results)

def get_da_diff(stats_rows):
    if not stats_rows: return None
    for r in stats_rows:
        if r.get('label') == 'Ataques Peligrosos':
            try:
                return int(r.get('home', 0)) - int(r.get('away', 0))
            except: pass
    return None

def normalize_ah_bucket(ah):
    if ah is None: return None
    ah = abs(float(ah))
    if ah >= 2.0: return '2+'
    if ah >= 1.5: return '1.5'
    if ah >= 1.0: return '1.0'
    if ah >= 0.5: return '0.5'
    return '0'

def get_movement_direction(mov_str):
    if not mov_str or ('>' not in mov_str and '->' not in mov_str): return None
    try:
        import re
        parts = re.split(r'->|>', mov_str)
        if len(parts) < 2: return None
        old_val = float(parts[0].strip())
        new_val = float(parts[1].strip())
        diff = new_val - old_val
        if diff > 0.3: return 'UP'
        elif diff < -0.3: return 'DOWN'
        else: return 'SAME'
    except:
        return None

# --- Carga de Datos ---

def load_all_matches():
    matches = []
    all_files = list(DATA_DIR.glob('data_ah_*.json')) + list(DATA_DIR.glob('data_minus_ah_*.json'))
    
    for f in all_files:
        print(f"Cargando {f.name}...")
        try:
            with open(f, 'r', encoding='utf-8') as file:
                data = json.load(file)
                for m in data:
                    score = m.get('final_score')
                    hg, ag = parse_score(score)
                    if hg is None: continue
                    
                    odds_data = m.get('main_match_odds') or {}
                    ah_raw = odds_data.get('ah_linea') or m.get('handicap')
                    if ah_raw is None: continue
                    try:
                        ah = float(ah_raw)
                    except: continue
                    
                    fav_result, dog_result = calculate_ah_result(hg, ag, ah)
                    is_home_fav = ah >= 0
                    
                    ph = m.get('last_home_match') or {}
                    pa = m.get('last_away_match') or {}
                    ma = m.get('market_analysis_data') or {}
                    stadium = ma.get('stadium') or {}
                    general = ma.get('general') or {}
                    fav_prev = ph if is_home_fav else pa
                    
                    features = {
                        'ah_bucket': normalize_ah_bucket(ah),
                        'h2h_stadium_res': stadium.get('evaluation'),
                        'h2h_stadium_mov': get_movement_direction(stadium.get('movement')),
                        'h2h_general_res': general.get('evaluation'),
                        'h2h_general_mov': get_movement_direction(general.get('movement')),
                        'fav_result': fav_result,
                        'dog_result': dog_result,
                        'is_home_fav': is_home_fav
                    }
                    
                    # DA del favorito
                    fav_da = get_da_diff(fav_prev.get('stats_rows'))
                    if fav_da is not None:
                        if not is_home_fav: fav_da = -fav_da
                        if fav_da > 15: features['fav_da_cat'] = 'DOMINANT'
                        elif fav_da > 5: features['fav_da_cat'] = 'FAVORABLE'
                        elif fav_da < -15: features['fav_da_cat'] = 'DOMINATED'
                        elif fav_da < -5: features['fav_da_cat'] = 'UNFAVORABLE'
                        else: features['fav_da_cat'] = 'NEUTRAL'
                    
                    # Delta de favoritismo
                    try:
                        prev_ah = abs(float(fav_prev.get('handicap_line_raw', 0) or 0))
                        curr_ah = abs(ah)
                        if curr_ah > prev_ah + 0.25: features['ah_delta'] = 'MORE_FAV'
                        elif curr_ah < prev_ah - 0.25: features['ah_delta'] = 'LESS_FAV'
                        else: features['ah_delta'] = 'SAME_FAV'
                    except:
                        pass
                    
                    matches.append(features)
        except Exception as e:
            print(f"Error: {e}")
    
    print(f"Total: {len(matches)} partidos")
    return matches

# --- Minería ---

def mine_rules(matches):
    filter_values = {
        'h2h_stadium_res': ['CUBIERTO', 'NO CUBIERTO', 'PUSH'],
        'h2h_stadium_mov': ['UP', 'DOWN', 'SAME'],
        'h2h_general_res': ['CUBIERTO', 'NO CUBIERTO', 'PUSH'],
        'h2h_general_mov': ['UP', 'DOWN', 'SAME'],
        'ah_delta': ['MORE_FAV', 'LESS_FAV', 'SAME_FAV'],
        'fav_da_cat': ['DOMINANT', 'FAVORABLE', 'NEUTRAL', 'UNFAVORABLE', 'DOMINATED'],
        'ah_bucket': ['0', '0.5', '1.0', '1.5', '2+'],
    }
    
    rules = []
    filter_keys = list(filter_values.keys())
    
    # Combinaciones de 1, 2, 3 y 4 filtros
    for num_filters in range(1, 5):
        print(f"Probando combinaciones de {num_filters} filtros...")
        for combo in itertools.combinations(filter_keys, num_filters):
            value_lists = [filter_values[k] for k in combo]
            
            for values in itertools.product(*value_lists):
                conditions = dict(zip(combo, values))
                
                # Filtrar partidos
                fav_results = []
                dog_results = []
                for m in matches:
                    match_all = True
                    for fk, fv in conditions.items():
                        if m.get(fk) != fv:
                            match_all = False
                            break
                    if match_all:
                        fav_results.append(m['fav_result'])
                        dog_results.append(m['dog_result'])
                
                if len(fav_results) < MIN_SAMPLES:
                    continue
                
                # Calcular ROI para FAV
                roi_fav = calculate_roi(fav_results)
                if roi_fav >= MIN_ROI:
                    win_rate = sum(1 for x in fav_results if x > 0) / len(fav_results)
                    rules.append({
                        'name': generate_name(conditions, 'FAV'),
                        'conditions': conditions,
                        'pick': 'FAV',
                        'samples': len(fav_results),
                        'roi': round(roi_fav * 100, 1),
                        'win_rate': round(win_rate * 100, 1),
                        'probability': round(win_rate * 100)
                    })
                
                # Calcular ROI para DOG (no favorito)
                roi_dog = calculate_roi(dog_results)
                if roi_dog >= MIN_ROI:
                    win_rate = sum(1 for x in dog_results if x > 0) / len(dog_results)
                    rules.append({
                        'name': generate_name(conditions, 'DOG'),
                        'conditions': conditions,
                        'pick': 'DOG',
                        'samples': len(dog_results),
                        'roi': round(roi_dog * 100, 1),
                        'win_rate': round(win_rate * 100, 1),
                        'probability': round(win_rate * 100)
                    })
    
    # Ordenar y deduplicar
    rules.sort(key=lambda x: -x['roi'])
    seen = set()
    unique = []
    for r in rules:
        key = (frozenset(r['conditions'].items()), r['pick'])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    
    return unique[:TARGET_RULES]

def generate_name(conditions, pick):
    parts = []
    
    mapping = {
        'h2h_stadium_res': {'CUBIERTO': 'EstOK', 'NO CUBIERTO': 'EstX', 'PUSH': 'Est='},
        'h2h_general_res': {'CUBIERTO': 'GenOK', 'NO CUBIERTO': 'GenX', 'PUSH': 'Gen='},
        'h2h_stadium_mov': {'UP': 'MovE+', 'DOWN': 'MovE-', 'SAME': 'MovE='},
        'h2h_general_mov': {'UP': 'MovG+', 'DOWN': 'MovG-', 'SAME': 'MovG='},
        'ah_delta': {'MORE_FAV': '+Fav', 'LESS_FAV': '-Fav', 'SAME_FAV': '=Fav'},
        'fav_da_cat': {'DOMINANT': 'DA++', 'FAVORABLE': 'DA+', 'NEUTRAL': 'DA=', 'UNFAVORABLE': 'DA-', 'DOMINATED': 'DA--'},
        'ah_bucket': {'0': 'AH0', '0.5': 'AH05', '1.0': 'AH1', '1.5': 'AH15', '2+': 'AH2+'},
    }
    
    for key, val in conditions.items():
        if key in mapping and val in mapping[key]:
            parts.append(mapping[key][val])
    
    return f"[{pick}] {' '.join(parts)}"

# --- Main ---

def main():
    print("=" * 60)
    print("MINADOR DE REGLAS V3 - AMPLIADO")
    print("=" * 60)
    print(f"ROI minimo: {MIN_ROI*100}% | Muestras: {MIN_SAMPLES} | Objetivo: {TARGET_RULES}")
    print()
    
    matches = load_all_matches()
    if len(matches) < 100:
        print("Pocos partidos")
        return
    
    print("\nMinando reglas...")
    rules = mine_rules(matches)
    
    print(f"\nReglas encontradas: {len(rules)}")
    
    if rules:
        with open(OUTPUT_RULES, 'w', encoding='utf-8') as f:
            json.dump(rules, f, ensure_ascii=False, indent=2)
        print(f"Guardadas en: {OUTPUT_RULES}")
        
        # Contar FAV vs DOG
        fav_rules = [r for r in rules if r['pick'] == 'FAV']
        dog_rules = [r for r in rules if r['pick'] == 'DOG']
        print(f"\nFAV: {len(fav_rules)} | DOG: {len(dog_rules)}")
        
        print("\n=== TOP 15 ===")
        for i, r in enumerate(rules[:15], 1):
            print(f"{i:2}. {r['name']:<40} ROI:{r['roi']:>5}% WR:{r['win_rate']:>5}% n={r['samples']}")
    else:
        print("No se encontraron reglas.")

if __name__ == "__main__":
    main()
