"""
=== APLICADOR DE REGLAS V3 ===
Aplica las reglas minadas (FAV/DOG) a partidos de precacheo.
Traduce FAV/DOG a HOME/AWAY según quién sea el favorito.
"""

import json
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / 'data'
RULES_FILE = DATA_DIR / 'mined_rules.json'

_rules_cache = None

def load_rules():
    global _rules_cache
    if _rules_cache is not None:
        return _rules_cache
    try:
        with open(RULES_FILE, 'r', encoding='utf-8') as f:
            _rules_cache = json.load(f)
        return _rules_cache
    except Exception as e:
        print(f"Error cargando reglas: {e}")
        return []

def get_movement_direction(mov_str):
    if not mov_str or ('>' not in mov_str and '->' not in mov_str):
        return None
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

def extract_features(match):
    """Extrae features desde la perspectiva del FAVORITO."""
    odds = match.get('main_match_odds') or {}
    ah_raw = odds.get('ah_linea') or match.get('handicap')
    
    try:
        ah = float(ah_raw)
    except:
        ah = 0
    
    is_home_fav = ah >= 0
    
    ma = match.get('market_analysis_data') or {}
    stadium = ma.get('stadium') or {}
    general = ma.get('general') or {}
    
    ph = match.get('last_home_match') or {}
    pa = match.get('last_away_match') or {}
    fav_prev = ph if is_home_fav else pa
    
    features = {
        'ah_bucket': normalize_ah_bucket(ah),
        'h2h_stadium_res': stadium.get('evaluation'),
        'h2h_stadium_mov': get_movement_direction(stadium.get('movement')),
        'h2h_general_res': general.get('evaluation'),
        'h2h_general_mov': get_movement_direction(general.get('movement')),
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
    
    return features

def apply_rules_to_match(match):
    """
    Aplica las reglas al partido y devuelve el mejor pick.
    Traduce FAV/DOG a HOME/AWAY según quién sea el favorito.
    """
    rules = load_rules()
    if not rules:
        return None
    
    features = extract_features(match)
    is_home_fav = features.get('is_home_fav', True)
    
    best_match = None
    best_roi = -999
    
    for rule in rules:
        conditions = rule.get('conditions', {})
        
        # Verificar condiciones
        match_all = True
        for cond_key, cond_val in conditions.items():
            if features.get(cond_key) != cond_val:
                match_all = False
                break
        
        if match_all and rule['roi'] > best_roi:
            best_roi = rule['roi']
            
            # Traducir FAV/DOG a HOME/AWAY
            rule_pick = rule['pick']
            if rule_pick == 'FAV':
                actual_pick = 'HOME' if is_home_fav else 'AWAY'
            else:  # DOG
                actual_pick = 'AWAY' if is_home_fav else 'HOME'
            
            best_match = {
                'rule_name': rule['name'],
                'pick': actual_pick,
                'probability': rule['probability'],
                'roi': rule['roi'],
                'samples': rule['samples']
            }
    
    return best_match

# Test
if __name__ == "__main__":
    try:
        with open(DATA_DIR / 'data_precacheo.json', 'r', encoding='utf-8') as f:
            matches = json.load(f)
        
        hits = 0
        fav_picks = 0
        dog_picks = 0
        
        for m in matches[:50]:
            result = apply_rules_to_match(m)
            if result:
                hits += 1
                if 'FAV' in result['rule_name']:
                    fav_picks += 1
                else:
                    dog_picks += 1
                print(f"{m.get('home_name')[:20]:<20} vs {m.get('away_name')[:20]:<20}")
                print(f"  Pick: {result['pick']} | ROI: {result['roi']}% | {result['rule_name']}")
        
        print(f"\nTotal hits: {hits}/50 | FAV: {fav_picks} | DOG: {dog_picks}")
    except Exception as e:
        print(f"Error: {e}")
