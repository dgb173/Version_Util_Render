
import json
import random
import copy
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import math

# Configuration
DATA_DIR = Path("data")
RESULTS_DIR = Path("backtest_results")
TARGET_ACCURACY = 75.0
MIN_SAMPLES = 15
GENERATIONS = 150

def parse_score(score_str) -> Optional[Tuple[int, int]]:
    if not score_str or score_str == '??' or score_str == '?:?': return None
    s = str(score_str).replace(':', '-').replace(' ', '')
    if '-' not in s: return None
    try:
        p = s.split('-')
        return int(p[0]), int(p[1])
    except:
        return None

def parse_ah(ah_str):
    if not ah_str or ah_str == 'N/A' or ah_str == '': return None
    try:
        return float(ah_str)
    except:
        return None

def get_asian_result_category(team_goals, opp_goals, ah):
    # Lógica del Usuario: AH Positivo = Favorito
    # Fórmula: (GolesTeam - GolesOpp) - AH
    diff = (team_goals - opp_goals) - ah
    if diff > 0.25: return 'WIN'
    if diff < -0.25: return 'LOSS'
    return 'PUSH' # or DRAW

def get_wdl_rank(score_str, is_home):
    s = parse_score(score_str)
    if not s: return None
    hg, ag = s
    team_goals = hg if is_home else ag
    opp_goals = ag if is_home else hg
    
    if team_goals > opp_goals: return 2 # Win
    if team_goals == opp_goals: return 1 # Draw
    return 0 # Loss

def get_movement_direction(mov_str):
    if not mov_str or '->' not in mov_str: return 'N/A'
    try:
        parts = mov_str.split('->')
        start = float(parts[0])
        end = float(parts[1])
        if end > start: return 'UP'
        if end < start: return 'DOWN'
        return 'EQUAL'
    except:
        return 'N/A'

def extract_advanced_features(m):
    """
    Extracts complex features based on Frontend Logic:
    - H2H Col 3 (Mejora/Iguala/Empeora)
    - Prev Home/Away Context w/ Handicap
    - Line Movements
    """
    f = {}
    
    # --- 1. Basic Data & Target ---
    odds = m.get('main_match_odds', {})
    current_ah = parse_ah(odds.get('ah_linea', 0)) or 0.0
    f['current_ah'] = current_ah
    
    # Target: Did Home Win/Cover?
    score = m.get('final_score')
    if not score or score == '??': return None
    s = parse_score(score)
    if not s: return None
    
    # Calculate actual result for training
    # We want to predict if the FAVORITE covers or a specific side covers?
    # Simple model predicts HOME_COVER (LOCAL) or AWAY_COVER (VISITA).
    # Using User Logic: 
    #   Perspective Local: result = get_asian_result_category(hg, ag, current_ah)
    #   If result == WIN -> LOCAL covers
    #   If result == LOSS -> VISITA covers
    
    real_res = get_asian_result_category(s[0], s[1], current_ah)
    if real_res == 'PUSH': return None # Skip pushes for training
    
    m_result = 'LOCAL' if real_res == 'WIN' else 'VISITA'
    
    # --- 2. Feature Engineering ---
    
    # Context: Who is favorite? (AH > 0 = Local Fav)
    fav_is_local = current_ah > 0
    fav_is_visitor = current_ah <= 0 # Assuming AH<=0 is Visitor fav (or equal)
    
    f['fav_is_home'] = fav_is_local
    f['is_heavy_fav'] = abs(current_ah) >= 1.0
    
    # --- 3. Prev Home / Prev Away Performance ---
    # Need to check coverage of CURRENT handicap in previous matches
    
    # Prev Home
    pc_home = m.get('last_home_match')
    f['ph_exists'] = bool(pc_home and pc_home.get('score'))
    if f['ph_exists']:
        ph_score = pc_home.get('score')
        # Does Prev Home cover CURRENT AH?
        # Perspective: Local
        ph_cover = get_asian_result_category(
            *parse_score(ph_score), current_ah
        )
        f['ph_covers_current'] = (ph_cover == 'WIN')
        f['ph_loses_current'] = (ph_cover == 'LOSS')
        
    # Prev Away
    pc_away = m.get('last_away_match')
    f['pa_exists'] = bool(pc_away and pc_away.get('score'))
    if f['pa_exists']:
        pa_score = pc_away.get('score')
        # Does Prev Away cover CURRENT AH?
        # Perspective: Visitor (so we invert logic? No, get_asian_result_category treats args as Team vs Opp)
        # But wait, frontend logic for Prev Away:
        # calculateWDL(lam.score, currentAh, false) -> false = Away Perspective
        # Inside calculateWDL(false): getAsianResultCategory(a, h, -ah)
        # Logic: (AwayGoals - HomeGoals) - (-AH) = (Away - Home) + AH
        
        # Let's replicate strict frontend calls:
        s_pa = parse_score(pa_score) # h, a
        # Away perspective: Team=Away, Opp=Home. AH for away = -current_ah
        # Formula: (Away - Home) - (-current_ah) = (Away - Home) + current_ah
        
        pa_diff = (s_pa[1] - s_pa[0]) - (-current_ah)
        
        if pa_diff > 0.25: pa_res = 'WIN'
        elif pa_diff < -0.25: pa_res = 'LOSS'
        else: pa_res = 'PUSH'
        
        f['pa_covers_current'] = (pa_res == 'WIN')
        f['pa_loses_current'] = (pa_res == 'LOSS')

    # --- 4. H2H Col 3 Logic (The Complex Part) ---
    col3 = m.get('h2h_col3', {})
    f['has_col3'] = col3.get('status') == 'found'
    
    if f['has_col3']:
        # Need match names to determine mirrors
        home_team = (m.get('home_team') or '').lower().strip()
        away_team = (m.get('away_team') or '').lower().strip()
        
        # 1. Fav WDL on Prev Match
        fav_wdl = None
        mirror_team = None
        
        if fav_is_local and f['ph_exists']:
             # Fav is Local -> Look at Last Home Match
             # Did Fav play home in previous?
             prev_h_home = (pc_home.get('home_team') or '').lower().strip()
             fav_played_home = home_team in prev_h_home # Simplified check
             fav_wdl = get_wdl_rank(pc_home.get('score'), fav_played_home)
             
             # Mirror is Visitor's Rival in Last Away Match
             if f['pa_exists']:
                 prev_a_home = (pc_away.get('home_team') or '').lower().strip()
                 prev_a_away = (pc_away.get('away_team') or '').lower().strip()
                 # Visitor (away_team) played in prev_a. Who was rival?
                 # If visitor was home, rival is away.
                 if away_team in prev_a_home: mirror_team = prev_a_away
                 else: mirror_team = prev_a_home
                 
        elif fav_is_visitor and f['pa_exists']:
             # Fav is Visitor -> Look at Last Away Match
             prev_a_home = (pc_away.get('home_team') or '').lower().strip()
             fav_played_home_in_prev = away_team in prev_a_home
             fav_wdl = get_wdl_rank(pc_away.get('score'), fav_played_home_in_prev)
             
             # Mirror is Local's Rival in Last Home Match
             if f['ph_exists']:
                 prev_h_home = (pc_home.get('home_team') or '').lower().strip()
                 prev_h_away = (pc_home.get('away_team') or '').lower().strip()
                 # Local (home_team) played in prev_h. Rival?
                 if home_team in prev_h_home: mirror_team = prev_h_away
                 else: mirror_team = prev_h_home
        
        # 2. Mirror WDL in H2H Col3
        mirror_wdl = None
        if mirror_team:
            h2h_home = (col3.get('h2h_home_team_name') or '').lower().strip()
            # h2h_away = (col3.get('h2h_away_team_name') or '').lower().strip()
            
            # Did mirror play home?
            mirror_played_home = mirror_team in h2h_home
            
            # Score
            h2h_score = f"{col3.get('goles_home', 0)}-{col3.get('goles_away', 0)}" # Using raw cols
            mirror_wdl = get_wdl_rank(h2h_score, mirror_played_home)
        
        # 3. Compare (MEJORA / IGUALA / EMPEORA)
        if fav_wdl is not None and mirror_wdl is not None:
            if fav_wdl > mirror_wdl: f['col3_mejora'] = True
            elif fav_wdl == mirror_wdl: f['col3_iguala'] = True
            elif fav_wdl < mirror_wdl: f['col3_empeora'] = True
        
        # 4. Directa / Inversa
        # Logic: 
        #   If Mirror Home in H2H == Directa (if Fav Local) OR Inversa (if Fav Visitor)??
        #   Replicating precacheo line 1986:
        #   mirrorIsHomeInH2H -> baseType = 'directa'
        #   If favIsVisitor -> invert baseType.
        
        mirror_is_home_h2h = mirror_team and (mirror_team in (col3.get('h2h_home_team_name') or '').lower().strip())
        
        base_type = 'directa' if mirror_is_home_h2h else 'inversa'
        if fav_is_visitor:
            final_type = 'inversa' if base_type == 'directa' else 'directa'
        else:
            final_type = base_type
            
        f['col3_directa'] = (final_type == 'directa')
        f['col3_inversa'] = (final_type == 'inversa')

    # --- 5. Movements ---
    stadium = m.get('market_analysis_data', {}).get('stadium', {})
    general = m.get('market_analysis_data', {}).get('general', {})
    
    st_mov = get_movement_direction(stadium.get('movement'))
    gen_mov = get_movement_direction(general.get('movement'))
    
    f['mov_stadium_up'] = (st_mov == 'UP')
    f['mov_stadium_down'] = (st_mov == 'DOWN')
    f['mov_general_up'] = (gen_mov == 'UP')
    f['mov_general_down'] = (gen_mov == 'DOWN')
    
    # Combined Mov
    f['mov_both_up'] = f['mov_stadium_up'] and f['mov_general_up']
    f['mov_both_down'] = f['mov_stadium_down'] and f['mov_general_down']
    
    return f, m_result



def load_all_data():
    dataset = []
    # Scan all JSONs
    files = list(RESULTS_DIR.glob("backtest_*.json")) + list(DATA_DIR.glob("data_*.json"))
    if not files:
        v = Path("validation_detailed_292.json")
        if v.exists(): files = [v]
    
    # Sort files by size to process largest first/or logic
    print(f"Found {len(files)} files.")
    
    for f_path in files:
        try:
            with open(f_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                matches_list = data if isinstance(data, list) else data.get('matches', [])
                dataset.extend(matches_list)
        except Exception as e:
            print(f"Skipping {f_path}: {e}")
            
    return dataset

def evaluate_individual(rule, features):
    # Rule is dict {feature: value}
    # Features is dict {feature: value} + targets
    for k, v in rule.items():
        if features.get(k) != v: return False
    return True

def run_genetic_miner():
    print("Loading data...")
    matches = load_all_data()
    print(f"Dataset size: {len(matches)} matches.")
    RESULTS_FILE = RESULTS_DIR / "advanced_rules_col3.json"
    
    # Pre-calculate features for speed
    print("Pre-calculating features...")
    data_points = []
    
    for m in matches:
        f = extract_advanced_features(m) # returns f, m_result tuple currently?
        # WAIT, current extract_advanced_features returns (f, m_result) tuple.
        # Let's adjust usage or wrapper.
        pass # implemented below
        
    # Re-implement feature extraction wrapper to handle strict tuple return from current func
    # or adjust extract_advanced_features to rely on previous definition
    # Actually, previous definition returns (f, m_result).
    # We need to adapt loop.
    
    # ...
    
    data_points = []
    valid_count = 0
    
    for m in matches:
        res = extract_advanced_features(m)
        if not res: continue
        f_dict, m_outcome = res
        
        # Add targets
        # m_outcome is 'LOCAL' or 'VISITA' (based on AH cover)
        f_dict['_target_home'] = (m_outcome == 'LOCAL')
        f_dict['_target_away'] = (m_outcome == 'VISITA')
        
        data_points.append(f_dict)
        valid_count += 1
            
    print(f"Training Data Points: {len(data_points)}")
    
    # Normalize Keys: Ensure all points have all keys found in any point
    all_keys = set()
    for p in data_points:
        all_keys.update(p.keys())
        
    # Remove targets from feature list
    feature_keys = [k for k in all_keys if not k.startswith('_')]
    
    # Fill missing with False (assuming boolean/existence features) or None?
    # Better to fill with False for booleans, 0.0 for floats?
    # Safer: just use .get() in genetic loop logic, but population seeding does: rule[feat] = sample[feat] which crashes if missing.
    # So we MUST fill.
    for p in data_points:
        for k in feature_keys:
            if k not in p:
                if 'current_ah' in k: p[k] = 0.0
                else: p[k] = False 
                
    # Load existing rules
    existing_rules = []
    if RESULTS_FILE.exists():
        try:
            with open(RESULTS_FILE, 'r') as f: existing_rules = json.load(f)
        except: pass
        
    found_rules_hashes = set()
    for r in existing_rules:
        # Create a hash of conditions
        # r['conditions'] is dict
        conds = tuple(sorted(r['conditions'].items()))
        found_rules_hashes.add(conds)
        
    new_rules = []

    def save_callback(rule, prediction_target):
        # rule is dict of conditions
        conds_tuple = tuple(sorted(rule.items()))
        if conds_tuple in found_rules_hashes: return
        found_rules_hashes.add(conds_tuple)
        
        # Verify
        hits = 0
        total = 0
        target_key = '_target_home' if prediction_target == 'LOCAL' else '_target_away'
        
        for p in data_points:
            if evaluate_individual(rule, p):
                total += 1
                if p[target_key]: hits += 1
                
        if total < MIN_SAMPLES: return
        acc = (hits / total) * 100
        
        if acc >= TARGET_ACCURACY:
            r_obj = {
                "name": f"ADV_R_{len(existing_rules) + len(new_rules)}_{prediction_target}_{int(acc)}",
                "conditions": rule,
                "prediction": prediction_target,
                "accuracy": round(acc, 2),
                "samples": total,
                "algorithm": "ADVANCED"
            }
            new_rules.append(r_obj)
            print(f"  💎 FOUND: {prediction_target} | Acc: {acc:.1f}% ({hits}/{total}) | {rule}")

    # --- MINER CONFIGURATION ---
    targets = [('LOCAL', '_target_home'), ('VISITANTE', '_target_away')]
    
    for pred_label, target_key in targets:
        print(f"\n⛏️  MINING FOR TARGET: {pred_label}")
        
        population_size = 300
        population = []
        possible_features = [k for k in data_points[0].keys() if not k.startswith('_')]
        
        # Seed population
        for _ in range(population_size):
            rule = {}
            num_conds = random.randint(2, 5)
            sample = random.choice(data_points)
            chosen_feats = random.sample(possible_features, num_conds)
            for feat in chosen_feats:
                rule[feat] = sample[feat]
            population.append(rule)
            
        # Evolution
        for gen in range(GENERATIONS):
            scored_pop = []
            for indiv in population:
                hits = 0
                total = 0
                for p in data_points:
                    if evaluate_individual(indiv, p):
                        total += 1
                        if p[target_key]: hits += 1
                
                if total < MIN_SAMPLES: 
                    fitness = 0
                    acc = 0
                else:
                    acc = (hits / total) * 100
                    fitness = acc
                
                scored_pop.append((indiv, fitness, acc, total))
                
                if acc >= TARGET_ACCURACY and total >= MIN_SAMPLES:
                    save_callback(indiv, pred_label)
            
            scored_pop.sort(key=lambda x: x[1], reverse=True)
            
            best = scored_pop[0]
            if gen % 10 == 0:
                print(f"  Gen {gen}: Best {best[2]:.1f}% ({int(best[2]*best[3]/100)}/{best[3]})")
                
            next_pop = [x[0] for x in scored_pop[:50]] # Elitism
            
            while len(next_pop) < population_size:
                p1 = random.choice(scored_pop[:100])[0]
                p2 = random.choice(scored_pop[:100])[0]
                child = p1.copy()
                keys2 = list(p2.keys())
                if random.random() < 0.5 and keys2:
                    k = random.choice(keys2)
                    child[k] = p2[k]
                
                if random.random() < 0.3:
                    action = random.choice(['change', 'add', 'remove'])
                    if action == 'change' and child:
                        k = random.choice(list(child.keys()))
                        child[k] = random.choice(data_points)[k]
                    elif action == 'add':
                        k = random.choice(possible_features)
                        child[k] = random.choice(data_points)[k]
                    elif action == 'remove' and len(child) > 1:
                        k = random.choice(list(child.keys()))
                        del child[k]
                next_pop.append(child)
            population = next_pop

    print(f"\nDone. Found {len(new_rules)} NEW valid rules.")
    
    if new_rules:
        final_list = existing_rules + new_rules
        with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_list, f, indent=2)
        print(f"Saved total {len(final_list)} rules to {RESULTS_FILE}")

if __name__ == "__main__":
    run_genetic_miner()
