
"""
UNIVERSAL HANDICAP SPECIALIST
=============================
Generates 'Infallible' prediction patterns for ANY handicap.
Target: 100% Accuracy (No Failures).

Usage:
    python scripts/universal_specialist.py --handicap 0.5 --min_acc 100
    python scripts/universal_specialist.py --handicap -1.5 --min_acc 100
"""

import json
import random
import copy
import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Constants
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

# Data Mapping (Handicap -> File)
# We will load ALL data if specific file doesn't exist, filtering by handicap later.
DATA_FILES = [
    DATA_DIR / 'data_ah_0.json',
    DATA_DIR / 'data_ah_0.5.json', 
    DATA_DIR / 'data_ah_1.5.json',
    DATA_DIR / 'data_ah_2_plus.json',
    DATA_DIR / 'data_minus_ah_0.5.json',
    DATA_DIR / 'data_minus_ah_1.5.json',
    DATA_DIR / 'data_minus_ah_2_plus.json',
]

# ==================== UTILS ====================

def parse_score(score_str) -> Optional[Tuple[int, int]]:
    """'2-1' or '2:1' -> (2, 1)"""
    if not score_str: return None
    s = str(score_str).replace(':', '-').replace(' ', '')
    if '-' not in s: return None
    try:
        p = s.split('-')
        return int(p[0]), int(p[1])
    except:
        return None

def parse_stats_rows(rows: list) -> Dict:
    res = {}
    if not rows: return res
    for r in rows:
        lbl = (r.get('label') or '').strip()
        try:
            res[lbl] = {'h': float(r.get('home', 0)), 'a': float(r.get('away', 0))}
        except: pass
    return res

def get_ah_result(home_goals, away_goals, ah_line) -> str:
    """Returns 'LOCAL', 'VISITA', 'PUSH'"""
    # AH is usually from home perspective. 
    # If AH is -0.5: (Home - Away) - 0.5 > 0 ?
    # Let's align with common notation: Score + AH (if AH is added to home)
    # Actually, standard logic: (Home + AH) vs Away? No.
    # Usually: (HomeGoals - AwayGoals) - AH_Line > 0.25 -> Home Win
    # E.g. H=1, A=0, AH=-0.5 -> (1-0) - 0.5 = 0.5 > 0.25 -> YES (Home Covers)
    # E.g. H=0, A=0, AH=-0.5 -> (0-0) - 0.5 = -0.5 < -0.25 -> NO (Away Covers)
    
    diff = (home_goals - away_goals) - ah_line
    if diff > 0.25: return 'LOCAL'
    if diff < -0.25: return 'VISITA'
    return 'PUSH'

def get_ou_result(home_goals, away_goals, ou_line) -> str:
    """Returns 'OVER', 'UNDER', 'PUSH'"""
    total = home_goals + away_goals
    if total > ou_line: return 'OVER'
    if total < ou_line: return 'UNDER'
    return 'PUSH'

def generate_pattern_name(conditions: List[Tuple]) -> str:
    """Generates a descriptive name for the pattern."""
    # Mapping simpler names
    name_parts = []
    for feat, op, val in conditions:
        readable_feat = feat.replace('_', ' ').title()
        readable_op = op
        if op == '==': readable_op = 'is'
        elif op == '>=': readable_op = '>='
        elif op == '<=': readable_op = '<='
        
        # Simplify specific features
        if 'Prev Home Danger Edge' in readable_feat:
            readable_feat = 'Home Momemtum (Attack)'
        if 'Prev Away Close Loss' in readable_feat:
            readable_feat = 'Away Resilient Loss'
        if 'Rank Diff' in readable_feat:
            readable_feat = 'Rank Advantage'
            
        name_parts.append(f"[{readable_feat} {readable_op} {val}]")
        
    return " + ".join(name_parts)

# ==================== FEATURE EXTRACTION ====================

# ==================== FEATURE EXTRACTION ====================

def days_between(d1_str, d2_str):
    if not d1_str or not d2_str: return 9999
    def parse_dt(d):
        for fmt in ["%Y-%m-%d", "%m/%d/%Y"]:
            try: return datetime.strptime(d[:10], fmt)
            except: pass
        return None
    dt1 = parse_dt(d1_str)
    dt2 = parse_dt(d2_str)
    if dt1 and dt2: return abs((dt1 - dt2).days)
    return 9999

def extract_features(match: Dict, target_line: float, line_type: str = 'AH') -> Dict:
    f = {}
    
    # 1. Odds & Handicaps
    main_odds = match.get('main_match_odds') or {}
    try:
        current_ah = float(main_odds.get('ah_linea', 0) or 0)
        current_ou = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except:
        return {} 

    f['current_ah'] = current_ah
    f['line_value'] = current_ah if line_type == 'AH' else current_ou
    f['current_ah_val'] = current_ah
    f['current_ou_val'] = current_ou

    # Match Date (for 20 days check) - Training data usually has 'match_date' or 'date'
    match_date = match.get('match_date') or match.get('date') or datetime.now().strftime("%Y-%m-%d")

    # 2. Ranks & Standings
    hs = match.get('home_standings') or {}
    as_ = match.get('away_standings') or {}
    try:
        hr = int(hs.get('ranking', 0) or 0)
        ar = int(as_.get('ranking', 0) or 0)
        if hr > 0 and ar > 0:
            f['rank_diff'] = ar - hr 
            f['home_better_rank'] = (hr < ar)
            f['context_fav_home'] = (current_ah < 0)
        else:
            f['rank_diff'] = 0
            f['home_better_rank'] = False
            f['context_fav_home'] = False
    except: 
        f['rank_diff'] = 0
        f['home_better_rank'] = False

    # 3. Market Inertia (Movement)
    md = match.get('market_analysis_data') or {}
    stad = md.get('stadium') or {}
    gen = md.get('general') or {}
    
    def parse_movement(mov_str):
        if not mov_str or '→' not in mov_str: return 0.0
        try:
            parts = mov_str.split('→')
            start = float(parts[0].strip())
            end = float(parts[1].strip())
            return end - start
        except: return 0.0

    f['market_inertia_stadium'] = parse_movement(stad.get('movement'))
    f['market_inertia_general'] = parse_movement(gen.get('movement'))

    # 4. H2H Col3 (Triangulation)
    col3 = match.get('h2h_col3') or {}
    try:
            gh = float(col3.get('goles_home') or 0)
            ga = float(col3.get('goles_away') or 0)
            f['col3_home_advantage'] = gh - ga
            f['has_col3'] = (col3.get('status') == 'found')
    except:
            f['col3_home_advantage'] = 0
            f['has_col3'] = False

    # 5. Dominance (Previous Match Stats) with 20 DAYS RULE
    def get_dominance_and_recency(last_match, ref_date):
        if not last_match: return 0, False
        
        # Check Recency
        lm_date = last_match.get('date')
        days = days_between(ref_date, lm_date)
        is_recent = days <= 20
        
        if not is_recent: return 0, False
            
        rows = parse_stats_rows(last_match.get('stats_rows'))
        if not rows: return 0, True
        
        # Extract DA
        da = rows.get('Ataques Peligrosos', {'h':0, 'a':0})
        pressure_diff = (da['h'] - da['a']) 
        return pressure_diff, True

    lhm = match.get('last_home_match')
    lam = match.get('last_away_match')
    
    dom_h, valid_h = get_dominance_and_recency(lhm, match_date)
    dom_a, valid_a = get_dominance_and_recency(lam, match_date)
    
    f['home_prev_dominance'] = dom_h
    f['away_prev_dominance'] = dom_a
    f['valid_recent_data'] = (valid_h and valid_a)
    
    # Unlucky Loss?
    lhm_score = parse_score(lhm.get('score')) if lhm else None
    if lhm_score and valid_h:
        # If Home lost (h < a) but dominance > 15
        f['home_unlucky_loss'] = (lhm_score[0] < lhm_score[1]) and (dom_h > 15)
    else:
        f['home_unlucky_loss'] = False
        
    # Indirect
    comp = match.get('comparativas_indirectas') or {}
    l = comp.get('left')
    r = comp.get('right')
    if l and r:
        try:
            l_sc = parse_score(l.get('score'))
            r_sc = parse_score(r.get('score'))
            if l_sc and r_sc:
                l_marg = (l_sc[0] - l_sc[1]) if l.get('localia') == 'H' else (l_sc[1] - l_sc[0])
                r_marg = (r_sc[0] - r_sc[1]) if r.get('localia') == 'A' else (r_sc[1] - r_sc[0]) 
                f['ind_margin_diff'] = l_marg - r_marg
        except: pass

    return f


# ==================== GENETIC ALGORITHM ====================

class Rule:
    def __init__(self, conditions, prediction):
        self.conditions = conditions # List of (feat, op, val)
        self.prediction = prediction # 'LOCAL' or 'VISITA'
        self.name = generate_pattern_name(conditions)
        self.stats = {'total': 0, 'correct': 0}
        
    def matches(self, features: Dict) -> bool:
        for feat, op, val in self.conditions:
            if feat not in features: return False
            curr = features[feat]
            
            try:
                if op == '==':
                    if curr != val: return False
                elif op == '>':
                    if not (curr > val): return False
                elif op == '<':
                    if not (curr < val): return False
                elif op == '>=':
                    if not (curr >= val): return False
                elif op == '<=':
                    if not (curr <= val): return False
                elif op == 'in':
                    if curr not in val: return False
            except: return False
        return True

    def evaluate(self, match_features_list: List[Tuple[Dict, str]]):
        """
        match_features_list: List of (features, result)
        result is 'LOCAL', 'VISITA', or 'PUSH'
        """
        t, c = 0, 0
        for feats, result in match_features_list:
            if result == 'PUSH': continue
            if self.matches(feats):
                t += 1
                if self.prediction == result:
                    c += 1
        self.stats = {'total': t, 'correct': c}
        
    @property
    def accuracy(self):
        return (self.stats['correct'] / self.stats['total'] * 100) if self.stats['total'] > 0 else 0

def genetic_search(data_pool: List[Tuple[Dict, str]], 
                   features_meta: Dict[str, List], 
                   target_acc: float = 100.0,
                   generations: int = 50,
                   possible_outcomes: List[str] = ['LOCAL', 'VISITA']):
    
    population_size = 200
    population = []
    
    # Initialize random population
    keys = list(features_meta.keys())
    for _ in range(population_size):
        conds = []
        # Create 2-4 random conditions
        for _ in range(random.randint(2, 4)):
            k = random.choice(keys)
            op = random.choice(['>', '<', '==', '>=', '<='])
            # Pick a sensible value? Or random from observed?
            # We'll simplified: random from observed range if needed, or numeric.
            # For boolean, just True/False
            meta = features_meta[k]
            if meta['type'] == bool:
                val = random.choice([True, False])
                op = '=='
            elif meta['type'] == float or meta['type'] == int:
                val = random.choice(meta['values']) # Pick a value seen in data
            else: continue
            conds.append((k, op, val))
        
        pred = random.choice(possible_outcomes)
        population.append(Rule(conds, pred))

    best_rules = []

    print(f"Starting Genetic Search. Generations: {generations}, Target Acc: {target_acc}%")
    
    for gen in range(generations):
        # Evaluate
        for r in population:
            r.evaluate(data_pool)
        
        # Filter Survivors (High Accuracy, Min Samples)
        # Min Samples is tricky. For 100% accuracy, we might accept fewer samples (e.g. 5-10)
        # but risk overfitting. Let's precise min samples 12.
        survivors = [r for r in population if r.stats['total'] >= 10 and r.accuracy >= (target_acc * 0.9)] 
        # * 0.9 allow slight evolution path, but we prefer strict.
        
        # Sort by Accuracy then Samples
        survivors.sort(key=lambda r: (r.accuracy, r.stats['total']), reverse=True)
        survivors = survivors[:50] # Keep top 50
        
        # Save absolute best
        for s in survivors:
            if s.accuracy >= target_acc:
                # Deduplicate
                found = False
                for b in best_rules:
                    if b.name == s.name and b.prediction == s.prediction:
                        found = True; break
                if not found:
                    best_rules.append(copy.deepcopy(s))

        # Reproduction (Mutation/Crossover)
        next_gen = survivors[:] # Elitism
        while len(next_gen) < population_size:
            if not survivors:
                # Restart random
                keys = list(features_meta.keys())
                conds = []
                for _ in range(random.randint(2, 4)):
                    k = random.choice(keys)
                    meta = features_meta[k]
                    if meta['type'] == bool: val = random.choice([True, False]); op='=='
                    elif meta['type'] in [int, float]: val = random.choice(meta['values']); op=random.choice(['>','<', '>=', '<='])
                    else: continue
                    conds.append((k, op, val))
                next_gen.append(Rule(conds, random.choice(possible_outcomes)))
                continue

            parent = random.choice(survivors)
            child_conds = copy.deepcopy(parent.conditions)
            
            # Mutate
            if random.random() < 0.3: # Modify
                idx = random.randint(0, len(child_conds)-1)
                k = child_conds[idx][0]
                meta = features_meta[k]
                if meta['type'] in [int, float]:
                    child_conds[idx] = (k, child_conds[idx][1], random.choice(meta['values']))
            
            if random.random() < 0.3: # Add
                k = random.choice(keys)
                meta = features_meta[k]
                if meta['type'] == bool: val = random.choice([True, False]); op='=='
                elif meta['type'] in [int, float]: val = random.choice(meta['values']); op=random.choice(['>','<'])
                child_conds.append((k, op, val))

            if random.random() < 0.2 and len(child_conds) > 1: # Remove
                child_conds.pop(random.randint(0, len(child_conds)-1))
                
            next_gen.append(Rule(child_conds, parent.prediction))
            
        population = next_gen
        
        if gen % 10 == 0:
            print(f"Gen {gen}: Found {len(best_rules)} Infallible Rules so far.")

    return best_rules

# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(description='Universal Handicap Specialist')
    parser.add_argument('--handicap', type=float, required=True, help='Target Line (Handicap or OU Line)')
    parser.add_argument('--type', type=str, default='AH', choices=['AH', 'OU'], help='Prediction Type: AH or OU')
    parser.add_argument('--min_acc', type=float, default=100.0, help='Minimum Accuracy Target (default 100)')
    parser.add_argument('--min_samples', type=int, default=30, help='Minimum Data Points (default 30)')
    parser.add_argument('--generations', type=int, default=50, help='Generations (default 50)')
    parser.add_argument('--league_id', type=str, default=None, help='Filter by League ID (Sniper Mode)')
    
    args = parser.parse_args()
    target_val = args.handicap
    pred_type = args.type
    target_acc = args.min_acc
    min_samples = args.min_samples
    generations = args.generations
    league_filter = args.league_id
    
    print(f"🎯 UNIVERSAL SPECIALIST TRAINING ({pred_type})")
    print(f"   Target Line: {target_val}")
    print(f"   Required Accuracy: {target_acc}%")
    print(f"   Min Samples: {min_samples}")
    print(f"   Generations: {generations}")
    if league_filter:
        print(f"   🔫 SNIPER MODE: Target League ID {league_filter}")
    print(f"   Loading Data (this may take a moment)...")
    

    
    # Logic Reminder based on User Feedback:
    # "When Handicap is NEGATIVE, favorite is VISITOR"
    # "When Handicap is POSITIVE, favorite is LOCAL"
    # This aligns with standard Asian Handicap:
    # AH -0.5 (Home Team gives 0.5) -> Home is Fav? 
    # WAIT. User said: "Cuando es negativo el favorito es el visitante".
    # Usually: Team A (Home) vs Team B (Away). 
    # AH is usually displayed for Home Team. 
    # If AH is -0.5, Home Team starts with -0.5. So Home Team is favorite (must win).
    # If User says "Negative = Away Fav", he might mean the handicap is displayed relative to the FAVORITE?
    # OR he implies "Away Handicap" perspective?
    # Standard Nowgoal/Bet365:
    # AH -0.5 -> Home is Favorite. AH +0.5 -> Home is Underdog (Away is Fav).
    # User says: "Negative -> Away Fav". "Positive -> Local Fav".
    # This is INVERTED from standard Home-based AH.
    # IF "Positive -> Local Fav", then AH +0.5 means Local is Fav?
    # Usually +0.5 means Local starts with +0.5 goal. That implies Local is WEAKER (Underdog).
    # So if User says "Positive = Local Fav", he might mean the NUMBER is associated with the favored team?
    # Let's trust the User's explicit rule for HIS system context:
    # "Negative -> Fav Visitor". "Positive -> Fav Local".
    
    all_matches = []
    for fp in DATA_FILES:
        if fp.exists():
            try:
                with open(fp, 'r', encoding='utf-8') as f:
                    all_matches.extend(json.load(f))
            except: pass
            
    # Filter matches
    dataset = []
    feature_values = {} 
    valid_count = 0
    
    for m in all_matches:
        # SNIPER FILTER
        if league_filter:
            lid = str(m.get('league_id') or m.get('id_league') or '')
            if lid != league_filter:
                continue

        odds = m.get('main_match_odds') or {}
        try:
            # Check Target Match
            current_line = 0.0
            is_valid_match = False
            
            if pred_type == 'AH':
                current_line = float(odds.get('ah_linea', -999))
            else:
                current_line = float(odds.get('goals_linea', -999))
            
            # BUCKET MATCHING: Group handicaps into ranges
            # 0.25, 0.5, 0.75 -> bucket 0.5
            # 1.0, 1.25, 1.5, 1.75 -> bucket 1.5
            # -0.25, -0.5, -0.75 -> bucket -0.5
            # -1.0, -1.25, -1.5, -1.75 -> bucket -1.5
            # 2.0+ -> bucket 2.0
            # -2.0 and below -> bucket -2.0
            
            def get_bucket(val):
                if val == 0: return 0.0
                abs_val = abs(val)
                sign = 1 if val > 0 else -1
                if 0.25 <= abs_val <= 0.75: return sign * 0.5
                if 1.0 <= abs_val <= 1.75: return sign * 1.5
                if abs_val >= 2.0: return sign * 2.0
                return val  # fallback
            
            target_bucket = get_bucket(target_val)
            current_bucket = get_bucket(current_line)
            
            if target_bucket == current_bucket:
                is_valid_match = True
                
            if not is_valid_match: continue

            # Get Result
            sc = parse_score(m.get('final_score') or m.get('score'))
            if not sc: continue
            
            res = 'PUSH'
            if pred_type == 'AH':
                res = get_ah_result(sc[0], sc[1], current_line)
            else:
                res = get_ou_result(sc[0], sc[1], current_line)
            
            if res == 'PUSH': continue
            
            # Extract Features
            feats = extract_features(m, target_val, pred_type)
            if not feats: continue
            
            dataset.append((feats, res))
            
            for k, v in feats.items():
                if k not in feature_values:
                    feature_values[k] = {'type': type(v), 'values': set()}
                if isinstance(v, (int, float, bool)):
                    feature_values[k]['values'].add(v)
                    
            valid_count += 1
        except: continue
        
    print(f"   Found {valid_count} matches matching {pred_type} {target_val}.")
    
    if valid_count < min_samples:
        print(f"❌ Not enough data points ({valid_count} found, need {min_samples}).")
        return

    # Convert sets
    for k in feature_values:
        feature_values[k]['values'] = list(feature_values[k]['values'])
        if feature_values[k]['type'] in [int, float]:
             feature_values[k]['values'].sort()

    # Train
    outcomes = ['LOCAL', 'VISITA'] if pred_type == 'AH' else ['OVER', 'UNDER']
    rules = genetic_search(dataset, feature_values, target_acc=target_acc, generations=generations, possible_outcomes=outcomes)
    
    # Filter by Min Samples
    rules = [r for r in rules if r.stats['total'] >= min_samples]
    
    # Sort
    rules.sort(key=lambda r: (r.stats['total']), reverse=True)

    output_rules = []
    print(f"\n🏆 FOUND PATTERNS (Sample):")
    for r in rules[:10]:
        print(f"   [{r.prediction}] {r.name} -> Acc: {r.accuracy:.1f}% ({r.stats['correct']}/{r.stats['total']})")
        output_rules.append({
            'name': r.name,
            'prediction': r.prediction,
            'conditions': r.conditions,
            'accuracy': r.accuracy,
            'stats': r.stats
        })
        
    out_file = RESULTS_DIR / f'specialist_{pred_type.lower()}_{target_val}.json'
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(output_rules, f, indent=2)
        
    print(f"\n💾 Saved {len(output_rules)} rules to {out_file}")

if __name__ == '__main__':
    main()
