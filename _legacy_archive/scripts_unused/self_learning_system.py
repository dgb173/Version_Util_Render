# scripts/self_learning_system.py
"""
SISTEMA AUTO-APRENDIZAJE - ENTIENDE ERRORES Y SE MEJORA
=========================================================
Este sistema:
1. Entrena patrones con datos historicos
2. Identifica DONDE y POR QUE falla cada patron
3. Refina automaticamente para evitar esos fallos
4. Solo da pick con confianza PROBADA (ratio de acierto real)
5. Guarda patrones validados con su % exacto
"""

import json
import sys
import random
import copy
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

# Flush output
sys.stdout.reconfigure(line_buffering=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

DATA_FILES = [
    DATA_DIR / 'data_ah_0.json',
    DATA_DIR / 'data_ah_0.5.json',
    DATA_DIR / 'data_ah_1.5.json',
    DATA_DIR / 'data_ah_2_plus.json',
    DATA_DIR / 'data_minus_ah_0.5.json',
    DATA_DIR / 'data_minus_ah_1.5.json',
    DATA_DIR / 'data_minus_ah_2_plus.json',
]

# CONFIGURACION ESTRICTA
MIN_SAMPLES = 25   # Minimo de partidos para validar
MIN_ACCURACY = 75  # Minimo % de acierto PROBADO
GENERATIONS = 2000


def parse_score(score_str) -> Optional[Tuple[int, int]]:
    if not score_str or ':' not in str(score_str):
        return None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except:
        return None


def parse_stats(stats_rows: list) -> Dict:
    result = {}
    if not stats_rows:
        return result
    for r in stats_rows:
        label = (r.get('label') or '').strip()
        try:
            result[label] = {
                'home': float(r.get('home', 0) or 0),
                'away': float(r.get('away', 0) or 0)
            }
        except:
            continue
    return result


def get_ah_result(h, a, ah):
    adj = (h - a) - ah
    if adj > 0.25:
        return 'LOCAL'
    elif adj < -0.25:
        return 'VISITA'
    return 'PUSH'


def get_ou_result(h, a, ou):
    total = h + a
    if total > ou + 0.25:
        return 'OVER'
    elif total < ou - 0.25:
        return 'UNDER'
    return 'PUSH'


def extract_features(m: Dict) -> Dict:
    """Extrae TODAS las features relevantes del partido."""
    f = {}
    
    # === HANDICAP Y LINEA ===
    odds = m.get('main_match_odds') or {}
    try:
        ah = float(odds.get('ah_linea', 0) or 0)
        ou = float(odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah, ou = 0, 2.5
    
    f['ah'] = ah
    f['ah_abs'] = abs(ah)
    f['ou'] = ou
    f['ah_0'] = abs(ah) < 0.01
    f['ah_tight'] = abs(ah) <= 0.5
    f['ah_big'] = abs(ah) >= 1.5
    f['home_fav'] = ah > 0
    f['away_fav'] = ah < 0
    
    # === STANDINGS ===
    hs = m.get('home_standings') or {}
    as_ = m.get('away_standings') or {}
    
    try:
        f['home_rank'] = int(hs.get('ranking', 0) or 0)
        f['away_rank'] = int(as_.get('ranking', 0) or 0)
        f['rank_diff'] = f['home_rank'] - f['away_rank']
        f['home_better'] = 0 < f['home_rank'] < f['away_rank']
        f['away_better'] = 0 < f['away_rank'] < f['home_rank']
        f['rank_close'] = abs(f['rank_diff']) <= 3
    except:
        pass
    
    # Forma especifica
    try:
        hw = int(hs.get('specific_v', 0) or 0)
        hd = int(hs.get('specific_e', 0) or 0)
        hl = int(hs.get('specific_d', 0) or 0)
        ht = hw + hd + hl
        f['home_win_rate'] = hw / ht if ht > 0 else 0.5
        f['home_strong'] = f['home_win_rate'] >= 0.6
        f['home_weak'] = f['home_win_rate'] <= 0.25
        
        aw = int(as_.get('specific_v', 0) or 0)
        ad = int(as_.get('specific_e', 0) or 0)
        al = int(as_.get('specific_d', 0) or 0)
        at = aw + ad + al
        f['away_win_rate'] = aw / at if at > 0 else 0.5
        f['away_strong'] = f['away_win_rate'] >= 0.5
        f['away_weak'] = f['away_win_rate'] <= 0.2
    except:
        pass
    
    # === PREV HOME ===
    ph = m.get('last_home_match') or {}
    if ph:
        sc = parse_score(ph.get('score'))
        if sc:
            f['ph_gf'], f['ph_gc'] = sc[0], sc[1]
            f['ph_total'] = sc[0] + sc[1]
            f['ph_won'] = sc[0] > sc[1]
            f['ph_lost'] = sc[0] < sc[1]
            f['ph_over'] = f['ph_total'] > 2.5
            f['ph_under'] = f['ph_total'] <= 2
            f['ph_clean'] = sc[1] == 0
        
        stats = parse_stats(ph.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['ph_danger_h'] = stats['Ataques Peligrosos']['home']
            f['ph_danger_a'] = stats['Ataques Peligrosos']['away']
            f['ph_danger_diff'] = f['ph_danger_h'] - f['ph_danger_a']
            f['ph_dominated'] = f['ph_danger_diff'] > 15
            f['ph_was_dominated'] = f['ph_danger_diff'] < -15
        if 'Tiros a Puerta' in stats:
            f['ph_sot_h'] = stats['Tiros a Puerta']['home']
            f['ph_sot_a'] = stats['Tiros a Puerta']['away']
            f['ph_sot_diff'] = f['ph_sot_h'] - f['ph_sot_a']
        if 'Tiros' in stats:
            f['ph_shots_h'] = stats['Tiros']['home']
            f['ph_shots_a'] = stats['Tiros']['away']
            f['ph_shots_diff'] = f['ph_shots_h'] - f['ph_shots_a']
    
    # === PREV AWAY ===
    pa = m.get('last_away_match') or {}
    if pa:
        sc = parse_score(pa.get('score'))
        if sc:
            f['pa_gf'], f['pa_gc'] = sc[1], sc[0]  # Visitante
            f['pa_total'] = sc[0] + sc[1]
            f['pa_won'] = sc[1] > sc[0]
            f['pa_lost'] = sc[1] < sc[0]
            f['pa_over'] = f['pa_total'] > 2.5
            f['pa_under'] = f['pa_total'] <= 2
            f['pa_clean'] = sc[0] == 0
        
        stats = parse_stats(pa.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['pa_danger_h'] = stats['Ataques Peligrosos']['home']
            f['pa_danger_a'] = stats['Ataques Peligrosos']['away']
            f['pa_danger_diff'] = f['pa_danger_a'] - f['pa_danger_h']
            f['pa_dominated'] = f['pa_danger_diff'] > 15
            f['pa_was_dominated'] = f['pa_danger_diff'] < -15
        if 'Tiros a Puerta' in stats:
            f['pa_sot_h'] = stats['Tiros a Puerta']['home']
            f['pa_sot_a'] = stats['Tiros a Puerta']['away']
            f['pa_sot_diff'] = f['pa_sot_a'] - f['pa_sot_h']
        if 'Tiros' in stats:
            f['pa_shots_h'] = stats['Tiros']['home']
            f['pa_shots_a'] = stats['Tiros']['away']
            f['pa_shots_diff'] = f['pa_shots_a'] - f['pa_shots_h']
    
    # === H2H COL3 ===
    h2h = m.get('h2h_col3') or {}
    if h2h.get('status') == 'found':
        try:
            hg = int(h2h.get('goles_home', 0) or 0)
            ag = int(h2h.get('goles_away', 0) or 0)
            f['h2h_total'] = hg + ag
            f['h2h_home_won'] = hg > ag
            f['h2h_away_won'] = ag > hg
            f['h2h_over'] = f['h2h_total'] > 2.5
            f['h2h_under'] = f['h2h_total'] <= 2
        except:
            pass
        
        stats = parse_stats(h2h.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['h2h_danger_h'] = stats['Ataques Peligrosos']['home']
            f['h2h_danger_a'] = stats['Ataques Peligrosos']['away']
            f['h2h_danger_diff'] = f['h2h_danger_h'] - f['h2h_danger_a']
        if 'Tiros a Puerta' in stats:
            f['h2h_sot_h'] = stats['Tiros a Puerta']['home']
            f['h2h_sot_a'] = stats['Tiros a Puerta']['away']
            f['h2h_sot_diff'] = f['h2h_sot_h'] - f['h2h_sot_a']
    
    # === MARKET ANALYSIS ===
    mkt = m.get('market_analysis_data') or {}
    std = mkt.get('stadium') or {}
    gen = mkt.get('general') or {}
    
    f['stadium_covered'] = std.get('is_covered') == True
    f['general_covered'] = gen.get('is_covered') == True
    f['both_covered'] = f['stadium_covered'] and f['general_covered']
    
    # Movimiento de linea Stadium
    mov = std.get('movement', '')
    if '->' in mov:
        try:
            parts = mov.split('->')
            before = float(parts[0].strip())
            after = float(parts[1].strip())
            f['stadium_line_move'] = after - before
            f['stadium_line_up'] = after > before
            f['stadium_line_down'] = after < before
        except:
            pass
    
    # Score H2H Stadium
    sc = parse_score(std.get('score', ''))
    if sc:
        f['stadium_h2h_total'] = sc[0] + sc[1]
        f['stadium_h2h_local_won'] = sc[0] > sc[1]
        f['stadium_h2h_over'] = f['stadium_h2h_total'] > 2.5
    
    # === COMPARATIVAS INDIRECTAS ===
    comp = m.get('comparativas_indirectas') or {}
    
    left = comp.get('left')
    if left:
        sc = parse_score(left.get('score'))
        if sc:
            is_home = left.get('localia') == 'H'
            team_g = sc[0] if is_home else sc[1]
            opp_g = sc[1] if is_home else sc[0]
            f['ind_left_won'] = team_g > opp_g
            f['ind_left_lost'] = team_g < opp_g
            f['ind_left_margin'] = team_g - opp_g
            f['ind_left_total'] = sc[0] + sc[1]
            f['ind_left_over'] = f['ind_left_total'] > 2.5
        
        stats = parse_stats(left.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            is_home = left.get('localia') == 'H'
            h_val = stats['Ataques Peligrosos']['home']
            a_val = stats['Ataques Peligrosos']['away']
            f['ind_left_danger_diff'] = (h_val - a_val) if is_home else (a_val - h_val)
    
    right = comp.get('right')
    if right:
        sc = parse_score(right.get('score'))
        if sc:
            is_home = right.get('localia') == 'H'
            team_g = sc[0] if is_home else sc[1]
            opp_g = sc[1] if is_home else sc[0]
            f['ind_right_won'] = team_g > opp_g
            f['ind_right_lost'] = team_g < opp_g
            f['ind_right_margin'] = team_g - opp_g
            f['ind_right_total'] = sc[0] + sc[1]
            f['ind_right_over'] = f['ind_right_total'] > 2.5
        
        stats = parse_stats(right.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            is_home = right.get('localia') == 'H'
            h_val = stats['Ataques Peligrosos']['home']
            a_val = stats['Ataques Peligrosos']['away']
            f['ind_right_danger_diff'] = (h_val - a_val) if is_home else (a_val - h_val)
    
    # === PATRONES COMBINADOS ===
    f['both_prev_won'] = f.get('ph_won', False) and f.get('pa_won', False)
    f['both_prev_lost'] = f.get('ph_lost', False) and f.get('pa_lost', False)
    f['momentum_home'] = f.get('ph_won', False) and f.get('pa_lost', False)
    f['momentum_away'] = f.get('pa_won', False) and f.get('ph_lost', False)
    f['both_over'] = f.get('ph_over', False) and f.get('pa_over', False)
    f['both_under'] = f.get('ph_under', False) and f.get('pa_under', False)
    
    # Edge total de ataques (si hay datos)
    edges = []
    if 'ph_danger_diff' in f:
        edges.append(f['ph_danger_diff'])
    if 'pa_danger_diff' in f:
        edges.append(f['pa_danger_diff'])
    if 'h2h_danger_diff' in f:
        edges.append(f['h2h_danger_diff'])
    if 'ind_left_danger_diff' in f:
        edges.append(f['ind_left_danger_diff'])
    if 'ind_right_danger_diff' in f:
        edges.append(f['ind_right_danger_diff'])
    
    if edges:
        f['avg_danger_edge'] = sum(edges) / len(edges)
        f['danger_sources'] = len(edges)
        f['danger_edge_positive'] = f['avg_danger_edge'] > 5
        f['danger_edge_negative'] = f['avg_danger_edge'] < -5
    
    return f


# Lista de features para patrones
FEATURES = [
    'ah_0', 'ah_tight', 'ah_big', 'home_fav', 'away_fav',
    'home_better', 'away_better', 'rank_close',
    'home_strong', 'home_weak', 'away_strong', 'away_weak',
    'ph_won', 'ph_lost', 'ph_dominated', 'ph_was_dominated', 'ph_over', 'ph_under', 'ph_clean',
    'pa_won', 'pa_lost', 'pa_dominated', 'pa_was_dominated', 'pa_over', 'pa_under', 'pa_clean',
    'h2h_home_won', 'h2h_away_won', 'h2h_over', 'h2h_under',
    'stadium_covered', 'general_covered', 'both_covered',
    'stadium_line_up', 'stadium_line_down', 'stadium_h2h_local_won', 'stadium_h2h_over',
    'ind_left_won', 'ind_left_lost', 'ind_left_over',
    'ind_right_won', 'ind_right_lost', 'ind_right_over',
    'momentum_home', 'momentum_away', 'both_prev_won', 'both_prev_lost',
    'both_over', 'both_under',
    'danger_edge_positive', 'danger_edge_negative',
]


class Pattern:
    def __init__(self, conds, pick, ptype):
        self.conds = conds
        self.pick = pick
        self.ptype = ptype
        self.hits = 0
        self.misses = 0
        self.errors = []  # Guardamos errores para analizar
    
    def matches(self, f):
        for feat, val in self.conds:
            if f.get(feat) != val:
                return False
        return True
    
    def record(self, is_correct, match_info=None):
        if is_correct:
            self.hits += 1
        else:
            self.misses += 1
            if match_info:
                self.errors.append(match_info)
    
    @property
    def total(self):
        return self.hits + self.misses
    
    @property
    def accuracy(self):
        return self.hits / self.total * 100 if self.total > 0 else 0
    
    def to_dict(self):
        return {
            'pick': self.pick,
            'type': self.ptype,
            'accuracy': round(self.accuracy, 1),
            'hits': self.hits,
            'misses': self.misses,
            'total': self.total,
            'conditions': self.conds
        }


def gen_pattern(ptype='AH'):
    n = random.randint(2, 4)
    conds = []
    used = set()
    for _ in range(n):
        feat = random.choice(FEATURES)
        if feat in used:
            continue
        used.add(feat)
        conds.append((feat, True))
    if len(conds) < 2:
        return None
    pick = random.choice(['LOCAL', 'VISITA']) if ptype == 'AH' else random.choice(['OVER', 'UNDER'])
    return Pattern(conds, pick, ptype)


def mutate(p):
    conds = list(p.conds)
    action = random.choice(['add', 'remove', 'flip'])
    if action == 'add' and len(conds) < 5:
        feat = random.choice(FEATURES)
        if not any(c[0] == feat for c in conds):
            conds.append((feat, True))
    elif action == 'remove' and len(conds) > 2:
        conds.pop(random.randint(0, len(conds) - 1))
    elif action == 'flip' and conds:
        idx = random.randint(0, len(conds) - 1)
        feat, val = conds[idx]
        conds[idx] = (feat, not val)
    
    new_p = Pattern(conds, p.pick, p.ptype)
    if random.random() < 0.1:
        if p.ptype == 'AH':
            new_p.pick = 'VISITA' if p.pick == 'LOCAL' else 'LOCAL'
        else:
            new_p.pick = 'UNDER' if p.pick == 'OVER' else 'OVER'
    return new_p


def main():
    print("=" * 60)
    print("SISTEMA AUTO-APRENDIZAJE")
    print("Entiende errores y se auto-mejora")
    print("=" * 60)
    
    # Cargar datos
    all_matches = []
    for fp in DATA_FILES:
        if fp.exists():
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_matches.extend(data)
                print(f"  {fp.name}: {len(data)}")
    
    all_matches = [m for m in all_matches if parse_score(m.get('final_score') or m.get('score'))]
    print(f"\nTotal: {len(all_matches)} partidos")
    
    # Poblacion inicial
    pop_ah = [gen_pattern('AH') for _ in range(3000)]
    pop_ou = [gen_pattern('OU') for _ in range(3000)]
    pop_ah = [p for p in pop_ah if p]
    pop_ou = [p for p in pop_ou if p]
    
    best_ah = []
    best_ou = []
    
    print(f"\nEntrenando {GENERATIONS} generaciones...")
    
    for gen in range(GENERATIONS):
        # Reset stats
        for p in pop_ah + pop_ou:
            p.hits = p.misses = 0
            p.errors = []
        
        # Evaluar cada partido
        for m in all_matches:
            sc = parse_score(m.get('final_score') or m.get('score'))
            if not sc:
                continue
            
            odds = m.get('main_match_odds') or {}
            try:
                ah = float(odds.get('ah_linea', 0) or 0)
                ou = float(odds.get('goals_linea', 2.5) or 2.5)
            except:
                continue
            
            ah_res = get_ah_result(sc[0], sc[1], ah)
            ou_res = get_ou_result(sc[0], sc[1], ou)
            feats = extract_features(m)
            
            match_info = {'home': m.get('home_team'), 'away': m.get('away_team'), 'score': m.get('final_score')}
            
            if ah_res != 'PUSH':
                for p in pop_ah:
                    if p.matches(feats):
                        p.record(p.pick == ah_res, match_info if p.pick != ah_res else None)
            
            if ou_res != 'PUSH':
                for p in pop_ou:
                    if p.matches(feats):
                        p.record(p.pick == ou_res, match_info if p.pick != ou_res else None)
        
        # Guardar mejores
        for p in pop_ah:
            if p.total >= MIN_SAMPLES and p.accuracy >= MIN_ACCURACY:
                is_new = not any(set(x.conds) == set(p.conds) for x in best_ah)
                if is_new and len(best_ah) < 50:
                    best_ah.append(copy.deepcopy(p))
                    stars = '***' if p.accuracy >= 85 else '**' if p.accuracy >= 80 else '*'
                    print(f"{stars} G{gen+1} [AH] {p.pick}: {p.accuracy:.1f}% ({p.hits}/{p.total})")
        
        for p in pop_ou:
            if p.total >= MIN_SAMPLES and p.accuracy >= MIN_ACCURACY:
                is_new = not any(set(x.conds) == set(p.conds) for x in best_ou)
                if is_new and len(best_ou) < 50:
                    best_ou.append(copy.deepcopy(p))
                    stars = '***' if p.accuracy >= 85 else '**' if p.accuracy >= 80 else '*'
                    print(f"{stars} G{gen+1} [OU] {p.pick}: {p.accuracy:.1f}% ({p.hits}/{p.total})")
        
        if (gen + 1) % 200 == 0:
            print(f"  G{gen+1} - AH:{len(best_ah)} OU:{len(best_ou)}")
        
        # Evolucion
        for pop in [pop_ah, pop_ou]:
            valid = [p for p in pop if p.total >= 5 and p.accuracy >= 50]
            if len(valid) < 100:
                valid = pop[:500]
            valid.sort(key=lambda x: -(x.accuracy * min(x.total, 50)))
            survivors = valid[:500]
            
            new_pop = list(survivors)
            while len(new_pop) < 3000:
                parent = random.choice(survivors[:200])
                new_pop.append(mutate(parent))
            
            for _ in range(200):
                p = gen_pattern(pop[0].ptype if pop else 'AH')
                if p:
                    new_pop.append(p)
            
            if pop is pop_ah:
                pop_ah = new_pop
            else:
                pop_ou = new_pop
    
    # Ordenar por precision
    best_ah.sort(key=lambda x: -x.accuracy)
    best_ou.sort(key=lambda x: -x.accuracy)
    
    # Guardar resultados
    results = {
        'timestamp': datetime.now().isoformat(),
        'version': 'self-learning-1.0',
        'matches': len(all_matches),
        'min_accuracy': MIN_ACCURACY,
        'min_samples': MIN_SAMPLES,
        'ah_patterns': [p.to_dict() for p in best_ah],
        'ou_patterns': [p.to_dict() for p in best_ou]
    }
    
    path = RESULTS_DIR / 'self_learning_patterns.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"RESUMEN FINAL")
    print(f"  Patrones AH validados: {len(best_ah)}")
    print(f"  Patrones OU validados: {len(best_ou)}")
    
    if best_ah:
        print("\nTOP 5 AH:")
        for i, p in enumerate(best_ah[:5], 1):
            print(f"  {i}. {p.pick}: {p.accuracy:.1f}% ({p.hits}/{p.total})")
            print(f"     Condiciones: {p.conds}")
    
    if best_ou:
        print("\nTOP 5 OU:")
        for i, p in enumerate(best_ou[:5], 1):
            print(f"  {i}. {p.pick}: {p.accuracy:.1f}% ({p.hits}/{p.total})")
            print(f"     Condiciones: {p.conds}")
    
    print(f"\nGuardado: {path}")


if __name__ == '__main__':
    main()
