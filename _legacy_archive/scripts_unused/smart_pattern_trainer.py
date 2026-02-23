# scripts/smart_pattern_trainer.py
"""
SISTEMA INTELIGENTE CON CORRELACIONES DE HANDICAP
==================================================
Detecta patrones de las casas de apuestas analizando:
1. Handicap previo vs actual = dificultad del rival
2. Ataques peligrosos NORMALIZADOS por dificultad
3. Movimiento de linea = senal del mercado
"""

import json
import random
import copy
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Flush output inmediatamente
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

# Configuracion
GENERATIONS = 4000
POPULATION_SIZE = 6000
MIN_SAMPLES = 25
MIN_ACCURACY = 78


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


def get_ah_result(home_goals, away_goals, ah_line):
    adjusted = (home_goals - away_goals) - ah_line
    if adjusted > 0.25:
        return 'LOCAL'
    elif adjusted < -0.25:
        return 'VISITA'
    return 'PUSH'


def get_ou_result(home_goals, away_goals, ou_line):
    total = home_goals + away_goals
    if total > ou_line + 0.25:
        return 'OVER'
    elif total < ou_line - 0.25:
        return 'UNDER'
    return 'PUSH'


def extract_smart_features(match: Dict) -> Dict:
    """
    Features INTELIGENTES con correlaciones de handicap.
    """
    f = {}
    
    # === HANDICAP ACTUAL ===
    odds = match.get('main_match_odds') or {}
    try:
        ah = float(odds.get('ah_linea', 0) or 0)
        ou = float(odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah, ou = 0, 2.5
    
    f['ah'] = ah
    f['ah_abs'] = abs(ah)
    f['ou'] = ou
    
    # Buckets de handicap
    f['ah_0'] = ah == 0
    f['ah_tight'] = abs(ah) <= 0.5
    f['ah_medium'] = 0.5 < abs(ah) <= 1
    f['ah_big'] = abs(ah) >= 1.5
    f['home_fav'] = ah > 0  # Local es favorito (da ventaja)
    f['away_fav'] = ah < 0  # Visitante es favorito
    
    # === STANDINGS ===
    home_std = match.get('home_standings') or {}
    away_std = match.get('away_standings') or {}
    
    try:
        f['home_rank'] = int(home_std.get('ranking', 0) or 0)
        f['away_rank'] = int(away_std.get('ranking', 0) or 0)
        f['rank_diff'] = f['home_rank'] - f['away_rank']
        f['home_better'] = f['home_rank'] < f['away_rank'] if f['home_rank'] > 0 and f['away_rank'] > 0 else False
        f['away_better'] = f['away_rank'] < f['home_rank'] if f['home_rank'] > 0 and f['away_rank'] > 0 else False
    except:
        f['home_rank'] = f['away_rank'] = f['rank_diff'] = 0
    
    # Forma en casa/fuera
    try:
        hw = int(home_std.get('specific_v', 0) or 0)
        hd = int(home_std.get('specific_e', 0) or 0)
        hl = int(home_std.get('specific_d', 0) or 0)
        ht = hw + hd + hl
        f['home_win_rate'] = hw / ht if ht > 0 else 0.5
        f['home_strong'] = f['home_win_rate'] >= 0.6
        f['home_weak'] = f['home_win_rate'] <= 0.3
        
        aw = int(away_std.get('specific_v', 0) or 0)
        ad = int(away_std.get('specific_e', 0) or 0)
        al = int(away_std.get('specific_d', 0) or 0)
        at = aw + ad + al
        f['away_win_rate'] = aw / at if at > 0 else 0.5
        f['away_strong'] = f['away_win_rate'] >= 0.5
        f['away_weak'] = f['away_win_rate'] <= 0.2
    except:
        pass
    
    # === PREV HOME - con HANDICAP del partido previo ===
    prev_home = match.get('last_home_match') or {}
    if prev_home:
        score = parse_score(prev_home.get('score'))
        if score:
            f['prev_home_gf'] = score[0]
            f['prev_home_gc'] = score[1]
            f['prev_home_goals'] = score[0] + score[1]
            f['prev_home_won'] = score[0] > score[1]
            f['prev_home_lost'] = score[0] < score[1]
            f['prev_home_clean'] = score[1] == 0
            f['prev_home_over'] = f['prev_home_goals'] > 2.5
            f['prev_home_under'] = f['prev_home_goals'] <= 2
        
        # AH del partido previo (si disponible)
        prev_ah_str = prev_home.get('ah_linea') or prev_home.get('handicap')
        if prev_ah_str:
            try:
                prev_ah = float(prev_ah_str)
                f['prev_home_ah'] = prev_ah
                # CORRELACION CLAVE: diferencia de handicap
                f['home_ah_diff'] = ah - prev_ah
                # Si ah_diff > 0, el rival actual es MAS FACIL
                f['home_easier_rival'] = f['home_ah_diff'] > 0.25
                f['home_harder_rival'] = f['home_ah_diff'] < -0.25
            except:
                pass
        
        # Ataques peligrosos
        stats = parse_stats(prev_home.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            dh = stats['Ataques Peligrosos']['home']
            da = stats['Ataques Peligrosos']['away']
            f['prev_home_danger'] = dh
            f['prev_home_danger_diff'] = dh - da
            f['prev_home_dominated'] = f['prev_home_danger_diff'] > 20
            f['prev_home_was_dominated'] = f['prev_home_danger_diff'] < -20
            # NORMALIZADO por resultado: domino pero perdio = mala senal
            if f.get('prev_home_lost') and f.get('prev_home_dominated'):
                f['home_dominated_but_lost'] = True
        if 'Tiros a Puerta' in stats:
            sh = stats['Tiros a Puerta']['home']
            sa = stats['Tiros a Puerta']['away']
            f['prev_home_sot'] = sh
            f['prev_home_sot_diff'] = sh - sa
            f['prev_home_sot_dominated'] = f['prev_home_sot_diff'] >= 4
    
    # === PREV AWAY - con HANDICAP del partido previo ===
    prev_away = match.get('last_away_match') or {}
    if prev_away:
        score = parse_score(prev_away.get('score'))
        if score:
            f['prev_away_gf'] = score[1]  # Visitante
            f['prev_away_gc'] = score[0]
            f['prev_away_goals'] = score[0] + score[1]
            f['prev_away_won'] = score[1] > score[0]
            f['prev_away_lost'] = score[1] < score[0]
            f['prev_away_clean'] = score[0] == 0
            f['prev_away_over'] = f['prev_away_goals'] > 2.5
            f['prev_away_under'] = f['prev_away_goals'] <= 2
        
        # AH del partido previo
        prev_ah_str = prev_away.get('ah_linea') or prev_away.get('handicap')
        if prev_ah_str:
            try:
                prev_ah = float(prev_ah_str)
                f['prev_away_ah'] = prev_ah
                f['away_ah_diff'] = (-ah) - prev_ah  # -ah porque ahora es visitante
                f['away_easier_rival'] = f['away_ah_diff'] > 0.25
                f['away_harder_rival'] = f['away_ah_diff'] < -0.25
            except:
                pass
        
        stats = parse_stats(prev_away.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            dh = stats['Ataques Peligrosos']['home']
            da = stats['Ataques Peligrosos']['away']
            f['prev_away_danger'] = da
            f['prev_away_danger_diff'] = da - dh
            f['prev_away_dominated'] = f['prev_away_danger_diff'] > 20
            f['prev_away_was_dominated'] = f['prev_away_danger_diff'] < -20
            if f.get('prev_away_lost') and f.get('prev_away_dominated'):
                f['away_dominated_but_lost'] = True
        if 'Tiros a Puerta' in stats:
            sh = stats['Tiros a Puerta']['home']
            sa = stats['Tiros a Puerta']['away']
            f['prev_away_sot'] = sa
            f['prev_away_sot_diff'] = sa - sh
            f['prev_away_sot_dominated'] = f['prev_away_sot_diff'] >= 4
    
    # === H2H COL3 ===
    h2h = match.get('h2h_col3') or {}
    if h2h.get('status') == 'found':
        try:
            hg = int(h2h.get('goles_home', 0) or 0)
            ag = int(h2h.get('goles_away', 0) or 0)
            f['h2h_goals'] = hg + ag
            f['h2h_home_won'] = hg > ag
            f['h2h_away_won'] = ag > hg
            f['h2h_over'] = f['h2h_goals'] > 2.5
            f['h2h_under'] = f['h2h_goals'] <= 2
        except:
            pass
    
    # === MARKET ANALYSIS ===
    market = match.get('market_analysis_data') or {}
    stadium = market.get('stadium') or {}
    general = market.get('general') or {}
    
    f['h2h_stadium_covered'] = stadium.get('is_covered') == True
    f['h2h_general_covered'] = general.get('is_covered') == True
    f['h2h_both_covered'] = f['h2h_stadium_covered'] and f['h2h_general_covered']
    
    # Movimiento de linea
    mov = stadium.get('movement') or ''
    if '->' in mov or '→' in mov:
        parts = mov.replace('→', '->').split('->')
        if len(parts) == 2:
            try:
                before = float(parts[0].strip())
                after = float(parts[1].strip())
                f['line_move'] = after - before
                f['line_up'] = after > before  # Linea sube = mercado favorece LOCAL
                f['line_down'] = after < before  # Linea baja = mercado favorece VISITA
                f['line_big_move'] = abs(after - before) >= 0.5
            except:
                pass
    
    # === COMPARATIVAS INDIRECTAS ===
    comp = match.get('comparativas_indirectas') or {}
    
    left = comp.get('left')
    if left:
        score = parse_score(left.get('score'))
        if score:
            is_home = left.get('localia') == 'H'
            team_g = score[0] if is_home else score[1]
            opp_g = score[1] if is_home else score[0]
            f['ind_left_won'] = team_g > opp_g
            f['ind_left_lost'] = team_g < opp_g
            f['ind_left_margin'] = team_g - opp_g
            f['ind_left_big_win'] = f['ind_left_margin'] >= 2
    
    right = comp.get('right')
    if right:
        score = parse_score(right.get('score'))
        if score:
            is_home = right.get('localia') == 'H'
            team_g = score[0] if is_home else score[1]
            opp_g = score[1] if is_home else score[0]
            f['ind_right_won'] = team_g > opp_g
            f['ind_right_lost'] = team_g < opp_g
            f['ind_right_margin'] = team_g - opp_g
            f['ind_right_big_win'] = f['ind_right_margin'] >= 2
    
    # === PATRONES COMBINADOS INTELIGENTES ===
    
    # Momentum: Local gano + visitante perdio
    f['momentum_home'] = f.get('prev_home_won', False) and f.get('prev_away_lost', False)
    f['momentum_away'] = f.get('prev_away_won', False) and f.get('prev_home_lost', False)
    
    # Dominio total: domino en ataques Y gano
    f['home_total_dom'] = f.get('prev_home_dominated', False) and f.get('prev_home_won', False)
    f['away_total_dom'] = f.get('prev_away_dominated', False) and f.get('prev_away_won', False)
    
    # Señal del mercado: linea sube + forma buena
    f['market_home'] = f.get('line_up', False) and f.get('home_strong', False)
    f['market_away'] = f.get('line_down', False) and f.get('away_strong', False)
    
    # Trampa: parece bueno pero...
    f['trap_home'] = f.get('home_better', False) and f.get('home_weak', False)
    f['trap_away'] = f.get('away_better', False) and f.get('away_weak', False)
    
    # O/U combinados
    f['both_over'] = f.get('prev_home_over', False) and f.get('prev_away_over', False)
    f['both_under'] = f.get('prev_home_under', False) and f.get('prev_away_under', False)
    f['both_clean'] = f.get('prev_home_clean', False) and f.get('prev_away_clean', False)
    
    return f


# Lista de features
FEATURES = [
    # Handicap
    'ah_0', 'ah_tight', 'ah_medium', 'ah_big', 'home_fav', 'away_fav',
    # Rankings
    'home_better', 'away_better', 'rank_diff',
    # Forma
    'home_strong', 'home_weak', 'away_strong', 'away_weak', 'home_win_rate', 'away_win_rate',
    # Prev home
    'prev_home_won', 'prev_home_lost', 'prev_home_clean', 'prev_home_over', 'prev_home_under',
    'prev_home_dominated', 'prev_home_was_dominated', 'prev_home_sot_dominated',
    'home_easier_rival', 'home_harder_rival', 'home_dominated_but_lost',
    # Prev away
    'prev_away_won', 'prev_away_lost', 'prev_away_clean', 'prev_away_over', 'prev_away_under',
    'prev_away_dominated', 'prev_away_was_dominated', 'prev_away_sot_dominated',
    'away_easier_rival', 'away_harder_rival', 'away_dominated_but_lost',
    # H2H
    'h2h_home_won', 'h2h_away_won', 'h2h_both_covered', 'h2h_over', 'h2h_under',
    # Market
    'line_up', 'line_down', 'line_big_move',
    # Indirectas
    'ind_left_won', 'ind_left_lost', 'ind_left_big_win',
    'ind_right_won', 'ind_right_lost', 'ind_right_big_win',
    # Combinados
    'momentum_home', 'momentum_away', 'home_total_dom', 'away_total_dom',
    'market_home', 'market_away', 'trap_home', 'trap_away',
    'both_over', 'both_under', 'both_clean',
]


class Pattern:
    def __init__(self, conds, pick, ptype):
        self.conds = conds
        self.pick = pick
        self.ptype = ptype  # 'AH' o 'OU'
        self.total = 0
        self.correct = 0
        self.name = None
    
    def matches(self, f):
        for feat, op, val in self.conds:
            fv = f.get(feat)
            if fv is None:
                return False
            try:
                if isinstance(val, bool):
                    if bool(fv) != val:
                        return False
                elif op == '>=' and float(fv) < val:
                    return False
                elif op == '<=' and float(fv) > val:
                    return False
                elif op == '>' and float(fv) <= val:
                    return False
                elif op == '<' and float(fv) >= val:
                    return False
            except:
                return False
        return True
    
    def acc(self):
        return self.correct / self.total * 100 if self.total > 0 else 0
    
    def gen_name(self):
        keys = [c[0] for c in self.conds if c[2] == True]
        if 'market_home' in keys:
            return 'MARKET_HOME'
        elif 'market_away' in keys:
            return 'MARKET_AWAY'
        elif 'home_total_dom' in keys:
            return 'DOMINIO_LOCAL'
        elif 'away_total_dom' in keys:
            return 'DOMINIO_VISITA'
        elif 'momentum_home' in keys:
            return 'MOMENTUM_HOME'
        elif 'momentum_away' in keys:
            return 'MOMENTUM_AWAY'
        elif 'home_easier_rival' in keys:
            return 'RIVAL_FACIL_HOME'
        elif 'away_easier_rival' in keys:
            return 'RIVAL_FACIL_AWAY'
        elif 'trap_home' in keys:
            return 'TRAMPA_HOME'
        elif 'trap_away' in keys:
            return 'TRAMPA_AWAY'
        elif 'both_under' in keys:
            return 'DEFENSIVOS'
        elif 'both_over' in keys:
            return 'GOLEADORES'
        elif 'line_up' in keys:
            return 'LINEA_SUBE'
        elif 'line_down' in keys:
            return 'LINEA_BAJA'
        return f'PAT_{self.ptype}_{len(self.conds)}'
    
    def to_dict(self):
        return {
            'name': self.name or self.gen_name(),
            'pick': self.pick,
            'type': self.ptype,
            'acc': round(self.acc(), 1),
            'n': self.total,
            'conds': self.conds
        }


def gen_cond(feat):
    if any(x in feat for x in ['rate', 'diff', 'margin']):
        return (feat, random.choice(['>=', '<=']), round(random.uniform(-3, 3), 1))
    return (feat, '==', True)


def gen_pattern(ptype='AH'):
    n = random.randint(3, 5)
    conds = []
    used = set()
    for _ in range(n):
        feat = random.choice(FEATURES)
        if feat in used:
            continue
        used.add(feat)
        conds.append(gen_cond(feat))
    if len(conds) < 3:
        return None
    pick = random.choice(['LOCAL', 'VISITA']) if ptype == 'AH' else random.choice(['OVER', 'UNDER'])
    return Pattern(conds, pick, ptype)


def mutate(p):
    conds = list(p.conds)
    action = random.choice(['add', 'remove', 'modify'])
    if action == 'add' and len(conds) < 6:
        feat = random.choice(FEATURES)
        if not any(c[0] == feat for c in conds):
            conds.append(gen_cond(feat))
    elif action == 'remove' and len(conds) > 3:
        conds.pop(random.randint(0, len(conds) - 1))
    elif action == 'modify' and conds:
        idx = random.randint(0, len(conds) - 1)
        feat, op, val = conds[idx]
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            conds[idx] = (feat, op, round(val + random.uniform(-1, 1), 1))
    
    new_p = Pattern(conds, p.pick, p.ptype)
    if random.random() < 0.05:
        if p.ptype == 'AH':
            new_p.pick = 'VISITA' if p.pick == 'LOCAL' else 'LOCAL'
        else:
            new_p.pick = 'UNDER' if p.pick == 'OVER' else 'OVER'
    return new_p


def main():
    print("=" * 60)
    print("SMART PATTERN TRAINER - CORRELACIONES INTELIGENTES")
    print("=" * 60)
    print(f"Generaciones: {GENERATIONS}, Poblacion: {POPULATION_SIZE}")
    print(f"Min acc: {MIN_ACCURACY}%, Min n: {MIN_SAMPLES}")
    print()
    
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
    
    # Poblaciones
    pop_ah = [gen_pattern('AH') for _ in range(POPULATION_SIZE)]
    pop_ou = [gen_pattern('OU') for _ in range(POPULATION_SIZE)]
    pop_ah = [p for p in pop_ah if p]
    pop_ou = [p for p in pop_ou if p]
    
    best_ah = []
    best_ou = []
    
    for gen in range(GENERATIONS):
        for p in pop_ah + pop_ou:
            p.total = p.correct = 0
        
        for m in all_matches:
            score = parse_score(m.get('final_score') or m.get('score'))
            if not score:
                continue
            
            odds = m.get('main_match_odds') or {}
            try:
                ah = float(odds.get('ah_linea', 0) or 0)
                ou = float(odds.get('goals_linea', 2.5) or 2.5)
            except:
                continue
            
            ah_res = get_ah_result(score[0], score[1], ah)
            ou_res = get_ou_result(score[0], score[1], ou)
            
            feats = extract_smart_features(m)
            
            if ah_res != 'PUSH':
                for p in pop_ah:
                    if p.matches(feats):
                        p.total += 1
                        if p.pick == ah_res:
                            p.correct += 1
            
            if ou_res != 'PUSH':
                for p in pop_ou:
                    if p.matches(feats):
                        p.total += 1
                        if p.pick == ou_res:
                            p.correct += 1
        
        # Guardar mejores
        for p in pop_ah:
            if p.total >= MIN_SAMPLES and p.acc() >= MIN_ACCURACY:
                is_new = not any(abs(x.acc() - p.acc()) < 2 and x.pick == p.pick for x in best_ah)
                if is_new and len(best_ah) < 80:
                    p.name = p.gen_name()
                    best_ah.append(copy.deepcopy(p))
                    stars = '***' if p.acc() >= 88 else '**' if p.acc() >= 83 else '*'
                    print(f"{stars} G{gen+1} [AH {p.name}] {p.pick}: {p.acc():.1f}% n={p.total}")
        
        for p in pop_ou:
            if p.total >= MIN_SAMPLES and p.acc() >= MIN_ACCURACY:
                is_new = not any(abs(x.acc() - p.acc()) < 2 and x.pick == p.pick for x in best_ou)
                if is_new and len(best_ou) < 80:
                    p.name = p.gen_name()
                    best_ou.append(copy.deepcopy(p))
                    stars = '***' if p.acc() >= 88 else '**' if p.acc() >= 83 else '*'
                    print(f"{stars} G{gen+1} [OU {p.name}] {p.pick}: {p.acc():.1f}% n={p.total}")
        
        if (gen + 1) % 500 == 0:
            print(f"  G{gen+1} - AH:{len(best_ah)} OU:{len(best_ou)}")
        
        # Evolucion
        for pop in [pop_ah, pop_ou]:
            valid = [p for p in pop if p.total >= 8 and p.acc() >= 45]
            if not valid:
                valid = pop[:200]
            valid.sort(key=lambda x: -x.acc())
            survivors = valid[:600]
            
            new_pop = list(survivors)
            while len(new_pop) < POPULATION_SIZE:
                new_pop.append(mutate(random.choice(survivors)))
            
            for _ in range(200):
                p = gen_pattern(pop[0].ptype if pop else 'AH')
                if p:
                    new_pop.append(p)
            
            if pop == pop_ah:
                pop_ah = new_pop
            else:
                pop_ou = new_pop
    
    # Guardar
    best_ah.sort(key=lambda x: -x.acc())
    best_ou.sort(key=lambda x: -x.acc())
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'version': 'smart-1.0',
        'matches': len(all_matches),
        'ah': [p.to_dict() for p in best_ah[:40]],
        'ou': [p.to_dict() for p in best_ou[:40]]
    }
    
    path = RESULTS_DIR / 'smart_patterns.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"RESUMEN: AH={len(best_ah)} | OU={len(best_ou)}")
    print("TOP 5 AH:")
    for i, p in enumerate(best_ah[:5], 1):
        print(f"  {i}. [{p.name}] {p.pick}: {p.acc():.1f}% n={p.total}")
    print("TOP 5 OU:")
    for i, p in enumerate(best_ou[:5], 1):
        print(f"  {i}. [{p.name}] {p.pick}: {p.acc():.1f}% n={p.total}")
    print(f"\nGuardado: {path}")


if __name__ == '__main__':
    main()
