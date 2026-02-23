# scripts/handicap_specialist_trainer.py
"""
ENTRENADOR ESPECIALISTA POR HANDICAP
=====================================
Entrena patrones separados para cada tipo de handicap:
- AH 0 (parejo)
- AH 0.25-0.5 (ligero favorito)
- AH 0.75-1 (favorito claro)
- AH 1.5+ (favorito muy claro)

Y también para O/U por línea:
- OU 2.0
- OU 2.5
- OU 3.0+
"""

import json
import sys
import random
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

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

MIN_SAMPLES = 20
MIN_ACCURACY = 72
GENERATIONS = 1500


def parse_score(s) -> Optional[Tuple[int, int]]:
    if not s or ':' not in str(s):
        return None
    try:
        p = str(s).replace('-', ':').split(':')
        return int(p[0]), int(p[1])
    except:
        return None


def parse_stats(rows: list) -> Dict:
    r = {}
    if not rows:
        return r
    for x in rows:
        lbl = (x.get('label') or '').strip()
        try:
            r[lbl] = {'h': float(x.get('home', 0) or 0), 'a': float(x.get('away', 0) or 0)}
        except:
            pass
    return r


def get_ah_result(h, a, ah):
    adj = (h - a) - ah
    if adj > 0.25: return 'LOCAL'
    if adj < -0.25: return 'VISITA'
    return 'PUSH'


def get_ou_result(h, a, ou):
    t = h + a
    if t > ou + 0.25: return 'OVER'
    if t < ou - 0.25: return 'UNDER'
    return 'PUSH'


def extract_features(m: Dict) -> Dict:
    f = {}
    
    # Handicap y linea
    odds = m.get('main_match_odds') or {}
    try:
        ah = float(odds.get('ah_linea', 0) or 0)
        ou = float(odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah, ou = 0, 2.5
    
    f['ah'] = ah
    f['ah_abs'] = abs(ah)
    f['ou'] = ou
    f['home_fav'] = ah > 0
    f['away_fav'] = ah < 0
    
    # Standings
    hs = m.get('home_standings') or {}
    aws = m.get('away_standings') or {}
    
    try:
        f['h_rank'] = int(hs.get('ranking', 0) or 0)
        f['a_rank'] = int(aws.get('ranking', 0) or 0)
        f['rank_diff'] = f['h_rank'] - f['a_rank']
        f['h_top'] = f['h_rank'] <= 5 and f['h_rank'] > 0
        f['a_top'] = f['a_rank'] <= 5 and f['a_rank'] > 0
        f['h_bottom'] = f['h_rank'] >= 15
        f['a_bottom'] = f['a_rank'] >= 15
    except:
        pass
    
    # Forma especifica
    try:
        hv = int(hs.get('specific_v', 0) or 0)
        he = int(hs.get('specific_e', 0) or 0)
        hd = int(hs.get('specific_d', 0) or 0)
        ht = hv + he + hd
        f['h_wr'] = hv / ht if ht > 0 else 0.5
        f['h_strong'] = f['h_wr'] >= 0.6
        f['h_weak'] = f['h_wr'] <= 0.25
        
        av = int(aws.get('specific_v', 0) or 0)
        ae = int(aws.get('specific_e', 0) or 0)
        ad = int(aws.get('specific_d', 0) or 0)
        at = av + ae + ad
        f['a_wr'] = av / at if at > 0 else 0.5
        f['a_strong'] = f['a_wr'] >= 0.5
        f['a_weak'] = f['a_wr'] <= 0.2
    except:
        pass
    
    # Goles stats
    try:
        f['h_gf'] = int(hs.get('specific_gf', 0) or 0)
        f['h_gc'] = int(hs.get('specific_gc', 0) or 0)
        f['h_pj'] = int(hs.get('specific_pj', 1) or 1)
        f['a_gf'] = int(aws.get('specific_gf', 0) or 0)
        f['a_gc'] = int(aws.get('specific_gc', 0) or 0)
        f['a_pj'] = int(aws.get('specific_pj', 1) or 1)
        
        f['h_avg'] = (f['h_gf'] + f['h_gc']) / f['h_pj']
        f['a_avg'] = (f['a_gf'] + f['a_gc']) / f['a_pj']
        f['total_avg'] = (f['h_avg'] + f['a_avg']) / 2
        f['goleador'] = f['total_avg'] >= 3
        f['defensivo'] = f['total_avg'] < 2.3
    except:
        pass
    
    # Prev home
    ph = m.get('last_home_match') or {}
    sc = parse_score(ph.get('score'))
    if sc:
        f['ph_gf'], f['ph_gc'] = sc[0], sc[1]
        f['ph_total'] = sc[0] + sc[1]
        f['ph_won'] = sc[0] > sc[1]
        f['ph_lost'] = sc[0] < sc[1]
        f['ph_over'] = f['ph_total'] > 2.5
        f['ph_under'] = f['ph_total'] <= 2
        f['ph_rec3'] = sc[1] >= 3  # Recibio 3+
        
        stats = parse_stats(ph.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['ph_dg_diff'] = stats['Ataques Peligrosos']['h'] - stats['Ataques Peligrosos']['a']
            f['ph_dom'] = f['ph_dg_diff'] > 15
        if 'Tiros a Puerta' in stats:
            f['ph_sot_diff'] = stats['Tiros a Puerta']['h'] - stats['Tiros a Puerta']['a']
    
    # Prev away
    pa = m.get('last_away_match') or {}
    sc = parse_score(pa.get('score'))
    if sc:
        f['pa_gf'], f['pa_gc'] = sc[1], sc[0]
        f['pa_total'] = sc[0] + sc[1]
        f['pa_won'] = sc[1] > sc[0]
        f['pa_lost'] = sc[1] < sc[0]
        f['pa_over'] = f['pa_total'] > 2.5
        f['pa_under'] = f['pa_total'] <= 2
        f['pa_rec3'] = sc[0] >= 3
        
        stats = parse_stats(pa.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            f['pa_dg_diff'] = stats['Ataques Peligrosos']['a'] - stats['Ataques Peligrosos']['h']
            f['pa_dom'] = f['pa_dg_diff'] > 15
        if 'Tiros a Puerta' in stats:
            f['pa_sot_diff'] = stats['Tiros a Puerta']['a'] - stats['Tiros a Puerta']['h']
    
    # H2H
    h2h = m.get('h2h_col3') or {}
    if h2h.get('status') == 'found':
        try:
            hg = int(h2h.get('goles_home', 0) or 0)
            ag = int(h2h.get('goles_away', 0) or 0)
            f['h2h_total'] = hg + ag
            f['h2h_h_won'] = hg > ag
            f['h2h_a_won'] = ag > hg
            f['h2h_over'] = f['h2h_total'] > 2.5
            f['h2h_under'] = f['h2h_total'] <= 2
        except:
            pass
    
    # Market
    mkt = m.get('market_analysis_data') or {}
    std = mkt.get('stadium') or {}
    gen = mkt.get('general') or {}
    f['std_cov'] = std.get('is_covered') == True
    f['gen_cov'] = gen.get('is_covered') == True
    f['both_cov'] = f['std_cov'] and f['gen_cov']
    
    mov = std.get('movement', '')
    if '->' in mov:
        try:
            p = mov.split('->')
            b, a = float(p[0].strip()), float(p[1].strip())
            f['line_up'] = a > b
            f['line_down'] = a < b
        except:
            pass
    
    # Comparativas
    comp = m.get('comparativas_indirectas') or {}
    left = comp.get('left')
    if left:
        sc = parse_score(left.get('score'))
        if sc:
            ih = left.get('localia') == 'H'
            tg = sc[0] if ih else sc[1]
            og = sc[1] if ih else sc[0]
            f['ind_l_won'] = tg > og
            f['ind_l_margin'] = tg - og
    
    right = comp.get('right')
    if right:
        sc = parse_score(right.get('score'))
        if sc:
            ih = right.get('localia') == 'H'
            tg = sc[0] if ih else sc[1]
            og = sc[1] if ih else sc[0]
            f['ind_r_won'] = tg > og
            f['ind_r_margin'] = tg - og
    
    # Combinados
    f['both_won'] = f.get('ph_won', False) and f.get('pa_won', False)
    f['both_lost'] = f.get('ph_lost', False) and f.get('pa_lost', False)
    f['mom_h'] = f.get('ph_won', False) and f.get('pa_lost', False)
    f['mom_a'] = f.get('pa_won', False) and f.get('ph_lost', False)
    f['both_over'] = f.get('ph_over', False) and f.get('pa_over', False)
    f['both_under'] = f.get('ph_under', False) and f.get('pa_under', False)
    
    return f


FEATURES = [
    'h_top', 'a_top', 'h_bottom', 'a_bottom',
    'h_strong', 'h_weak', 'a_strong', 'a_weak',
    'goleador', 'defensivo',
    'ph_won', 'ph_lost', 'ph_dom', 'ph_over', 'ph_under', 'ph_rec3',
    'pa_won', 'pa_lost', 'pa_dom', 'pa_over', 'pa_under', 'pa_rec3',
    'h2h_h_won', 'h2h_a_won', 'h2h_over', 'h2h_under',
    'std_cov', 'gen_cov', 'both_cov',
    'line_up', 'line_down',
    'ind_l_won', 'ind_r_won',
    'both_won', 'both_lost', 'mom_h', 'mom_a',
    'both_over', 'both_under',
]


def train_specialist(matches, ah_range, ou_range, name):
    """Entrena patrones para un rango específico de AH y OU."""
    print(f"\n{'='*50}")
    print(f"ENTRENANDO: {name}")
    print(f"  AH: {ah_range}, OU: {ou_range}")
    
    # Filtrar partidos
    filtered = []
    for m in matches:
        odds = m.get('main_match_odds') or {}
        try:
            ah = float(odds.get('ah_linea', 0) or 0)
            ou = float(odds.get('goals_linea', 2.5) or 2.5)
        except:
            continue
        
        if ah_range[0] <= abs(ah) <= ah_range[1]:
            if ou_range[0] <= ou <= ou_range[1]:
                filtered.append(m)
    
    print(f"  Partidos: {len(filtered)}")
    
    if len(filtered) < 30:
        print(f"  [!] Muy pocos partidos, saltando")
        return []
    
    # Generar patrones aleatorios
    patterns = []
    for _ in range(2000):
        n_conds = random.randint(2, 4)
        conds = random.sample(FEATURES, min(n_conds, len(FEATURES)))
        pick_ah = random.choice(['LOCAL', 'VISITA'])
        pick_ou = random.choice(['OVER', 'UNDER'])
        patterns.append({'conds': [(c, True) for c in conds], 'ah': pick_ah, 'ou': pick_ou})
    
    results_ah = []
    results_ou = []
    
    # Evaluar cada patrón
    for gen in range(GENERATIONS):
        for p in patterns:
            p['ah_hits'] = p['ah_miss'] = 0
            p['ou_hits'] = p['ou_miss'] = 0
        
        for m in filtered:
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
            
            for p in patterns:
                match = all(feats.get(c) == v for c, v in p['conds'])
                if match:
                    if ah_res != 'PUSH':
                        if p['ah'] == ah_res:
                            p['ah_hits'] += 1
                        else:
                            p['ah_miss'] += 1
                    if ou_res != 'PUSH':
                        if p['ou'] == ou_res:
                            p['ou_hits'] += 1
                        else:
                            p['ou_miss'] += 1
        
        # Buscar buenos
        for p in patterns:
            ah_t = p['ah_hits'] + p['ah_miss']
            ou_t = p['ou_hits'] + p['ou_miss']
            
            if ah_t >= MIN_SAMPLES:
                acc = p['ah_hits'] / ah_t * 100
                if acc >= MIN_ACCURACY:
                    is_new = not any(set(x['conds']) == set(p['conds']) and x['pick'] == p['ah'] for x in results_ah)
                    if is_new and len(results_ah) < 30:
                        results_ah.append({
                            'name': name,
                            'type': 'AH',
                            'pick': p['ah'],
                            'acc': round(acc, 1),
                            'hits': p['ah_hits'],
                            'total': ah_t,
                            'conds': p['conds']
                        })
                        stars = '**' if acc >= 80 else '*'
                        print(f"  {stars} AH {p['ah']}: {acc:.1f}% ({p['ah_hits']}/{ah_t})")
            
            if ou_t >= MIN_SAMPLES:
                acc = p['ou_hits'] / ou_t * 100
                if acc >= MIN_ACCURACY:
                    is_new = not any(set(x['conds']) == set(p['conds']) and x['pick'] == p['ou'] for x in results_ou)
                    if is_new and len(results_ou) < 30:
                        results_ou.append({
                            'name': name,
                            'type': 'OU',
                            'pick': p['ou'],
                            'acc': round(acc, 1),
                            'hits': p['ou_hits'],
                            'total': ou_t,
                            'conds': p['conds']
                        })
                        stars = '**' if acc >= 80 else '*'
                        print(f"  {stars} OU {p['ou']}: {acc:.1f}% ({p['ou_hits']}/{ou_t})")
        
        # Mutacion
        new_patterns = []
        for p in patterns:
            ah_t = p['ah_hits'] + p['ah_miss']
            ou_t = p['ou_hits'] + p['ou_miss']
            if ah_t > 5 or ou_t > 5:
                new_patterns.append(p)
        
        while len(new_patterns) < 2000:
            parent = random.choice(patterns)
            child = {'conds': list(parent['conds']), 'ah': parent['ah'], 'ou': parent['ou']}
            
            # Mutar
            if random.random() < 0.3 and len(child['conds']) < 5:
                new_feat = random.choice(FEATURES)
                if not any(c[0] == new_feat for c in child['conds']):
                    child['conds'].append((new_feat, True))
            if random.random() < 0.2 and len(child['conds']) > 2:
                child['conds'].pop(random.randint(0, len(child['conds'])-1))
            if random.random() < 0.1:
                child['ah'] = 'VISITA' if child['ah'] == 'LOCAL' else 'LOCAL'
            if random.random() < 0.1:
                child['ou'] = 'UNDER' if child['ou'] == 'OVER' else 'OVER'
            
            new_patterns.append(child)
        
        patterns = new_patterns[:2000]
        
        if (gen + 1) % 300 == 0:
            print(f"    G{gen+1} - AH:{len(results_ah)} OU:{len(results_ou)}")
    
    return results_ah + results_ou


def main():
    print("=" * 60)
    print("ENTRENADOR ESPECIALISTA POR HANDICAP")
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
    
    all_results = []
    
    # Entrenar por rango de handicap
    specialists = [
        ((0, 0.01), (2, 3), "AH_0_OU25"),
        ((0.25, 0.5), (2, 3), "AH_05_OU25"),
        ((0.75, 1), (2, 3), "AH_1_OU25"),
        ((1.25, 2.5), (2, 3), "AH_15_OU25"),
        ((0, 0.5), (1.5, 2.25), "AH_BAJO_OU2"),
        ((0, 0.5), (2.75, 4), "AH_BAJO_OU3"),
        ((1, 2.5), (2.75, 4), "AH_ALTO_OU3"),
    ]
    
    for ah_range, ou_range, name in specialists:
        results = train_specialist(all_matches, ah_range, ou_range, name)
        all_results.extend(results)
    
    # Guardar
    output = {
        'timestamp': datetime.now().isoformat(),
        'version': 'specialist-1.0',
        'matches': len(all_matches),
        'patterns': sorted(all_results, key=lambda x: -x['acc'])
    }
    
    path = RESULTS_DIR / 'specialist_patterns.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"RESUMEN: {len(all_results)} patrones encontrados")
    
    if all_results:
        print("\nTOP 10:")
        for i, p in enumerate(sorted(all_results, key=lambda x: -x['acc'])[:10], 1):
            print(f"  {i}. [{p['name']}] {p['type']} {p['pick']}: {p['acc']}% ({p['hits']}/{p['total']})")
    
    print(f"\nGuardado: {path}")


if __name__ == '__main__':
    main()
