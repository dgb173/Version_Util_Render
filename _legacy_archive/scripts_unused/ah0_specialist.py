# scripts/ah0_specialist.py
"""
ESPECIALISTA AH 0 (Partido Parejo)
===================================
Entrena patrones SOLO para handicap 0
En AH 0 no hay favorito, el mercado dice 50-50.
La clave está en: forma, stats, momentum, rankings.
"""

import json
import sys
import random
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional

sys.stdout.reconfigure(line_buffering=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

DATA_FILES = [DATA_DIR / 'data_ah_0.json']

MIN_SAMPLES = 18
MIN_ACCURACY = 70
GENERATIONS = 2500


def parse_score(s) -> Optional[Tuple[int, int]]:
    if not s or ':' not in str(s):
        return None
    try:
        p = str(s).replace('-', ':').split(':')
        return int(p[0]), int(p[1])
    except:
        return None


def parse_stats(rows):
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


def extract_features(m: Dict) -> Dict:
    f = {}
    odds = m.get('main_match_odds') or {}
    try:
        ou = float(odds.get('goals_linea', 2.5) or 2.5)
    except:
        ou = 2.5
    f['ou'] = ou
    f['ou_bajo'] = ou <= 2.25
    f['ou_alto'] = ou >= 2.75
    
    hs = m.get('home_standings') or {}
    aws = m.get('away_standings') or {}
    
    try:
        hr = int(hs.get('ranking', 0) or 0)
        ar = int(aws.get('ranking', 0) or 0)
        f['h_top5'] = 0 < hr <= 5
        f['a_top5'] = 0 < ar <= 5
        f['h_top10'] = 0 < hr <= 10
        f['a_top10'] = 0 < ar <= 10
        f['h_bottom'] = hr >= 15
        f['a_bottom'] = ar >= 15
        f['h_mejor'] = hr > 0 and ar > 0 and hr < ar
        f['a_mejor'] = hr > 0 and ar > 0 and ar < hr
        f['ranks_muy_cercanos'] = hr > 0 and ar > 0 and abs(hr - ar) <= 2
    except:
        pass
    
    try:
        hv = int(hs.get('specific_v', 0) or 0)
        he = int(hs.get('specific_e', 0) or 0)
        hd = int(hs.get('specific_d', 0) or 0)
        ht = hv + he + hd
        f['h_wr'] = hv / ht if ht > 0 else 0.5
        f['h_fuerte'] = f['h_wr'] >= 0.6
        f['h_muy_fuerte'] = f['h_wr'] >= 0.7
        f['h_debil'] = f['h_wr'] <= 0.3
        f['h_invicto'] = hd == 0 and ht >= 3
        
        av = int(aws.get('specific_v', 0) or 0)
        ae = int(aws.get('specific_e', 0) or 0)
        ad = int(aws.get('specific_d', 0) or 0)
        at = av + ae + ad
        f['a_wr'] = av / at if at > 0 else 0.5
        f['a_fuerte'] = f['a_wr'] >= 0.5
        f['a_muy_fuerte'] = f['a_wr'] >= 0.6
        f['a_debil'] = f['a_wr'] <= 0.25
        f['a_invicto'] = ad == 0 and at >= 3
    except:
        pass
    
    try:
        h_gf = int(hs.get('specific_gf', 0) or 0)
        h_gc = int(hs.get('specific_gc', 0) or 0)
        h_pj = int(hs.get('specific_pj', 1) or 1)
        a_gf = int(aws.get('specific_gf', 0) or 0)
        a_gc = int(aws.get('specific_gc', 0) or 0)
        a_pj = int(aws.get('specific_pj', 1) or 1)
        
        f['h_anota'] = h_gf / h_pj >= 1.5
        f['a_anota'] = a_gf / a_pj >= 1.5
        f['h_recibe_poco'] = h_gc / h_pj <= 0.8
        f['a_recibe_poco'] = a_gc / a_pj <= 0.8
        f['h_goleador'] = (h_gf + h_gc) / h_pj >= 3
        f['a_goleador'] = (a_gf + a_gc) / a_pj >= 3
        f['h_defensivo'] = (h_gf + h_gc) / h_pj < 2.2
        f['a_defensivo'] = (a_gf + a_gc) / a_pj < 2.2
    except:
        pass
    
    ph = m.get('last_home_match') or {}
    sc = parse_score(ph.get('score'))
    if sc:
        f['ph_gano'] = sc[0] > sc[1]
        f['ph_goleo'] = sc[0] >= 3
        f['ph_porteria0'] = sc[1] == 0
        f['ph_perdio'] = sc[0] < sc[1]
        f['ph_over'] = sc[0] + sc[1] > 2.5
        f['ph_under'] = sc[0] + sc[1] <= 2
        stats = parse_stats(ph.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            diff = stats['Ataques Peligrosos']['h'] - stats['Ataques Peligrosos']['a']
            f['ph_domino'] = diff > 15
            f['ph_dominado'] = diff < -15
    
    pa = m.get('last_away_match') or {}
    sc = parse_score(pa.get('score'))
    if sc:
        f['pa_gano'] = sc[1] > sc[0]
        f['pa_goleo'] = sc[1] >= 3
        f['pa_porteria0'] = sc[0] == 0
        f['pa_perdio'] = sc[1] < sc[0]
        f['pa_over'] = sc[0] + sc[1] > 2.5
        f['pa_under'] = sc[0] + sc[1] <= 2
        stats = parse_stats(pa.get('stats_rows', []))
        if 'Ataques Peligrosos' in stats:
            diff = stats['Ataques Peligrosos']['a'] - stats['Ataques Peligrosos']['h']
            f['pa_domino'] = diff > 15
            f['pa_dominado'] = diff < -15
    
    h2h = m.get('h2h_col3') or {}
    if h2h.get('status') == 'found':
        try:
            hg = int(h2h.get('goles_home', 0) or 0)
            ag = int(h2h.get('goles_away', 0) or 0)
            f['h2h_h_gano'] = hg > ag
            f['h2h_a_gano'] = ag > hg
            f['h2h_over'] = hg + ag > 2.5
            f['h2h_under'] = hg + ag <= 2
        except:
            pass
    
    mkt = m.get('market_analysis_data') or {}
    std = mkt.get('stadium') or {}
    gen = mkt.get('general') or {}
    f['std_cov'] = std.get('is_covered') == True
    f['gen_cov'] = gen.get('is_covered') == True
    f['h2h_cov'] = f.get('std_cov', False) and f.get('gen_cov', False)
    
    mov = std.get('movement', '')
    if '->' in mov:
        try:
            p = mov.split('->')
            b, a = float(p[0].strip()), float(p[1].strip())
            f['linea_subio'] = a > b
            f['linea_bajo'] = a < b
        except:
            pass
    
    comp = m.get('comparativas_indirectas') or {}
    left = comp.get('left')
    if left:
        sc = parse_score(left.get('score'))
        if sc:
            ih = left.get('localia') == 'H'
            tg = sc[0] if ih else sc[1]
            og = sc[1] if ih else sc[0]
            f['ind_l_gano'] = tg > og
            f['ind_l_goleo'] = tg >= 3
    
    right = comp.get('right')
    if right:
        sc = parse_score(right.get('score'))
        if sc:
            ih = right.get('localia') == 'H'
            tg = sc[0] if ih else sc[1]
            og = sc[1] if ih else sc[0]
            f['ind_r_gano'] = tg > og
            f['ind_r_goleo'] = tg >= 3
    
    f['h_momentum'] = f.get('ph_gano', False) and f.get('pa_perdio', False)
    f['a_momentum'] = f.get('pa_gano', False) and f.get('ph_perdio', False)
    f['ambos_ganaron'] = f.get('ph_gano', False) and f.get('pa_gano', False)
    f['ambos_perdieron'] = f.get('ph_perdio', False) and f.get('pa_perdio', False)
    
    return f


FEATURES = [
    'ou_bajo', 'ou_alto',
    'h_top5', 'a_top5', 'h_top10', 'a_top10', 'h_bottom', 'a_bottom',
    'h_mejor', 'a_mejor', 'ranks_muy_cercanos',
    'h_fuerte', 'h_muy_fuerte', 'h_debil', 'h_invicto',
    'a_fuerte', 'a_muy_fuerte', 'a_debil', 'a_invicto',
    'h_anota', 'a_anota', 'h_recibe_poco', 'a_recibe_poco',
    'h_goleador', 'a_goleador', 'h_defensivo', 'a_defensivo',
    'ph_gano', 'ph_goleo', 'ph_porteria0', 'ph_perdio', 'ph_over', 'ph_under', 'ph_domino', 'ph_dominado',
    'pa_gano', 'pa_goleo', 'pa_porteria0', 'pa_perdio', 'pa_over', 'pa_under', 'pa_domino', 'pa_dominado',
    'h2h_h_gano', 'h2h_a_gano', 'h2h_over', 'h2h_under',
    'std_cov', 'gen_cov', 'h2h_cov', 'linea_subio', 'linea_bajo',
    'ind_l_gano', 'ind_l_goleo', 'ind_r_gano', 'ind_r_goleo',
    'h_momentum', 'a_momentum', 'ambos_ganaron', 'ambos_perdieron',
]


def main():
    print("=" * 60)
    print("ESPECIALISTA AH 0 (Partido Parejo)")
    print("=" * 60)
    
    all_matches = []
    for fp in DATA_FILES:
        if fp.exists():
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_matches.extend(data)
                print(f"  {fp.name}: {len(data)}")
    
    all_matches = [m for m in all_matches if parse_score(m.get('final_score') or m.get('score'))]
    print(f"\nTotal: {len(all_matches)} partidos AH 0")
    
    patterns_ah = []
    patterns_ou = []
    for _ in range(2500):
        n = random.randint(2, 4)
        conds = random.sample(FEATURES, min(n, len(FEATURES)))
        patterns_ah.append({'conds': [(c, True) for c in conds], 'pick': random.choice(['LOCAL', 'VISITA']), 'hits': 0, 'miss': 0})
        patterns_ou.append({'conds': [(c, True) for c in conds], 'pick': random.choice(['OVER', 'UNDER']), 'hits': 0, 'miss': 0})
    
    results_ah = []
    results_ou = []
    
    print(f"\nEntrenando {GENERATIONS} generaciones...")
    
    for gen in range(GENERATIONS):
        for p in patterns_ah + patterns_ou:
            p['hits'] = p['miss'] = 0
        
        for m in all_matches:
            sc = parse_score(m.get('final_score') or m.get('score'))
            if not sc:
                continue
            
            odds = m.get('main_match_odds') or {}
            try:
                ou = float(odds.get('goals_linea', 2.5) or 2.5)
            except:
                ou = 2.5
            
            if sc[0] > sc[1]:
                ah_res = 'LOCAL'
            elif sc[1] > sc[0]:
                ah_res = 'VISITA'
            else:
                ah_res = None
            
            total = sc[0] + sc[1]
            if total > ou + 0.25:
                ou_res = 'OVER'
            elif total < ou - 0.25:
                ou_res = 'UNDER'
            else:
                ou_res = None
            
            feats = extract_features(m)
            
            if ah_res:
                for p in patterns_ah:
                    if all(feats.get(c) == v for c, v in p['conds']):
                        if p['pick'] == ah_res:
                            p['hits'] += 1
                        else:
                            p['miss'] += 1
            
            if ou_res:
                for p in patterns_ou:
                    if all(feats.get(c) == v for c, v in p['conds']):
                        if p['pick'] == ou_res:
                            p['hits'] += 1
                        else:
                            p['miss'] += 1
        
        for p in patterns_ah:
            total = p['hits'] + p['miss']
            if total >= MIN_SAMPLES:
                acc = p['hits'] / total * 100
                if acc >= MIN_ACCURACY:
                    is_new = not any(set(x['conds']) == set(p['conds']) for x in results_ah)
                    if is_new and len(results_ah) < 40:
                        results_ah.append({'pick': p['pick'], 'acc': round(acc, 1), 'hits': p['hits'], 'total': total, 'conds': p['conds']})
                        stars = '***' if acc >= 80 else '**' if acc >= 75 else '*'
                        print(f"{stars} AH {p['pick']} {acc:.1f}% ({p['hits']}/{total}) <- {[c[0] for c in p['conds']]}")
        
        for p in patterns_ou:
            total = p['hits'] + p['miss']
            if total >= MIN_SAMPLES:
                acc = p['hits'] / total * 100
                if acc >= MIN_ACCURACY:
                    is_new = not any(set(x['conds']) == set(p['conds']) for x in results_ou)
                    if is_new and len(results_ou) < 40:
                        results_ou.append({'pick': p['pick'], 'acc': round(acc, 1), 'hits': p['hits'], 'total': total, 'conds': p['conds']})
                        stars = '***' if acc >= 80 else '**' if acc >= 75 else '*'
                        print(f"{stars} OU {p['pick']} {acc:.1f}% ({p['hits']}/{total}) <- {[c[0] for c in p['conds']]}")
        
        for patterns in [patterns_ah, patterns_ou]:
            good = [p for p in patterns if p['hits'] + p['miss'] >= 5]
            good.sort(key=lambda x: -(x['hits'] / (x['hits'] + x['miss'] + 0.01)))
            survivors = good[:400] if len(good) >= 400 else good
            
            new_patterns = list(survivors)
            while len(new_patterns) < 2500:
                if survivors:
                    parent = random.choice(survivors[:150] if len(survivors) >= 150 else survivors)
                    child = {'conds': list(parent['conds']), 'pick': parent['pick'], 'hits': 0, 'miss': 0}
                    if random.random() < 0.3 and len(child['conds']) < 5:
                        f = random.choice(FEATURES)
                        if not any(c[0] == f for c in child['conds']):
                            child['conds'].append((f, True))
                    if random.random() < 0.2 and len(child['conds']) > 2:
                        child['conds'].pop(random.randint(0, len(child['conds'])-1))
                    if random.random() < 0.1:
                        child['pick'] = 'VISITA' if child['pick'] == 'LOCAL' else 'LOCAL' if child['pick'] == 'VISITA' else ('UNDER' if child['pick'] == 'OVER' else 'OVER')
                    new_patterns.append(child)
                else:
                    n = random.randint(2, 4)
                    conds = random.sample(FEATURES, min(n, len(FEATURES)))
                    new_patterns.append({'conds': [(c, True) for c in conds], 'pick': random.choice(['LOCAL', 'VISITA'] if patterns == patterns_ah else ['OVER', 'UNDER']), 'hits': 0, 'miss': 0})
            
            if patterns == patterns_ah:
                patterns_ah = new_patterns[:2500]
            else:
                patterns_ou = new_patterns[:2500]
        
        if (gen + 1) % 250 == 0:
            print(f"  G{gen+1} - AH:{len(results_ah)} OU:{len(results_ou)}")
    
    output = {'timestamp': datetime.now().isoformat(), 'version': 'ah0-specialist', 'matches': len(all_matches), 'ah_patterns': sorted(results_ah, key=lambda x: -x['acc']), 'ou_patterns': sorted(results_ou, key=lambda x: -x['acc'])}
    path = RESULTS_DIR / 'ah0_patterns.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"RESUMEN AH 0: AH={len(results_ah)} | OU={len(results_ou)}")
    print(f"Guardado: {path}")


if __name__ == '__main__':
    main()
