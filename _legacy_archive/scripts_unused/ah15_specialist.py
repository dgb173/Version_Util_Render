# scripts/ah15_specialist.py
"""
ESPECIALISTA AH 1.5+ (Favorito Muy Claro - Goleada Esperada)
"""

import json, sys, random
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional

sys.stdout.reconfigure(line_buffering=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

DATA_FILES = [DATA_DIR / 'data_ah_1.5.json', DATA_DIR / 'data_ah_2_plus.json', 
              DATA_DIR / 'data_minus_ah_1.5.json', DATA_DIR / 'data_minus_ah_2_plus.json']

MIN_SAMPLES, MIN_ACCURACY, GENERATIONS = 15, 70, 2000


def parse_score(s):
    if not s or ':' not in str(s): return None
    try:
        p = str(s).replace('-', ':').split(':')
        return int(p[0]), int(p[1])
    except: return None


def extract_features(m):
    f = {}
    odds = m.get('main_match_odds') or {}
    try:
        ah = float(odds.get('ah_linea', 0) or 0)
        ou = float(odds.get('goals_linea', 2.5) or 2.5)
    except: ah, ou = 1.5, 2.5
    
    f['ah'] = ah
    f['fav_local'] = ah > 0
    f['fav_visita'] = ah < 0
    f['ah_15'] = 1.4 <= abs(ah) <= 1.6
    f['ah_2plus'] = abs(ah) >= 1.9
    f['ou_alto'] = ou >= 2.75
    f['ou_muy_alto'] = ou >= 3
    
    hs, aws = m.get('home_standings') or {}, m.get('away_standings') or {}
    try:
        hr, ar = int(hs.get('ranking', 0) or 0), int(aws.get('ranking', 0) or 0)
        f['h_top3'] = 0 < hr <= 3
        f['a_bottom'] = ar >= 15
        f['h_bottom'] = hr >= 15
        f['a_top3'] = 0 < ar <= 3
        f['gran_dif'] = abs(hr - ar) >= 10 if hr > 0 and ar > 0 else False
    except: pass
    
    try:
        hv, hd = int(hs.get('specific_v', 0) or 0), int(hs.get('specific_d', 0) or 0)
        ht = hv + int(hs.get('specific_e', 0) or 0) + hd
        f['h_fuerte'] = hv / ht >= 0.6 if ht > 0 else False
        f['h_invicto'] = hd == 0 and ht >= 3
        
        av, ad = int(aws.get('specific_v', 0) or 0), int(aws.get('specific_d', 0) or 0)
        at = av + int(aws.get('specific_e', 0) or 0) + ad
        f['a_fuerte'] = av / at >= 0.5 if at > 0 else False
        f['a_debil'] = av / at <= 0.2 if at > 0 else False
        
        f['fav_fuerte'] = (ah > 0 and f['h_fuerte']) or (ah < 0 and f['a_fuerte'])
        f['nofav_debil'] = (ah > 0 and f['a_debil']) or (ah < 0 and f.get('h_debil', False))
    except: pass
    
    ph = m.get('last_home_match') or {}
    sc = parse_score(ph.get('score'))
    if sc:
        f['ph_gano'] = sc[0] > sc[1]
        f['ph_goleo'] = sc[0] >= 3
        f['ph_over'] = sc[0] + sc[1] > 2.5
    
    pa = m.get('last_away_match') or {}
    sc = parse_score(pa.get('score'))
    if sc:
        f['pa_gano'] = sc[1] > sc[0]
        f['pa_perdio'] = sc[1] < sc[0]
        f['pa_over'] = sc[0] + sc[1] > 2.5
    
    f['fav_gano'] = (ah > 0 and f.get('ph_gano', False)) or (ah < 0 and f.get('pa_gano', False))
    f['nofav_perdio'] = (ah > 0 and f.get('pa_perdio', False)) or (ah < 0 and f.get('ph_perdio', False))
    
    h2h = m.get('h2h_col3') or {}
    if h2h.get('status') == 'found':
        try:
            hg, ag = int(h2h.get('goles_home', 0) or 0), int(h2h.get('goles_away', 0) or 0)
            f['h2h_fav_gano'] = (ah > 0 and hg > ag) or (ah < 0 and ag > hg)
            f['h2h_goleada'] = hg + ag >= 4
        except: pass
    
    mkt = m.get('market_analysis_data') or {}
    f['h2h_cov'] = (mkt.get('stadium') or {}).get('is_covered') == True
    
    f['todo_favor'] = f.get('fav_fuerte', False) and f.get('fav_gano', False) and f.get('gran_dif', False)
    f['aplasta'] = f.get('todo_favor', False) and f.get('h2h_cov', False)
    
    return f


FEATURES = ['fav_local', 'fav_visita', 'ah_15', 'ah_2plus', 'ou_alto', 'ou_muy_alto',
    'h_top3', 'a_bottom', 'h_bottom', 'a_top3', 'gran_dif',
    'h_fuerte', 'h_invicto', 'a_fuerte', 'a_debil', 'fav_fuerte', 'nofav_debil',
    'ph_gano', 'ph_goleo', 'ph_over', 'pa_gano', 'pa_perdio', 'pa_over',
    'fav_gano', 'nofav_perdio', 'h2h_fav_gano', 'h2h_goleada', 'h2h_cov',
    'todo_favor', 'aplasta']


def main():
    print("=" * 60)
    print("ESPECIALISTA AH 1.5+ (Goleada Esperada)")
    print("=" * 60)
    
    all_matches = []
    for fp in DATA_FILES:
        if fp.exists():
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for m in data:
                    odds = m.get('main_match_odds') or {}
                    try:
                        ah = abs(float(odds.get('ah_linea', 0) or 0))
                    except: ah = 0
                    if ah >= 1.4:
                        all_matches.append(m)
                print(f"  {fp.name}: {len(data)}")
    
    all_matches = [m for m in all_matches if parse_score(m.get('final_score') or m.get('score'))]
    print(f"\nFiltrado AH 1.5+: {len(all_matches)} partidos")
    
    patterns = [{'conds': [(c, True) for c in random.sample(FEATURES, random.randint(2, 4))], 
                 'pick': random.choice(['FAV', 'NOFAV']), 'hits': 0, 'miss': 0} for _ in range(2500)]
    patterns_ou = [{'conds': [(c, True) for c in random.sample(FEATURES, random.randint(2, 4))], 
                   'pick': random.choice(['OVER', 'UNDER']), 'hits': 0, 'miss': 0} for _ in range(2500)]
    
    results_ah, results_ou = [], []
    
    print(f"\nEntrenando {GENERATIONS} generaciones...")
    
    for gen in range(GENERATIONS):
        for p in patterns + patterns_ou: p['hits'] = p['miss'] = 0
        
        for m in all_matches:
            sc = parse_score(m.get('final_score') or m.get('score'))
            if not sc: continue
            
            odds = m.get('main_match_odds') or {}
            try:
                ah = float(odds.get('ah_linea', 0) or 0)
                ou = float(odds.get('goals_linea', 2.5) or 2.5)
            except: continue
            
            adj = (sc[0] - sc[1]) - ah
            ah_res = 'FAV' if adj > 0.25 else ('NOFAV' if adj < -0.25 else None)
            if ah < 0: ah_res = 'FAV' if adj < -0.25 else ('NOFAV' if adj > 0.25 else None)
            
            total = sc[0] + sc[1]
            ou_res = 'OVER' if total > ou + 0.25 else ('UNDER' if total < ou - 0.25 else None)
            
            feats = extract_features(m)
            
            if ah_res:
                for p in patterns:
                    if all(feats.get(c) == v for c, v in p['conds']):
                        p['hits' if p['pick'] == ah_res else 'miss'] += 1
            if ou_res:
                for p in patterns_ou:
                    if all(feats.get(c) == v for c, v in p['conds']):
                        p['hits' if p['pick'] == ou_res else 'miss'] += 1
        
        for p, res in [(patterns, results_ah), (patterns_ou, results_ou)]:
            for pat in p:
                t = pat['hits'] + pat['miss']
                if t >= MIN_SAMPLES:
                    acc = pat['hits'] / t * 100
                    if acc >= MIN_ACCURACY and not any(set(x['conds']) == set(pat['conds']) for x in res) and len(res) < 35:
                        res.append({'pick': pat['pick'], 'acc': round(acc, 1), 'hits': pat['hits'], 'total': t, 'conds': pat['conds']})
                        stars = '***' if acc >= 80 else '**' if acc >= 75 else '*'
                        print(f"{stars} {pat['pick']} {acc:.1f}% ({pat['hits']}/{t}) <- {[c[0] for c in pat['conds']]}")
        
        if (gen + 1) % 250 == 0: print(f"  G{gen+1} - AH:{len(results_ah)} OU:{len(results_ou)}")
    
    output = {'timestamp': datetime.now().isoformat(), 'version': 'ah15-specialist', 'matches': len(all_matches),
              'ah_patterns': sorted(results_ah, key=lambda x: -x['acc']), 'ou_patterns': sorted(results_ou, key=lambda x: -x['acc'])}
    path = RESULTS_DIR / 'ah15_patterns.json'
    with open(path, 'w', encoding='utf-8') as f: json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}\nRESUMEN AH 1.5+: AH={len(results_ah)} | OU={len(results_ou)}\nGuardado: {path}")


if __name__ == '__main__': main()
