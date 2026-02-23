# scripts/over_hunter.py
"""
CAZADOR DE PATRONES OVER
=========================
Busca SOLO patrones OVER con lógica:
- Equipos goleadores
- Defensas flojas
- Historial de muchos goles
- Favoritos muy claros
- Top vs Bottom
"""

import json
import sys
import random
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

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

MIN_SAMPLES = 18
MIN_ACCURACY = 70
GENERATIONS = 2000


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


def extract_over_features(m: Dict) -> Dict:
    """Features enfocadas en detectar partidos con MUCHOS GOLES."""
    f = {}
    
    # Handicap y linea
    odds = m.get('main_match_odds') or {}
    try:
        ah = float(odds.get('ah_linea', 0) or 0)
        ou = float(odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah, ou = 0, 2.5
    
    f['ah_abs'] = abs(ah)
    f['ou'] = ou
    f['fav_claro'] = abs(ah) >= 1  # Favorito muy claro = más goles
    f['fav_muy_claro'] = abs(ah) >= 1.5
    f['partido_parejo'] = abs(ah) <= 0.25
    
    # Standings
    hs = m.get('home_standings') or {}
    aws = m.get('away_standings') or {}
    
    try:
        hr = int(hs.get('ranking', 0) or 0)
        ar = int(aws.get('ranking', 0) or 0)
        f['h_top5'] = 0 < hr <= 5
        f['a_top5'] = 0 < ar <= 5
        f['h_bottom5'] = hr >= 15
        f['a_bottom5'] = ar >= 15
        f['top_vs_bottom'] = (f['h_top5'] and f['a_bottom5']) or (f['a_top5'] and f['h_bottom5'])
        f['ambos_top'] = f['h_top5'] and f['a_top5']
    except:
        pass
    
    # GOLES - Lo más importante para OVER
    try:
        h_gf = int(hs.get('specific_gf', 0) or 0)
        h_gc = int(hs.get('specific_gc', 0) or 0)
        h_pj = int(hs.get('specific_pj', 1) or 1)
        a_gf = int(aws.get('specific_gf', 0) or 0)
        a_gc = int(aws.get('specific_gc', 0) or 0)
        a_pj = int(aws.get('specific_pj', 1) or 1)
        
        # Promedios de goles
        f['h_goles_avg'] = (h_gf + h_gc) / h_pj
        f['a_goles_avg'] = (a_gf + a_gc) / a_pj
        f['total_avg'] = (f['h_goles_avg'] + f['a_goles_avg']) / 2
        
        # Goleadores
        f['h_goleador'] = f['h_goles_avg'] >= 3
        f['a_goleador'] = f['a_goles_avg'] >= 3
        f['ambos_goleadores'] = f['h_goleador'] and f['a_goleador']
        f['total_alto'] = f['total_avg'] >= 3
        f['total_muy_alto'] = f['total_avg'] >= 3.5
        
        # Goles marcados
        f['h_anota_mucho'] = h_gf / h_pj >= 1.5
        f['a_anota_mucho'] = a_gf / a_pj >= 1.5
        f['ambos_anotan'] = f['h_anota_mucho'] and f['a_anota_mucho']
        
        # Defensas flojas (reciben muchos)
        f['h_recibe_mucho'] = h_gc / h_pj >= 1.5
        f['a_recibe_mucho'] = a_gc / a_pj >= 1.5
        f['ambas_defensas_flojas'] = f['h_recibe_mucho'] and f['a_recibe_mucho']
        f['una_defensa_floja'] = f['h_recibe_mucho'] or f['a_recibe_mucho']
    except:
        pass
    
    # Prev home
    ph = m.get('last_home_match') or {}
    sc = parse_score(ph.get('score'))
    if sc:
        f['ph_total'] = sc[0] + sc[1]
        f['ph_over'] = f['ph_total'] > 2.5
        f['ph_over3'] = f['ph_total'] >= 4
        f['ph_muchos'] = f['ph_total'] >= 5
        f['ph_recibio3'] = sc[1] >= 3
        f['ph_marco3'] = sc[0] >= 3
    
    # Prev away
    pa = m.get('last_away_match') or {}
    sc = parse_score(pa.get('score'))
    if sc:
        f['pa_total'] = sc[0] + sc[1]
        f['pa_over'] = f['pa_total'] > 2.5
        f['pa_over3'] = f['pa_total'] >= 4
        f['pa_muchos'] = f['pa_total'] >= 5
        f['pa_recibio3'] = sc[0] >= 3
        f['pa_marco3'] = sc[1] >= 3
    
    # H2H
    h2h = m.get('h2h_col3') or {}
    if h2h.get('status') == 'found':
        try:
            hg = int(h2h.get('goles_home', 0) or 0)
            ag = int(h2h.get('goles_away', 0) or 0)
            f['h2h_total'] = hg + ag
            f['h2h_over'] = f['h2h_total'] > 2.5
            f['h2h_over3'] = f['h2h_total'] >= 4
            f['h2h_muchos'] = f['h2h_total'] >= 5
        except:
            pass
    
    # Market
    mkt = m.get('market_analysis_data') or {}
    std = mkt.get('stadium') or {}
    sc = parse_score(std.get('score', ''))
    if sc:
        f['std_total'] = sc[0] + sc[1]
        f['std_over'] = f['std_total'] > 2.5
        f['std_over3'] = f['std_total'] >= 4
    
    gen = mkt.get('general') or {}
    sc = parse_score(gen.get('score', ''))
    if sc:
        f['gen_total'] = sc[0] + sc[1]
        f['gen_over'] = f['gen_total'] > 2.5
    
    # Comparativas indirectas
    comp = m.get('comparativas_indirectas') or {}
    left = comp.get('left')
    if left:
        sc = parse_score(left.get('score'))
        if sc:
            f['ind_l_total'] = sc[0] + sc[1]
            f['ind_l_over'] = f['ind_l_total'] > 2.5
            f['ind_l_over3'] = f['ind_l_total'] >= 4
    
    right = comp.get('right')
    if right:
        sc = parse_score(right.get('score'))
        if sc:
            f['ind_r_total'] = sc[0] + sc[1]
            f['ind_r_over'] = f['ind_r_total'] > 2.5
            f['ind_r_over3'] = f['ind_r_total'] >= 4
    
    # COMBINADOS PARA OVER
    f['ambos_prev_over'] = f.get('ph_over', False) and f.get('pa_over', False)
    f['ambos_prev_over3'] = f.get('ph_over3', False) and f.get('pa_over3', False)
    f['todo_over'] = f.get('ambos_prev_over', False) and f.get('h2h_over', False)
    f['historial_goleador'] = f.get('h2h_over', False) and f.get('std_over', False)
    f['alguien_recibio3'] = f.get('ph_recibio3', False) or f.get('pa_recibio3', False)
    f['ambos_ind_over'] = f.get('ind_l_over', False) and f.get('ind_r_over', False)
    
    return f


# Features enfocadas en OVER
OVER_FEATURES = [
    'fav_claro', 'fav_muy_claro', 'partido_parejo',
    'h_top5', 'a_top5', 'h_bottom5', 'a_bottom5', 'top_vs_bottom', 'ambos_top',
    'h_goleador', 'a_goleador', 'ambos_goleadores', 'total_alto', 'total_muy_alto',
    'h_anota_mucho', 'a_anota_mucho', 'ambos_anotan',
    'h_recibe_mucho', 'a_recibe_mucho', 'ambas_defensas_flojas', 'una_defensa_floja',
    'ph_over', 'ph_over3', 'ph_muchos', 'ph_recibio3', 'ph_marco3',
    'pa_over', 'pa_over3', 'pa_muchos', 'pa_recibio3', 'pa_marco3',
    'h2h_over', 'h2h_over3', 'h2h_muchos',
    'std_over', 'std_over3', 'gen_over',
    'ind_l_over', 'ind_l_over3', 'ind_r_over', 'ind_r_over3',
    'ambos_prev_over', 'ambos_prev_over3', 'todo_over', 'historial_goleador',
    'alguien_recibio3', 'ambos_ind_over',
]


def main():
    print("=" * 60)
    print("CAZADOR DE PATRONES OVER")
    print("Buscando patrones para partidos con MUCHOS GOLES")
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
    
    # Contar OVERs en datos
    over_count = 0
    under_count = 0
    for m in all_matches:
        sc = parse_score(m.get('final_score') or m.get('score'))
        if sc:
            odds = m.get('main_match_odds') or {}
            try:
                ou = float(odds.get('goals_linea', 2.5) or 2.5)
            except:
                ou = 2.5
            total = sc[0] + sc[1]
            if total > ou + 0.25:
                over_count += 1
            elif total < ou - 0.25:
                under_count += 1
    
    print(f"\nDistribucion: OVER={over_count} ({over_count*100/(over_count+under_count):.1f}%) | UNDER={under_count}")
    
    # Generar patrones SOLO para OVER
    patterns = []
    for _ in range(4000):
        n_conds = random.randint(2, 4)
        conds = random.sample(OVER_FEATURES, min(n_conds, len(OVER_FEATURES)))
        patterns.append({'conds': [(c, True) for c in conds], 'hits': 0, 'miss': 0})
    
    results = []
    
    print(f"\nBuscando patrones OVER en {GENERATIONS} generaciones...")
    
    for gen in range(GENERATIONS):
        for p in patterns:
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
            
            total = sc[0] + sc[1]
            if total > ou + 0.25:
                is_over = True
            elif total < ou - 0.25:
                is_over = False
            else:
                continue  # Push
            
            feats = extract_over_features(m)
            
            for p in patterns:
                match = all(feats.get(c) == v for c, v in p['conds'])
                if match:
                    if is_over:
                        p['hits'] += 1
                    else:
                        p['miss'] += 1
        
        # Buscar buenos
        for p in patterns:
            total = p['hits'] + p['miss']
            if total >= MIN_SAMPLES:
                acc = p['hits'] / total * 100
                if acc >= MIN_ACCURACY:
                    is_new = not any(set(x['conds']) == set(p['conds']) for x in results)
                    if is_new and len(results) < 60:
                        results.append({
                            'pick': 'OVER',
                            'acc': round(acc, 1),
                            'hits': p['hits'],
                            'total': total,
                            'conds': p['conds']
                        })
                        stars = '***' if acc >= 80 else '**' if acc >= 75 else '*'
                        cond_names = [c[0] for c in p['conds']]
                        print(f"{stars} OVER {acc:.1f}% ({p['hits']}/{total}) <- {cond_names}")
        
        # Mutacion - sesgo hacia OVER
        new_patterns = []
        good = [p for p in patterns if p['hits'] + p['miss'] >= 5 and p['hits'] / (p['hits'] + p['miss'] + 0.01) >= 0.5]
        if len(good) < 100:
            good = patterns[:500]
        good.sort(key=lambda x: -(x['hits'] / (x['hits'] + x['miss'] + 0.01)))
        survivors = good[:500]
        
        for p in survivors:
            new_patterns.append(p)
        
        while len(new_patterns) < 4000:
            parent = random.choice(survivors[:200])
            child = {'conds': list(parent['conds']), 'hits': 0, 'miss': 0}
            
            if random.random() < 0.3 and len(child['conds']) < 5:
                new_feat = random.choice(OVER_FEATURES)
                if not any(c[0] == new_feat for c in child['conds']):
                    child['conds'].append((new_feat, True))
            if random.random() < 0.2 and len(child['conds']) > 2:
                child['conds'].pop(random.randint(0, len(child['conds'])-1))
            
            new_patterns.append(child)
        
        # Nuevos aleatorios
        for _ in range(300):
            n_conds = random.randint(2, 4)
            conds = random.sample(OVER_FEATURES, min(n_conds, len(OVER_FEATURES)))
            new_patterns.append({'conds': [(c, True) for c in conds], 'hits': 0, 'miss': 0})
        
        patterns = new_patterns[:4000]
        
        if (gen + 1) % 200 == 0:
            print(f"  G{gen+1} - Patrones OVER encontrados: {len(results)}")
    
    # Ordenar por precision
    results.sort(key=lambda x: -x['acc'])
    
    # Guardar
    output = {
        'timestamp': datetime.now().isoformat(),
        'version': 'over-hunter-1.0',
        'matches': len(all_matches),
        'over_rate': round(over_count * 100 / (over_count + under_count), 1),
        'patterns': results[:50]
    }
    
    path = RESULTS_DIR / 'over_patterns.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"RESUMEN: {len(results)} patrones OVER encontrados")
    
    if results:
        print("\nTOP 15 PATRONES OVER:")
        for i, p in enumerate(results[:15], 1):
            cond_names = [c[0] for c in p['conds']]
            print(f"  {i}. OVER {p['acc']}% ({p['hits']}/{p['total']})")
            print(f"     Conds: {cond_names}")
    
    print(f"\nGuardado: {path}")


if __name__ == '__main__':
    main()
