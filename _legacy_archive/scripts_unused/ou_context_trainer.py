# scripts/ou_context_trainer.py
"""
ENTRENADOR O/U CON CONTEXTO DE HANDICAP
========================================
Entiende la relación entre:
- Handicap alto = favorito claro = suele haber goles (OVER)
- Handicap cerrado = partido parejo = menos goles (UNDER)
- Línea de goles vs realidad histórica
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


def extract_context_features(m: Dict) -> Dict:
    """Features que relacionan Handicap con expectativas de goles."""
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
    
    # Categorías de Handicap
    f['ah_0'] = abs(ah) < 0.01  # Partido muy parejo
    f['ah_025'] = 0.2 <= abs(ah) <= 0.3  # Ligera ventaja
    f['ah_05'] = 0.4 <= abs(ah) <= 0.6  # Favorito leve
    f['ah_075'] = 0.7 <= abs(ah) <= 0.8  # Favorito moderado
    f['ah_1'] = 0.9 <= abs(ah) <= 1.1  # Favorito claro
    f['ah_15'] = 1.4 <= abs(ah) <= 1.6  # Favorito muy claro
    f['ah_2plus'] = abs(ah) >= 1.9  # Goleada esperada
    
    # Categorías de Línea O/U
    f['ou_bajo'] = ou <= 2.25  # Línea baja
    f['ou_normal'] = 2.25 < ou <= 2.75  # Línea normal
    f['ou_alto'] = ou >= 2.75  # Línea alta
    
    # RELACIÓN Handicap vs Línea O/U (CLAVE)
    # Si el handicap es alto, el mercado espera goles
    f['fav_claro_linea_baja'] = abs(ah) >= 1 and ou <= 2.5  # Contradicción = OVER probable
    f['fav_claro_linea_alta'] = abs(ah) >= 1 and ou >= 2.75  # Coherente
    f['parejo_linea_alta'] = abs(ah) <= 0.25 and ou >= 2.75  # Contradicción = UNDER probable
    f['parejo_linea_baja'] = abs(ah) <= 0.25 and ou <= 2.25  # Coherente
    
    # Expectativa vs realidad
    goles_esperados = 2.5 + abs(ah) * 0.3  # Aproximación simple
    f['linea_baja_para_ah'] = ou < goles_esperados - 0.3
    f['linea_alta_para_ah'] = ou > goles_esperados + 0.3
    f['linea_correcta'] = abs(ou - goles_esperados) <= 0.3
    
    # === STANDINGS ===
    hs = m.get('home_standings') or {}
    aws = m.get('away_standings') or {}
    
    try:
        hr = int(hs.get('ranking', 0) or 0)
        ar = int(aws.get('ranking', 0) or 0)
        f['h_top'] = 0 < hr <= 5
        f['a_top'] = 0 < ar <= 5
        f['h_bottom'] = hr >= 15
        f['a_bottom'] = ar >= 15
        f['top_vs_bottom'] = (f['h_top'] and f['a_bottom']) or (f['a_top'] and f['h_bottom'])
        f['ambos_top'] = f['h_top'] and f['a_top']
        f['ambos_bottom'] = f['h_bottom'] and f['a_bottom']
    except:
        pass
    
    # === GOLES PROMEDIO ===
    try:
        h_gf = int(hs.get('specific_gf', 0) or 0)
        h_gc = int(hs.get('specific_gc', 0) or 0)
        h_pj = int(hs.get('specific_pj', 1) or 1)
        a_gf = int(aws.get('specific_gf', 0) or 0)
        a_gc = int(aws.get('specific_gc', 0) or 0)
        a_pj = int(aws.get('specific_pj', 1) or 1)
        
        f['h_avg'] = (h_gf + h_gc) / h_pj
        f['a_avg'] = (a_gf + a_gc) / a_pj
        f['total_avg'] = (f['h_avg'] + f['a_avg']) / 2
        
        # Comparar promedio real vs línea
        f['avg_over_linea'] = f['total_avg'] > ou  # Historial dice más goles que línea
        f['avg_under_linea'] = f['total_avg'] < ou  # Historial dice menos goles
        f['avg_igual_linea'] = abs(f['total_avg'] - ou) <= 0.3
        
        # Combinación handicap + promedio
        f['ah_alto_avg_alto'] = abs(ah) >= 1 and f['total_avg'] >= 3
        f['ah_bajo_avg_bajo'] = abs(ah) <= 0.5 and f['total_avg'] < 2.5
        
        # Goles específicos
        f['h_goleador'] = f['h_avg'] >= 3
        f['a_goleador'] = f['a_avg'] >= 3
        f['ambos_goleadores'] = f['h_goleador'] and f['a_goleador']
        f['h_defensivo'] = f['h_avg'] < 2.2
        f['a_defensivo'] = f['a_avg'] < 2.2
        f['ambos_defensivos'] = f['h_defensivo'] and f['a_defensivo']
        
        # Defensas
        f['h_recibe_mucho'] = h_gc / h_pj >= 1.5
        f['a_recibe_mucho'] = a_gc / a_pj >= 1.5
        f['ambas_flojas'] = f['h_recibe_mucho'] and f['a_recibe_mucho']
        
        # Anotan
        f['h_anota'] = h_gf / h_pj >= 1.3
        f['a_anota'] = a_gf / a_pj >= 1.3
        f['ambos_anotan'] = f['h_anota'] and f['a_anota']
    except:
        pass
    
    # === PARTIDOS PREVIOS ===
    ph = m.get('last_home_match') or {}
    sc = parse_score(ph.get('score'))
    if sc:
        f['ph_total'] = sc[0] + sc[1]
        f['ph_over'] = f['ph_total'] > 2.5
        f['ph_over3'] = f['ph_total'] >= 4
        f['ph_under'] = f['ph_total'] <= 2
        f['ph_goleada'] = f['ph_total'] >= 5
    
    pa = m.get('last_away_match') or {}
    sc = parse_score(pa.get('score'))
    if sc:
        f['pa_total'] = sc[0] + sc[1]
        f['pa_over'] = f['pa_total'] > 2.5
        f['pa_over3'] = f['pa_total'] >= 4
        f['pa_under'] = f['pa_total'] <= 2
        f['pa_goleada'] = f['pa_total'] >= 5
    
    # Combinados previos
    f['ambos_prev_over'] = f.get('ph_over', False) and f.get('pa_over', False)
    f['ambos_prev_under'] = f.get('ph_under', False) and f.get('pa_under', False)
    f['ambos_prev_over3'] = f.get('ph_over3', False) and f.get('pa_over3', False)
    f['algun_prev_goleada'] = f.get('ph_goleada', False) or f.get('pa_goleada', False)
    
    # === H2H ===
    h2h = m.get('h2h_col3') or {}
    if h2h.get('status') == 'found':
        try:
            hg = int(h2h.get('goles_home', 0) or 0)
            ag = int(h2h.get('goles_away', 0) or 0)
            f['h2h_total'] = hg + ag
            f['h2h_over'] = f['h2h_total'] > 2.5
            f['h2h_under'] = f['h2h_total'] <= 2
            f['h2h_goleada'] = f['h2h_total'] >= 5
        except:
            pass
    
    # Market stadium
    mkt = m.get('market_analysis_data') or {}
    std = mkt.get('stadium') or {}
    sc = parse_score(std.get('score', ''))
    if sc:
        f['std_total'] = sc[0] + sc[1]
        f['std_over'] = f['std_total'] > 2.5
        f['std_under'] = f['std_total'] <= 2
    
    # === PATRONES CONTEXTUALES COMBINADOS ===
    # El mercado dice X pero el historial dice Y
    f['mercado_over_hist_under'] = f.get('avg_over_linea', False) and f.get('ambos_prev_under', False)
    f['mercado_under_hist_over'] = f.get('avg_under_linea', False) and f.get('ambos_prev_over', False)
    
    # Favorito claro + historial goleador = OVER seguro
    f['fav_claro_goleadores'] = abs(ah) >= 1 and f.get('ambos_goleadores', False)
    f['fav_claro_prev_over'] = abs(ah) >= 1 and f.get('ambos_prev_over', False)
    
    # Partido parejo + defensivos = UNDER seguro
    f['parejo_defensivos'] = abs(ah) <= 0.25 and f.get('ambos_defensivos', False)
    f['parejo_prev_under'] = abs(ah) <= 0.25 and f.get('ambos_prev_under', False)
    
    # Top vs Bottom con handicap alto = OVER
    f['top_bottom_ah_alto'] = f.get('top_vs_bottom', False) and abs(ah) >= 1
    
    return f


CONTEXT_FEATURES = [
    # Handicap
    'ah_0', 'ah_025', 'ah_05', 'ah_075', 'ah_1', 'ah_15', 'ah_2plus',
    # Línea
    'ou_bajo', 'ou_normal', 'ou_alto',
    # Relaciones
    'fav_claro_linea_baja', 'fav_claro_linea_alta', 'parejo_linea_alta', 'parejo_linea_baja',
    'linea_baja_para_ah', 'linea_alta_para_ah', 'linea_correcta',
    # Rankings
    'h_top', 'a_top', 'h_bottom', 'a_bottom', 'top_vs_bottom', 'ambos_top', 'ambos_bottom',
    # Promedios  
    'avg_over_linea', 'avg_under_linea', 'avg_igual_linea',
    'ah_alto_avg_alto', 'ah_bajo_avg_bajo',
    'h_goleador', 'a_goleador', 'ambos_goleadores',
    'h_defensivo', 'a_defensivo', 'ambos_defensivos',
    'h_recibe_mucho', 'a_recibe_mucho', 'ambas_flojas',
    'h_anota', 'a_anota', 'ambos_anotan',
    # Previos
    'ph_over', 'ph_over3', 'ph_under', 'ph_goleada',
    'pa_over', 'pa_over3', 'pa_under', 'pa_goleada',
    'ambos_prev_over', 'ambos_prev_under', 'ambos_prev_over3', 'algun_prev_goleada',
    # H2H
    'h2h_over', 'h2h_under', 'h2h_goleada',
    'std_over', 'std_under',
    # Contextuales
    'mercado_over_hist_under', 'mercado_under_hist_over',
    'fav_claro_goleadores', 'fav_claro_prev_over',
    'parejo_defensivos', 'parejo_prev_under',
    'top_bottom_ah_alto',
]


def main():
    print("=" * 60)
    print("ENTRENADOR O/U CON CONTEXTO DE HANDICAP")
    print("Entiende relacion Handicap <-> Expectativas de Goles")
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
    
    # Estadísticas por handicap
    ah_stats = defaultdict(lambda: {'over': 0, 'under': 0})
    for m in all_matches:
        sc = parse_score(m.get('final_score') or m.get('score'))
        if not sc:
            continue
        odds = m.get('main_match_odds') or {}
        try:
            ah = abs(float(odds.get('ah_linea', 0) or 0))
            ou = float(odds.get('goals_linea', 2.5) or 2.5)
        except:
            continue
        
        total = sc[0] + sc[1]
        bucket = 'AH_0' if ah < 0.1 else 'AH_0.5' if ah <= 0.6 else 'AH_1' if ah <= 1.1 else 'AH_1.5+'
        if total > ou + 0.25:
            ah_stats[bucket]['over'] += 1
        elif total < ou - 0.25:
            ah_stats[bucket]['under'] += 1
    
    print("\nDistribucion O/U por Handicap:")
    for bucket, stats in sorted(ah_stats.items()):
        total = stats['over'] + stats['under']
        if total > 0:
            print(f"  {bucket}: OVER {stats['over']*100/total:.1f}% | UNDER {stats['under']*100/total:.1f}%")
    
    # Generar patrones
    patterns = []
    for _ in range(4000):
        n = random.randint(2, 4)
        conds = random.sample(CONTEXT_FEATURES, min(n, len(CONTEXT_FEATURES)))
        pick = random.choice(['OVER', 'UNDER'])
        patterns.append({'conds': [(c, True) for c in conds], 'pick': pick, 'hits': 0, 'miss': 0})
    
    over_results = []
    under_results = []
    
    print(f"\nBuscando patrones en {GENERATIONS} generaciones...")
    
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
                result = 'OVER'
            elif total < ou - 0.25:
                result = 'UNDER'
            else:
                continue
            
            feats = extract_context_features(m)
            
            for p in patterns:
                match = all(feats.get(c) == v for c, v in p['conds'])
                if match:
                    if p['pick'] == result:
                        p['hits'] += 1
                    else:
                        p['miss'] += 1
        
        # Buscar buenos
        for p in patterns:
            total = p['hits'] + p['miss']
            if total >= MIN_SAMPLES:
                acc = p['hits'] / total * 100
                if acc >= MIN_ACCURACY:
                    target = over_results if p['pick'] == 'OVER' else under_results
                    is_new = not any(set(x['conds']) == set(p['conds']) for x in target)
                    if is_new and len(target) < 40:
                        target.append({
                            'pick': p['pick'],
                            'acc': round(acc, 1),
                            'hits': p['hits'],
                            'total': total,
                            'conds': p['conds']
                        })
                        stars = '***' if acc >= 80 else '**' if acc >= 75 else '*'
                        cond_names = [c[0] for c in p['conds']]
                        print(f"{stars} {p['pick']} {acc:.1f}% ({p['hits']}/{total}) <- {cond_names}")
        
        # Mutacion
        new_patterns = []
        good = [p for p in patterns if p['hits'] + p['miss'] >= 5]
        good.sort(key=lambda x: -(x['hits'] / (x['hits'] + x['miss'] + 0.01)))
        survivors = good[:500]
        
        for p in survivors:
            new_patterns.append(p)
        
        while len(new_patterns) < 4000:
            parent = random.choice(survivors[:200] if len(survivors) >= 200 else survivors)
            child = {'conds': list(parent['conds']), 'pick': parent['pick'], 'hits': 0, 'miss': 0}
            
            if random.random() < 0.3 and len(child['conds']) < 5:
                new_feat = random.choice(CONTEXT_FEATURES)
                if not any(c[0] == new_feat for c in child['conds']):
                    child['conds'].append((new_feat, True))
            if random.random() < 0.2 and len(child['conds']) > 2:
                child['conds'].pop(random.randint(0, len(child['conds'])-1))
            if random.random() < 0.1:
                child['pick'] = 'UNDER' if child['pick'] == 'OVER' else 'OVER'
            
            new_patterns.append(child)
        
        for _ in range(300):
            n = random.randint(2, 4)
            conds = random.sample(CONTEXT_FEATURES, min(n, len(CONTEXT_FEATURES)))
            pick = random.choice(['OVER', 'UNDER'])
            new_patterns.append({'conds': [(c, True) for c in conds], 'pick': pick, 'hits': 0, 'miss': 0})
        
        patterns = new_patterns[:4000]
        
        if (gen + 1) % 200 == 0:
            print(f"  G{gen+1} - OVER:{len(over_results)} UNDER:{len(under_results)}")
    
    # Guardar
    all_results = sorted(over_results + under_results, key=lambda x: -x['acc'])
    
    output = {
        'timestamp': datetime.now().isoformat(),
        'version': 'ou-context-1.0',
        'matches': len(all_matches),
        'over_patterns': sorted(over_results, key=lambda x: -x['acc']),
        'under_patterns': sorted(under_results, key=lambda x: -x['acc'])
    }
    
    path = RESULTS_DIR / 'ou_context_patterns.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"RESUMEN: OVER={len(over_results)} | UNDER={len(under_results)}")
    
    if over_results:
        print("\nTOP 10 OVER:")
        for i, p in enumerate(sorted(over_results, key=lambda x: -x['acc'])[:10], 1):
            cond_names = [c[0] for c in p['conds']]
            print(f"  {i}. OVER {p['acc']}% ({p['hits']}/{p['total']}) <- {cond_names}")
    
    if under_results:
        print("\nTOP 10 UNDER:")
        for i, p in enumerate(sorted(under_results, key=lambda x: -x['acc'])[:10], 1):
            cond_names = [c[0] for c in p['conds']]
            print(f"  {i}. UNDER {p['acc']}% ({p['hits']}/{p['total']}) <- {cond_names}")
    
    print(f"\nGuardado: {path}")


if __name__ == '__main__':
    main()
