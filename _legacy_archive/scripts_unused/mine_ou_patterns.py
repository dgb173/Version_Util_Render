
import json
import itertools
from pathlib import Path

DATA_FILES = ['training_data_1465.json', 'validation_data_39_2024-2025.json']

def p_s(rows):
    r = {}
    for row in (rows or []):
        try:
            val_h = str(row.get('home','0')).replace('%','')
            val_a = str(row.get('away','0')).replace('%','')
            r[row.get('label')] = {'h':int(val_h), 'a':int(val_a)}
        except: pass
    return r

def extract_ou(m):
    # Goles anotados/recibidos promedio (simplificado de last matches)
    lhm = m.get('last_home_match') or {}; lam = m.get('last_away_match') or {}
    sh = p_s(lhm.get('stats_rows')); sa = p_s(lam.get('stats_rows'))
    
    # Score final para el target
    s_r = m.get('final_score') or m.get('score')
    if not s_r or ':' not in s_r or '?' in s_r: return None
    parts = s_r.replace('-', ':').split(':')
    try: hg, ag = int(parts[0]), int(parts[1]); total_g = hg + ag
    except: return None
    
    # Línea de O/U (usamos 2.5 como base si no hay)
    o_raw = (m.get('main_match_odds') or {}).get('goals_linea', 2.5)
    try: ou_line = float(o_raw)
    except: ou_line = 2.5
    
    # AH para correlación
    ah_raw = (m.get('main_match_odds') or {}).get('ah_linea', 0)
    try: ah = float(ah_raw)
    except: ah = 0

    f = {
        'total_g': total_g,
        'ou_line': ou_line,
        'ah': ah,
        # Presión ofensiva
        'h_da': sh.get('Ataques Peligrosos', {}).get('h', 0),
        'a_da': sa.get('Ataques Peligrosos', {}).get('a', 0),
        'h_sot': sh.get('Tiros a Puerta', {}).get('h', 0),
        'a_sot': sa.get('Tiros a Puerta', {}).get('a', 0),
        # Correlación AH
        'fav_is_local': ah > 0,
        # H2H Col3 / Indirectas
        'ind': 1 if (m.get('comparativas_indirectas', {}).get('left') or m.get('comparativas_indirectas', {}).get('right')) else 0,
        'col3': 1 if m.get('h2h_col3', {}).get('status') == 'found' else 0,
        'col3_perf': m.get('h2h_col3', {}).get('performance', 'N/A')
    }
    
    # Goles en últimos partidos (proxy de tendencia)
    f['h_avg_g'] = (sh.get('Goles', {}).get('h', 0) + sh.get('Goles', {}).get('a', 0)) / 1.0 # Ya es total de ese match
    f['a_avg_g'] = (sa.get('Goles', {}).get('h', 0) + sa.get('Goles', {}).get('a', 0)) / 1.0
    
    return f

def calculate_ou_roi(matches, pick):
    if not matches: return -1, 0
    total_ret = 0
    for m in matches:
        diff = m['total_g'] - m['ou_line']
        res = 0 # 1=W, 0.5=HW, 0=P, -0.5=HL, -1=L
        if pick == 'OVER':
            if diff > 0.25: res = 1
            elif diff == 0.25: res = 0.5
            elif diff == 0: res = 0
            elif diff == -0.25: res = -0.5
            else: res = -1
        else: # UNDER
            if diff < -0.25: res = 1
            elif diff == -0.25: res = 0.5
            elif diff == 0: res = 0
            elif diff == 0.25: res = -0.5
            else: res = -1
            
        # ROI simple asumiendo cuota 1.90
        if res == 1: total_ret += 1.90
        elif res == 0.5: total_ret += 1.45
        elif res == 0: total_ret += 1.0
        elif res == -0.5: total_ret += 0.5
        # res -1 -> 0
    return (total_ret - len(matches)) / len(matches), len(matches)

def main():
    all_f = []
    for fn in DATA_FILES:
        if Path(fn).exists():
            with open(fn, encoding='utf-8') as fh:
                for m in json.load(fh):
                    ext = extract_ou(m)
                    if ext: all_f.append(ext)
    
    print(f"Minería O/U sobre {len(all_f)} partidos.")
    results = []
    
    # Filtros base
    flts = [
        ('DA_Total>100', lambda x: x['h_da'] + x['a_da'] > 100),
        ('DA_Total>80', lambda x: x['h_da'] + x['a_da'] > 80),
        ('DA_Total<70', lambda x: x['h_da'] + x['a_da'] < 70),
        ('DA_Total<60', lambda x: x['h_da'] + x['a_da'] < 60),
        ('SOT_Total>10', lambda x: x['h_sot'] + x['a_sot'] > 10),
        ('SOT_Total>8', lambda x: x['h_sot'] + x['a_sot'] > 8),
        ('SOT_Total<5', lambda x: x['h_sot'] + x['a_sot'] < 5),
        ('SOT_Total<4', lambda x: x['h_sot'] + x['a_sot'] < 4),
        ('Fav_Local', lambda x: x['fav_is_local']),
        ('Fav_Visita', lambda x: not x['fav_is_local']),
        ('Ind_Y', lambda x: x['ind']),
        ('Col3_Y', lambda x: x['col3']),
        ('Col3_MEJORA', lambda x: x['col3_perf'] == 'MEJORA'),
        ('Col3_EMPEORA', lambda x: x['col3_perf'] == 'EMPEORA'),
        ('Trend_Over', lambda x: x['h_avg_g'] + x['a_avg_g'] > 5),
        ('Trend_High_Over', lambda x: x['h_avg_g'] + x['a_avg_g'] > 6),
        ('Trend_Under', lambda x: x['h_avg_g'] + x['a_avg_g'] < 3.5),
        ('AH_GE_1', lambda x: abs(x['ah']) >= 1.0),
        ('AH_0', lambda x: abs(x['ah']) == 0),
        ('DA_Diff_H>20', lambda x: x['h_da'] - x['a_da'] > 20),
        ('DA_Diff_V>20', lambda x: x['a_da'] - x['h_da'] > 20),
        ('SOT_Diff_H>3', lambda x: x['h_sot'] - x['a_sot'] > 3),
        ('SOT_Diff_V>3', lambda x: x['a_sot'] - x['h_sot'] > 3),
        ('DA_Ratio_H>2', lambda x: x['h_da'] / max(1, x['a_da']) > 2),
        ('DA_Ratio_V>2', lambda x: x['a_da'] / max(1, x['h_da']) > 2),
        ('Trend_Low_Under', lambda x: x['h_avg_g'] + x['a_avg_g'] < 2.5),
        ('Trend_Med_Over', lambda x: x['h_avg_g'] + x['a_avg_g'] > 4)
    ]
    
    for r_len in range(1, 5):
        for combo in itertools.combinations(flts, r_len):
            def f_final(x):
                for _, fn in combo:
                    if not fn(x): return False
                return True
            
            subset = [x for x in all_f if f_final(x)]
            if len(subset) >= 12:
                for pick in ['OVER', 'UNDER']:
                    roi, n = calculate_ou_roi(subset, pick)
                    if roi >= 0.15:
                        results.append({
                            'name': f"{' + '.join([c[0] for c in combo])} ({pick})",
                            'roi': roi,
                            'n': n,
                            'pick': pick
                        })

    # Sort & Dedeplicate
    results.sort(key=lambda x: x['roi'], reverse=True)
    final = []
    seen = set()
    for r in results:
        if r['roi'] >= 0.20 and r['n'] >= 18 and r['name'] not in seen:
            final.append(r)
            seen.add(r['name'])
            
    print(f"Patrones O/U encontrados: {len(final)}")
    
    mapping = {
        'DA_Total>100': ['da_total', '>', 100], 'DA_Total>80': ['da_total', '>', 80],
        'DA_Total<70': ['da_total', '<', 70], 'DA_Total<60': ['da_total', '<', 60],
        'SOT_Total>10': ['sot_total', '>', 10], 'SOT_Total>8': ['sot_total', '>', 8],
        'SOT_Total<5': ['sot_total', '<', 5], 'SOT_Total<4': ['sot_total', '<', 4],
        'Fav_Local': ['fav_is_local', '==', True], 'Fav_Visita': ['fav_is_local', '==', False],
        'Ind_Y': ['ind', '==', 1], 'Col3_Y': ['col3', '==', 1],
        'Col3_MEJORA': ['col3_perf', '==', 'MEJORA'], 'Col3_EMPEORA': ['col3_perf', '==', 'EMPEORA'],
        'Trend_Over': ['trend', '>', 5], 'Trend_High_Over': ['trend', '>', 6],
        'Trend_Under': ['trend', '<', 3.5], 'Trend_Low_Under': ['trend', '<', 2.5],
        'Trend_Med_Over': ['trend', '>', 4],
        'AH_GE_1': ['ah_abs', '>=', 1.0], 'AH_0': ['ah_abs', '==', 0],
        'DA_Ratio_H>2': ['da_ratio_h', '>', 2], 'DA_Ratio_V>2': ['da_ratio_v', '>', 2],
        'DA_Diff_H>20': ['da_diff_h', '>', 20], 'DA_Diff_V>20': ['da_diff_v', '>', 20],
        'SOT_Diff_H>3': ['sot_diff_h', '>', 3], 'SOT_Diff_V>3': ['sot_diff_v', '>', 3]
    }
    
    out_file = Path('backtest_results/specialist_ou_patterns.json')
    with open(out_file, 'w', encoding='utf-8') as f:
        json_data = []
        for i, r in enumerate(final[:205]):
            cond_names = r['name'].replace(f" ({r['pick']})", "").split(' + ')
            parsed_conds = [mapping[c] for c in cond_names if c in mapping]
            
            json_data.append({
                "id": f"OU_PTN_{i+1:03d}",
                "name": r['name'],
                "pick": r['pick'],
                "roi": round(r['roi']*100, 2),
                "samples": r['n'],
                "type": "OU",
                "is_new": True,
                "algorithm": "ADVANCED_MINED",
                "conditions": parsed_conds
            })
        json.dump(json_data, f, indent=2, ensure_ascii=False)
    print(f"Guardados {len(json_data)} patrones O/U.")

if __name__ == "__main__":
    main()
