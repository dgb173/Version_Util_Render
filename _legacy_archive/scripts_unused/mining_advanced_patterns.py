
import json
import itertools
from pathlib import Path

DATA_FILES = ['training_data_1465.json', 'validation_data_39_2024-2025.json']
PROJECT_ROOT = Path(__file__).resolve().parent.parent

def p_s(rows):
    r = {}
    for row in (rows or []):
        try:
            val_h = str(row.get('home','0')).replace('%','')
            val_a = str(row.get('away','0')).replace('%','')
            r[row.get('label')] = {'h':int(val_h), 'a':int(val_a)}
        except: pass
    return r

def get_asian_outcome(hg, ag, ah):
    diff = hg - ag
    if abs(ah % 1.0) in [0.25, 0.75]:
        lines = [ah - 0.25, ah + 0.25]
        v = [1 if diff + l > 0 else (-1 if diff + l < 0 else 0) for l in lines]
        return sum(v) / 2
    return 1.0 if diff + ah > 0 else (-1.0 if diff + ah < 0 else 0.0)

def extract(m):
    o = m.get('main_match_odds') or {}
    ah_r = o.get('ah_linea'); ai_ah = m.get('ah_open_home')
    ah_val = ah_r if ah_r and ah_r != '-' else ai_ah
    if not ah_val: return None
    try: ah = float(ah_val)
    except: return None
    s_r = m.get('final_score') or m.get('score')
    if not s_r or ':' not in s_r or '?' in s_r: return None
    parts = s_r.replace('-', ':').split(':')
    try: hg, ag = int(parts[0]), int(parts[1])
    except: return None
    f = {'ah': ah, 'lf': ah > 0}
    if f['lf']:
        f['of'] = get_asian_outcome(hg, ag, -abs(ah)); f['ou'] = get_asian_outcome(ag, hg, abs(ah))
    else:
        f['of'] = get_asian_outcome(ag, hg, -abs(ah)); f['ou'] = get_asian_outcome(hg, ag, abs(ah))
    lhm = m.get('last_home_match') or {}; lam = m.get('last_away_match') or {}
    sh = p_s(lhm.get('stats_rows')); sa = p_s(lam.get('stats_rows'))
    f['h_da_d'] = sh.get('Ataques Peligrosos', {}).get('h', 0) - sh.get('Ataques Peligrosos', {}).get('a', 0)
    f['v_da_d'] = sa.get('Ataques Peligrosos', {}).get('a', 0) - sa.get('Ataques Peligrosos', {}).get('h', 0)
    f['da_g'] = f['h_da_d'] - f['v_da_d']
    f['h_sot_r'] = sh.get('Tiros a Puerta', {}).get('h', 0) / max(1, sh.get('Tiros', {}).get('h', 0))
    f['v_sot_r'] = sa.get('Tiros a Puerta', {}).get('a', 0) / max(1, sa.get('Tiros', {}).get('a', 0))
    f['ind'] = 1 if (m.get('comparativas_indirectas', {}).get('left') or m.get('comparativas_indirectas', {}).get('right')) else 0
    f['col3'] = 1 if m.get('h2h_col3', {}).get('status') == 'found' else 0
    return f

def main():
    all_f = []
    for fn in DATA_FILES:
        try:
            with open(fn, encoding='utf-8') as fh:
                for m in json.load(fh):
                    e = extract(m)
                    if e: all_f.append(e)
        except: pass
    
    results = []
    flts = [
        ('DA_G>10', lambda x: x['da_g'] > 10), ('DA_G>0', lambda x: x['da_g'] > 0), ('DA_G<-5', lambda x: x['da_g'] < -5),
        ('H_DA>10', lambda x: x['h_da_d'] > 10), ('V_DA>10', lambda x: x['v_da_d'] > 10),
        ('SOT_H>0.3', lambda x: x['h_sot_r'] > 0.3), ('SOT_V>0.3', lambda x: x['v_sot_r'] > 0.3),
        ('SOT_H<0.2', lambda x: x['h_sot_r'] < 0.2), ('SOT_V<0.2', lambda x: x['v_sot_r'] < 0.2),
        ('Ind_Y', lambda x: x['ind']), ('Col3_Y', lambda x: x['col3']),
        ('DA_G>15', lambda x: x['da_g'] > 15), ('H_DA_High', lambda x: x['h_da_d'] > 25)
    ]
    ahs = [0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5]
    bets = [('Fav', 'of'), ('Und', 'ou')]
    
    for ah, (b_name, b_key) in itertools.product(ahs + ['ALL'], bets):
        base = [f for f in all_f if (abs(f['ah']) == ah if ah != 'ALL' else True)]
        if len(base) < 15: continue
        
        for r_len in range(1, 4):
            for combo in itertools.combinations(flts, r_len):
                def final_f(x):
                    for _, fn in combo:
                        if not fn(x): return False
                    return True
                s = [x for x in base if final_f(x)]
                if len(s) >= 15:
                    total = sum([1.9 if f[b_key]==1 else (1.45 if f[b_key]==0.5 else (1.0 if f[b_key]==0 else (0.5 if f[b_key]==-0.5 else 0))) for f in s])
                    roi = (total - len(s)) / len(s)
                    if roi >= 0.18: # Slightly lower threshold for candidate generation
                        results.append({'name': f"AH:{ah} + {'+'.join([c[0] for c in combo])} ({b_name})", 'roi': roi, 'n': len(s)})

    results.sort(key=lambda x: (x['roi'], x['n']), reverse=True)
    final = []
    seen = set()
    for r in results:
        if r['roi'] >= 0.20 and r['n'] >= 20 and r['name'] not in seen:
            final.append(r)
            seen.add(r['name'])
        elif r['roi'] >= 0.22 and r['n'] >= 15 and r['name'] not in seen:
            final.append(r)
            seen.add(r['name'])
    
    print(f"Patterns found: {len(final)}")
    out_p = 'backtest_results/specialist_mined_patterns.json'
    with open(out_p, 'w', encoding='utf-8') as fh:
        json.dump(final[:100], fh, indent=2)

if __name__ == "__main__":
    main()
