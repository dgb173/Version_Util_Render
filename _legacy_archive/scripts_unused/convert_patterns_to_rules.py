
import json
from pathlib import Path

MINED_PATTERNS_FILE = Path('backtest_results/specialist_mined_patterns.json')
OUTPUT_RULE_FILE = Path('backtest_results/advanced_rules_col3.json')

def convert():
    if not MINED_PATTERNS_FILE.exists():
        print("No se encontró el archivo de patrones minados.")
        return

    with open(MINED_PATTERNS_FILE, 'r', encoding='utf-8') as f:
        mined = json.load(f)

    rules = []
    for i, p in enumerate(mined):
        # ROI y N para identificar calidad
        roi_val = p.get('roi') or p.get('roi_percent', 0) / 100.0
        n_val = p.get('n') or p.get('samples', 0)
        
        # Extraer target_line del nombre si existe
        target_line = 0.0
        if 'AH:' in p['name']:
            try:
                line_part = p['name'].split(' ')[0].split(':')[1]
                if line_part != 'ALL':
                    target_line = float(line_part)
            except: pass

        # Parse conditions from name
        # f"AH:{ah} + {'+'.join([c[0] for c in combo])} ({b_name})"
        raw_conds = p['name'].split(' + ')
        clean_conds = []
        for rc in raw_conds:
            if rc.startswith('AH:'): continue
            # Split by '+' and handle parts
            parts = rc.split('+')
            for part in parts:
                part = part.strip()
                if part.startswith('('): continue
                # Handle space-separated parts inside (like Col3_Y (Und))
                subparts = part.split(' ')
                for sp in subparts:
                    if sp.startswith('('): continue
                    if not sp: continue
                    clean_conds.append(sp)
        
        mapping = {
            'DA_G>10': ['da_g', '>', 10], 'DA_G>0': ['da_g', '>', 0], 'DA_G<-5': ['da_g', '<', -5],
            'DA_G>15': ['da_g', '>', 15], 'H_DA>10': ['h_da_d', '>', 10], 'V_DA>10': ['v_da_d', '>', 10],
            'SOT_H>0.3': ['h_sot_r', '>', 0.3], 'SOT_V>0.3': ['v_sot_r', '>', 0.3],
            'SOT_H<0.2': ['h_sot_r', '<', 0.2], 'SOT_V<0.2': ['v_sot_r', '<', 0.2],
            'Ind_Y': ['ind', '==', 1], 'Col3_Y': ['col3', '==', 1], 'H_DA_High': ['h_da_d', '>', 25]
        }
        
        parsed_conditions = [mapping[c] for c in clean_conds if c in mapping]
        
        # Mandatory: Rule must match its target AH line
        if target_line != 0.0 or "AH:0" in p['name']:
             parsed_conditions.append(['current_ah', '==', target_line])

        rule = {
            "id": f"MINED_RULE_AH_{i+1:03d}",
            "name": p['name'],
            "pick": "UNDERDOG" if "(Und)" in p['name'] or "BetUnd" in p['name'] else "FAVORITE",
            "accuracy": round(roi_val * 100, 1),
            "hits": n_val,
            "total": n_val,
            "roi": round(roi_val * 100, 2),
            "type": "AH",
            "algorithm": "ADVANCED_MINED",
            "target_line": target_line,
            "is_new": True,
            "conditions": parsed_conditions
        }
        rules.append(rule)

    with open(OUTPUT_RULE_FILE, 'w', encoding='utf-8') as f:
        json.dump(rules, f, indent=2, ensure_ascii=False)
    
    print(f"Convertidas {len(rules)} reglas AH con tag is_new.")

if __name__ == "__main__": convert()
