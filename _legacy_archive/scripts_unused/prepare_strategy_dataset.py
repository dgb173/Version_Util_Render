
import json
import csv
import os
import re
from pathlib import Path

# Configuración de Rutas
DATA_DIR = Path('data')
OUTPUT_CSV = DATA_DIR / 'strategy_dataset.csv'

def parse_score(score_str):
    if not score_str: return None, None
    s = str(score_str).replace(':', '-').replace(' ', '')
    if '-' not in s: return None, None
    try:
        parts = s.split('-')
        return int(parts[0]), int(parts[1])
    except:
        return None, None

def asian_result(team_goals, opp_goals, ah_line):
    diff = team_goals - opp_goals
    line = float(ah_line)
    
    lines = []
    if abs(line % 0.5) == 0.25:
        if line > 0: lines = [line - 0.25, line + 0.25]
        else: lines = [line + 0.25, line - 0.25]
    else:
        lines = [line]
        
    results = []
    for l in lines:
        val = diff + l
        if val > 0: results.append(1)
        elif val < 0: results.append(-1)
        else: results.append(0)
        
    avg_res = sum(results) / len(results)
    
    category = 'PUSH'
    if avg_res > 0: category = 'COVER'
    elif avg_res < 0: category = 'NO_COVER'
    
    return category

def extract_da_diff(stats_rows):
    if not stats_rows: return 0
    for r in stats_rows:
        if r.get('label') == 'Ataques Peligrosos':
            try:
                h = int(r.get('home', 0))
                a = int(r.get('away', 0))
                return h - a
            except: pass
    return 0

def parse_movement_val(mov_str):
    if not mov_str or '→' not in mov_str and '->' not in mov_str: return None, None
    try:
        parts = re.split(r'→|->', mov_str)
        return float(parts[0].strip()), float(parts[1].strip())
    except:
        return None, None

def process_file(file_path):
    print(f"Procesando {file_path.name}...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except:
        print(f"Error cargando {file_path}")
        return []

    rows = []
    for m in data:
        # Requeridos para el target
        score = m.get('final_score')
        hg, ag = parse_score(score)
        if hg is None: continue
        
        odds = m.get('main_match_odds', {})
        ah_raw = odds.get('ah_linea') or m.get('handicap')
        if ah_raw is None: continue
        try:
            ah = float(ah_raw)
        except: continue
        
        # Target variable
        target_cover = asian_result(hg, ag, ah)
        
        # Features
        row = {
            'match_id': m.get('match_id'),
            'home': m.get('home_name'),
            'away': m.get('away_name'),
            'current_ah': ah,
            'current_ou': odds.get('goals_linea'),
            'target_cover': target_cover,
            'hg': hg,
            'ag': ag
        }
        
        # Prev Home Match
        ph = m.get('last_home_match') or {}
        ph_score = ph.get('score')
        ph_ah = ph.get('handicap_line_raw')
        row['ph_ah'] = ph_ah
        row['ph_da_diff'] = extract_da_diff(ph.get('stats_rows'))
        
        # Prev Away Match
        pa = m.get('last_away_match') or {}
        pa_score = pa.get('score')
        pa_ah = pa.get('handicap_line_raw')
        row['pa_ah'] = pa_ah
        row['pa_da_diff'] = extract_da_diff(pa.get('stats_rows'))
        
        # H2H - Inferred from market_analysis_data if available
        ma = m.get('market_analysis_data') or {}
        stadium = ma.get('stadium') or {}
        general = ma.get('general') or {}
        row['h2h_stadium_res'] = stadium.get('evaluation', 'N/A')
        row['h2h_stadium_mov'] = stadium.get('movement', 'N/A')
        row['h2h_general_res'] = general.get('evaluation', 'N/A')
        row['h2h_general_mov'] = general.get('movement', 'N/A')
        
        # Derivadas (Deltas)
        try:
            if ph_ah:
                # Si hégemonía es local (ah > 0)
                if ah > 0:
                    row['ah_delta_ph'] = ah - float(ph_ah)
                else:
                    row['ah_delta_ph'] = 0
            else:
                row['ah_delta_ph'] = 0
        except: row['ah_delta_ph'] = 0
            
        rows.append(row)
    return rows

def main():
    all_data = []
    for f in DATA_DIR.glob('data_ah_*.json'):
        all_data.extend(process_file(f))
    
    # También incluir data_precacheo.json si tiene resultados finales
    f_pre = DATA_DIR / 'data_precacheo.json'
    if f_pre.exists():
        all_data.extend(process_file(f_pre))

    if not all_data:
        print("No se encontraron partidos válidos.")
        return

    # Escribir a CSV
    keys = all_data[0].keys()
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(all_data)
        
    print(f"¡Éxito! Dataset creado en {OUTPUT_CSV} con {len(all_data)} registros.")

if __name__ == "__main__":
    main()
