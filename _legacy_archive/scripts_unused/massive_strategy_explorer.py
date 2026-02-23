"""
MASSIVE STRATEGY EXPLORER
Explora miles de combinaciones de factores complejos para encontrar estrategias ganadoras.

Factores a considerar:
- AH Trend: ¿El AH subió, bajó o se mantuvo respecto al último partido?
- Prev Danger: Diferencia de ataques peligrosos en últimos partidos.
- Prev Cover: ¿Cubrieron el AH en el último partido?
- H2H Col3: Comparativa indirecta (Mejora/Empeora/Iguala)
- Prev Matches: WDL del último partido en casa/fuera
- AH Zone: Favoritismo
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
from collections import defaultdict
from datetime import datetime

# =============================================================================
# 1. CORE LOGIC & FEATURES
# =============================================================================

def parse_score(score_str):
    if not score_str or score_str in ['-', '?:?', '??', '']:
        return None, None
    score_str = str(score_str).replace('-', ':')
    parts = score_str.split(':')
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except:
        return None, None

def get_winner(home_g, away_g, ah_line):
    # AH positivo (+1) = Local Fav (se resta) -> diff = (h-a) - ah
    # AH negativo (-1) = Away Fav (se suma) -> diff = (h-a) - ah = (h-a) + abs(ah)
    diff = (home_g - away_g) - ah_line
    
    if diff > 0.25: return 'HOME'
    elif diff > 0: return 'HOME_HALF'
    elif diff == 0: return 'PUSH'
    elif diff >= -0.25: return 'AWAY_HALF'
    else: return 'AWAY'

def categorize_ah(ah):
    if ah <= -1.5: return 'AWAY_BIG_FAV' # AH -1.75
    elif ah <= -0.5: return 'AWAY_FAV'   # AH -0.75
    elif ah < 0: return 'AWAY_SLIGHT'    # AH -0.25
    elif ah == 0: return 'PICK_EM'
    elif ah < 0.5: return 'HOME_SLIGHT'  # AH +0.25
    elif ah < 1.5: return 'HOME_FAV'     # AH +0.75
    else: return 'HOME_BIG_FAV'          # AH +1.75

def get_wdl(score_str):
    h, a = parse_score(score_str)
    if h is None: return None
    if h > a: return 'W'
    elif h < a: return 'L'
    else: return 'D'

def get_stats_diff(stats_rows, label_key='Ataques Peligrosos'):
    """Calcula diferencia de una estadística (Home - Away)."""
    if not stats_rows: return 0
    
    home_val, away_val = 0, 0
    for row in stats_rows:
        if label_key.lower() in row.get('label', '').lower():
            try:
                home_val = int(row.get('home', 0))
                away_val = int(row.get('away', 0))
            except: pass
            break
            
    return home_val - away_val

def categorize_danger(diff):
    if diff >= 30: return 'DOMINANCE'      # +30 AP más
    elif diff >= 15: return 'SUPERIOR'     # +15 AP
    elif diff >= 5: return 'BETTER'        # +5 AP
    elif diff > -5: return 'EQUAL'          # +/- 5
    elif diff > -15: return 'WORSE'        # -5 a -15
    elif diff > -30: return 'INFERIOR'     # -15 a -30
    else: return 'DOMINATED'               # -30 AP menos

def check_cover(home_g, away_g, ah_line, team_side):
    """
    Verifica si el equipo (team_side: 'HOME' o 'AWAY') cubrió la línea.
    Formula: diff = (home - away) - ah_line
    """
    try:
        # Payout desde perspectiva HOME
        diff = (home_g - away_g) - ah_line
        
        home_res = 'PUSH'
        if diff > 0.25: home_res = 'COVER'
        elif diff < -0.25: home_res = 'FAIL'
        
        if team_side == 'HOME':
            return home_res
        else:
            # Invertir para AWAY
            if home_res == 'COVER': return 'FAIL'
            elif home_res == 'FAIL': return 'COVER'
            else: return 'PUSH'
    except:
        return None

def extract_complex_features(match):
    """Extrae features avanzadas para patrones complejos."""
    f = {}
    
    # Basics
    score = match.get('final_score')
    h, a = parse_score(score)
    if h is None: return None
    
    try:
        ah = float(match.get('main_match_odds', {}).get('ah_linea', 0))
    except:
        return None
        
    f['winner'] = get_winner(h, a, ah)
    f['is_home_win'] = f['winner'] in ['HOME', 'HOME_HALF']
    f['is_away_win'] = f['winner'] in ['AWAY', 'AWAY_HALF']
    
    # 1. AH Zone
    f['ah_zone'] = categorize_ah(ah)
    
    # 2. Prev Matches Analysis
    prev_home = match.get('last_home_match') or {}
    prev_away = match.get('last_away_match') or {}
    
    # 2.1 WDL
    f['prev_home_wdl'] = get_wdl(prev_home.get('score'))
    f['prev_away_wdl'] = get_wdl(prev_away.get('score'))
    
    # 2.2 Dangerous Attacks (Pressure)
    ph_stats = prev_home.get('stats_rows', [])
    ph_danger = get_stats_diff(ph_stats)
    
    actual_home = match.get('home_name')
    if prev_home.get('away_team') == actual_home:
        ph_danger = -ph_danger # Invertir si jugó de visitante
        
    f['prev_home_danger'] = categorize_danger(ph_danger)
    
    # Para prev_away
    pa_stats = prev_away.get('stats_rows', [])
    pa_danger = get_stats_diff(pa_stats)
    
    actual_away = match.get('away_name')
    if prev_away.get('away_team') == actual_away:
        pa_danger = -pa_danger
    elif prev_away.get('home_team') == actual_away:
        pass
        
    f['prev_away_danger'] = categorize_danger(pa_danger)

    # 2.3 AH Cover History
    # Prev Home Cover
    try:
        ph_h, ph_a = parse_score(prev_home.get('score'))
        ph_ah = float(prev_home.get('handicap_line_raw', 0))
        if prev_home.get('home_team') == actual_home: # Jugó Local
            f['prev_home_cover'] = check_cover(ph_h, ph_a, ph_ah, 'HOME')
        else: # Jugó Visitante
            f['prev_home_cover'] = check_cover(ph_h, ph_a, ph_ah, 'AWAY')
    except:
        f['prev_home_cover'] = None
        
    # Prev Away Cover
    try:
        pa_h, pa_a = parse_score(prev_away.get('score'))
        pa_ah = float(prev_away.get('handicap_line_raw', 0))
        if prev_away.get('home_team') == actual_away: # Jugó Local
             f['prev_away_cover'] = check_cover(pa_h, pa_a, pa_ah, 'HOME')
        else: # Jugó Visitante
             f['prev_away_cover'] = check_cover(pa_h, pa_a, pa_ah, 'AWAY')
    except:
        f['prev_away_cover'] = None

    # 3. AH Trend (vs Last Match)
    try:
        last_ah_home = float(prev_home.get('handicap_line_raw', 0)) # Use raw line
        # Simplificación: Comparar magnitud absoluta del favoritismo hacia ELLOS.
        # Esto es muy complejo de normalizar rápido. Usaremos valor raw asumiendo consistencia localia.
        if ah > last_ah_home: f['ah_trend'] = 'UP'
        elif ah < last_ah_home: f['ah_trend'] = 'DOWN'
        else: f['ah_trend'] = 'SAME'
    except:
        f['ah_trend'] = None
        
    # 4. H2H Col3 (Directa/Inversa/Mejora)
    h2h_col3 = match.get('h2h_col3', {})
    res1 = h2h_col3.get('res1') # Score pasado
    if res1:
        h3, a3 = parse_score(res1)
        if h3 is not None:
            # Si aplicamos the handicap ACTUAL al resultado PASADO, ¿cubre?
            sim_winner = get_winner(h3, a3, ah)
            if 'HOME' in sim_winner: f['h2h_col3_perf'] = 'COVERS'
            elif 'AWAY' in sim_winner: f['h2h_col3_perf'] = 'FAILS'
            else: f['h2h_col3_perf'] = 'PUSH'
        else:
            f['h2h_col3_perf'] = None
    else:
        f['h2h_col3_perf'] = None

    # 5. Market Movement (Dentro del partido: Apertura -> Cierre)
    # Se ve en la imagen como "0.25 -> 0.5". 
    # Si sube (0.25 -> 0.5): El mercado confía más en el equipo de referencia (usualmente Home en API raw, pero cuidado).
    # Asumimos que movement es sobre el handicap listado.
    try:
        mkt = match.get('market_analysis_data', {})
        stadium = mkt.get('stadium', {})
        mov = stadium.get('movement', '') # "0.25 -> 0.5"
        
        if '→' in mov or '->' in mov:
            p = mov.replace('→', '->').split('->')
            open_ah = float(p[0].strip())
            close_ah = float(p[1].strip())
            
            if close_ah > open_ah: f['mkt_move'] = 'RISES' # Sube handicap (Ej: 0.25 a 0.5, se vuelve mas underdog el local?? Ojo con signo)
            # Si AH es positivo (Home Underdog): 0.25 -> 0.5 significa que se vuelve MAS Underdog (Mercado va con Away).
            # Si AH es negativo (Home Fav): -0.25 -> -0.5 significa que se vuelve MAS Favorito (Mercado va Con Home).
            # Es complejo normalizar "Confianza en Home".
            # Simplificación: RISES (Valor numérico sube) / DROPS (Valor numérico baja).
            elif close_ah < open_ah: f['mkt_move'] = 'DROPS'
            else: f['mkt_move'] = 'STABLE'
        else:
            f['mkt_move'] = 'STABLE'
    except:
        f['mkt_move'] = None

    # 6. Role Change (Dynamic Logic per User Request)
    # "Viene de ganar como favorito (-0.5) y ahora es underdog (+0.5)" -> FLIP_TO_UNDERDOG
    try:
        current_ah = ah
        prev_ah = float(prev_home.get('handicap_line_raw', 0) if match.get('home_name') == prev_home.get('home_team') else prev_home.get('handicap_line_raw', 0))
        # Nota: handicap_line_raw siempre es respecto al home team de ESE partido.
        
        # Determinar si ERA favorito o underdog en su partido anterior
        # Si jugó de HOME: AH -0.5 -> ERA FAVORITO.
        # Si jugó de AWAY: AH +0.5 -> ERA FAVORITO (Recibió handicap? No, away +0.5 es home -0.5. Wait.)
        # Convención raw: AH siempre es Home Handicap.
        # Si era home: raw < 0 => Era fav.
        # Si era away: raw > 0 => Home era Underdog => Away era Fav.
        
        was_home = (prev_home.get('home_team') == actual_home)
        prev_raw = float(prev_home.get('handicap_line_raw', 0))
        
        idx_prev_fav = False # Era favorito?
        if was_home:
            if prev_raw < 0: idx_prev_fav = True
        else:
            if prev_raw > 0: idx_prev_fav = True
            
        # Determinar si ES favorito ahora (Siempre es HOME en este dataset)
        is_now_fav = (current_ah < 0)
        
        if idx_prev_fav and not is_now_fav: f['role_change'] = 'FLIP_TO_UNDERDOG' # Era Fav -> Ahora Underdog
        elif not idx_prev_fav and is_now_fav: f['role_change'] = 'FLIP_TO_FAVORITE' # Era Underdog -> Ahora Fav
        elif idx_prev_fav and is_now_fav: f['role_change'] = 'SAME_FAV'
        else: f['role_change'] = 'SAME_UNDER'

    except:
        f['role_change'] = None

    # 7. Previous Margin (Contexto del 2-0 vs 4-5)
    try:
        ph_h, ph_a = parse_score(prev_home.get('score'))
        if ph_h is not None:
            diff_score = ph_h - ph_a
            # Ajustar para el equipo en cuestion
            if not was_home: diff_score = -diff_score
            
            if diff_score >= 2: f['prev_margin'] = 'BIG_WIN'
            elif diff_score == 1: f['prev_margin'] = 'NARROW_WIN'
            elif diff_score == 0: f['prev_margin'] = 'DRAW'
            elif diff_score == -1: f['prev_margin'] = 'NARROW_LOSS'
            else: f['prev_margin'] = 'BIG_LOSS'
        else:
            f['prev_margin'] = None
    except:
        f['prev_margin'] = None
        
    return f

# =============================================================================
# 2. EXPLORER ENGINE
# =============================================================================

def generate_combinations(features_list):
    """Genera todas las combinaciones posibles de condiciones (Más exhaustivo)."""
    
    # Definir dominios
    domains = {
        'ah_zone': ['HOME_BIG_FAV', 'HOME_FAV', 'HOME_SLIGHT', 'PICK_EM', 'AWAY_SLIGHT', 'AWAY_FAV', 'AWAY_BIG_FAV'],
        
        'ah_trend': ['UP', 'DOWN', 'SAME'],
        'mkt_move': ['RISES', 'DROPS', 'STABLE'],
        'role_change': ['FLIP_TO_UNDERDOG', 'FLIP_TO_FAVORITE', 'SAME_FAV', 'SAME_UNDER'], # Nueva estrella
        'prev_margin': ['BIG_WIN', 'NARROW_WIN', 'DRAW', 'NARROW_LOSS', 'BIG_LOSS'],
        
        'h2h_col3_perf': ['COVERS', 'FAILS', 'PUSH'],
        
        'prev_home_danger': ['DOMINANCE', 'SUPERIOR', 'BETTER', 'EQUAL', 'WORSE', 'INFERIOR', 'DOMINATED'],
        'prev_away_danger': ['DOMINANCE', 'SUPERIOR', 'BETTER', 'EQUAL', 'WORSE', 'INFERIOR', 'DOMINATED'],
        'prev_home_cover': ['COVER', 'FAIL'],
        'prev_away_cover': ['COVER', 'FAIL']
    }
    
    strategies = []
    keys = list(domains.keys())
    
    # Nivel 1: Single conditions
    for k in keys:
        for v in domains[k]:
            strategies.append([(k, v)])
            
    # Nivel 2: Pares Selectivos (Base + Dynamic)
    target_keys = ['ah_zone', 'role_change']
    secondary_keys = ['prev_home_danger', 'prev_home_cover', 'prev_margin', 'mkt_move']
    
    for k1 in target_keys:
        for k2 in secondary_keys:
            for v1 in domains[k1]:
                for v2 in domains[k2]:
                    strategies.append([(k1, v1), (k2, v2)])
    
    # Pares entre role y margin (La logica del usuario: Era fav, gano bien -> ahora underdog)
    for r in domains['role_change']:
        for m in domains['prev_margin']:
            strategies.append([('role_change', r), ('prev_margin', m)])

    # Nivel 3: Trios Potentes (User Scenario)
    # Role + Cover + Margin (Dynamic Profile)
    for r in domains['role_change']:
        for c in domains['prev_home_cover']:
            for m in domains['prev_margin']:
                strategies.append([('role_change', r), ('prev_home_cover', c), ('prev_margin', m)])

    # Role + Danger + Zone
    for r in domains['role_change']:
        for d in domains['prev_home_danger']:
            for z in domains['ah_zone']:
                strategies.append([('role_change', r), ('prev_home_danger', d), ('ah_zone', z)])

    print(f"DEBUG: Generadas {len(strategies)} estrategias potenciales.")
    return strategies


def evaluate_strategies(matches_features, strategies):
    """Prueba todas las estrategias y guarda las ganadoras (ELITE: WR >= 65%)."""
    
    results = []
    
    # ROI ~15-20% con odds 1.85 requiere WR ~65%
    MIN_WIN_RATE = 0.65
    MIN_SAMPLES = 15
    
    print(f"Evaluando {len(strategies)} estrategias (WR>={MIN_WIN_RATE*100:.0f}%, samples>={MIN_SAMPLES})...")
    
    for strat in strategies:
        matches = matches_features
        valid = True
        for feat, val in strat:
            matches = [m for m in matches if m.get(feat) == val]
            if len(matches) < MIN_SAMPLES: 
                valid = False
                break
        
        if not valid: continue
            
        home_wins = sum(1 for m in matches if m['is_home_win'])
        away_wins = sum(1 for m in matches if m['is_away_win'])
        total = len(matches)
        
        home_wr = home_wins / total
        away_wr = away_wins / total
        
        if home_wr >= MIN_WIN_RATE:
            results.append({
                'conditions': strat,
                'bet': 'HOME',
                'samples': total,
                'wr': home_wr
            })
        elif away_wr >= MIN_WIN_RATE:
            results.append({
                'conditions': strat,
                'bet': 'AWAY',
                'samples': total,
                'wr': away_wr
            })
            
    return sorted(results, key=lambda x: x['wr'], reverse=True)


def load_data():
    base = Path(__file__).parent.parent
    files = list((base / 'data').glob('data_*.json'))
    all_data = []
    for f in files:
        try:
            all_data.extend(json.load(open(f, encoding='utf-8')))
        except: pass
    return all_data

def main():
    print("="*60)
    print("MASSIVE STRATEGY EXPLORER (EXTREMELY ADVANCED)")
    print("="*60)
    
    # 1. Cargar datos
    data = load_data()
    print(f"Datos cargados: {len(data)} partidos")
    
    # 2. Extraer features
    features = [extract_complex_features(m) for m in data]
    features = [f for f in features if f is not None]
    print(f"Features extraídas: {len(features)}")
    
    # 3. Generar estrategias
    strategies = generate_combinations(features)
    print(f"Estrategias generadas: {len(strategies)}")
    
    # 4. Evaluar
    top_strats = evaluate_strategies(features, strategies)
    
    print(f"\nEstrategias Ganadoras Encontradas: {len(top_strats)}")
    print("-" * 60)
    
    unique_rules = []
    seen = set()
    
    for s in top_strats:
        name = "STRAT_" + "_".join([f"{k}={v}" for k,v in s['conditions']])
        if name in seen: continue
        seen.add(name)
        
        cond_str = " + ".join([f"{k}={v}" for k,v in s['conditions']])
        print(f"[WR {s['wr']*100:.1f}%] {s['bet']} ({s['samples']} gw) | {cond_str}")
        
        unique_rules.append({
            'name': name,
            'conditions': [{'feature': k, 'op': '==', 'value': v} for k,v in s['conditions']],
            'bet_side': s['bet'],
            'metrics': {'wr': s['wr'], 'samples': s['samples']}
        })
        
        if len(unique_rules) >= 100: break
        
    # Guardar
    out = Path(__file__).parent.parent / 'models' / 'top_rules.json'
    json.dump({'rules': unique_rules}, open(out, 'w', encoding='utf-8'), indent=2)
    print(f"\nGuardadas {len(unique_rules)} reglas en {out}")

if __name__ == '__main__':
    main()
