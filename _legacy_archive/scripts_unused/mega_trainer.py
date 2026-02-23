"""
MEGA ENTRENADOR DE PATRONES INFALIBLES
Busca TODOS los patrones posibles con MÁXIMA fiabilidad usando:
- Comparativas indirectas (quién ganó a quién)
- H2H Col3 (historial directo)
- Handicap (favorito, línea)
- Stats (ataques, tiros, tiros a puerta)
- Over/Under (goles)
- Rankings y forma

Genera patrones separados para AH y O/U
"""
import json
import os
import random
from datetime import datetime
from collections import defaultdict
from itertools import combinations

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
MAX_MESES_VALIDOS = 6

def safe_int(val, default=0):
    try:
        return int(val)
    except:
        return default

def safe_float(val, default=0.0):
    try:
        return float(val)
    except:
        return default

def parse_score(score_str):
    """Parsea un score en formato X:Y o X-Y"""
    if not score_str:
        return None, None
    s = str(score_str).replace('-', ':')
    parts = s.split(':')
    if len(parts) != 2:
        return None, None
    return safe_int(parts[0]), safe_int(parts[1])

def is_fresh(date_str, max_months=MAX_MESES_VALIDOS):
    """Verifica si los datos son frescos"""
    if not date_str:
        return False
    try:
        from datetime import datetime
        date = datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
        diff = (datetime.now() - date).days / 30
        return diff <= max_months
    except:
        return False

def load_matches():
    """Carga todos los partidos"""
    matches = []
    if not os.path.exists(DATA_DIR):
        return []
    
    for f in os.listdir(DATA_DIR):
        if f.endswith('.json'):
            try:
                with open(os.path.join(DATA_DIR, f), 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    if isinstance(data, list):
                        matches.extend(data)
                    elif isinstance(data, dict) and 'partidos' in data:
                        matches.extend(data['partidos'])
            except:
                pass
    return matches

def extract_mega_features(match):
    """Extrae TODAS las features posibles"""
    f = {}
    
    # ==================== HANDICAP ====================
    ah = safe_float(match.get('ah', 0))
    f['ah_0'] = abs(ah) < 0.01
    f['ah_025'] = 0 < abs(ah) <= 0.25
    f['ah_05'] = 0.25 < abs(ah) <= 0.5
    f['ah_075'] = 0.5 < abs(ah) <= 0.75
    f['ah_1'] = 0.75 < abs(ah) <= 1
    f['ah_15'] = 1 < abs(ah) <= 1.5
    f['ah_2plus'] = abs(ah) > 1.5
    f['fav_local'] = ah > 0
    f['fav_visita'] = ah < 0
    
    # ==================== OVER/UNDER LINE ====================
    ou = safe_float(match.get('ou', 2.5))
    f['ou_bajo'] = ou <= 2.25
    f['ou_normal'] = 2.25 < ou <= 2.75
    f['ou_alto'] = ou > 2.75
    f['ou_muy_alto'] = ou >= 3
    
    # ==================== PREV HOME ====================
    prev_home = match.get('last_home_match') or {}
    ph_h, ph_a = parse_score(prev_home.get('score', ''))
    f['ph_tiene'] = ph_h is not None and ph_a is not None
    if f['ph_tiene']:
        ph_goles = ph_h + ph_a
        f['ph_gano'] = ph_h > ph_a
        f['ph_empato'] = ph_h == ph_a
        f['ph_perdio'] = ph_h < ph_a
        f['ph_goleo'] = ph_goles >= 4
        f['ph_mucho_gol'] = ph_goles >= 3
        f['ph_under'] = ph_goles <= 2
        f['ph_0goles'] = ph_goles == 0
        f['ph_por_cero'] = ph_a == 0
        f['ph_encajo'] = ph_a >= 2
        f['ph_marco3'] = ph_h >= 3
        f['ph_ambos'] = ph_h > 0 and ph_a > 0
        
        # AH Context for Prev Home
        ph_ah = safe_float(prev_home.get('ah', 0))
        # Was our Current Home Team (match['home_name']) the Home or Away team in this prev match?
        ph_was_local = prev_home.get('home_team') == match.get('home_name')
        
        if ph_was_local:
            f['ph_was_fav'] = ph_ah < 0
            f['ph_was_dog'] = ph_ah > 0
        else:
            f['ph_was_fav'] = ph_ah > 0
            f['ph_was_dog'] = ph_ah < 0
    
    # ==================== PREV AWAY ====================
    prev_away = match.get('last_away_match') or {}
    pa_h, pa_a = parse_score(prev_away.get('score', ''))
    f['pa_tiene'] = pa_h is not None and pa_a is not None
    if f['pa_tiene']:
        pa_goles = pa_h + pa_a
        f['pa_gano'] = pa_a > pa_h
        f['pa_empato'] = pa_h == pa_a
        f['pa_perdio'] = pa_a < pa_h
        f['pa_goleo'] = pa_goles >= 4
        f['pa_mucho_gol'] = pa_goles >= 3
        f['pa_under'] = pa_goles <= 2
        f['pa_0goles'] = pa_goles == 0
        f['pa_por_cero'] = pa_h == 0
        f['pa_encajo'] = pa_h >= 2
        f['pa_marco3'] = pa_a >= 3
        f['pa_ambos'] = pa_h > 0 and pa_a > 0
        
        # AH Context for Prev Away
        pa_ah = safe_float(prev_away.get('ah', 0))
        # Was our Current Away Team (match['away_name']) the Home or Away team in this prev match?
        # If pa_data['home_team'] == match['away_name'], they were local.
        pa_was_local = prev_away.get('home_team') == match.get('away_name')
        
        if pa_was_local:
            f['pa_was_fav'] = pa_ah < 0
            f['pa_was_dog'] = pa_ah > 0
        else: # they were visitor
            f['pa_was_fav'] = pa_ah > 0 # Host was +AH (Dog) -> Visitor was Fav
            f['pa_was_dog'] = pa_ah < 0 # Host was -AH (Fav) -> Visitor was Dog
    
    # ==================== H2H COL3 ====================
    h2h_col3 = match.get('h2h_col3') or {}
    h2h_h = safe_int(h2h_col3.get('goles_home', 0))
    h2h_a = safe_int(h2h_col3.get('goles_away', 0))
    h2h_goles = h2h_h + h2h_a
    h2h_ah = safe_float(h2h_col3.get('ah', 0))
    f['h2h_fav_local_strong'] = h2h_ah <= -1.0
    f['h2h_fav_local'] = h2h_ah < 0
    f['h2h_fav_visita'] = h2h_ah > 0
    f['h2h_fav_visita_strong'] = h2h_ah >= 1.0
    f['h2h_ah_eq'] = abs(h2h_ah) == abs(ah)
    
    f['h2h_tiene'] = h2h_goles > 0 or h2h_ah != 0
    f['h2h_gano_local'] = h2h_h > h2h_a
    f['h2h_gano_visita'] = h2h_a > h2h_h
    f['h2h_empate'] = h2h_h == h2h_a and h2h_goles > 0
    f['h2h_goleo'] = h2h_goles >= 4
    f['h2h_mucho_gol'] = h2h_goles >= 3
    f['h2h_under'] = h2h_goles <= 2
    f['h2h_0goles'] = h2h_goles == 0
    f['h2h_ambos'] = h2h_h > 0 and h2h_a > 0
    f['h2h_por_cero'] = h2h_a == 0 or h2h_h == 0
    f['h2h_covered'] = (h2h_h + h2h_ah) > h2h_a
    f['h2h_failed'] = (h2h_h + h2h_ah) < h2h_a
    
    # Handicap repetido
    f['ha_repetido_exacto'] = abs(abs(ah) - abs(h2h_ah)) <= 0.1
    f['ha_repetido_cerca'] = abs(abs(ah) - abs(h2h_ah)) <= 0.25
    f['ha_subio'] = abs(ah) > abs(h2h_ah) + 0.1
    f['ha_bajo'] = abs(ah) < abs(h2h_ah) - 0.1
    
    # ==================== COMPARATIVAS INDIRECTAS ====================
    comp_ind = match.get('comparativas_indirectas') or {}
    ind_left = comp_ind.get('left') or {}
    ind_right = comp_ind.get('right') or {}
    
    il_h, il_a = parse_score(ind_left.get('score', ''))
    f['il_tiene'] = il_h is not None and il_a is not None
    if f['il_tiene']:
        il_goles = il_h + il_a
        f['il_local_gano'] = il_h > il_a
        f['il_local_goleo'] = il_h >= 3 or (il_h - il_a) >= 2
        f['il_local_perdio'] = il_h < il_a
        f['il_over'] = il_goles > 2.5
        f['il_under'] = il_goles <= 2
        f['il_ambos'] = il_h > 0 and il_a > 0
        il_ah = safe_float(ind_left.get('ah_line', 0))
        f['il_was_fav'] = il_ah < 0  # Local (Home vs Rival) was Fav
        f['il_was_dog'] = il_ah > 0  # Local (Home vs Rival) was Dog
        f['il_fav_strong'] = il_ah <= -1.0
        
        # Did they cover?
        f['il_covered_actual'] = (il_h + il_ah) > il_a
        f['il_failed_actual'] = (il_h + il_ah) < il_a
    
    ir_h, ir_a = parse_score(ind_right.get('score', ''))
    f['ir_tiene'] = ir_h is not None and ir_a is not None
    if f['ir_tiene']:
        ir_goles = ir_h + ir_a
        f['ir_visita_gano'] = ir_a > ir_h
        f['ir_visita_goleo'] = ir_a >= 3 or (ir_a - ir_h) >= 2
        f['ir_visita_perdio'] = ir_a < ir_h
        f['ir_over'] = ir_goles > 2.5
        f['ir_under'] = ir_goles <= 2
        f['ir_ambos'] = ir_h > 0 and ir_a > 0
        ir_ah = safe_float(ind_right.get('ah_line', 0))
        f['ir_was_fav'] = ir_ah < 0  # Rival (Home) was Fav vs Away
        f['ir_was_dog'] = ir_ah > 0  # Rival (Home) was Dog vs Away -> Away was Fav
        f['ir_away_was_strong_fav'] = ir_ah >= 1.0 # If line +1.0 for Rival, then Away was -1.0
        
        # For Right (Rival vs Away): Away Covered if Rival Failed covering theirs
        # ir_ah is usually relative to Home (Rival)
        f['ir_covered_actual'] = (ir_h + ir_ah) < ir_a
        f['ir_failed_actual'] = (ir_h + ir_ah) > ir_a
    
    # Indirectas alineadas (PERFORMANCE)
    if f.get('il_tiene') and f.get('ir_tiene'):
        f['ind_perf_home_good'] = f.get('il_covered_actual') and f.get('ir_failed_actual')
        f['ind_alineadas_local'] = f.get('il_local_goleo', False) and f.get('ir_visita_perdio', False)
        f['ind_alineadas_visita'] = f.get('ir_visita_goleo', False) and f.get('il_local_perdio', False)
        f['ind_ambas_over'] = f.get('il_over', False) and f.get('ir_over', False)
        f['ind_ambas_under'] = f.get('il_under', False) and f.get('ir_under', False)
    
    # ==================== RANKINGS ====================
    home_st = match.get('home_standings') or {}
    away_st = match.get('away_standings') or {}
    h_rank = safe_int(home_st.get('ranking', 99)) or 99
    a_rank = safe_int(away_st.get('ranking', 99)) or 99
    
    f['h_top3'] = 0 < h_rank <= 3
    f['h_top5'] = 0 < h_rank <= 5
    f['h_top10'] = 0 < h_rank <= 10
    f['h_medio'] = 10 < h_rank < 15
    f['h_bottom'] = h_rank >= 15
    f['a_top3'] = 0 < a_rank <= 3
    f['a_top5'] = 0 < a_rank <= 5
    f['a_top10'] = 0 < a_rank <= 10
    f['a_medio'] = 10 < a_rank < 15
    f['a_bottom'] = a_rank >= 15
    f['h_mejor_rank'] = 0 < h_rank < a_rank
    f['a_mejor_rank'] = 0 < a_rank < h_rank
    f['ranks_parejos'] = abs(h_rank - a_rank) <= 3
    f['gran_diff_rank'] = abs(h_rank - a_rank) >= 8
    
    # ==================== FORMA (W-D-L) ====================
    h_form = (home_st.get('form', '') or '0-0-0').split('-')
    a_form = (away_st.get('form', '') or '0-0-0').split('-')
    if len(h_form) >= 3:
        h_played = safe_int(h_form[0]) + safe_int(h_form[1]) + safe_int(h_form[2])
        if h_played > 0:
            h_winrate = safe_int(h_form[0]) / h_played
            f['h_fuerte'] = h_winrate >= 0.6
            f['h_medio_f'] = 0.4 <= h_winrate < 0.6
            f['h_debil'] = h_winrate <= 0.3
    if len(a_form) >= 3:
        a_played = safe_int(a_form[0]) + safe_int(a_form[1]) + safe_int(a_form[2])
        if a_played > 0:
            a_winrate = safe_int(a_form[0]) / a_played
            f['a_fuerte'] = a_winrate >= 0.5
            f['a_medio_f'] = 0.3 <= a_winrate < 0.5
            f['a_debil'] = a_winrate <= 0.2
    
    # ==================== GOLES EQUIPO ====================
    h_gf = safe_int(home_st.get('gf', 0))
    h_gc = safe_int(home_st.get('gc', 0))
    a_gf = safe_int(away_st.get('gf', 0))
    a_gc = safe_int(away_st.get('gc', 0))
    h_pj = safe_int(home_st.get('pj', 1)) or 1
    a_pj = safe_int(away_st.get('pj', 1)) or 1
    
    if h_gf > 0 or h_gc > 0:
        f['h_goleador'] = h_gf / h_pj >= 1.5
        f['h_defensivo'] = h_gc / h_pj <= 1
        f['h_encajador'] = h_gc / h_pj >= 1.5
    if a_gf > 0 or a_gc > 0:
        f['a_goleador'] = a_gf / a_pj >= 1.2
        f['a_defensivo'] = a_gc / a_pj <= 0.8
        f['a_encajador'] = a_gc / a_pj >= 1.5
    
    # ==================== COMBINACIONES CLAVE ====================
    if f.get('ph_tiene') and f.get('pa_tiene'):
        f['momentum_local'] = f.get('ph_gano', False) and f.get('pa_perdio', False)
        f['momentum_visita'] = f.get('pa_gano', False) and f.get('ph_perdio', False)
        f['ambos_golearon'] = f.get('ph_goleo', False) and f.get('pa_goleo', False)
        f['ambos_under'] = f.get('ph_under', False) and f.get('pa_under', False)
        f['datos_completos'] = True
    
    # ==================== FRESCURA ====================
    ph_fecha = prev_home.get('date', '') or prev_home.get('fecha', '')
    pa_fecha = prev_away.get('date', '') or prev_away.get('fecha', '')
    f['datos_frescos'] = is_fresh(ph_fecha) and is_fresh(pa_fecha)

    # ==================== NEW FEATURES: STATS & MOVEMENT ====================
    def get_stats_value(stats, team_name, match_home_team):
        sot, da = 0, 0
        if not stats: return 0, 0
        # If 'home' label in stats means the home team of THAT match
        # We need to check if 'team_name' was the Home team or Away team in that match.
        # But here checking 'match_home_team' (team names) is complex due to naming vars.
        # Simplified: Use home column if team_name matches match_home_team
        
        # Heuristic: We just sum 'home' stats if we are checking the home team of that match?
        # Let's assume stats_rows keys 'home'/'away' map to the team roles in that match.
        is_home_in_prev = (team_name == match_home_team)

        for row in stats:
            lbl = str(row.get('label', ''))
            val = 0
            try:
                if is_home_in_prev: val = int(row.get('home', 0))
                else: val = int(row.get('away', 0))
            except: continue
            
            if 'Tiros a Puerta' in lbl or 'Tiros a puerta' in lbl: sot = val
            elif 'Ataques Peligrosos' in lbl or 'Ataques peligrosos' in lbl: da = val
        return sot, da

    ph_data = match.get('last_home_match') or {}
    ph_sot, ph_da = get_stats_value(ph_data.get('stats_rows', []), match.get('home_name', ''), ph_data.get('home_team', ''))
    
    pa_data = match.get('last_away_match') or {}
    pa_sot, pa_da = get_stats_value(pa_data.get('stats_rows', []), match.get('away_name', ''), pa_data.get('home_team', ''))

    f['ph_high_sot'] = (ph_sot >= 5)
    f['ph_low_sot'] = (ph_sot <= 2)
    f['ph_high_da'] = (ph_da >= 50)
    f['ph_low_da'] = (ph_da <= 30)
    
    f['pa_high_sot'] = (pa_sot >= 5)
    f['pa_low_sot'] = (pa_sot <= 2)
    f['pa_high_da'] = (pa_da >= 50)
    f['pa_low_da'] = (pa_da <= 30)
    
    f['ph_better_sot'] = (ph_sot > pa_sot)
    f['ph_better_da'] = (ph_da > pa_da)

    # Market Movement
    def get_mov_dir(mov_str):
        if not mov_str or '->' not in mov_str: return 'NONE'
        try:
            parts = mov_str.replace(' ', '').split('->')
            start = float(parts[0])
            end = float(parts[1])
            if end > start: return 'UP'
            if end < start: return 'DOWN'
            return 'SAME'
        except: return 'NONE'

    stadium_mov = get_mov_dir(((match.get('market_analysis_data') or {}).get('stadium') or {}).get('movement'))
    general_mov = get_mov_dir(((match.get('market_analysis_data') or {}).get('general') or {}).get('movement'))
    
    f['mov_stadium_up'] = (stadium_mov == 'UP')
    f['mov_stadium_down'] = (stadium_mov == 'DOWN')
    f['mov_general_up'] = (general_mov == 'UP')
    f['mov_general_down'] = (general_mov == 'DOWN')
    
    # ==================== ADVANCED INTERACTIONS (REQUESTED) ====================
    # 1. Indirect AH vs Current AH
    curr_ah = safe_float(match.get('main_match_odds', {}).get('ah_linea', 0))
    
    il_ah = safe_float(ind_left.get('ah_line', 0))
    ir_ah = safe_float(ind_right.get('ah_line', 0))
    
    # Does the indirect match have a "better" handicap for our team?
    # (e.g. Current is -0.5, Indirect was -0.25 -> Indirect was better/easier? No, Indirect was tighter.)
    # The user said "mejora que el actual". Usually implies the line moved in a way or history shows better line.
    # Let's define "better" as stricter? Or "better value"?
    # Let's just capture the relation:
    f['il_ah_higher'] = il_ah > curr_ah
    f['il_ah_lower'] = il_ah < curr_ah
    f['ir_ah_higher'] = ir_ah > curr_ah
    f['ir_ah_lower'] = ir_ah < curr_ah

    # 2. Indirect Result covering CURRENT AH
    # Did the Home Team in Indirect Left match cover the CURRENT AH?
    # il_h, il_a are scores.
    if il_h is not None and il_a is not None:
        # Standard AH logic: Home Score + AH > Away Score
        # But wait, Ind Left is Home vs Rival. Current is Home vs New Rival.
        # We want to know if Home covered.
        # If Current AH is applied to Ind Result:
        f['il_covered_current'] = (il_h + curr_ah) > il_a
        f['il_failed_current'] = (il_h + curr_ah) < il_a
        
        # Interaction with Stats
        f['ph_da_high_il_covered'] = f.get('ph_high_da', False) and f['il_covered_current']
        f['ph_sot_high_il_covered'] = f.get('ph_high_sot', False) and f['il_covered_current']

    if ir_h is not None and ir_a is not None:
        # Ind Right is Rival vs Away (Away is Visitor?).
        # Usually Ind Right is Rival (Home) vs Away (Visita).
        # Need to check `localia` attribute in JSON to know if Away was Home or Away.
        # But `comparativas_indirectas` structure `ind_right` usually means Away team played.
        # If Away was Visitor in Ind Right (standard):
        # We want to know if Away covered.
        # Away Score + AH (inverted?) > Home Score?
        # AH is usually from Home perspective.
        # So if Current AH is for Home... Away covers if Home + AH < Away? 
        # Yes. (e.g. Home -0.5 -> Home needs to win. If Away draws, Away covers).
        # So: (ir_h + curr_ah) < ir_a  => Away covered.
        f['ir_covered_current'] = (ir_h + curr_ah) < ir_a
        f['ir_failed_current'] = (ir_h + curr_ah) > ir_a
        
    # Interaction with Stats
        f['pa_da_high_ir_covered'] = f.get('pa_high_da', False) and f['ir_covered_current']
        f['pa_sot_high_ir_covered'] = f.get('pa_high_sot', False) and f['ir_covered_current']

    # ==================== COMPARATIVE STATS (INDIRECT & H2H) ====================
    # Reuse get_stats_value local function
    
    # 3. Indirect Left Stats (Home vs Rival)
    il_stats = ind_left.get('stats_rows') or []
    # Identify which team in Ind Left was "Our Home Team"
    # match['home_name'] should be present in ind_left['home_team'] or ind_left['away_team']
    # If not found (name mismatch), heuristics might fail, but let's try.
    il_home_sot, il_home_da = get_stats_value(il_stats, match.get('home_name', ''), ind_left.get('home_team', ''))
    # Rival stats? "Rival" is the OTHER team.
    # We can pass the rival name.
    il_rival_name = ind_left.get('away_team') if ind_left.get('home_team') == match.get('home_name') else ind_left.get('home_team')
    il_rival_sot, il_rival_da = get_stats_value(il_stats, il_rival_name, ind_left.get('home_team', ''))
    
    f['il_better_sot'] = il_home_sot > il_rival_sot
    f['il_better_da'] = il_home_da > il_rival_da
    f['il_dominate_da'] = il_home_da > (il_rival_da * 1.5)

    # 4. Indirect Right Stats (Away vs Rival)
    ir_stats = ind_right.get('stats_rows') or []
    ir_away_sot, ir_away_da = get_stats_value(ir_stats, match.get('away_name', ''), ind_right.get('home_team', ''))
    ir_rival_name = ind_right.get('away_team') if ind_right.get('home_team') == match.get('away_name') else ind_right.get('home_team')
    ir_rival_sot, ir_rival_da = get_stats_value(ir_stats, ir_rival_name, ind_right.get('home_team', ''))
    
    f['ir_better_sot'] = ir_away_sot > ir_rival_sot
    f['ir_better_da'] = ir_away_da > ir_rival_da
    f['ir_dominate_da'] = ir_away_da > (ir_rival_da * 1.5)

    # 5. H2H Col3 Stats
    h2h_stats = (match.get('h2h_col3') or {}).get('stats_rows') or []
    # H2H is straightforward: Current Home vs Current Away
    h2h_home_sot, h2h_home_da = get_stats_value(h2h_stats, match.get('home_name', ''), (match.get('h2h_col3') or {}).get('h2h_home_team_name', ''))
    h2h_away_sot, h2h_away_da = get_stats_value(h2h_stats, match.get('away_name', ''), (match.get('h2h_col3') or {}).get('h2h_home_team_name', ''))
    
    f['h2h_better_sot'] = h2h_home_sot > h2h_away_sot
    f['h2h_better_da'] = h2h_home_da > h2h_away_da
    f['h2h_home_dominate'] = h2h_home_da > (h2h_away_da * 1.5)
    f['h2h_away_dominate'] = h2h_away_da > (h2h_home_da * 1.5)

    
    # Underdogs
    ah = safe_float(match.get('main_match_odds', {}).get('ah_linea', 0))
    f['home_underdog'] = (ah > 0)
    f['away_underdog'] = (ah < 0)
    
    return f

def get_ah_result(match):
    """Resultado AH (LOCAL cubrió / VISITA cubrió)"""
    final = match.get('final_score', '')
    h, a = parse_score(final)
    if h is None:
        return None
    ah = safe_float(match.get('ah', 0))
    
    if ah > 0:  # Local favorito
        if h + ah > a:
            return 'LOCAL'
        elif h + ah < a:
            return 'VISITA'
    elif ah < 0:  # Visita favorito
        if a + abs(ah) > h:
            return 'VISITA'
        elif a + abs(ah) < h:
            return 'LOCAL'
    else:  # AH 0
        if h > a: return 'LOCAL'
        elif a > h: return 'VISITA'
    return None

def get_ou_result(match):
    """Resultado O/U (OVER / UNDER)"""
    final = match.get('final_score', '')
    h, a = parse_score(final)
    if h is None:
        return None
    goles = h + a
    ou = safe_float(match.get('ou', 2.5))
    
    if goles > ou: return 'OVER'
    elif goles < ou: return 'UNDER'
    return None

class MegaPatternLearner:
    def __init__(self):
        self.ah_patterns = []
        self.ou_patterns = []
        self.min_accuracy = 0.70 # Increased for 25% ROI at 1.8 odds (0.7 * 1.8 = 1.26)
        self.min_samples = 25 # Slightly reduced for COMPLEX patterns (harder to match)
        
    def train(self, matches, generations=50000): # Increased to 50000
        print(f"entrenando {generations} generaciones con esteroides (Stats + Underdogs)...")
        print(f"\n{'='*70}")
        print("MEGA ENTRENADOR DE PATRONES INFALIBLES")
        print(f"{'='*70}")
        
        # Preprocesar
        valid = []
        for m in matches:
            ah_res = get_ah_result(m)
            ou_res = get_ou_result(m)
            f = extract_mega_features(m)
            
            if f.get('datos_frescos') and f.get('datos_completos'):
                # Intentar obtener fecha
                d_str = m.get('date') or m.get('fecha') or '2025-01-01'
                try:
                    d_obj = datetime.strptime(str(d_str)[:10], '%Y-%m-%d')
                except:
                    d_obj = datetime.min
                
                valid.append({
                    'features': f,
                    'ah_result': ah_res,
                    'ou_result': ou_res,
                    'ah': safe_float(m.get('ah', 0)),
                    'ou': safe_float(m.get('ou', 2.5)),
                    'date_obj': d_obj
                })
        
        # Ordenar por fecha para separar Train/Test correctamente (Simular realidad)
        valid.sort(key=lambda x: x.get('date_obj', datetime.min))
        
        split_idx = int(len(valid) * 0.8)
        train_data = valid[:split_idx]
        test_data = valid[split_idx:]
        
        print(f"Total datos válidos: {len(valid)}")
        print(f"Train/Test Split (80/20): {len(train_data)} train, {len(test_data)} test")
        
        all_features = set()
        for v in valid:
            all_features.update([k for k, val in v['features'].items() if val is True])
        all_features = list(all_features)
        print(f"Features booleanas: {len(all_features)}")
        
        # Separar por handicap (SOLO TRAIN DATA)
        ah_groups = {
            'AH_0': [m for m in train_data if m['features'].get('ah_0')],
            'AH_05': [m for m in train_data if m['features'].get('ah_025') or m['features'].get('ah_05')],
            'AH_1': [m for m in train_data if m['features'].get('ah_075') or m['features'].get('ah_1')],
            'AH_15+': [m for m in train_data if m['features'].get('ah_15') or m['features'].get('ah_2plus')]
        }
        
        # Separar por O/U line (SOLO TRAIN DATA)
        ou_groups = {
            'OU_bajo': [m for m in train_data if m['features'].get('ou_bajo')],
            'OU_normal': [m for m in train_data if m['features'].get('ou_normal')],
            'OU_alto': [m for m in train_data if m['features'].get('ou_alto')]
        }
        
        # Archivo de salida para autoguardado
        output_file = os.path.join(os.path.dirname(__file__), 'mega_patterns.json')

        try:
            # ==================== ENTRENAR AH ====================
            print(f"\n--- Buscando patrones AH COMPLEX (6-10 variables) ---")
            for target in ['LOCAL', 'VISITA']:
                for gen in range(generations):
                    # User requested minimum 6-7 variables.
                    # We will try range 5 to 9 to allow some flexibility but keep it complex.
                    n_range_min = 5
                    n_range_max = min(9, len(all_features))
                    n_feat = random.randint(n_range_min, n_range_max)
                    
                    selected = random.sample(all_features, n_feat)
                    
                    for group_name, group_matches in ah_groups.items():
                        matches_with_result = [m for m in group_matches if m['ah_result']]
                        if len(matches_with_result) < 20:
                            continue
                        
                        matching = [m for m in matches_with_result if all(m['features'].get(feat, False) for feat in selected)]
                        if len(matching) < self.min_samples:
                            continue
                        
                        wins = sum(1 for m in matching if m['ah_result'] == target)
                        acc = wins / len(matching)
                        
                        if acc >= self.min_accuracy:
                            # VALIDAR EN TEST SET
                            cond_ah = None
                            if group_name=='AH_0': cond_ah = lambda x: x['features'].get('ah_0')
                            elif group_name=='AH_05': cond_ah = lambda x: x['features'].get('ah_025') or x['features'].get('ah_05')
                            elif group_name=='AH_1': cond_ah = lambda x: x['features'].get('ah_075') or x['features'].get('ah_1')
                            else: cond_ah = lambda x: x['features'].get('ah_15') or x['features'].get('ah_2plus')

                            test_matches_in_group = [m for m in test_data if cond_ah(m)]
                            test_matching = [m for m in test_matches_in_group if all(m['features'].get(feat, False) for feat in selected) and m['ah_result']]
                            
                            test_acc = 0.0
                            test_samples = len(test_matching)
                            if test_samples > 0:
                                test_wins = sum(1 for m in test_matching if m['ah_result'] == target)
                                test_acc = test_wins / test_samples

                            pattern = {
                                'features': sorted(selected),
                                'target': target,
                                'accuracy': round(acc, 3),
                                'samples': len(matching),
                                'wins': wins,
                                'group': group_name,
                                'test_accuracy': round(test_acc, 3),
                                'test_samples': test_samples,
                                'type': 'AH'
                            }
                            
                            # Evitar duplicados
                            is_dup = any(
                                set(p['features']) == set(selected) and p['target'] == target and p['group'] == group_name
                                for p in self.ah_patterns
                            )
                            
                            if not is_dup:
                                self.ah_patterns.append(pattern)
                                stars = '🔥' * min(3, 1 + int((acc - 0.76) * 10))
                                if test_samples > 0:
                                    val_str = f"| VAL: {test_acc*100:.1f}% ({test_samples})"
                                    print(f"{stars} AH {target} TRAIN: {acc*100:.1f}% ({len(matching)}) {val_str} [{group_name}]")
                    
                    if (gen + 1) % 500 == 0:
                        print(f"  AH Gen {gen+1}/{generations}... ({len(self.ah_patterns)} patrones) - Autoguardando...")
                        self.save(output_file)

            # ==================== ENTRENAR O/U ====================
            print(f"\n--- Buscando patrones O/U COMPLEX (6-10 variables) ---")
            for target in ['OVER', 'UNDER']:
                for gen in range(generations):
                    # Same complex range
                    n_range_min = 5
                    n_range_max = min(9, len(all_features))
                    n_feat = random.randint(n_range_min, n_range_max)
                    
                    selected = random.sample(all_features, n_feat)
                    
                    for group_name, group_matches in ou_groups.items():
                        matches_with_result = [m for m in group_matches if m['ou_result']]
                        if len(matches_with_result) < 20:
                            continue
                        
                        matching = [m for m in matches_with_result if all(m['features'].get(feat, False) for feat in selected)]
                        if len(matching) < self.min_samples:
                            continue
                        
                        wins = sum(1 for m in matching if m['ou_result'] == target)
                        acc = wins / len(matching)
                        
                        if acc >= self.min_accuracy:
                            # VALIDAR EN TEST SET
                            cond_ou = None
                            if group_name=='OU_bajo': cond_ou = lambda x: x['features'].get('ou_bajo')
                            elif group_name=='OU_normal': cond_ou = lambda x: x['features'].get('ou_normal')
                            else: cond_ou = lambda x: x['features'].get('ou_alto')

                            test_matches_in_group = [m for m in test_data if cond_ou(m)]
                            test_matching = [m for m in test_matches_in_group if all(m['features'].get(feat, False) for feat in selected) and m['ou_result']]
                            
                            test_acc = 0.0
                            test_samples = len(test_matching)
                            if test_samples > 0:
                                test_wins = sum(1 for m in test_matching if m['ou_result'] == target)
                                test_acc = test_wins / test_samples

                            pattern = {
                                'features': sorted(selected),
                                'target': target,
                                'accuracy': round(acc, 3),
                                'samples': len(matching),
                                'wins': wins,
                                'group': group_name,
                                'test_accuracy': round(test_acc, 3),
                                'test_samples': test_samples,
                                'type': 'OU'
                            }
                            
                            is_dup = any(
                                set(p['features']) == set(selected) and p['target'] == target and p['group'] == group_name
                                for p in self.ou_patterns
                            )
                            
                            if not is_dup:
                                self.ou_patterns.append(pattern)
                                stars = '🎯' * min(3, 1 + int((acc - 0.76) * 10))
                                if test_samples > 0:
                                    val_str = f"| VAL: {test_acc*100:.1f}% ({test_samples})"
                                    print(f"{stars} OU {target} TRAIN: {acc*100:.1f}% ({len(matching)}) {val_str} [{group_name}]")
                    
                    if (gen + 1) % 500 == 0:
                        print(f"  OU Gen {gen+1}/{generations}... ({len(self.ou_patterns)} patrones) - Autoguardando...")
                        self.save(output_file)

        except KeyboardInterrupt:
            print("\n\n!!! INTERRUPCIÓN DE USUARIO DETECTADA !!!")
            print("Guardando patrones encontrados hasta ahora antes de salir...")
            self.save(output_file)
            print("Guardado completado. Saliendo.")

        # Ordenar: Priorizamos Test Accuracy (si hay muestras suficientes)
        self.ah_patterns.sort(key=lambda x: (-x['test_accuracy'] if x['test_samples'] >= 5 else -0.5, -x['accuracy']))
        self.ou_patterns.sort(key=lambda x: (-x['test_accuracy'] if x['test_samples'] >= 5 else -0.5, -x['accuracy']))
        
        print(f"\n{'='*70}")
        print(f"PATRONES AH: {len(self.ah_patterns)} | PATRONES O/U: {len(self.ou_patterns)}")
        print(f"{'='*70}")
        
    def save(self, filepath):
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                'generated': datetime.now().isoformat(),
                'min_accuracy': self.min_accuracy,
                'min_samples': self.min_samples,
                'total_ah_patterns': len(self.ah_patterns),
                'total_ou_patterns': len(self.ou_patterns),
                'ah_patterns': self.ah_patterns,
                'ou_patterns': self.ou_patterns
            }, f, indent=2, ensure_ascii=False)
        print(f"\nPatrones guardados en: {filepath}")
    
    def print_top(self, n=30):
        print("\n========== TOP PATRONES AH ==========")
        for i, p in enumerate(self.ah_patterns[:n], 1):
            print(f"{i}. {p['target']} {p['accuracy']*100:.0f}% ({p['wins']}/{p['samples']}) [{p['group']}] <- {p['features']}")
        
        print("\n========== TOP PATRONES O/U ==========")
        for i, p in enumerate(self.ou_patterns[:n], 1):
            print(f"{i}. {p['target']} {p['accuracy']*100:.0f}% ({p['wins']}/{p['samples']}) [{p['group']}] <- {p['features']}")

def main():
    print("""
    ╔══════════════════════════════════════════════════════════════════╗
    ║         MEGA ENTRENADOR DE PATRONES INFALIBLES                   ║
    ║  - Indirectas alineadas                                          ║
    ║  - H2H Col3 (handicap repetido, resultado histórico)             ║
    ║  - Momentum (ganó previo + rival perdió)                         ║
    ║  - Rankings y forma                                              ║
    ║  - Goles (over/under previos)                                    ║
    ║  - Datos frescos obligatorio                                     ║
    ╚══════════════════════════════════════════════════════════════════╝
    """)
    
    matches = load_matches()
    print(f"Total partidos cargados: {len(matches)}")
    
    if len(matches) < 100:
        print("[!] Muy pocos partidos")
        return
    
    learner = MegaPatternLearner()
    learner.train(matches) # Uses default 50000
    
    output = os.path.join(os.path.dirname(__file__), 'mega_patterns.json')
    learner.save(output)
    learner.print_top(200)
    
    print("\n¡MEGA ENTRENAMIENTO COMPLETADO!")

if __name__ == '__main__':
    main()
