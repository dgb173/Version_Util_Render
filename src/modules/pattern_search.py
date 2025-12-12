import json
import math
import datetime
from pathlib import Path
import re

# --- A) Normalización de AH (Bucket) ---
def normalize_ah_bucket(ah: float) -> float:
    """
    Normaliza un AH al 'bucket' de 0.5 más cercano.
    Regla:
    - Enteros (.0) se quedan igual.
    - .25, .5, .75 se van al .5 del mismo entero.
    Ejemplos:
      -2.25 -> -2.5
      -2.5  -> -2.5
      -2.75 -> -2.5
      -2.0  -> -2.0
       0.25 -> 0.5
    """
    if ah is None:
        return None
    
    # Si es entero, devolver tal cual
    if abs(ah % 1.0) < 1e-9:
        return float(ah)
    
    # Si tiene decimales, buscar el entero base
    sign = 1 if ah >= 0 else -1
    abs_ah = abs(ah)
    base = math.floor(abs_ah)
    
    # Cualquier decimal (.25, .5, .75) se convierte a .5
    # Por tanto, el bucket es base + 0.5
    # Ej: 2.25 -> base 2 -> 2.5
    # Ej: 2.75 -> base 2 -> 2.5
    
    return sign * (base + 0.5)

# --- B) Resultado Asiático ---
def asian_result(team_goals, opp_goals, ah_line):
    """
    Calcula el resultado de una apuesta AH.
    Devuelve dict con:
      - result_code: +1 (Win), +0.5 (Half Win), 0 (Push), -0.5 (Half Loss), -1 (Loss)
      - category: 'COVER', 'PUSH', 'NO_COVER'
    """
    if team_goals is None or opp_goals is None or ah_line is None:
        return {'result_code': None, 'category': 'UNKNOWN'}
    
    diff = team_goals - opp_goals
    line = float(ah_line)
    
    # Ajustar diff con la línea: diff + line
    # Ej: Home gana 2-1 (diff +1), AH -0.5. Total = 1 - 0.5 = 0.5 > 0 -> Win
    # Ej: Home gana 1-0 (diff +1), AH -1.5. Total = 1 - 1.5 = -0.5 < 0 -> Loss
    
    # Manejo de cuartos (split bet)
    # Si la línea termina en .25 o .75, se divide en dos apuestas.
    # Pero la función pide un resultado "numérico tipo".
    # Vamos a calcular el resultado neto.
    
    # Descomponer línea si es cuarto
    lines = []
    if abs(line % 0.5) == 0.25:
        # Es cuarto
        # Ej: -0.75 -> -0.5 y -1.0
        # Ej: -0.25 -> 0.0 y -0.5
        # Regla: line +/- 0.25
        # Si es positivo: 0.75 -> 0.5 y 1.0
        if line > 0:
            lines = [line - 0.25, line + 0.25]
        else:
            lines = [line + 0.25, line - 0.25]
    else:
        lines = [line]
        
    results = []
    for l in lines:
        val = diff + l
        if val > 0: results.append(1)
        elif val < 0: results.append(-1)
        else: results.append(0)
        
    avg_res = sum(results) / len(results)
    
    # Categoría
    # COVER: Win (+1) o Half Win (+0.5)
    # NO_COVER: Loss (-1) o Half Loss (-0.5)
    # PUSH: Push (0)
    
    category = 'UNKNOWN'
    if avg_res > 0: category = 'COVER'
    elif avg_res < 0: category = 'NO_COVER'
    else: category = 'PUSH'
    
    return {'result_code': avg_res, 'category': category}

def get_wdl_result(score_str, is_home_perspective=True):
    """
    Returns 'W', 'D', 'L' based on score string 'H:A' or 'H-A'.
    is_home_perspective: True if we are evaluating the Home team's result, False for Away team.
    """
    if not score_str or ':' not in score_str.replace('-', ':'):
        return None
    
    try:
        parts = score_str.replace('-', ':').split(':')
        h = int(parts[0])
        a = int(parts[1])
        
        diff = h - a
        if not is_home_perspective:
            diff = -diff
            
        if diff > 0: return 'W'
        elif diff < 0: return 'L'
        else: return 'D'
    except:
        return None

# --- C) Would Cover Current Line ---
def would_cover_current_line(match, target_ah_home):
    """
    Evalúa si el resultado del partido histórico 'match' habría cubierto
    la línea 'target_ah_home'.
    """
    try:
        score = match.get('final_score')
        if not score or ':' not in score or '?' in score:
             return {'home': 'UNKNOWN', 'away': 'UNKNOWN'}
            
        parts = score.split(':')
        home_goals = int(parts[0])
        away_goals = int(parts[1])
        
        # Home perspective
        res_home = asian_result(home_goals, away_goals, target_ah_home)
        
        # Away perspective (AH invertido)
        # Si target_ah_home es -0.5, target_ah_away es +0.5
        target_ah_away = -1 * float(target_ah_home)
        res_away = asian_result(away_goals, home_goals, target_ah_away)
        
        return {
            'home': res_home['category'],
            'away': res_away['category'],
            'home_margin': res_home['result_code'],
            'away_margin': res_away['result_code']
        }
    except Exception as e:
        print(f"Error en would_cover: {e}")
        return {'home': 'ERROR', 'away': 'ERROR'}

# --- D) Get Previous Match ---
def get_previous_match(team_name, current_date_str, all_matches, required_venue=None):
    """
    Busca el partido inmediatamente anterior de 'team_name' antes de 'current_date_str'.
    required_venue: 'home' (solo casa), 'away' (solo fuera), o None (cualquiera).
    """
    if not current_date_str:
        return None
        
    try:
        # Intentar parsear la fecha actual (target)
        if ' ' in current_date_str:
            current_date = datetime.datetime.strptime(current_date_str, "%Y-%m-%d %H:%M:%S")
        elif 'T' in current_date_str:
            current_date = datetime.datetime.strptime(current_date_str, "%Y-%m-%dT%H:%M:%S")
        else:
            current_date = datetime.datetime.strptime(current_date_str, "%Y-%m-%d")
    except:
        return None

    candidates = []
    
    target_norm = team_name.strip().lower()

    for m in all_matches:
        # 1. Obtener fecha del partido histórico
        m_date_val = m.get('match_date') or m.get('date') or m.get('cached_at') or m.get('time_obj')
            
        if not m_date_val: continue
            
        try:
            if ' ' in m_date_val:
                m_date = datetime.datetime.strptime(m_date_val.replace('/', '-'), "%Y-%m-%d %H:%M:%S")
            elif 'T' in m_date_val:
                m_date = datetime.datetime.strptime(m_date_val, "%Y-%m-%dT%H:%M:%S")
            else:
                m_date = datetime.datetime.strptime(m_date_val, "%Y-%m-%d")
        except:
            continue
            
        # 2. Comparar fechas
        if m_date < current_date:
            # Verificar si el equipo jugó
            h_name = (m.get('home_name') or m.get('home_team') or '').strip().lower()
            a_name = (m.get('away_name') or m.get('away_team') or '').strip().lower()
            
            is_home = h_name == target_norm
            is_away = a_name == target_norm
            
            if not (is_home or is_away):
                continue

            # Filtrar por venue si se requiere
            if required_venue == 'home' and not is_home:
                continue
            if required_venue == 'away' and not is_away:
                continue
            
            candidates.append({
                'match': m,
                'date': m_date,
                'is_home': is_home
            })
                
    # Ordenar por fecha descendente (el más reciente primero)
    candidates.sort(key=lambda x: x['date'], reverse=True)
    
    if candidates:
        return candidates[0]
    return None

# --- F) Get H2H History ---
def get_h2h_history(home_team, away_team, match_date_str, all_matches):
    """
    Busca el último enfrentamiento directo (H2H) entre home_team y away_team
    antes de match_date_str.
    """
    if not match_date_str:
        return None
        
    try:
        if ' ' in match_date_str:
            match_date = datetime.datetime.strptime(match_date_str, "%Y-%m-%d %H:%M:%S")
        elif 'T' in match_date_str:
            match_date = datetime.datetime.strptime(match_date_str, "%Y-%m-%dT%H:%M:%S")
        else:
            match_date = datetime.datetime.strptime(match_date_str, "%Y-%m-%d")
    except:
        return None

    candidates = []
    
    h_norm = home_team.strip().lower()
    a_norm = away_team.strip().lower()

    for m in all_matches:
        # Fecha
        m_date_val = m.get('match_date') or m.get('date') or m.get('cached_at') or m.get('time_obj')
        if not m_date_val: continue
        
        try:
            if ' ' in m_date_val:
                m_date = datetime.datetime.strptime(m_date_val.replace('/', '-'), "%Y-%m-%d %H:%M:%S")
            elif 'T' in m_date_val:
                m_date = datetime.datetime.strptime(m_date_val, "%Y-%m-%dT%H:%M:%S")
            else:
                m_date = datetime.datetime.strptime(m_date_val, "%Y-%m-%d")
        except:
            continue
            
        if m_date >= match_date:
            continue
            
        # Equipos
        mh = (m.get('home_name') or m.get('home_team') or '').strip().lower()
        ma = (m.get('away_name') or m.get('away_team') or '').strip().lower()
        
        # Coincidencia directa o inversa
        if (mh == h_norm and ma == a_norm) or (mh == a_norm and ma == h_norm):
            candidates.append({
                'match': m,
                'date': m_date
            })
            
    # Ordenar más reciente primero
    candidates.sort(key=lambda x: x['date'], reverse=True)
    
    if candidates:
        best = candidates[0]['match']
        # Extraer datos relevantes
        score = best.get('final_score') or best.get('score', '0:0').replace(' - ', ':').replace('-', ':')
        
        # AH
        odds = best.get('main_match_odds', {})
        ah = odds.get('ah_linea') or best.get('handicap')
        
        # Stats
        stats = best.get('stats_rows', [])
        
        return {
            'score': score,
            'date': candidates[0]['date'].strftime("%Y-%m-%d"),
            'ah': ah,
            'home_team': best.get('home_name') or best.get('home_team'),
            'away_team': best.get('away_name') or best.get('away_team'),
            'stats': stats
        }
    return None

# --- E) Find Similar Patterns ---
# --- E) Find Similar Patterns (STRICT MODE) ---
def find_similar_patterns(upcoming_match, datajson, config=None):
    """
    Encuentra patrones similares con reglas ESTRICTAS:
    1. Favorito: 
       - HA < 0 -> Visitante
       - HA = 0 -> Visitante
       - HA > 0 -> Local
    2. Filtro HA: Exacto (mismo valor, mismo favorito).
    3. Filtro Resultado: Si 'upcoming_match' tiene resultado, filtrar por mismo W/D/L del favorito.
    """
    results = []
    
    # 1. Determinar datos del partido actual (Target)
    def safe_float(val):
        try:
            return float(val) 
        except: 
            return None

    target_ah_raw = upcoming_match.get('ah_open_home')
    target_ah = safe_float(target_ah_raw)
    
    if target_ah is None:
        return []

    # Determinar Favorito Target y Lado Target
    # Regla 1: HA < 0 -> Away Fav
    # Regla 2: HA = 0 -> Away Fav
    # Regla 3: HA > 0 -> Home Fav
    
    target_fav_side = 'UNKNOWN'
    if target_ah < 0:
        target_fav_side = 'AWAY'
    elif target_ah == 0:
        target_fav_side = 'AWAY'
    else:
        target_fav_side = 'HOME'
        
    # Obtener Resultado Target (si existe) para filtrar W/D/L
    target_score = upcoming_match.get('score')
    target_wdl = None 
    
    if target_score and ':' in target_score and '?' not in target_score:
        try:
            th, ta = map(int, target_score.replace('-', ':').split(':'))
            diff = th - ta
            
            # W/D/L desde la perspectiva del FAVORITO
            if target_fav_side == 'HOME':
                if diff > 0: target_wdl = 'W'
                elif diff < 0: target_wdl = 'L'
                else: target_wdl = 'D'
            else: # AWAY FAVORITE
                if diff < 0: target_wdl = 'W' # Away gana (Home score < Away score)
                elif diff > 0: target_wdl = 'L'
                else: target_wdl = 'D'
        except:
            pass

    # Iterar sobre histórico
    for match in datajson:
        # A. Filtro de HANDICAP EXACTO
        odds = match.get('main_match_odds', {})
        hist_ah_raw = odds.get('ah_linea') or match.get('handicap')
        hist_ah = safe_float(hist_ah_raw)
        
        if hist_ah is None: continue
        
        # Debe coincidir EXACTAMENTE con el target_ah
        # Nota: Como target_ah define el favorito, si coinciden en valor, coinciden en favorito implícitamente
        # dado que estamos comparando siempre desde perspectiva HOME AH.
        # Pero ojo: Si es 0.5 (Home Fav) y el histórico es 0.5 (Home Fav) -> OK.
        # Si fuese -0.5 (Away Fav) y histórico -0.5 (Away Fav) -> OK.
        
        # Comparación con tolerancia mínima por floats
        if abs(hist_ah - target_ah) > 0.001:
            continue
            
        # B. Filtro de RESULTADO (W/D/L del favorito)
        m_score = match.get('final_score') or match.get('score')
        if not m_score or ':' not in m_score or '?' in m_score:
            continue
            
        try:
            mh, ma = map(int, m_score.replace('-', ':').split(':'))
            mdiff = mh - ma
            
            m_wdl = None
            if target_fav_side == 'HOME':
                if mdiff > 0: m_wdl = 'W'
                elif mdiff < 0: m_wdl = 'L'
                else: m_wdl = 'D'
            else: # AWAY FAVORITE
                if mdiff < 0: m_wdl = 'W'
                elif mdiff > 0: m_wdl = 'L'
                else: m_wdl = 'D'
                
            # Si buscamos un resultado específico, debe coincidir
            if target_wdl and m_wdl != target_wdl:
                continue
                
        except:
            continue

        # C. Recopilar Datos (Prev Home/Away, etc) para visualización
        # Reutilizamos lógica de extracción pero SIMPLIFICADA para display
        home_team = match.get('home_name') or match.get('home_team')
        away_team = match.get('away_name') or match.get('away_team')
        match_date_str = match.get('match_date') or match.get('date') or match.get('cached_at')
        match_date_display = match_date_str.split(' ')[0] if match_date_str else 'N/A'

        # Prev Home
        prev_home_data = None
        lhm = match.get('last_home_match')
        if lhm and isinstance(lhm, dict):
            prev_home_data = {
                'rival': lhm.get('away_team'),
                'score': lhm.get('score', '').replace('-', ':'),
                'ah': lhm.get('handicap_line_raw'),
                'date': lhm.get('date')
            }
        
        # Prev Away
        prev_away_data = None
        lam = match.get('last_away_match')
        if lam and isinstance(lam, dict):
             prev_away_data = {
                'rival': lam.get('home_team'),
                'score': lam.get('score', '').replace('-', ':'),
                'ah': lam.get('handicap_line_raw'),
                'date': lam.get('date')
            }

        # Build Result Object
        res_obj = {
            'candidate': {
                'date': match_date_display,
                'league': match.get('league_name'),
                'home': home_team,
                'away': away_team,
                'score': m_score.replace('-', ':'),
                'ah_real': hist_ah,
                'wdl': m_wdl 
            },
            'prev_home': prev_home_data,
            'prev_away': prev_away_data,
            'match_id': match.get('match_id') or match.get('id')
        }
        
        results.append(res_obj)
        
    # Ordenar por fecha (más reciente primero)
    results.sort(key=lambda x: x['candidate']['date'], reverse=True)
    
    return results

# --- F) Explore Matches (New) ---
def explore_matches(datajson, filters=None):
    """
    Explora partidos históricos aplicando filtros.
    filters: {
        'handicap': float or None,
        'result': 'HOME_WIN' | 'AWAY_WIN' | 'DRAW' | None,
        'team': str or None,
        'limit': int
    }
    """
    results = []
    filters = filters or {}
    
    # --- 0. Crear Mapa de Partidos para Búsqueda Rápida ---
    match_map = {}
    for m in datajson:
        mid = m.get('match_id') or m.get('id')
        if mid:
            match_map[str(mid)] = m

    def extract_analysis_data(html_content, section_type):
        """
        Extracts movement and score from a specific section of the market_analysis_html.
        section_type: 'STADIUM' or 'GENERAL'
        """
        if not html_content: return None, None
        
        stadium_marker = "Análisis del Precedente en Este Estadio"
        general_marker = "Análisis del H2H General Más Reciente"
        
        idx_stadium = html_content.find(stadium_marker)
        idx_general = html_content.find(general_marker)
        
        target_block = ""
        
        if section_type == 'STADIUM':
            if idx_stadium == -1: return None, None
            if idx_general != -1 and idx_general > idx_stadium:
                target_block = html_content[idx_stadium:idx_general]
            else:
                target_block = html_content[idx_stadium:]
        elif section_type == 'GENERAL':
            if idx_general == -1: return None, None
            target_block = html_content[idx_general:]
            
        if not target_block: return None, None
        
        # Extract Movement
        movement = None
        match_mov = re.search(r'movimiento:.*?>\s*([+-]?\d*\.?\d+)\s*(?:→|\->|➜)\s*([+-]?\d*\.?\d+)', target_block)
        if match_mov:
            movement = f"{match_mov.group(1)} -> {match_mov.group(2)}"
            
        # Extract Result/Score from text like "Con el resultado (4:2)"
        score = None
        match_score = re.search(r'resultado\s*\(\s*(\d+[:\-]\d+)\s*\)', target_block)
        if match_score:
            score = match_score.group(1).replace('-', ':')
            
        return movement, score

    def safe_float_ah(val):
        if val is None: return None
        try:
            return float(val)
        except:
            return None
            
    def format_ah(val):
        if val is None: return "?"
        s = str(val)
        if s.endswith('.0'):
            return s[:-2]
        return s

    target_ah_bucket = None
    if filters.get('handicap') is not None:
        try:
            target_ah_bucket = normalize_ah_bucket(float(filters['handicap']))
        except:
            pass
            
    target_result = filters.get('result')
    target_team = filters.get('team')
    if target_team:
        target_team = target_team.lower().strip()
        
    target_prev_home_wdl = filters.get('prev_home_wdl')
    target_prev_away_wdl = filters.get('prev_away_wdl')
    
    # New H2H Filters
    target_stadium_mov = filters.get('h2h_stadium_mov')
    target_stadium_res = filters.get('h2h_stadium_res')
    target_general_mov = filters.get('h2h_general_mov')
    target_general_res = filters.get('h2h_general_res')
    
    exclude_empty = filters.get('exclude_empty', False)
    only_with_history = filters.get('only_with_history', False)

    limit = filters.get('limit', 100)
    
    count = 0
    
    for match in datajson:
        if count >= limit:
            break
            
        # --- 1. Filtros Básicos ---
        if target_team:
            h = (match.get('home_name') or match.get('home_team') or '').lower()
            a = (match.get('away_name') or match.get('away_team') or '').lower()
            if target_team not in h and target_team not in a:
                continue
                
        odds = match.get('main_match_odds', {})
        hist_ah_raw = odds.get('ah_linea') or match.get('handicap')
        
        if hist_ah_raw is None:
            continue
            
        try:
            hist_ah = float(hist_ah_raw)
        except:
            continue
            
        if target_ah_bucket is not None:
            hist_bucket = normalize_ah_bucket(hist_ah)
            if target_ah_bucket >= 2.5:
                if hist_bucket < 2.5: continue
            elif target_ah_bucket <= -2.5:
                if hist_bucket > -2.5: continue
            else:
                if hist_bucket != target_ah_bucket: continue
        else:
            hist_bucket = normalize_ah_bucket(hist_ah)

        score = match.get('final_score') or match.get('score')
        if not score: continue
        score = score.replace(' - ', ':').replace('-', ':')
        
        if target_result:
            wdl = get_wdl_result(score, is_home_perspective=True)
            if wdl != target_result:
                continue

        # --- 2. Cover Status ---
        try:
            parts = score.split(':')
            hg, ag = int(parts[0]), int(parts[1])
            res_home = asian_result(hg, ag, hist_ah)
            res_away = asian_result(ag, hg, -hist_ah)
            
            cover_status = {
                'home': res_home['category'],
                'away': res_away['category']
            }
        except:
            cover_status = {'home': 'UNKNOWN', 'away': 'UNKNOWN'}

        home_team = (match.get('home_name') or match.get('home_team') or '').strip()
        away_team = (match.get('away_name') or match.get('away_team') or '').strip()
        match_date_str = match.get('match_date') or match.get('date') or match.get('cached_at') or match.get('time_obj')

        # Helper for Simulated WDL (Backtest)
        def get_simulated_wdl(score_str, target_ah, is_home_team):
            if not score_str or target_ah is None:
                return None
            try:
                parts = score_str.split(':')
                hg, ag = int(parts[0]), int(parts[1])
                magnitude = abs(target_ah)
                ah_to_test = -magnitude if is_home_team else magnitude
                
                if is_home_team:
                    res = asian_result(hg, ag, ah_to_test)
                else:
                    res = asian_result(ag, hg, ah_to_test)
                
                cat = res['category']
                if cat == 'COVER' or cat == 'HALF_COVER':
                     return 'HOME_WIN' 
                elif cat == 'NO_COVER':
                     return 'AWAY_WIN'
                else:
                     return 'DRAW'
            except:
                return None

        # --- 3. Prev Home (Last Home Match from JSON) ---
        prev_home_data = None
        lhm = match.get('last_home_match')
        if lhm and isinstance(lhm, dict) and lhm.get('score'):
            p_score = lhm.get('score', '').replace(' - ', ':').replace('-', ':')
            p_ah_raw = lhm.get('handicap_line_raw')
            p_ah = safe_float_ah(p_ah_raw)
            
            sim_wdl = get_simulated_wdl(p_score, hist_ah, True)
            
            # Movement: Prev AH -> Current AH
            movement = None
            if p_ah is not None:
                movement = f"{format_ah(p_ah)} -> {format_ah(hist_ah)}"
            
            prev_home_data = {
                'rival': lhm.get('away_team'),
                'score': p_score,
                'ah': p_ah,
                'wdl': sim_wdl, 
                'movement': movement
            }
        
        # Fallback: Search in datajson
        if not prev_home_data:
            ph_entry = get_previous_match(home_team, match_date_str, datajson, required_venue='home')
            if ph_entry:
                pm = ph_entry['match']
                
                p_odds = pm.get('main_match_odds', {})
                p_ah_raw = p_odds.get('ah_linea') or pm.get('handicap')
                p_ah = safe_float_ah(p_ah_raw)
                
                p_score = pm.get('final_score') or pm.get('score')
                if p_score:
                    p_score = p_score.replace(' - ', ':').replace('-', ':')
                
                sim_wdl = get_simulated_wdl(p_score, hist_ah, True)
                
                movement = None
                if p_ah is not None:
                    movement = f"{format_ah(p_ah)} -> {format_ah(hist_ah)}"
                
                prev_home_data = {
                    'rival': pm.get('away_name') or pm.get('away_team'),
                    'score': p_score,
                    'ah': p_ah,
                    'wdl': sim_wdl,
                    'movement': movement
                }
            
        # --- 4. Prev Away (Last Away Match from JSON) ---
        prev_away_data = None
        lam = match.get('last_away_match')
        if lam and isinstance(lam, dict) and lam.get('score'):
            p_score = lam.get('score', '').replace(' - ', ':').replace('-', ':')
            p_ah_raw = lam.get('handicap_line_raw')
            p_ah = safe_float_ah(p_ah_raw)
            # Do NOT invert - user wants original sign displayed
            
            sim_wdl = get_simulated_wdl(p_score, hist_ah, False)
            
            # Movement: Prev AH -> Current AH
            movement = None
            if p_ah is not None:
                movement = f"{format_ah(p_ah)} -> {format_ah(hist_ah)}"
            
            prev_away_data = {
                'rival': lam.get('home_team'),
                'score': p_score,
                'ah': p_ah,
                'wdl': sim_wdl,
                'movement': movement
            }

        # Fallback: Search in datajson
        if not prev_away_data:
            pa_entry = get_previous_match(away_team, match_date_str, datajson, required_venue='away')
            if pa_entry:
                pm = pa_entry['match']
                
                p_odds = pm.get('main_match_odds', {})
                p_ah_raw = p_odds.get('ah_linea') or pm.get('handicap')
                p_ah = safe_float_ah(p_ah_raw)
                # Do NOT invert - user wants original sign displayed
                
                p_score = pm.get('final_score') or pm.get('score')
                if p_score:
                    p_score = p_score.replace(' - ', ':').replace('-', ':')

                sim_wdl = get_simulated_wdl(p_score, hist_ah, False)

                movement = None
                if p_ah is not None:
                    movement = f"{format_ah(p_ah)} -> {format_ah(hist_ah)}"

                prev_away_data = {
                    'rival': pm.get('home_name') or pm.get('home_team'), 
                    'score': p_score,
                    'ah': p_ah,
                    'wdl': sim_wdl,
                    'movement': movement
                }

        # --- 5. NEW: H2H Stadium and H2H General ---
        # Prioridad: market_analysis_data (Nuevo JSON estructurado)
        # Fallback: market_analysis_html (Legacy HTML parsing)
        
        market_data = match.get('market_analysis_data')
        h2h_stadium_data = None
        h2h_general_data = None

        def get_movement_direction(movement_str):
            """Returns 'UP', 'DOWN', or 'SAME' based on movement string like '0.5 -> 1'"""
            if not movement_str:
                return None
            try:
                parts = movement_str.replace(' ', '').split('->')
                if len(parts) == 2:
                    start = float(parts[0])
                    end = float(parts[1])
                    if end > start:
                        return 'UP'
                    elif end < start:
                        return 'DOWN'
                    else:
                        return 'SAME'
            except:
                pass
            except:
                pass
            return None
            
        def get_real_wdl_helper(score_str, is_home_perspective=True):
            if not score_str: return None
            try:
                parts = score_str.replace(' ', '').replace('-', ':').split(':')
                if len(parts) == 2:
                    hg, ag = int(parts[0]), int(parts[1])
                    diff = hg - ag
                    if not is_home_perspective: diff = -diff
                    
                    if diff > 0: return 'WIN'
                    elif diff < 0: return 'LOSS'
                    return 'DRAW'
            except: pass
            return None

        if market_data and isinstance(market_data, dict):
            # --- STRUCTURED DATA ---
            stadium_node = market_data.get('stadium')
            if stadium_node:
                mov_stadium = stadium_node.get('movement')
                score_stadium = stadium_node.get('result') or stadium_node.get('score')
                sim_wdl_stadium = get_simulated_wdl(score_stadium, hist_ah, True)
                mov_dir_stadium = get_movement_direction(mov_stadium)
                real_wdl_stadium = get_real_wdl_helper(score_stadium, True)
                h2h_stadium_data = {
                    'movement': mov_stadium,
                    'score': score_stadium,
                    'wdl': sim_wdl_stadium,
                    'mov_direction': mov_dir_stadium,
                    'real_wdl': real_wdl_stadium
                }
            
            general_node = market_data.get('general')
            if general_node:
                mov_general = general_node.get('movement')
                score_general = general_node.get('result') or general_node.get('score')
                sim_wdl_general = get_simulated_wdl(score_general, hist_ah, False)
                mov_dir_general = get_movement_direction(mov_general)
                real_wdl_general = get_real_wdl_helper(score_general, True)
                h2h_general_data = {
                    'movement': mov_general,
                    'score': score_general,
                    'wdl': sim_wdl_general,
                    'mov_direction': mov_dir_general,
                    'real_wdl': real_wdl_general
                }
                
        else:
            # --- LEGACY HTML PARSING ---
            market_html = match.get('market_analysis_html') or ""
            
            mov_stadium, score_stadium = extract_analysis_data(market_html, 'STADIUM')
            if mov_stadium or score_stadium:
                sim_wdl_stadium = get_simulated_wdl(score_stadium, hist_ah, True)
                mov_dir_stadium = get_movement_direction(mov_stadium)
                real_wdl_stadium = get_real_wdl_helper(score_stadium, True)
                h2h_stadium_data = {
                    'movement': mov_stadium,
                    'score': score_stadium,
                    'wdl': sim_wdl_stadium,
                    'mov_direction': mov_dir_stadium,
                    'real_wdl': real_wdl_stadium
                }
            
            mov_general, score_general = extract_analysis_data(market_html, 'GENERAL')
            if mov_general or score_general:
                sim_wdl_general = get_simulated_wdl(score_general, hist_ah, False)
                mov_dir_general = get_movement_direction(mov_general)
                h2h_general_data = {
                    'movement': mov_general,
                    'score': score_general,
                    'wdl': sim_wdl_general,
                    'mov_direction': mov_dir_general
                }

        # --- 6. Filtros de Previos ---
        # only_with_history: Requiere datos en AMBOS Prev Home y Prev Away
        if only_with_history:
            if not prev_home_data or not prev_away_data:
                continue
        
        if exclude_empty:
            if not prev_home_data: continue
            
        if target_prev_home_wdl:
            if not prev_home_data: continue
            if prev_home_data.get('wdl') != target_prev_home_wdl: continue

        if filters.get('prev_home_ah'):
            if not prev_home_data: continue
            try:
                target_ph_bucket = normalize_ah_bucket(float(filters.get('prev_home_ah')))
                ph_bucket = normalize_ah_bucket(prev_home_data.get('ah'))
                
                if target_ph_bucket >= 2.5:
                    if ph_bucket < 2.5: continue
                elif target_ph_bucket <= -2.5:
                    if ph_bucket > -2.5: continue
                else:
                    if ph_bucket != target_ph_bucket: continue
            except: pass
            
        if target_prev_away_wdl:
            if not prev_away_data: continue
            if prev_away_data.get('wdl') != target_prev_away_wdl: continue

        if filters.get('prev_away_ah'):
            if not prev_away_data: continue
            try:
                target_pa_bucket = normalize_ah_bucket(float(filters.get('prev_away_ah')))
                pa_bucket = normalize_ah_bucket(prev_away_data.get('ah'))
                
                if target_pa_bucket >= 2.5:
                    if pa_bucket < 2.5: continue
                elif target_pa_bucket <= -2.5:
                    if pa_bucket > -2.5: continue
                else:
                    if pa_bucket != target_pa_bucket: continue
            except: pass
        
        # --- 6.5 H2H Filters ---
        if target_stadium_mov:
            if not h2h_stadium_data: continue
            if h2h_stadium_data.get('mov_direction') != target_stadium_mov: continue
            
        if target_stadium_res:
            if not h2h_stadium_data: continue
            if h2h_stadium_data.get('real_wdl') != target_stadium_res: continue
            
        if target_general_mov:
            if not h2h_general_data: continue
            if h2h_general_data.get('mov_direction') != target_general_mov: continue
            
        if target_general_res:
            if not h2h_general_data: continue
            if h2h_general_data.get('real_wdl') != target_general_res: continue

        # --- 7. H2H Col3 ---
        h2h_col3_data = None
        pre_h2h = match.get('h2h_col3')
        if pre_h2h and isinstance(pre_h2h, dict) and pre_h2h.get('status') == 'found':
            h2h_col3_data = {
                'score': f"{pre_h2h.get('goles_home')}:{pre_h2h.get('goles_away')}",
                'date': pre_h2h.get('date'),
                'ah': pre_h2h.get('handicap'),
                'home_team': pre_h2h.get('h2h_home_team_name'),
                'away_team': pre_h2h.get('h2h_away_team_name')
            }
        
        if not h2h_col3_data:
            h2h_res = get_h2h_history(home_team, away_team, match_date_str, datajson)
            if h2h_res:
                h2h_col3_data = h2h_res

        match_date_display = match_date_str.split(' ')[0] if match_date_str else 'N/A'

        res_obj = {
            'candidate': {
                'date': match_date_display,
                'league': match.get('league_name'),
                'home': home_team,
                'away': away_team,
                'score': score,
                'ah_real': hist_ah,
                'ou_line': odds.get('goals_linea'),
                'bucket': hist_bucket
            },
            'evaluation': {
                'home': cover_status['home'],
                'away': cover_status['away']
            },
            'prev_home': prev_home_data,
            'prev_away': prev_away_data,
            'h2h_stadium': h2h_stadium_data,
            'h2h_general': h2h_general_data,
            'h2h_col3': h2h_col3_data,
            'ind_local': match.get('comparativas_indirectas', {}).get('left') if match.get('comparativas_indirectas') else None,
            'ind_visitante': match.get('comparativas_indirectas', {}).get('right') if match.get('comparativas_indirectas') else None,
            'match_id': match.get('match_id') or match.get('id')
        }
        
        results.append(res_obj)
        count += 1
        
    return results

