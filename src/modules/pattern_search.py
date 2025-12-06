import json
import math
import datetime
from pathlib import Path

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
            h_name = (m.get('home_name') or m.get('home_team') or '').strip()
            a_name = (m.get('away_name') or m.get('away_team') or '').strip()
            t_name = team_name.strip()
            
            is_home = h_name == t_name
            is_away = a_name == t_name
            
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
def find_similar_patterns(upcoming_match, datajson, config=None):
    """
    Encuentra patrones similares.
    config: { 'filter_mode': 'global' | 'home_strict' | 'away_strict' }
    """
    results = []
    filter_mode = config.get('filter_mode', 'global') if config else 'global'
    
    def safe_float(val):
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    target_ah = safe_float(upcoming_match.get('ah_open_home', 0))
    if target_ah is None:
        return []

    target_bucket = normalize_ah_bucket(target_ah)
    
    # Targets opcionales de los partidos previos
    target_prev_home_ah = upcoming_match.get('prev_home_ah')
    target_prev_away_ah = upcoming_match.get('prev_away_ah')
    
    target_prev_home_bucket = None
    if target_prev_home_ah is not None:
        val = safe_float(target_prev_home_ah)
        if val is not None:
            target_prev_home_bucket = normalize_ah_bucket(val)
    
    target_prev_away_bucket = None
    if target_prev_away_ah is not None:
        val = safe_float(target_prev_away_ah)
        if val is not None:
            target_prev_away_bucket = normalize_ah_bucket(-val)

    # WDL Targets for Strict Mode
    target_prev_home_score = upcoming_match.get('prev_home_score')
    target_prev_away_score = upcoming_match.get('prev_away_score')
    
    target_prev_home_wdl = get_wdl_result(target_prev_home_score, is_home_perspective=True)
    target_prev_away_wdl = get_wdl_result(target_prev_away_score, is_home_perspective=False)

    # Fecha de referencia para buscar previos
    ref_date = upcoming_match.get('date', datetime.datetime.now().strftime("%Y-%m-%d"))
    
    for match in datajson:
        # 1. Filtrar por bucket AH Principal
        odds = match.get('main_match_odds', {})
        hist_ah_raw = odds.get('ah_linea')
        
        # Fallback for data.json structure
        if hist_ah_raw is None:
            hist_ah_raw = match.get('handicap')
            
        if hist_ah_raw is None: continue
        
        try:
            hist_ah = float(hist_ah_raw)
        except:
            continue
            
        hist_bucket = normalize_ah_bucket(hist_ah)
        
        if target_bucket is not None:
            if target_bucket >= 2.5:
                if hist_bucket < 2.5: continue
            elif target_bucket <= -2.5:
                if hist_bucket > -2.5: continue
            else:
                if hist_bucket != target_bucket: continue
            
        # 2. Calcular Cover vs Target
        if 'final_score' not in match and 'score' in match:
             match['final_score'] = match['score'].replace(' - ', ':').replace('-', ':')

        cover_status = would_cover_current_line(match, target_ah)
        
        # 3. Buscar Previous Matches
        home_team = match.get('home_name') or match.get('home_team')
        away_team = match.get('away_name') or match.get('away_team')
        
        match_date_str = match.get('match_date') or match.get('date') or match.get('cached_at') or match.get('time_obj')
        
        # --- PREV HOME ---
        prev_home_data = None
        prev_home_matches_pattern = False
        
        # 1. Intentar usar datos pre-calculados (last_home_match)
        lhm = match.get('last_home_match')
        if lhm and isinstance(lhm, dict) and lhm.get('score'):
            try:
                p_ah_val = float(lhm.get('handicap_line_raw', 0))
                p_bucket = normalize_ah_bucket(p_ah_val)
                
                if target_prev_home_bucket is not None and p_bucket == target_prev_home_bucket:
                    prev_home_matches_pattern = True
                    
                p_score = lhm.get('score', '0:0')
                ph, pa = map(int, p_score.split(':'))
                p_res = asian_result(ph, pa, target_ah)
                
                prev_home_data = {
                    'rival': lhm.get('away_team'), 
                    'score': p_score,
                    'ah': p_ah_val,
                    'bucket': p_bucket,
                    'result': p_res['category'],
                    'matches_pattern': prev_home_matches_pattern,
                    'wdl': get_wdl_result(p_score, is_home_perspective=True)
                }
            except:
                pass
        
        # 2. Fallback: Buscar manualmente
        if not prev_home_data:
            prev_home_entry = get_previous_match(home_team, match_date_str, datajson, required_venue='home')
            if prev_home_entry:
                pm = prev_home_entry['match']
                p_odds = pm.get('main_match_odds', {})
                try:
                    p_ah_val_raw = p_odds.get('ah_linea') or pm.get('handicap')
                    p_ah_val = float(p_ah_val_raw) if p_ah_val_raw else 0
                    
                    if not prev_home_entry['is_home']:
                        p_ah_val = -p_ah_val
                    
                    p_bucket = normalize_ah_bucket(p_ah_val)
                    
                    if target_prev_home_bucket is not None and p_bucket == target_prev_home_bucket:
                        prev_home_matches_pattern = True

                    p_score = pm.get('final_score') or pm.get('score', '0:0').replace(' - ', ':').replace('-', ':')
                    ph, pa = map(int, p_score.split(':'))
                    
                    p_res = asian_result(ph, pa, target_ah)
                        
                    prev_home_data = {
                        'rival': pm.get('away_name') or pm.get('away_team') if prev_home_entry['is_home'] else pm.get('home_name') or pm.get('home_team'),
                        'score': p_score,
                        'ah': p_ah_val,
                        'bucket': p_bucket,
                        'result': p_res['category'],
                        'matches_pattern': prev_home_matches_pattern,
                        'wdl': get_wdl_result(p_score, is_home_perspective=True)
                    }
                except:
                    pass

        # --- PREV AWAY ---
        prev_away_data = None
        prev_away_matches_pattern = False
        
        # 1. Intentar usar datos pre-calculados (last_away_match)
        lam = match.get('last_away_match')
        if lam and isinstance(lam, dict) and lam.get('score'):
            try:
                p_ah_val = float(lam.get('handicap_line_raw', 0))
                # p_ah_val = -p_ah_val_raw # REMOVED: Keep consistent with UI (Home AH)
                
                p_bucket = normalize_ah_bucket(p_ah_val)
                
                if target_prev_away_bucket is not None and p_bucket == target_prev_away_bucket:
                    prev_away_matches_pattern = True

                p_score = lam.get('score', '0:0')
                ph, pa = map(int, p_score.split(':'))
                p_res = asian_result(pa, ph, -target_ah)
                
                prev_away_data = {
                    'rival': lam.get('home_team'), 
                    'score': p_score,
                    'ah': p_ah_val,
                    'bucket': p_bucket,
                    'result': p_res['category'],
                    'matches_pattern': prev_away_matches_pattern,
                    'wdl': get_wdl_result(p_score, is_home_perspective=False)
                }
            except:
                pass

        # 2. Fallback: Buscar manualmente
        if not prev_away_data:
            prev_away_entry = get_previous_match(away_team, match_date_str, datajson, required_venue='away')
            if prev_away_entry:
                pm = prev_away_entry['match']
                p_odds = pm.get('main_match_odds', {})
                try:
                    p_ah_val_raw = p_odds.get('ah_linea') or pm.get('handicap')
                    p_ah_val = float(p_ah_val_raw) if p_ah_val_raw else 0
                    
                    # if not prev_away_entry['is_home']:
                    #     p_ah_val = -p_ah_val # REMOVED: Keep consistent with UI (Home AH)
                    
                    p_bucket = normalize_ah_bucket(p_ah_val)
                    
                    if target_prev_away_bucket is not None and p_bucket == target_prev_away_bucket:
                        prev_away_matches_pattern = True

                    p_score = pm.get('final_score') or pm.get('score', '0:0').replace(' - ', ':').replace('-', ':')
                    ph, pa = map(int, p_score.split(':'))
                    
                    # For result calculation, we DO need the team's perspective if we want "Did THEY cover?"
                    # If p_ah_val is Home AH, and team is Away.
                    # Team AH is -p_ah_val.
                    # Result = (AwayGoals - HomeGoals) + (-p_ah_val)
                    # Result = (pa - ph) - p_ah_val
                    # asian_result(pa, ph, -target_ah) -> This uses -target_ah.
                    # If target_ah is p_ah_val (Home AH).
                    # Then -target_ah is Away AH.
                    # So passing -p_ah_val to asian_result is correct for Away Team result.
                    
                    p_res = asian_result(pa, ph, -p_ah_val)
                        
                    prev_away_data = {
                        'rival': pm.get('away_name') or pm.get('away_team') if prev_away_entry['is_home'] else pm.get('home_name') or pm.get('home_team'),
                        'score': p_score,
                        'ah': p_ah_val,
                        'bucket': p_bucket,
                        'result': p_res['category'],
                        'matches_pattern': prev_away_matches_pattern,
                        'wdl': get_wdl_result(p_score, is_home_perspective=False)
                    }
                except:
                    pass

        # --- FILTRADO STRICT ---
        if filter_mode == 'home_strict':
            if not prev_home_matches_pattern: continue
            if target_prev_home_wdl and prev_home_data and prev_home_data.get('wdl') != target_prev_home_wdl:
                continue
                
        elif filter_mode == 'away_strict':
            if not prev_away_matches_pattern: continue
            if target_prev_away_wdl and prev_away_data and prev_away_data.get('wdl') != target_prev_away_wdl:
                continue

        # --- H2H Col3 (New) ---
        h2h_col3_data = None
        
        # 1. Intentar usar datos pre-calculados (h2h_col3)
        pre_h2h = match.get('h2h_col3')
        if pre_h2h and isinstance(pre_h2h, dict) and pre_h2h.get('status') == 'found':
            try:
                score_str = f"{pre_h2h.get('goles_home')}:{pre_h2h.get('goles_away')}"
                h2h_col3_data = {
                    'score': score_str,
                    'date': pre_h2h.get('date'),
                    'ah': pre_h2h.get('handicap'),
                    'home_team': pre_h2h.get('h2h_home_team_name'),
                    'away_team': pre_h2h.get('h2h_away_team_name'),
                    'stats': pre_h2h.get('stats_rows', [])
                }
            except:
                pass

        # 2. Fallback: Buscar manualmente
        if not h2h_col3_data:
            h2h_col3_data = get_h2h_history(home_team, away_team, match_date_str, datajson)

        # Calcular Score de Similitud
        similarity_score = 1
        if prev_home_matches_pattern: similarity_score += 2
        if prev_away_matches_pattern: similarity_score += 2
        
        # Construir resultado
        match_date_display = match_date_str.split(' ')[0] if match_date_str else 'N/A'
        
        res_obj = {
            'candidate': {
                'date': match_date_display,
                'league': match.get('league_name'),
                'home': home_team,
                'away': away_team,
                'score': match.get('final_score'),
                'ah_real': hist_ah,
                'bucket': hist_bucket
            },
            'evaluation': {
                'home': cover_status['home'],
                'away': cover_status['away']
            },
            'prev_home': prev_home_data,
            'prev_away': prev_away_data,
            'h2h_col3': h2h_col3_data,
            'match_id': match.get('match_id'),
            'similarity_score': similarity_score
        }
        
        results.append(res_obj)
        
    # Ordenar por similitud descendente, luego por fecha
    results.sort(key=lambda x: (x['similarity_score'], x['candidate']['date']), reverse=True)
    
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
    
    def safe_float_ah(val):
        if val is None: return None
        try:
            return float(val)
        except:
            return None

    target_ah_bucket = None
    if filters.get('handicap') is not None:
        try:
            target_ah_bucket = normalize_ah_bucket(float(filters['handicap']))
        except:
            pass
            
    target_result = filters.get('result') # 'H', 'A', 'D'
    target_team = filters.get('team')
    if target_team:
        target_team = target_team.lower().strip()
        
    target_prev_home_wdl = filters.get('prev_home_wdl')
    target_prev_away_wdl = filters.get('prev_away_wdl')
    exclude_empty = filters.get('exclude_empty', False)

    limit = filters.get('limit', 100)
    
    count = 0
    
    for match in datajson:
        if count >= limit:
            break
            
        # --- 1. Filtros Rápidos ---
        
        # Filtro Equipo
        if target_team:
            h = (match.get('home_name') or match.get('home_team') or '').lower()
            a = (match.get('away_name') or match.get('away_team') or '').lower()
            if target_team not in h and target_team not in a:
                continue
                
        # Filtro Handicap
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

        # Filtro Resultado (WDL)
        score = match.get('final_score') or match.get('score')
        if not score: continue
        score = score.replace(' - ', ':').replace('-', ':')
        
        if target_result:
            wdl = get_wdl_result(score, is_home_perspective=True)
            if wdl != target_result:
                continue

        # --- 2. Procesamiento Detallado (Prev Home/Away) ---
        
        # Calcular Cover (vs su propia línea)
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

        # Prev Home
        prev_home_data = None
        # Try pre-calculated
        lhm = match.get('last_home_match')
        if lhm and isinstance(lhm, dict) and lhm.get('score'):
            try:
                # Determine perspective
                lhm_away = (lhm.get('away_team') or '').strip()
                is_home_perspective = True
                rival = lhm_away
                
                # If home team played as away in previous match
                if home_team == lhm_away:
                    is_home_perspective = False
                    rival = (lhm.get('home_team') or '').strip()

                p_ah = safe_float_ah(lhm.get('handicap_line_raw'))
                
                prev_home_data = {
                    'rival': rival,
                    'score': lhm.get('score'),
                    'ah': p_ah,
                    'bucket': normalize_ah_bucket(p_ah) if p_ah is not None else None,
                    'wdl': get_wdl_result(lhm.get('score'), is_home_perspective)
                }
            except: pass
        
        if not prev_home_data:
            ph_entry = get_previous_match(home_team, match_date_str, datajson, required_venue=None)
            if ph_entry:
                pm = ph_entry['match']
                p_ah_raw = pm.get('main_match_odds', {}).get('ah_linea') or pm.get('handicap')
                p_ah = safe_float_ah(p_ah_raw)
                
                if p_ah is not None and not ph_entry['is_home']:
                    p_ah = -p_ah
                    
                prev_home_data = {
                    'rival': pm.get('away_name') or pm.get('away_team') if ph_entry['is_home'] else pm.get('home_name') or pm.get('home_team'),
                    'score': pm.get('final_score') or pm.get('score'),
                    'ah': p_ah,
                    'bucket': normalize_ah_bucket(p_ah) if p_ah is not None else None,
                    'wdl': get_wdl_result(pm.get('final_score') or pm.get('score'), ph_entry['is_home'])
                }

        # Prev Away
        prev_away_data = None
        lam = match.get('last_away_match')
        if lam and isinstance(lam, dict) and lam.get('score'):
            try:
                # Determine perspective
                lam_away = (lam.get('away_team') or '').strip()
                is_home_perspective = True # Default to Home perspective for the TEAM (meaning they played Home)
                rival = lam_away
                
                # If away team played as away in previous match
                if away_team == lam_away:
                    is_home_perspective = False
                    rival = (lam.get('home_team') or '').strip()
                elif away_team != (lam.get('home_team') or '').strip():
                    # Name mismatch, try to infer
                    pass

                p_ah = safe_float_ah(lam.get('handicap_line_raw'))
                
                prev_away_data = {
                    'rival': rival,
                    'score': lam.get('score'),
                    'ah': p_ah,
                    'bucket': normalize_ah_bucket(p_ah) if p_ah is not None else None,
                    'wdl': get_wdl_result(lam.get('score'), is_home_perspective)
                }
            except: pass

        if not prev_away_data:
            pa_entry = get_previous_match(away_team, match_date_str, datajson, required_venue=None)
            if pa_entry:
                pm = pa_entry['match']
                p_ah_raw = pm.get('main_match_odds', {}).get('ah_linea') or pm.get('handicap')
                p_ah = safe_float_ah(p_ah_raw)
                
                # If we found a match where they played Away, p_ah is from Home perspective.
                # So their AH is -p_ah.
                # BUT, UI expects "Prev Away AH". Usually we show the AH relative to the team.
                # If they were Away and line was H -0.5, then A was +0.5.
                # If we want to show +0.5, we negate.
                # Logic: p_ah is always Home AH of that match.
                # If team was Away, their line was -p_ah.
                
                # Wait, in last_home_match logic above:
                # if not ph_entry['is_home']: p_ah = -p_ah
                # This converts Home AH to Team AH. Correct.
                
                if p_ah is not None and not pa_entry['is_home']:
                     p_ah = -p_ah

                prev_away_data = {
                    'rival': pm.get('away_name') or pm.get('away_team') if pa_entry['is_home'] else pm.get('home_name') or pm.get('home_team'),
                    'score': pm.get('final_score') or pm.get('score'),
                    'ah': p_ah,
                    'bucket': normalize_ah_bucket(p_ah) if p_ah is not None else None,
                    'wdl': get_wdl_result(pm.get('final_score') or pm.get('score'), pa_entry['is_home'])
                }

        # --- FILTROS DE PREV ---
        if exclude_empty:
            if not prev_home_data: continue
            
        if target_prev_home_wdl:
            if not prev_home_data: continue
            if prev_home_data.get('wdl') != target_prev_home_wdl: continue

        if filters.get('prev_home_ah'):
            if not prev_home_data: continue
            try:
                target_ph_bucket = normalize_ah_bucket(float(filters.get('prev_home_ah')))
                if prev_home_data.get('bucket') != target_ph_bucket: continue
            except: pass
            
        if target_prev_away_wdl:
            if not prev_away_data: continue
            if prev_away_data.get('wdl') != target_prev_away_wdl: continue

        if filters.get('prev_away_ah'):
            if not prev_away_data: continue
            try:
                target_pa_bucket = normalize_ah_bucket(float(filters.get('prev_away_ah')))
                if prev_away_data.get('bucket') != target_pa_bucket: continue
            except: pass

        # H2H Col3
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
                'bucket': hist_bucket
            },
            'evaluation': {
                'home': cover_status['home'],
                'away': cover_status['away']
            },
            'prev_home': prev_home_data,
            'prev_away': prev_away_data,
            'h2h_col3': h2h_col3_data,
            'match_id': match.get('match_id') or match.get('id')
        }
        
        results.append(res_obj)
        count += 1
        
    return results
