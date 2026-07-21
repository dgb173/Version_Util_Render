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


def _as_filter_values(raw_value):
    if raw_value in (None, ''):
        return []
    if isinstance(raw_value, (list, tuple, set)):
        values = raw_value
    else:
        values = [raw_value]
    parsed = []
    for value in values:
        try:
            parsed.append(float(value))
        except (TypeError, ValueError):
            continue
    return parsed


def ah_matches_any_bucket(row_ah, selected_values) -> bool:
    """
    Shared Explorer AH bucket matcher.
    Buckets are intentionally the same as the UI:
    0, +/-0.5 (0.25/0.5/0.75), +/-1, +/-1.5 (1.25/1.5/1.75),
    +/-2, and +/-2.5+ (2.25 and higher).
    """
    selected = _as_filter_values(selected_values)
    if not selected:
        return True
    try:
        row = float(row_ah)
    except (TypeError, ValueError):
        return False

    abs_row = abs(row)
    for selected_ah in selected:
        abs_selected = abs(selected_ah)
        if abs_selected >= 2.49:
            if selected_ah > 0 and row >= 2.24:
                return True
            if selected_ah < 0 and row <= -2.24:
                return True
        elif abs(abs_selected - 2) < 0.01:
            if selected_ah > 0 and abs(row - 2) < 0.01:
                return True
            if selected_ah < 0 and abs(row + 2) < 0.01:
                return True
        elif abs(abs_selected - 1.5) < 0.01:
            if selected_ah > 0 and 1.24 <= row <= 1.76:
                return True
            if selected_ah < 0 and -1.76 <= row <= -1.24:
                return True
        elif abs(abs_selected - 1) < 0.01:
            if selected_ah > 0 and abs(row - 1) < 0.01:
                return True
            if selected_ah < 0 and abs(row + 1) < 0.01:
                return True
        elif abs(abs_selected - 0.5) < 0.01:
            if selected_ah > 0 and 0.24 <= row <= 0.76:
                return True
            if selected_ah < 0 and -0.76 <= row <= -0.24:
                return True
        elif abs_selected < 0.01 and abs_row < 0.01:
            return True
        elif abs(row - selected_ah) < 0.01:
            return True

    return False

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
        # A. Filtro de HANDICAP SIMILARITUD (±0.5)
        odds = match.get('main_match_odds', {})
        hist_ah_raw = odds.get('ah_linea') or match.get('handicap')
        hist_ah = safe_float(hist_ah_raw)
        
        if hist_ah is None: continue
        
        # Permitir handicaps similares con tolerancia de ±0.5
        # Ejemplo: si target es 2.25, permitir 2, 2.25, 2.75 (rango 2.0 a 2.75)
        tolerance = 0.5
        if abs(hist_ah - target_ah) > tolerance:
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

        # C. FILTROS DE PROGRESIÓN DE HANDICAP (NUEVO)
        # Verificar si el favorito ganó su partido previo con handicap pequeño sin superar mucho la línea
        # Esto ayuda a encontrar patrones donde el mercado sobrevalora al favorito
        
        # Determinar quién es el favorito en este partido histórico
        hist_fav_side = 'HOME' if hist_ah > 0 else 'AWAY'
        
        # Obtener el partido previo del favorito histórico
        prev_fav_match = None
        if hist_fav_side == 'HOME':
            prev_fav_match = match.get('last_home_match')
        else:
            prev_fav_match = match.get('last_away_match')
        
        # Si queremos aplicar filtro de progresión y hay datos del partido previo
        apply_progression_filter = config and config.get('filter_progression', False)
        
        if apply_progression_filter and prev_fav_match and isinstance(prev_fav_match, dict):
            prev_score = prev_fav_match.get('score', '')
            prev_ah_raw = prev_fav_match.get('handicap_line_raw')
            
            if prev_score and ':' in prev_score and prev_ah_raw is not None:
                try:
                    prev_ah = float(prev_ah_raw)
                    prev_ah_abs = abs(prev_ah)
                    
                    # Filtro 1: El handicap previo debe ser pequeño (0.25, 0.5, 0.75)
                    if prev_ah_abs < 0.2 or prev_ah_abs > 0.8:
                        continue
                    
                    # Filtro 2: El favorito debe haber ganado su partido previo
                    prev_h, prev_a = map(int, prev_score.replace('-', ':').split(':'))
                    
                    # Determinar si ganó desde su perspectiva
                    if hist_fav_side == 'HOME':
                        # El equipo local es el favorito, su previo es cuando jugó de local
                        prev_won = prev_h > prev_a
                    else:
                        # El equipo visitante es el favorito, su previo es cuando jugó de visitante  
                        prev_won = prev_a > prev_h
                    
                    if not prev_won:
                        continue
                    
                    # Filtro 3: NO debe haber superado la línea del handicap previo (ganó "ajustado")
                    # Calcular si cubrió el handicap previo
                    if hist_fav_side == 'HOME':
                        prev_diff = prev_h - prev_a
                        adjusted_prev = prev_diff + prev_ah
                    else:
                        prev_diff = prev_a - prev_h
                        # Invertir el signo del AH para perspectiva visitante
                        adjusted_prev = prev_diff + (-prev_ah)
                    
                    # Si cubrió el handicap previo con margen (>0.5), descartar
                    # Queremos casos donde ganó pero NO cubrió o cubrió muy ajustado
                    if adjusted_prev > 0.5:
                        continue
                        
                except:
                    # Si hay error en el procesamiento, incluir el partido por seguridad
                    pass
        
        # D. Recopilar Datos (Prev Home/Away, etc) para visualización
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
            'match_id': match.get('match_id') or match.get('id'),
            'home_standings': match.get('home_standings'),
            'away_standings': match.get('away_standings')
        }
        
        results.append(res_obj)
        
    # Ordenar por fecha (más reciente primero)
    results.sort(key=lambda x: x['candidate']['date'], reverse=True)
    
    return results

# --- F.1) Helper: Normalize Movement String to Buckets (Start, End) ---
def get_movement_buckets(movement_str):
    if not movement_str or movement_str == 'N/A':
        return None
    try:
        # Soportar -> y →, y cambiar comas por puntos
        normalized = movement_str.replace(' ', '').replace('→', '->').replace(',', '.')
        parts = normalized.split('->')
        if len(parts) == 2:
            start = float(parts[0])
            end = float(parts[1])
            
            # Usar la función global normalize_ah_bucket
            b_start = normalize_ah_bucket(start)
            b_end = normalize_ah_bucket(end)
            
            return b_start, b_end
    except:
        pass
    return None

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
    include_stats = bool(filters.get('include_stats', True))
    
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
        
        # Robust markers: search for keywords ignoring emojis/decorations
        stadium_kw = "Análisis del Precedente en Este Estadio"
        general_kw = "Análisis del H2H General Más Reciente"
        
        match_stadium = re.search(re.escape(stadium_kw), html_content)
        match_general = re.search(re.escape(general_kw), html_content)
        
        idx_stadium = match_stadium.start() if match_stadium else -1
        idx_general = match_general.start() if match_general else -1
        
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

    def get_favorite_side(ah_value):
        """Convention used across the project: AH > 0 => HOME favorite, else AWAY favorite."""
        if ah_value is None:
            return None
        return 'HOME' if float(ah_value) > 0 else 'AWAY'

    def get_favorite_cover_result(score_str, ah_value, is_inverted=False):
        """
        Returns COVER / PUSH / NO_COVER from favorite perspective.
        `is_inverted` is used for H2H General where local/away can be flipped.
        """
        if not score_str or ah_value is None:
            return None
        try:
            parts = score_str.replace(' ', '').replace('-', ':').split(':')
            if len(parts) != 2:
                return None

            h, a = int(parts[0]), int(parts[1])
            fav_is_local = float(ah_value) > 0
            abs_ah = abs(float(ah_value))

            if is_inverted:
                diff = (a - h) if fav_is_local else (h - a)
            else:
                diff = (h - a) if fav_is_local else (a - h)

            if diff > abs_ah:
                return 'COVER'
            if diff < abs_ah:
                return 'NO_COVER'
            return 'PUSH'
        except:
            return None

    def get_team_real_wdl(score_str, team_is_home):
        """
        Real WDL from tracked team perspective.
        Returns HOME_WIN / DRAW / AWAY_WIN (same token family used by existing filters).
        """
        if not score_str:
            return None
        try:
            parts = score_str.replace(' ', '').replace('-', ':').split(':')
            if len(parts) != 2:
                return None
            h, a = int(parts[0]), int(parts[1])
            diff = (h - a) if team_is_home else (a - h)
            if diff > 0:
                return 'HOME_WIN'
            if diff < 0:
                return 'AWAY_WIN'
            return 'DRAW'
        except:
            return None

    def infer_team_is_home(prev_data, team_norm, default_is_home=True):
        """
        Infer if tracked team played as home/away in a previous match row.
        Falls back to provided default if names are missing or ambiguous.
        """
        if not isinstance(prev_data, dict):
            return default_is_home
        try:
            p_home = (prev_data.get('home_team') or '').strip().lower()
            p_away = (prev_data.get('away_team') or '').strip().lower()
            t_norm = (team_norm or '').strip().lower()
            if t_norm:
                if t_norm == p_home:
                    return True
                if t_norm == p_away:
                    return False
        except:
            pass
        return default_is_home
            
    def format_ah(val):
        if val is None: return "?"
        s = str(val)
        if s.endswith('.0'):
            return s[:-2]
        return s

    def get_simulated_wdl(score_str, target_ah, is_home_team):
        if not score_str or target_ah is None:
            return None
        try:
            parts = score_str.replace(' - ', ':').replace('-', ':').split(':')
            hg, ag = int(parts[0]), int(parts[1])
            home_ah = safe_float_ah(target_ah)
            if home_ah is None:
                return None

            # Proyecto /explorador:
            # - AH > 0 => local favorito
            # - El equipo local se evalúa con -AH
            # - El equipo visitante se evalúa con +AH
            team_ah = -home_ah if is_home_team else home_ah

            if is_home_team:
                res = asian_result(hg, ag, team_ah)
            else:
                res = asian_result(ag, hg, team_ah)

            cat = res['category']
            if cat in ('COVER', 'HALF_COVER'):
                return 'HOME_WIN'
            if cat == 'NO_COVER':
                return 'AWAY_WIN'
            return 'DRAW'
        except:
            return None

    _date_parse_cache = {}

    def _parse_match_datetime(raw_val):
        if raw_val is None:
            return None

        key = str(raw_val).strip()
        if not key:
            return None
        if key in _date_parse_cache:
            return _date_parse_cache[key]

        normalized = key.replace('/', '-')
        if 'T' in normalized:
            normalized = normalized.split('.')[0]

        dt = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.datetime.strptime(normalized, fmt)
                break
            except:
                continue

        _date_parse_cache[key] = dt
        return dt

    # Indexes for fast fallback lookups (prev matches and H2H).
    # This avoids repeatedly scanning all historical matches for every row.
    _team_home_index = {}
    _team_away_index = {}
    _h2h_index = {}

    for m in datajson:
        m_date_raw = m.get('match_date') or m.get('date') or m.get('cached_at') or m.get('time_obj')
        m_date = _parse_match_datetime(m_date_raw)
        if not m_date:
            continue

        h_norm = (m.get('home_name') or m.get('home_team') or '').strip().lower()
        a_norm = (m.get('away_name') or m.get('away_team') or '').strip().lower()

        if h_norm:
            _team_home_index.setdefault(h_norm, []).append((m_date, m))
        if a_norm:
            _team_away_index.setdefault(a_norm, []).append((m_date, m))

        if h_norm and a_norm:
            pair_key = tuple(sorted((h_norm, a_norm)))
            _h2h_index.setdefault(pair_key, []).append((m_date, m))

    for idx in (_team_home_index, _team_away_index, _h2h_index):
        for key, rows in idx.items():
            rows.sort(key=lambda x: x[0], reverse=True)

    def _indexed_previous_match(team_norm, current_date, required_venue=None):
        if not team_norm or not current_date:
            return None
        source = _team_home_index if required_venue == 'home' else _team_away_index
        for m_date, m in source.get(team_norm, []):
            if m_date < current_date:
                return {'match': m, 'date': m_date}
        return None

    def _indexed_h2h(home_norm, away_norm, current_date):
        if not home_norm or not away_norm or not current_date:
            return None
        pair_key = tuple(sorted((home_norm, away_norm)))
        for m_date, m in _h2h_index.get(pair_key, []):
            if m_date >= current_date:
                continue

            score = (m.get('final_score') or m.get('score') or '0:0').replace(' - ', ':').replace('-', ':')
            odds = m.get('main_match_odds', {})
            ah = odds.get('ah_linea') or m.get('handicap')
            return {
                'match_id': str(m.get('match_id') or ''),
                'score': score,
                'date': m_date.strftime("%Y-%m-%d"),
                'ah': ah,
                'home_team': m.get('home_name') or m.get('home_team'),
                'away_team': m.get('away_name') or m.get('away_team'),
                'stats': m.get('stats_rows', []) if include_stats else []
            }
        return None

    target_ah_values = _as_filter_values(filters.get('handicap'))
            
    target_result = filters.get('result')
    target_team = filters.get('team')
    if target_team:
        target_team = target_team.lower().strip()
        
    target_prev_home_wdl = filters.get('prev_home_wdl')
    target_prev_away_wdl = filters.get('prev_away_wdl')
    target_prev_home_real_wdl = (filters.get('prev_home_real_wdl') or '').upper().strip()
    if target_prev_home_real_wdl not in ('HOME_WIN', 'DRAW', 'AWAY_WIN'):
        target_prev_home_real_wdl = None
    target_prev_away_real_wdl = (filters.get('prev_away_real_wdl') or '').upper().strip()
    if target_prev_away_real_wdl not in ('HOME_WIN', 'DRAW', 'AWAY_WIN'):
        target_prev_away_real_wdl = None
    
    # New H2H Filters
    target_stadium_mov = filters.get('h2h_stadium_mov')
    target_stadium_res = filters.get('h2h_stadium_res')
    target_general_mov = filters.get('h2h_general_mov')
    target_general_res = filters.get('h2h_general_res')
    
    # Note: h2h_mov_start, h2h_mov_end, h2h_res_exact removed - now handled in frontend
    
    
    target_cover_res = filters.get('cover_result') # NEW: FT Cover
    target_ou_limit = filters.get('ou_limit') # NEW: Goals line bucket

    # Optional strict layer for "same favorite context" searches.
    target_exact_handicap = safe_float_ah(filters.get('exact_handicap'))
    target_favorite_side = (filters.get('favorite_side') or '').upper().strip()
    if target_favorite_side not in ('HOME', 'AWAY'):
        target_favorite_side = None

    target_favorite_result = (filters.get('favorite_result') or '').upper().strip()
    if target_favorite_result not in ('COVER', 'PUSH', 'NO_COVER'):
        target_favorite_result = None
    
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

        if target_exact_handicap is not None:
            if abs(hist_ah - target_exact_handicap) > 0.01:
                continue

        match_favorite_side = get_favorite_side(hist_ah)
        if target_favorite_side and match_favorite_side != target_favorite_side:
            continue
            
        if target_ah_values and not ah_matches_any_bucket(hist_ah, target_ah_values):
            continue
        else:
            hist_bucket = normalize_ah_bucket(hist_ah)

        score = match.get('final_score') or match.get('score')
        if not score: continue
        score = score.replace(' - ', ':').replace('-', ':')

        if target_favorite_result:
            fav_res = get_favorite_cover_result(score, hist_ah, False)
            if fav_res != target_favorite_result:
                continue
        
        match_result = get_wdl_result(score, is_home_perspective=True)
        if target_result:
            if match_result != target_result: continue

        # --- NEW: FT Cover Filter ---
        if target_cover_res:
            ah_val = match.get('main_match_odds', {}).get('ah_linea')
            if ah_val is None or score == '?:?': continue
            try:
                # Use handle_split_ah logic to determine cover status consistently
                ah_f = float(ah_val)
                score_parts = score.replace('-', ':').split(':')
                h_g, a_g = int(score_parts[0]), int(score_parts[1])
                
                # Convention: ah_f > 0 -> Home is Favorite
                fav_is_home = ah_f > 0
                abs_ah = abs(ah_f)
                
                diff = (h_g - a_g) if fav_is_home else (a_g - h_g)
                
                # Split handling
                if abs_ah % 0.5 == 0.25:
                    lines = [abs_ah - 0.25, abs_ah + 0.25]
                else:
                    lines = [abs_ah]
                    
                votes = []
                for l in lines:
                    if diff > l: votes.append(1)
                    elif diff < l: votes.append(-1)
                    else: votes.append(0)
                
                avg = sum(votes) / len(votes)
                status = 'COVER' if avg > 0 else 'NO_COVER' if avg < 0 else 'PUSH'
                
                if status != target_cover_res: continue
            except: continue

        # --- NEW: O/U Goals Filter ---
        if target_ou_limit:
            ou_val = match.get('main_match_odds', {}).get('goals_linea')
            if ou_val is None: continue
            try:
                if float(ou_val) != float(target_ou_limit): continue
            except: continue

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
        current_date_obj = _parse_match_datetime(match_date_str)
        home_team_norm = home_team.lower()
        away_team_norm = away_team.lower()

        # --- 3. Prev Home (Last Home Match from JSON) ---
        prev_home_data = None
        lhm = match.get('last_home_match')
        if lhm and isinstance(lhm, dict) and lhm.get('score'):
            p_score = lhm.get('score', '').replace(' - ', ':').replace('-', ':')
            p_ah_raw = lhm.get('handicap_line_raw')
            p_ah = safe_float_ah(p_ah_raw)
            
            sim_wdl = get_simulated_wdl(p_score, hist_ah, True)
            real_wdl = get_team_real_wdl(
                p_score,
                infer_team_is_home(lhm, home_team_norm, default_is_home=True)
            )
            
            # Movement: Prev AH -> Current AH
            movement = None
            if p_ah is not None:
                movement = f"{format_ah(p_ah)} -> {format_ah(hist_ah)}"
            
            prev_home_data = {
                'match_id': str(lhm.get('match_id') or ''),
                'rival': lhm.get('away_team'),
                'score': p_score,
                'ah': p_ah,
                'wdl': real_wdl,
                'wdl_simulated': sim_wdl,
                'movement': movement,
                'date': lhm.get('date'),
                'home_team': lhm.get('home_team') or home_team,
                'away_team': lhm.get('away_team'),
                'stats_rows': lhm.get('stats_rows', []) if include_stats else []
            }
        
        # Fallback: Search in datajson
        if not prev_home_data:
            ph_entry = _indexed_previous_match(home_team_norm, current_date_obj, required_venue='home')
            if ph_entry:
                pm = ph_entry['match']
                
                p_odds = pm.get('main_match_odds', {})
                p_ah_raw = p_odds.get('ah_linea') or pm.get('handicap')
                p_ah = safe_float_ah(p_ah_raw)
                
                p_score = pm.get('final_score') or pm.get('score')
                if p_score:
                    p_score = p_score.replace(' - ', ':').replace('-', ':')
                
                sim_wdl = get_simulated_wdl(p_score, hist_ah, True)
                real_wdl = get_team_real_wdl(
                    p_score,
                    infer_team_is_home(pm, home_team_norm, default_is_home=True)
                )
                
                movement = None
                if p_ah is not None:
                    movement = f"{format_ah(p_ah)} -> {format_ah(hist_ah)}"
                
                prev_home_data = {
                    'match_id': str(pm.get('match_id') or ''),
                    'rival': pm.get('away_name') or pm.get('away_team'),
                    'score': p_score,
                    'ah': p_ah,
                    'wdl': real_wdl,
                    'wdl_simulated': sim_wdl,
                    'movement': movement,
                    'date': ph_entry['date'].strftime('%Y-%m-%d') if hasattr(ph_entry.get('date'), 'strftime') else str(ph_entry.get('date', '')),
                    'home_team': pm.get('home_name') or pm.get('home_team'),
                    'away_team': pm.get('away_name') or pm.get('away_team'),
                    'stats_rows': pm.get('stats_rows', []) if include_stats else []
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
            real_wdl = get_team_real_wdl(
                p_score,
                infer_team_is_home(lam, away_team_norm, default_is_home=False)
            )
            
            # Movement: Prev AH -> Current AH
            movement = None
            if p_ah is not None:
                movement = f"{format_ah(p_ah)} -> {format_ah(hist_ah)}"
            
            prev_away_data = {
                'match_id': str(lam.get('match_id') or ''),
                'rival': lam.get('home_team'),
                'score': p_score,
                'ah': p_ah,
                'wdl': real_wdl,
                'wdl_simulated': sim_wdl,
                'movement': movement,
                'date': lam.get('date'),
                'home_team': lam.get('home_team'),
                'away_team': lam.get('away_team') or away_team,
                'stats_rows': lam.get('stats_rows', []) if include_stats else []
            }

        # Fallback: Search in datajson
        if not prev_away_data:
            pa_entry = _indexed_previous_match(away_team_norm, current_date_obj, required_venue='away')
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
                real_wdl = get_team_real_wdl(
                    p_score,
                    infer_team_is_home(pm, away_team_norm, default_is_home=False)
                )

                movement = None
                if p_ah is not None:
                    movement = f"{format_ah(p_ah)} -> {format_ah(hist_ah)}"

                prev_away_data = {
                    'match_id': str(pm.get('match_id') or ''),
                    'rival': pm.get('home_name') or pm.get('home_team'), 
                    'score': p_score,
                    'ah': p_ah,
                    'wdl': real_wdl,
                    'wdl_simulated': sim_wdl,
                    'movement': movement,
                    'date': pa_entry['date'].strftime('%Y-%m-%d') if hasattr(pa_entry.get('date'), 'strftime') else str(pa_entry.get('date', '')),
                    'home_team': pm.get('home_name') or pm.get('home_team'),
                    'away_team': pm.get('away_name') or pm.get('away_team'),
                    'stats_rows': pm.get('stats_rows', []) if include_stats else []
                }

        # --- 5. NEW: H2H Stadium and H2H General ---
        # Prioridad: market_analysis_data (Nuevo JSON estructurado)
        # Fallback: market_analysis_html (Legacy HTML parsing)
        
        market_data = match.get('market_analysis_data')
        h2h_stadium_data = None
        h2h_general_data = None

        def get_movement_direction(movement_str):
            """Returns 'UP', 'DOWN', or 'SAME' based on movement string like '0.5 -> 1'
            Uses bucket normalization: 0.25/0.5/0.75 are equivalent (bucket 0.5)
            """
            if not movement_str or movement_str == 'N/A':
                return None
            try:
                # Soportar tanto -> como → (Unicode)
                normalized = movement_str.replace(' ', '').replace('→', '->')
                parts = normalized.split('->')
                if len(parts) == 2:
                    start = float(parts[0])
                    end = float(parts[1])
                    
                    # Normalizar a buckets
                    def normalize_to_bucket(val):
                        if val == 0:
                            return 0
                        abs_val = abs(val)
                        sign = 1 if val >= 0 else -1
                        int_part = int(abs_val)
                        dec_part = abs_val - int_part
                        
                        # 0.0 -> 0, 0.25/0.5/0.75 -> 0.5, 1.0 -> 1, etc.
                        if dec_part < 0.01:
                            return sign * int_part
                        else:
                            return sign * (int_part + 0.5)
                    
                    bucket_start = normalize_to_bucket(start)
                    bucket_end = normalize_to_bucket(end)
                    
                    if bucket_end > bucket_start:
                        return 'UP'
                    elif bucket_end < bucket_start:
                        return 'DOWN'
                    else:
                        return 'SAME'
            except:
                pass
            return None

        def get_general_movement_direction(movement_str):
            """Movement direction for H2H General.
            Uses ABS buckets because local/away can be inverted in this block.
            """
            if not movement_str or movement_str == 'N/A':
                return None
            try:
                buckets = get_movement_buckets(movement_str)
                if not buckets:
                    return None
                b_start, b_end = buckets
                abs_start = abs(b_start)
                abs_end = abs(b_end)
                if abs_end > abs_start:
                    return 'UP'
                elif abs_end < abs_start:
                    return 'DOWN'
                return 'SAME'
            except:
                pass
            return None
            
        def get_real_wdl_helper(score_str, current_ah, is_inverted=False):
            if not score_str or score_str in ['N/A', '?:?', '?-?'] or current_ah is None: 
                return None
            try:
                parts = score_str.replace(' ', '').replace('-', ':').split(':')
                if len(parts) == 2:
                    h, a = int(parts[0]), int(parts[1])
                    
                    # Convención: current_ah > 0 -> Local es Favorito
                    fav_is_local = current_ah > 0
                    abs_ah = abs(current_ah)
                    
                    # Diferencia desde la perspectiva del favorito
                    if is_inverted:
                        # En H2H General, las localías suelen estar invertidas respecto al partido actual
                        diff = (a - h) if fav_is_local else (h - a)
                    else:
                        # H2H Estadio normal
                        diff = (h - a) if fav_is_local else (a - h)
                    
                    if diff > abs_ah: return 'COVER'
                    elif diff < abs_ah: return 'NO_COVER'
                    return 'PUSH'
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
                real_wdl_stadium = get_real_wdl_helper(score_stadium, hist_ah, False)
                h2h_stadium_data = {
                    'movement': mov_stadium,
                    'score': score_stadium,
                    'wdl': sim_wdl_stadium,
                    'mov_direction': mov_dir_stadium,
                    'real_wdl': real_wdl_stadium,
                    'date': stadium_node.get('date')
                }
            
            general_node = market_data.get('general')
            if general_node:
                mov_general = general_node.get('movement')
                score_general = general_node.get('result') or general_node.get('score')
                sim_wdl_general = get_simulated_wdl(score_general, hist_ah, False)
                mov_dir_general = get_general_movement_direction(mov_general)
                real_wdl_general = get_real_wdl_helper(score_general, hist_ah, True)
                h2h_general_data = {
                    'movement': mov_general,
                    'score': score_general,
                    'wdl': sim_wdl_general,
                    'mov_direction': mov_dir_general,
                    'real_wdl': real_wdl_general,
                    'date': general_node.get('date')
                }
                
        else:
            # --- LEGACY HTML PARSING ---
            market_html = match.get('market_analysis_html') or ""
            
            mov_stadium, score_stadium = extract_analysis_data(market_html, 'STADIUM')
            if mov_stadium or score_stadium:
                sim_wdl_stadium = get_simulated_wdl(score_stadium, hist_ah, True)
                mov_dir_stadium = get_movement_direction(mov_stadium)
                real_wdl_stadium = get_real_wdl_helper(score_stadium, hist_ah, False)
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
                mov_dir_general = get_general_movement_direction(mov_general)
                real_wdl_general = get_real_wdl_helper(score_general, hist_ah, True) # FIX: Added missing real_wdl
                h2h_general_data = {
                    'movement': mov_general,
                    'score': score_general,
                    'wdl': sim_wdl_general,
                    'mov_direction': mov_dir_general,
                    'real_wdl': real_wdl_general
                }

        # --- 5.5 NEW: Manual Fallback for H2H Estadio & General ---
        # If no data found in market_analysis, search manually in indexed history
        fallback_h2h_res = None
        if not h2h_stadium_data or not h2h_general_data:
            fallback_h2h_res = _indexed_h2h(home_team_norm, away_team_norm, current_date_obj)

        if not h2h_stadium_data and fallback_h2h_res:
            h2h_res = fallback_h2h_res
            if h2h_res:
                # Only if venue matches current one (Home must be the same as current Local)
                h2h_home = (h2h_res.get('home_team') or '').strip().lower()
                curr_home = home_team.strip().lower()
                if h2h_home == curr_home:
                    # Calculate movement: H2H AH -> Current AH
                    h2h_ah = safe_float_ah(h2h_res.get('ah'))
                    mov_str = 'N/A'
                    mov_dir = 'SAME'
                    if h2h_ah is not None:
                        mov_str = f"{format_ah(h2h_ah)} -> {format_ah(hist_ah)}"
                        mov_dir = get_movement_direction(mov_str)
                    
                    h2h_stadium_data = {
                        'movement': mov_str,
                        'score': h2h_res['score'],
                        'wdl': get_simulated_wdl(h2h_res['score'], hist_ah, True),
                        'mov_direction': mov_dir,
                        'real_wdl': get_real_wdl_helper(h2h_res['score'], hist_ah, False),
                        'date': h2h_res['date']
                    }
        
        if not h2h_general_data and fallback_h2h_res:
            h2h_res = fallback_h2h_res
            if h2h_res:
                # Calculate movement: H2H AH (normalized to current local) -> Current AH
                h2h_ah_raw = safe_float_ah(h2h_res.get('ah'))
                mov_str = 'N/A'
                mov_dir = 'SAME'
                
                h2h_home = (h2h_res.get('home_team') or '').strip().lower()
                curr_home = home_team.strip().lower()
                is_reversed_h2h = bool(h2h_home and curr_home and h2h_home != curr_home)

                if h2h_ah_raw is not None:
                    # Mostrar siempre el signo REAL publicado en el precedente.
                    # La orientación se aplica únicamente al cálculo de cobertura, no a la cuota mostrada.
                    mov_str = f"{format_ah(h2h_ah_raw)} -> {format_ah(hist_ah)}"
                    mov_dir = get_general_movement_direction(mov_str)

                favorite_was_home = (match_favorite_side == 'HOME') != is_reversed_h2h

                h2h_general_data = {
                    'movement': mov_str,
                    'score': h2h_res['score'],
                    'wdl': get_simulated_wdl(h2h_res['score'], hist_ah, favorite_was_home),
                    'mov_direction': mov_dir,
                    'real_wdl': get_real_wdl_helper(h2h_res['score'], hist_ah, is_reversed_h2h),
                    'date': h2h_res['date'],
                    'home_team': h2h_res.get('home_team'),
                    'away_team': h2h_res.get('away_team'),
                    'is_reversed': is_reversed_h2h,
                    'historical_ah': h2h_ah_raw
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
            prev_home_sim = get_simulated_wdl(prev_home_data.get('score'), hist_ah, True)
            if prev_home_sim != target_prev_home_wdl: continue
        if target_prev_home_real_wdl:
            if not prev_home_data: continue
            prev_home_real = get_team_real_wdl(
                prev_home_data.get('score'),
                infer_team_is_home(prev_home_data, home_team_norm, default_is_home=True)
            )
            if prev_home_real != target_prev_home_real_wdl:
                continue

        if filters.get('prev_home_ah'):
            if not prev_home_data: continue
            if not ah_matches_any_bucket(prev_home_data.get('ah'), filters.get('prev_home_ah')):
                continue
            
        if target_prev_away_wdl:
            if not prev_away_data: continue
            prev_away_sim = get_simulated_wdl(prev_away_data.get('score'), hist_ah, False)
            if prev_away_sim != target_prev_away_wdl: continue
        if target_prev_away_real_wdl:
            if not prev_away_data: continue
            prev_away_real = get_team_real_wdl(
                prev_away_data.get('score'),
                infer_team_is_home(prev_away_data, away_team_norm, default_is_home=False)
            )
            if prev_away_real != target_prev_away_real_wdl:
                continue

        if filters.get('prev_away_ah'):
            if not prev_away_data: continue
            if not ah_matches_any_bucket(prev_away_data.get('ah'), filters.get('prev_away_ah')):
                continue
        
        # --- 6.5 H2H Filters ---
        if target_stadium_mov:
            if not h2h_stadium_data: continue
            if h2h_stadium_data.get('mov_direction') != target_stadium_mov: continue
            
        if target_stadium_res:
            if not h2h_stadium_data: continue
            if h2h_stadium_data.get('real_wdl') != target_stadium_res: continue
        
        # Consistent with frontend: hide General if it's the same as Stadium
        is_same_h2h = h2h_stadium_data and h2h_general_data and \
                      h2h_stadium_data.get('date') == h2h_general_data.get('date') and \
                      h2h_stadium_data.get('score') == h2h_general_data.get('score')
            
        if target_general_mov:
            if not h2h_general_data or is_same_h2h: continue
            if h2h_general_data.get('mov_direction') != target_general_mov: continue
            
        if target_general_res:
            if not h2h_general_data or is_same_h2h: continue
            if h2h_general_data.get('real_wdl') != target_general_res: continue

        # --- 6.6 H2H fuzzy filters removed (handled in frontend autoFilterH2H) ---

        # --- 7. H2H Col3 ---
        h2h_col3_data = None
        pre_h2h = match.get('h2h_col3')
        if pre_h2h and isinstance(pre_h2h, dict) and pre_h2h.get('status') == 'found':
            h2h_col3_data = {
                'match_id': str(pre_h2h.get('match_id') or ''),
                'score': f"{pre_h2h.get('goles_home')}:{pre_h2h.get('goles_away')}",
                'date': pre_h2h.get('date'),
                'ah': pre_h2h.get('handicap'),
                'home_team': pre_h2h.get('h2h_home_team_name'),
                'away_team': pre_h2h.get('h2h_away_team_name')
            }
        
        if not h2h_col3_data:
            h2h_res = fallback_h2h_res or _indexed_h2h(home_team_norm, away_team_norm, current_date_obj)
            if h2h_res:
                h2h_col3_data = h2h_res

        if h2h_col3_data and not include_stats and isinstance(h2h_col3_data, dict):
            if 'stats_rows' in h2h_col3_data:
                h2h_col3_data = dict(h2h_col3_data)
                h2h_col3_data['stats_rows'] = []
            elif 'stats' in h2h_col3_data:
                h2h_col3_data = dict(h2h_col3_data)
                h2h_col3_data['stats'] = []

        if filters.get('h2h_col3_ah'):
            if not h2h_col3_data:
                continue
            col3_ah = h2h_col3_data.get('handicap') or h2h_col3_data.get('ah') or h2h_col3_data.get('ah_line')
            if not ah_matches_any_bucket(col3_ah, filters.get('h2h_col3_ah')):
                continue

        match_date_display = match_date_str.split(' ')[0] if match_date_str else 'N/A'

        ind_local = match.get('comparativas_indirectas', {}).get('left') if match.get('comparativas_indirectas') else None
        ind_visitante = match.get('comparativas_indirectas', {}).get('right') if match.get('comparativas_indirectas') else None
        if filters.get('ind_local_ah'):
            if not ind_local:
                continue
            ind_local_ah = ind_local.get('ah_line') or ind_local.get('ah_linea') or ind_local.get('ah')
            if not ah_matches_any_bucket(ind_local_ah, filters.get('ind_local_ah')):
                continue

        if filters.get('ind_visitante_ah'):
            if not ind_visitante:
                continue
            ind_visitante_ah = ind_visitante.get('ah_line') or ind_visitante.get('ah_linea') or ind_visitante.get('ah')
            if not ah_matches_any_bucket(ind_visitante_ah, filters.get('ind_visitante_ah')):
                continue

        if not include_stats:
            if isinstance(ind_local, dict):
                ind_local = dict(ind_local)
                ind_local['stats_rows'] = []
            if isinstance(ind_visitante, dict):
                ind_visitante = dict(ind_visitante)
                ind_visitante['stats_rows'] = []

        res_obj = {
            'candidate': {
                'date': match_date_display,
                'league': match.get('league_name'),
                'home': home_team,
                'away': away_team,
                'score': score,
                'ah_real': hist_ah,
                'favorite_side': match_favorite_side,
                'favorite_result': get_favorite_cover_result(score, hist_ah, False),
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
            'ind_local': ind_local,
            'ind_visitante': ind_visitante,
            'match_id': match.get('match_id') or match.get('id'),
            'home_standings': match.get('home_standings'),
            'away_standings': match.get('away_standings')
        }
        
        results.append(res_obj)
        count += 1
        
    return results
