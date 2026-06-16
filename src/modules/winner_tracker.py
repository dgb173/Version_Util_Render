import datetime
from typing import Dict, List, Optional, Tuple, Any

from .data_manager import load_explorer_matches, flag_stale_prev_matches, parse_match_date
from .pattern_search import asian_result
from .estudio_scraper import _normalize_team_name

def classify_data_freshness(match_data: Dict[str, Any]) -> str:
    """
    DIM 1: Frescura de Datos (FRESH_*)
    Mide el gap de días entre el partido y sus previos.
    """
    stale_info = flag_stale_prev_matches(match_data)
    max_gap = stale_info.get('max_gap_days')
    
    if max_gap is None:
        return 'FRESH_MISSING'
    if max_gap <= 30:
        return 'FRESH_OK'
    if max_gap <= 60:
        return 'FRESH_WARN'
    return 'FRESH_STALE'


def classify_movement(market_analysis_data: Dict[str, Any]) -> str:
    """
    DIM 2: Movimiento de Línea (MOV_*)
    Clasifica cómo se movió el handicap en los previos.
    """
    if not market_analysis_data:
        return 'MOV_NONE'
        
    stadium = market_analysis_data.get('stadium') or {}
    movement_str = stadium.get('movement') or 'N/A'
    
    if movement_str == 'N/A':
        general = market_analysis_data.get('general') or {}
        movement_str = general.get('movement') or 'N/A'
        
    if movement_str == 'N/A':
        return 'MOV_NONE'
        
    try:
        normalized = movement_str.replace(' ', '').replace('→', '->').replace(',', '.')
        parts = normalized.split('->')
        if len(parts) == 2:
            start = float(parts[0])
            end = float(parts[1])
            
            if start == end:
                return 'MOV_HOLD'
                
            # FLIP: Cambio de favorito (cruza el 0)
            if (start > 0 and end < 0) or (start < 0 and end > 0):
                return 'MOV_FLIP'
                
            # UP / DOWN
            # Si el favorito actual (end) es local (end > 0):
            #   start -> end
            #   Si end > start (ej. 0.25 -> 0.75): Favorito gana más ventaja (MOV_UP)
            #   Si end < start (ej. 0.75 -> 0.25): Favorito pierde ventaja (MOV_DOWN)
            # Si el favorito actual es visitante (end < 0):
            #   Si end < start (ej. -0.25 -> -0.75): Favorito gana más ventaja (MOV_UP)
            #   Si end > start (ej. -0.75 -> -0.25): Favorito pierde ventaja (MOV_DOWN)
            if abs(end) > abs(start):
                return 'MOV_UP'
            else:
                return 'MOV_DOWN'
    except:
        pass
        
    return 'MOV_NONE'


def classify_underdog_result(match_data: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    """
    DIM 3: Resultado del Underdog (UND_*)
    Calcula si el no-favorito cubrió su línea o no.
    Retorna (UND_CODE, avg_net_result).
    """
    ah = match_data.get('handicap')
    if ah is None:
        ah = match_data.get('main_match_odds', {}).get('ah_linea')
        
    if ah is None or ah == 'N/A':
        return None, None
        
    try:
        ah_val = float(ah)
    except (TypeError, ValueError):
        return None, None
        
    if ah_val == 0:
        return None, None # Sin favorito / underdog definido
        
    score = match_data.get('score') or match_data.get('final_score')
    if not score or score == 'N/A' or ':' not in score:
        return None, None
        
    try:
        parts = score.split(':')
        h = int(parts[0])
        a = int(parts[1])
        
        # Determinar si el Local es favorito (ah_val > 0)
        fav_is_local = ah_val > 0
        
        # Underdog Goals y su handicap (positivo)
        # Si el Local es favorito, el visitante es el underdog con ventaja +ah_val
        # Si el Visitante es favorito, el local es el underdog con ventaja -ah_val
        und_goals = a if fav_is_local else h
        fav_goals = h if fav_is_local else a
        und_handicap = abs(ah_val)
        
        # Descomponer línea de cuartos
        lines = []
        if abs(und_handicap % 0.5) == 0.25:
            lines = [und_handicap - 0.25, und_handicap + 0.25]
        else:
            lines = [und_handicap]
            
        results = []
        for l in lines:
            val = und_goals - fav_goals + l
            if val > 0:
                results.append(1) # Win
            elif val < 0:
                results.append(-1) # Loss
            else:
                results.append(0) # Push
                
        avg_res = sum(results) / len(results)
        
        # Mapear a etiquetas
        if avg_res == 1.0:
            return 'UND_COVER', avg_res
        elif avg_res == 0.5:
            return 'UND_HALF_COVER', avg_res
        elif avg_res == 0.0:
            return 'UND_PUSH', avg_res
        elif avg_res == -0.5:
            return 'UND_HALF_BUST', avg_res
        elif avg_res == -1.0:
            return 'UND_BUST', avg_res
            
    except Exception as e:
        print(f"Error classifying underdog result: {e}")
        
    return None, None


def classify_underdog_form(match_data: Dict[str, Any], fav_is_local: bool) -> str:
    """
    DIM 4: Forma del Underdog en su Previo (UFORM_*)
    Evalúa si el underdog ganó, empató o perdió su partido previo.
    """
    prev_match = match_data.get('last_away_match') if fav_is_local else match_data.get('last_home_match')
    underdog_name = match_data.get('away_name') if fav_is_local else match_data.get('home_name')
    
    if not prev_match or not isinstance(prev_match, dict) or not underdog_name:
        return 'UFORM_NONE'
        
    score = prev_match.get('score')
    if not score or score == 'N/A' or ':' not in score:
        return 'UFORM_NONE'
        
    try:
        parts = score.split(':')
        h = int(parts[0])
        a = int(parts[1])
        
        prev_home = prev_match.get('home_team', '')
        prev_away = prev_match.get('away_team', '')
        
        team_norm = _normalize_team_name(underdog_name)
        home_norm = _normalize_team_name(prev_home)
        away_norm = _normalize_team_name(prev_away)
        
        is_home = (team_norm == home_norm) or (home_norm in team_norm) or (team_norm in home_norm)
        is_away = (team_norm == away_norm) or (away_norm in team_norm) or (team_norm in away_norm)
        
        if not is_home and not is_away:
            # Fallback
            is_home = True
            
        diff = (h - a) if is_home else (a - h)
        
        if diff > 0:
            return 'UFORM_WIN'
        elif diff < 0:
            return 'UFORM_LOSS'
        else:
            return 'UFORM_DRAW'
    except:
        return 'UFORM_NONE'


def _evaluate_prev_match_stats(prev_match: Any, is_home_context: bool) -> Optional[Dict[str, int]]:
    if not prev_match or not isinstance(prev_match, dict):
        return None
        
    stats_rows = prev_match.get('stats_rows', [])
    if not stats_rows:
        return None
        
    stats_dict = {}
    for row in stats_rows:
        label = row.get('label', '').strip()
        try:
            home_val = float(row.get('home', 0))
            away_val = float(row.get('away', 0))
            stats_dict[label] = {'home': home_val, 'away': away_val}
        except:
            continue
            
    stats_keys = ['Tiros', 'Tiros a Puerta', 'Ataques', 'Ataques Peligrosos']
    team_points = 0
    opp_points = 0
    valid_stats = 0
    
    for key in stats_keys:
        if key not in stats_dict:
            continue
            
        home_val = stats_dict[key]['home']
        away_val = stats_dict[key]['away']
        
        if is_home_context:
            t_val, o_val = home_val, away_val
        else:
            t_val, o_val = away_val, home_val
            
        if t_val > o_val:
            team_points += 1
        elif o_val > t_val:
            opp_points += 1
            
        valid_stats += 1
        
    if valid_stats == 0:
        return None
        
    return {
        'team_points': team_points,
        'opp_points': opp_points,
        'valid_count': valid_stats
    }


def classify_stats_dominance(match_data: Dict[str, Any], fav_is_local: bool) -> str:
    """
    DIM 5: Dominio de Stats (STATS_*)
    Compara las estadísticas de los previos de Favorito vs Underdog.
    """
    lhm = match_data.get('last_home_match')
    lam = match_data.get('last_away_match')
    
    local_stats = _evaluate_prev_match_stats(lhm, is_home_context=True)
    visit_stats = _evaluate_prev_match_stats(lam, is_home_context=False)
    
    if not local_stats and not visit_stats:
        return 'STATS_NONE'
        
    l_score = (local_stats['team_points'] - local_stats['opp_points']) if local_stats else 0
    v_score = (visit_stats['team_points'] - visit_stats['opp_points']) if visit_stats else 0
    
    fav_score = l_score if fav_is_local else v_score
    und_score = v_score if fav_is_local else l_score
    
    diff = fav_score - und_score
    
    if diff >= 3:
        return 'STATS_FAV_DOMINANT'
    elif diff >= 1:
        return 'STATS_FAV_EDGE'
    elif diff <= -3:
        return 'STATS_UND_DOMINANT'
    elif diff <= -1:
        return 'STATS_UND_EDGE'
    else:
        return 'STATS_EQUAL'


def classify_h2h_context(market_analysis_data: Dict[str, Any]) -> str:
    """
    DIM 6: Contexto H2H (H2H_*)
    Determina si el favorito suele cubrir en precedentes directos.
    """
    if not market_analysis_data:
        return 'H2H_NONE'
        
    stadium = market_analysis_data.get('stadium') or {}
    general = market_analysis_data.get('general') or {}
    
    cov_stadium = stadium.get('is_covered')
    cov_general = general.get('is_covered')
    
    evals = [e for e in [cov_stadium, cov_general] if e is not None]
    
    if not evals:
        return 'H2H_NONE'
        
    fav_covers = evals.count(True)
    total_evals = len(evals)
    
    if fav_covers == total_evals:
        return 'H2H_FAV_HISTORY'
    elif fav_covers == 0:
        return 'H2H_UND_HISTORY'
    else:
        return 'H2H_MIXED'


def build_full_label(match_data: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Genera la etiqueta compuesta de 6 dimensiones para un partido.
    Retorna (full_label, underdog_result_code, fresh_code).
    """
    ah = match_data.get('handicap')
    if ah is None:
        ah = match_data.get('main_match_odds', {}).get('ah_linea')
        
    if ah is None or ah == 'N/A':
        return None, None, None
        
    try:
        ah_val = float(ah)
    except:
        return None, None, None
        
    if ah_val == 0:
        return None, None, None
        
    fav_is_local = ah_val > 0
    market_data = match_data.get('market_analysis_data') or {}
    
    fresh = classify_data_freshness(match_data)
    mov = classify_movement(market_data)
    und_res, _ = classify_underdog_result(match_data)
    uform = classify_underdog_form(match_data, fav_is_local)
    stats = classify_stats_dominance(match_data, fav_is_local)
    h2h = classify_h2h_context(market_data)
    
    if not und_res:
        return None, None, None
        
    label = f"{fresh}|{mov}|{und_res}|{uform}|{stats}|{h2h}"
    return label, und_res, fresh


def get_roi_contribution(und_res: str, odds: float = 1.90) -> float:
    """
    Calcula el retorno neto apostando CONTRA el underdog (es decir, a favor del favorito).
    Retorno neto basado en 1 unidad apostada.
    """
    if und_res == 'UND_BUST': # El underdog pierde el handicap, el favorito cubre. Ganamos apuesta completa.
        return odds - 1.0
    elif und_res == 'UND_HALF_BUST': # Underdog pierde la mitad, el favorito gana la mitad.
        return (odds - 1.0) / 2.0
    elif und_res == 'UND_PUSH': # Empate exacto con la línea. Retorno nulo.
        return 0.0
    elif und_res == 'UND_HALF_COVER': # Underdog gana la mitad, el favorito pierde la mitad de la apuesta.
        return -0.5
    elif und_res == 'UND_COVER': # El underdog cubre. Perdemos la apuesta completa.
        return -1.0
    return 0.0


def scan_all_historical_buckets() -> List[Dict[str, Any]]:
    """
    Carga todos los partidos del histórico de explorer, los etiqueta
    y retorna la lista de partidos con metadatos de clasificación.
    """
    matches = load_explorer_matches()
    labeled_matches = []
    
    for m in matches:
        label, und_res, fresh = build_full_label(m)
        if not label:
            continue
            
        ah = m.get('handicap')
        if ah is None:
            ah = m.get('main_match_odds', {}).get('ah_linea')
            
        stale_info = flag_stale_prev_matches(m)
        
        labeled_matches.append({
            'match_id': m.get('match_id') or m.get('id'),
            'date': m.get('match_date') or m.get('date'),
            'home': m.get('home_name'),
            'away': m.get('away_name'),
            'league': m.get('league_name'),
            'ah': ah,
            'score': m.get('score') or m.get('final_score'),
            'label': label,
            'fresh_code': fresh,
            'underdog_result': und_res,
            'roi_contrib': get_roi_contribution(und_res),
            'max_gap_days': stale_info.get('max_gap_days'),
            'home_gap_days': stale_info.get('home_gap_days'),
            'away_gap_days': stale_info.get('away_gap_days'),
            'is_stale': stale_info.get('is_stale', False)
        })
        
    return labeled_matches


def get_underdog_bust_stats() -> Dict[str, Any]:
    """
    Genera estadísticas agregadas del comportamiento del underdog.
    """
    all_labeled = scan_all_historical_buckets()
    
    # Filtrar solo los datos no obsoletos para estadísticas principales de valor
    valid_matches = [m for m in all_labeled if m['fresh_code'] != 'FRESH_STALE']
    stale_matches = [m for m in all_labeled if m['fresh_code'] == 'FRESH_STALE']
    
    total_valid = len(valid_matches)
    if total_valid == 0:
        return {
            'total_matches': len(all_labeled),
            'total_valid': 0,
            'total_stale': len(stale_matches),
            'global_bust_rate': 0.0,
            'global_roi': 0.0,
            'patterns': []
        }
        
    # Calcular global bust rate (UND_BUST + UND_HALF_BUST)
    # Nota: consideraremos BUST total como 1.0 y BUST medio como 0.5 para la tasa ponderada de acierto.
    bust_count = sum(1 for m in valid_matches if m['underdog_result'] == 'UND_BUST')
    half_bust_count = sum(1 for m in valid_matches if m['underdog_result'] == 'UND_HALF_BUST')
    
    # Tasa de acierto de apuestas contra el underdog
    global_bust_rate = (bust_count + half_bust_count) / total_valid
    global_roi = sum(m['roi_contrib'] for m in valid_matches) / total_valid * 100.0
    
    # Agrupación por patrones parciales para encontrar combinaciones óptimas
    # Evaluamos combinaciones de dimensiones: Movimiento + Forma + Stats + H2H
    patterns_stats = {}
    
    for m in valid_matches:
        lbl_parts = m['label'].split('|')
        # Partes: 0:fresh, 1:mov, 2:und_res, 3:uform, 4:stats, 5:h2h
        # Patrones combinados interesantes:
        patterns_to_evaluate = [
            # 1. Movimiento individual
            lbl_parts[1],
            # 2. Forma individual
            lbl_parts[3],
            # 3. Stats individual
            lbl_parts[4],
            # 4. H2H individual
            lbl_parts[5],
            # 5. Combinados dobles comunes
            f"{lbl_parts[1]}|{lbl_parts[3]}", # Mov + Forma
            f"{lbl_parts[1]}|{lbl_parts[4]}", # Mov + Stats
            f"{lbl_parts[3]}|{lbl_parts[4]}", # Forma + Stats
            # 6. Combinación triple premium
            f"{lbl_parts[1]}|{lbl_parts[3]}|{lbl_parts[4]}", # Mov + Forma + Stats
            f"{lbl_parts[1]}|{lbl_parts[4]}|{lbl_parts[5]}", # Mov + Stats + H2H
        ]
        
        for pat in patterns_to_evaluate:
            if not pat or '_NONE' in pat or '_MISSING' in pat:
                continue
            if pat not in patterns_stats:
                patterns_stats[pat] = {
                    'pattern': pat,
                    'total': 0,
                    'busts': 0,
                    'half_busts': 0,
                    'roi_sum': 0.0
                }
            
            p_stat = patterns_stats[pat]
            p_stat['total'] += 1
            if m['underdog_result'] == 'UND_BUST':
                p_stat['busts'] += 1
            elif m['underdog_result'] == 'UND_HALF_BUST':
                p_stat['half_busts'] += 1
                
            p_stat['roi_sum'] += m['roi_contrib']
            
    # Formatear patrones ordenados por bust_rate / ROI
    formatted_patterns = []
    for pat, s in patterns_stats.items():
        if s['total'] < 5:  # Muestra mínima razonable para listar
            continue
            
        rate = (s['busts'] + s['half_busts']) / s['total']
        roi = s['roi_sum'] / s['total'] * 100.0
        
        formatted_patterns.append({
            'pattern': pat,
            'total': s['total'],
            'bust_rate': round(rate, 3),
            'roi': round(roi, 1)
        })
        
    # Ordenar por ROI descendente
    formatted_patterns.sort(key=lambda x: x['roi'], reverse=True)
    
    return {
        'total_matches': len(all_labeled),
        'total_valid': total_valid,
        'total_stale': len(stale_matches),
        'global_bust_rate': round(global_bust_rate, 3),
        'global_roi': round(global_roi, 1),
        'patterns': formatted_patterns
    }


def evaluate_precacheo_recommendation(match_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Evalúa un partido próximo del precacheo.
    Compara sus dimensiones con los patrones óptimos históricos de underdogs perdedores (Bust).
    Si coincide con un patrón rentable, genera un pick para apostar CONTRA el underdog (a favor del favorito).
    """
    ah = match_data.get('handicap')
    if ah is None:
        ah = match_data.get('main_match_odds', {}).get('ah_linea')
        
    if ah is None or ah == 'N/A':
        return None
        
    try:
        ah_val = float(ah)
    except:
        return None
        
    if ah_val == 0:
        return None  # Excluidos sin favorito claro
        
    fav_is_local = ah_val > 0
    market_data = match_data.get('market_analysis_data') or {}
    
    # Calcular dimensiones para este partido próximo
    fresh = classify_data_freshness(match_data)
    
    # Si los datos ya son obsoletos para este partido próximo, no recomendamos
    if fresh == 'FRESH_STALE':
        return None
        
    mov = classify_movement(market_data)
    uform = classify_underdog_form(match_data, fav_is_local)
    stats = classify_stats_dominance(match_data, fav_is_local)
    h2h = classify_h2h_context(market_data)
    
    # Obtener estadísticas de patrones históricos rentables
    stats_data = get_underdog_bust_stats()
    patterns = stats_data.get('patterns', [])
    
    # Filtrar patrones rentables con muestra mínima
    # ROI >= 5.0% y total >= 5 partidos
    optimal_patterns = [
        p for p in patterns 
        if p['roi'] >= 5.0 and p['total'] >= 5
    ]
    
    # Buscar el patrón coincidente con mayor ROI
    best_pattern = None
    for p in optimal_patterns:
        pat_str = p['pattern']
        # Un patrón es una cadena como "MOV_UP|UFORM_LOSS" o "MOV_UP"
        parts = pat_str.split('|')
        
        # Verificar si todas las partes del patrón coinciden con las dimensiones de este partido
        matches_pattern = True
        for part in parts:
            if part.startswith('MOV_') and part != mov:
                matches_pattern = False
            elif part.startswith('UFORM_') and part != uform:
                matches_pattern = False
            elif part.startswith('STATS_') and part != stats:
                matches_pattern = False
            elif part.startswith('H2H_') and part != h2h:
                matches_pattern = False
            elif part.startswith('FRESH_') and part != fresh:
                matches_pattern = False
                
        if matches_pattern:
            best_pattern = p
            break
            
    if best_pattern:
        return {
            'name': f"[Anti-Underdog] Patrón: {best_pattern['pattern']}",
            'pick': 'LOCAL' if fav_is_local else 'VISITANTE',
            'type': 'AH',
            'match_id': match_data.get('match_id') or match_data.get('id'),
            'accuracy': best_pattern['bust_rate'],
            'roi': best_pattern['roi'] / 100.0,
            'explanation': f"El underdog cumple con el patrón histórico de fallo {best_pattern['pattern']} (Bust Rate: {round(best_pattern['bust_rate']*100, 1)}%, ROI: {best_pattern['roi']}%).",
            'algorithm': 'ANTI_UNDERDOG'
        }
        
    return None
