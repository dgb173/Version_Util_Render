import math

def parse_line(line_str):
    if line_str is None or line_str == '' or line_str == 'N/A':
        return 0.0
    try:
        if '/' in str(line_str):
            parts = str(line_str).split('/')
            return (float(parts[0]) + float(parts[1])) / 2
        return float(line_str)
    except (ValueError, TypeError):
        return 0.0

def parse_score(score_str):
    if not score_str or ':' not in score_str:
        return None, None
    try:
        parts = score_str.split(':')
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        return None, None

def get_detailed_stats(stats_rows, team_name, home_team_in_match):
    if not stats_rows:
        return {"sot": 0, "da": 0, "efficiency": 0}
    is_home = (team_name == home_team_in_match)
    sot = 0
    da = 0
    for row in stats_rows:
        label = row.get('label', '').lower()
        h_val = str(row.get('home', '0'))
        a_val = str(row.get('away', '0'))
        val = int(h_val if is_home else a_val) if (h_val.isdigit() or a_val.isdigit()) else 0
        if 'tiros a puerta' in label or 'sot' in label: sot = val
        elif 'ataques peligrosos' in label or 'da' in label: da = val
    efficiency = (sot / da * 100) if da > 0 else 0
    return {"sot": sot, "da": da, "efficiency": efficiency}

def calculate_ive_infalible(score_str, ah_line, is_favorite_winner):
    """
    IVE Infalible: Calcula cuánto superó un equipo la línea.
    is_favorite_winner: True si el equipo analizado era el favorito.
    """
    h, a = parse_score(score_str)
    if h is None: return 0
    margin = abs(h - a)
    ah_abs = abs(parse_line(ah_line))
    
    # Si ganó el que debía ganar, el IVE es el margen menos lo que le pedían.
    # Si era -0.5 y ganó 3-0, margin=3, ah=0.5 -> IVE = 2.5
    # Si empató, margin=0, ah=0.5 -> IVE = -0.5
    return margin - ah_abs if is_favorite_winner else margin + ah_abs

def analyze_match_bookie_logic(match_data):
    home_name = match_data.get('home_name', 'Local')
    away_name = match_data.get('away_name', 'Visitante')
    
    # --- 1. MERCADO ACTUAL (CONVENCIÓN: MINUS = VISITANTE) ---
    odds = match_data.get('main_match_odds', {})
    ah_now = parse_line(odds.get('ah_linea', '0'))
    ou_now = parse_line(odds.get('goals_linea', '2.5'))

    # Quién es el favorito según tu lenguaje
    is_away_fav = ah_now < 0
    is_home_fav = ah_now > 0
    fav_name = away_name if is_away_fav else (home_name if is_home_fav else "Ninguno")

    # --- 2. DATOS FORENSES ---
    prev_h = match_data.get('last_home_match', {})
    prev_a = match_data.get('last_away_match', {})
    
    st_h = get_detailed_stats(prev_h.get('stats_rows', []), home_name, prev_h.get('home_team'))
    st_a = get_detailed_stats(prev_a.get('stats_rows', []), away_name, prev_a.get('home_team'))
    
    # IVEs precisos
    ive_h = calculate_ive_infalible(prev_h.get('score', ''), prev_h.get('handicap_line_raw', '0'), True)
    ive_a = calculate_ive_infalible(prev_a.get('score', ''), prev_a.get('handicap_line_raw', '0'), True)
    
    # --- 3. COL3 (DIFERENCIAL DE FUERZA) ---
    comp = match_data.get('comparativas_indirectas', {})
    ind_l = comp.get('left', {})
    ind_r = comp.get('right', {})
    score_l_h, score_l_r = parse_score(ind_l.get('score', ''))
    score_r_h, score_r_r = parse_score(ind_r.get('score', ''))
    f_ind_h = (score_l_h - score_l_r) if score_l_h is not None else -99
    f_ind_a = (score_r_r - score_r_h) if score_r_r is not None else -99

    report = {
        "universe": f"AH {ah_now} ({'V-Fav' if is_away_fav else 'L-Fav'}) | O/U {ou_now}",
        "labels": [], "justification": [], "recommendation": "Neutral", "confidence": "Baja"
    }

    # REGLA 1: FACTOR SVAY RIENG (LÍNEA ESTANCADA)
    fav_prev_ah = parse_line(prev_a.get('handicap_line_raw', '0')) if is_away_fav else parse_line(prev_h.get('handicap_line_raw', '0'))
    fav_ive = ive_a if is_away_fav else ive_h
    
    if fav_ive >= 1.5 and abs(ah_now) <= abs(fav_prev_ah):
        report["labels"].append("Infravaloración Crítica (Factor Svay Rieng)")
        report["justification"].append(f"El favorito ({fav_name}) destrozó su línea previa (IVE: {fav_ive}), pero el bookie mantiene el hándicap en {ah_now}. Es una trampa de desprecio absoluta.")
        report["recommendation"] = f"{fav_name} AH {ah_now}"; report["confidence"] = "Extrema"

    # REGLA 2: FACTOR GUASTATOYA (DIFERENCIAL COL3)
    diff_f = f_ind_h - f_ind_a
    if (is_home_fav and diff_f >= 1.5) or (is_away_fav and diff_f <= -1.5):
        report["labels"].append("Diferencial de Fuerza Crítico")
        report["justification"].append(f"La Col3 revela una superioridad de +{abs(diff_f)} goles que el hándicap actual no paga. {fav_name} es mucho más fuerte de lo que dice su cuota.")
        report["recommendation"] = f"{fav_name} AH {ah_now}"; report["confidence"] = "Extrema"

    # REGLA 3: PÓLVORA MOJADA (EL CASO SAN ANTONIO)
    if is_home_fav and ive_h <= 0 and st_h['sot'] >= 10:
        report["labels"].append("Aviso: Pólvora Mojada")
        report["justification"].append(f"{home_name} domina y tira (SOT: {st_h['sot']}), pero no concreta (IVE: {ive_h}). El hándicap es peligroso por falta de pegada real.")
        report["recommendation"] = "Evitar Local o buscar Under"; report["confidence"] = "Media"

    # REGLA 4: DIVERGENCIA OU
    if ou_now >= 3.0 and abs(ah_now) <= 0.25:
        report["labels"].append("Divergencia de Intercambio")
        report["justification"].append("La casa espera goleada pero no se moja con el favorito. El Over es más seguro que el hándicap.")
        report["recommendation"] = f"Over {ou_now}"; report["confidence"] = "Alta"

    if not report["labels"]:
        report["labels"].append("Mercado Sincero"); report["recommendation"] = "Neutral"

    return report
