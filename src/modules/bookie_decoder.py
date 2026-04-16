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
    h, a = parse_score(score_str)
    if h is None: return 0
    margin = abs(h - a)
    ah_abs = abs(parse_line(ah_line))
    return margin - ah_abs if is_favorite_winner else margin + ah_abs

def analyze_match_bookie_logic(match_data):
    home_name = match_data.get('home_name', 'Local')
    away_name = match_data.get('away_name', 'Visitante')
    home_rank = int(match_data.get('home_rank', 99))
    away_rank = int(match_data.get('away_rank', 99))
    
    odds = match_data.get('main_match_odds', {})
    ah_now = parse_line(odds.get('ah_linea', '0'))
    ou_now = parse_line(odds.get('goals_linea', '2.5'))

    is_away_fav = ah_now < 0
    is_home_fav = ah_now > 0
    fav_name = away_name if is_away_fav else (home_name if is_home_fav else "Ninguno")

    prev_h = match_data.get('last_home_match', {})
    prev_a = match_data.get('last_away_match', {})
    st_h = get_detailed_stats(prev_h.get('stats_rows', []), home_name, prev_h.get('home_team'))
    st_a = get_detailed_stats(prev_a.get('stats_rows', []), away_name, prev_a.get('home_team'))
    
    ive_h = calculate_ive_infalible(prev_h.get('score', ''), prev_h.get('handicap_line_raw', '0'), True)
    ive_a = calculate_ive_infalible(prev_a.get('score', ''), prev_a.get('handicap_line_raw', '0'), True)
    
    comp = match_data.get('comparativas_indirectas', {})
    ind_l = comp.get('left', {})
    ind_r = comp.get('right', {})
    f_ind_h = (parse_score(ind_l.get('score', ''))[0] - parse_score(ind_l.get('score', ''))[1]) if ind_l.get('score') else -99
    f_ind_a = (parse_score(ind_r.get('score', ''))[1] - parse_score(ind_r.get('score', ''))[0]) if ind_r.get('score') else -99

    report = {"universe": f"AH {ah_now} | O/U {ou_now}", "labels": [], "justification": [], "recommendation": "Neutral", "confidence": "Baja"}

    # --- REGLA NUEVA 1: LA BURBUJA DE PRESTIGIO (CASO MONTEGO BAY) ---
    fav_ive = ive_a if is_away_fav else ive_h
    fav_prev_ah = parse_line(prev_a.get('handicap_line_raw', '0')) if is_away_fav else parse_line(prev_h.get('handicap_line_raw', '0'))
    fav_rank = away_rank if is_away_fav else home_rank

    if is_away_fav and fav_rank <= 3 and fav_ive <= 0 and abs(ah_now) > abs(fav_prev_ah):
        report["labels"].append("Burbuja de Prestigio (Peligro)")
        report["justification"].append(f"El líder ({fav_name}) viene de NO cubrir su hándicap previo, pero la casa le SUBE la exigencia hoy fuera de casa. Es un inflado de cuota para atraer dinero del público. La estructura defensiva es frágil.")
        report["recommendation"] = f"Local AH +{abs(ah_now)}"; report["confidence"] = "Alta"
        return report # Abortar y priorizar esta alerta

    # --- REGLA NUEVA 2: DESEQUILIBRIO POR FATIGA (CASO SAN ANTONIO) ---
    if is_home_fav and st_h['sot'] >= 9 and ive_h <= 0:
        report["labels"].append("Desequilibrio Ofensivo-Defensivo")
        report["justification"].append(f"{home_name} genera mucho ataque (SOT: {st_h['sot']}) pero no concreta y concede goles. El hándicap negativo es un riesgo de frustración.")
        report["recommendation"] = "Evitar Local / Buscar Under"; report["confidence"] = "Media"

    # REGLA 3: FACTOR SVAY RIENG (LÍNEA ESTANCADA)
    if fav_ive >= 1.5 and abs(ah_now) <= abs(fav_prev_ah):
        report["labels"].append("Infravaloración Crítica (Factor Svay Rieng)")
        report["justification"].append(f"El favorito ({fav_name}) destrozó su línea previa, pero el bookie mantiene el hándicap. El soporte estructural es muy superior a la cuota.")
        report["recommendation"] = f"{fav_name} AH {ah_now}"; report["confidence"] = "Extrema"

    # REGLA 4: FACTOR GUASTATOYA (DIFERENCIAL COL3)
    diff_f = f_ind_h - f_ind_a
    if (is_home_fav and diff_f >= 1.5) or (is_away_fav and diff_f <= -1.5):
        report["labels"].append("Diferencial de Fuerza Crítico")
        report["justification"].append(f"La Col3 revela superioridad masiva de +{abs(diff_f)} goles. El hándicap actual no paga la pegada real.")
        report["recommendation"] = f"{fav_name} AH {ah_now}"; report["confidence"] = "Extrema"

    if not report["labels"]:
        report["labels"].append("Mercado Sincero"); report["recommendation"] = "Neutral"

    return report
