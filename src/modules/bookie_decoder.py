import math
from datetime import datetime

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
    
    # IVE de Rendimiento Inmediato
    ive_h = calculate_ive_infalible(prev_h.get('score', ''), prev_h.get('handicap_line_raw', '0'), True)
    ive_a = calculate_ive_infalible(prev_a.get('score', ''), prev_a.get('handicap_line_raw', '0'), True)

    # --- ANÁLISIS DE COL3 FORENSE (EXPECTATIVA VS REALIDAD) ---
    comp = match_data.get('comparativas_indirectas', {})
    ind_l = comp.get('left', {})
    ind_r = comp.get('right', {})
    
    score_l_h, score_l_r = parse_score(ind_l.get('score', ''))
    score_r_h, score_r_r = parse_score(ind_r.get('score', ''))
    
    # Realidad (Goles de margen)
    f_ind_h = (score_l_h - score_l_r) if score_l_h is not None else -99
    f_ind_a = (score_r_r - score_r_h) if score_r_r is not None else -99
    diff_f = f_ind_h - f_ind_a

    # Expectativa (El Hándicap que el Bookie les puso contra ese rival común)
    ah_col3_h = parse_line(ind_l.get('ah_line', '0'))
    ah_col3_a = parse_line(ind_r.get('ah_line', '0'))

    report = {"universe": f"AH {ah_now} | O/U {ou_now}", "labels": [], "justification": [], "recommendation": "Neutral", "confidence": "Baja"}

    # ========================================================================
    # LA LEY DE LA SOBRECOMPENSACIÓN POR PÁNICO (EL CASO ENERGETIK 2-1)
    # ========================================================================
    if f_ind_h != -99 and f_ind_a != -99:
        # Calculamos cuánto "Infló" el bookie la línea hoy respecto a lo que pensaba de ellos antes
        # Si el local juega contra el rival común con AH -0.25 (poca fe) y hoy sale a -2.25 (fe ciega)
        salto_expectativa_local = abs(ah_now) - abs(ah_col3_h) if is_home_fav else 0
        salto_expectativa_visit = abs(ah_now) - abs(ah_col3_a) if is_away_fav else 0
        
        # El Bookie se equivocó antes:
        # El equipo sobre-rindió brutalmente su AH previo en Col3 (Ej: Energetik 5-2 con AH -0.25)
        # El rival infra-rindió brutalmente su AH previo en Col3 (Ej: Difai 1-8 con AH -1.5)
        
        # Detección del Pánico:
        if is_home_fav and salto_expectativa_local >= 1.0 and diff_f > 0:
            report["labels"].append("Sobrecompensación por Pánico (Trampa AH)")
            report["justification"].append(f"INYECCIÓN DE REALIDAD: El bookie NO confiaba en {home_name} contra el rival común (AH {ah_col3_h}), pero confiaba ciegamente en el visitante (AH {ah_col3_a}). Como ambos le destrozaron sus previsiones, el bookie ha ENTRADO EN PÁNICO y ha inflado la línea de hoy a un irreal {ah_now} para protegerse. Esta línea no nace de una estructura sólida de +2 goles, nace del miedo. El favorito ganará, pero NO cubrirá este hándicap inventado.")
            report["recommendation"] = f"Visitante AH +{abs(ah_now)}"
            report["confidence"] = "Extrema (Infalible 10/10)"
            return report
            
        elif is_away_fav and salto_expectativa_visit >= 1.0 and diff_f < 0:
            report["labels"].append("Sobrecompensación por Pánico (Trampa AH)")
            report["justification"].append(f"INYECCIÓN DE REALIDAD: El bookie infló la línea visitante a {ah_now} por puro pánico reactivo tras fallar sus previsiones (AH {ah_col3_a} vs AH {ah_col3_h}) ante el rival común. Es una línea emocional, no estructural.")
            report["recommendation"] = f"Local AH +{abs(ah_now)}"
            report["confidence"] = "Extrema (Infalible 10/10)"
            return report

    # ========================================================================
    # LEY DE DOMINANCIA MINIMALISTA (Efecto 1-0 en Ligas Under)
    # ========================================================================
    if ou_now <= 2.25 and abs(ah_now) <= 0.75 and abs(diff_f) >= 1.0:
        if (is_home_fav and diff_f >= 1.0) or (is_away_fav and diff_f <= -1.0):
            report["labels"].append("Dominancia Minimalista")
            report["justification"].append(f"Línea sincera en liga Under. El diferencial Col3 (+{abs(diff_f)}) valida el hándicap corto. {fav_name} ganará por la mínima usando ley del mínimo esfuerzo.")
            report["recommendation"] = f"{fav_name} AH {ah_now}"; report["confidence"] = "Alta"
            return report

    # ========================================================================
    # FACTOR GUASTATOYA (Diferencial de Fuerza Crítico Sincero)
    # ========================================================================
    if (is_home_fav and diff_f >= 1.5) or (is_away_fav and diff_f <= -1.5):
        # Si NO hay salto de pánico, entonces la línea corta es un regalo
        if abs(ah_now) <= 1.0:
            report["labels"].append("Diferencial de Fuerza Sincero")
            report["justification"].append(f"La Col3 revela superioridad masiva de +{abs(diff_f)} goles. Como el hándicap actual ({ah_now}) se mantiene bajo (sin pánico de bookie), es una oportunidad real de inversión.")
            report["recommendation"] = f"{fav_name} AH {ah_now}"; report["confidence"] = "Extrema"
            return report

    # ========================================================================
    # REGLAS ESTÁNDAR DE FRICCIÓN
    # ========================================================================
    if is_home_fav and st_h['sot'] >= 9 and ive_h <= 0:
        report["labels"].append("Desequilibrio (Pólvora Mojada)")
        report["justification"].append("Ataca mucho pero no marca y concede goles. El hándicap es un riesgo.")
        report["recommendation"] = "Evitar Favorito"; report["confidence"] = "Media"

    if not report["labels"]:
        report["labels"].append("Mercado Sincero (Sin Anomalías)"); report["recommendation"] = "Neutral"

    return report
