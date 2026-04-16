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
    
    # Análisis de Recencia (Factor Crítico)
    # Si la goleada es vieja, bajamos el peso.
    st_h = get_detailed_stats(prev_h.get('stats_rows', []), home_name, prev_h.get('home_team'))
    st_a = get_detailed_stats(prev_a.get('stats_rows', []), away_name, prev_a.get('home_team'))
    
    # Col3 Forense
    comp = match_data.get('comparativas_indirectas', {})
    ind_l = comp.get('left', {})
    ind_r = comp.get('right', {})
    score_l_h, score_l_r = parse_score(ind_l.get('score', ''))
    score_r_h, score_r_r = parse_score(ind_r.get('score', ''))
    f_ind_h = (score_l_h - score_l_r) if score_l_h is not None else -99
    f_ind_a = (score_r_r - score_r_h) if score_r_r is not None else -99
    diff_f = f_ind_h - f_ind_a

    report = {"universe": f"AH {ah_now} | O/U {ou_now}", "labels": [], "justification": [], "recommendation": "Neutral", "confidence": "Baja"}

    # --- NUEVA REGLA: CEBO DE GOLEADA (CASO ENERGETIK - FAIL) ---
    if abs(ah_now) >= 2.0:
        # Si le piden ganar por 3 y no viene de golear en el ultimo, es peligroso.
        h_prev, a_prev = parse_score(prev_h.get('score', '')) if is_home_fav else parse_score(prev_a.get('score', ''))
        margen_prev = abs(h_prev - a_prev) if h_prev is not None else 0
        if margen_prev < 2:
            report["labels"].append("Cebo de Goleada (Peligro AH Largo)")
            report["justification"].append(f"La casa de apuestas exige una victoria por 3 o más goles ({ah_now}), pero el favorito no viene de una dinámica de goleada reciente. Es una trampa basada en el historial antiguo del rival. El AH negativo es de altísimo riesgo.")
            report["recommendation"] = f"Underdog AH +{abs(ah_now)} / Under {ou_now}"; report["confidence"] = "Media"
            return report

    # LEY DE DOMINANCIA MINIMALISTA (Para hándicaps cortos)
    if ou_now <= 2.25 and abs(ah_now) <= 0.75 and abs(diff_f) >= 1.0:
        if (is_home_fav and diff_f >= 1.0) or (is_away_fav and diff_f <= -1.0):
            report["labels"].append("Dominancia Minimalista (Efecto 1-0)")
            report["justification"].append(f"Superioridad táctica en liga de baja anotación. {fav_name} ganará por la mínima.")
            report["recommendation"] = f"{fav_name} AH {ah_now}"; report["confidence"] = "Extrema"
            return report

    # DIFERENCIAL DE FUERZA CRÍTICO (FACTOR GUASTATOYA)
    if (is_home_fav and diff_f >= 1.5) or (is_away_fav and diff_f <= -1.5):
        report["labels"].append("Diferencial de Fuerza Crítico")
        report["justification"].append(f"Superioridad masiva detectada en Col3 (+{abs(diff_f)}). El hándicap no refleja la realidad.")
        report["recommendation"] = f"{fav_name} AH {ah_now}"; report["confidence"] = "Alta"

    if not report["labels"]:
        report["labels"].append("Mercado Equilibrado"); report["recommendation"] = "Neutral"

    return report
