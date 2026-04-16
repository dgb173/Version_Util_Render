import math

def parse_line(line_str):
    if line_str is None or line_str == '' or line_str == 'N/A': return 0.0
    try:
        if '/' in str(line_str):
            parts = str(line_str).split('/')
            return (float(parts[0]) + float(parts[1])) / 2
        return float(line_str)
    except (ValueError, TypeError): return 0.0

def parse_score(score_str):
    if not score_str or ':' not in score_str: return None, None
    try:
        parts = score_str.split(':')
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError): return None, None

def get_detailed_stats(stats_rows, team_name, home_team_in_match):
    if not stats_rows: return {"sot": 0, "da": 0, "efficiency": 0}
    is_home = (team_name == home_team_in_match)
    sot, da = 0, 0
    for row in stats_rows:
        label = row.get('label', '').lower()
        h_val = str(row.get('home', '0'))
        a_val = str(row.get('away', '0'))
        val = int(h_val if is_home else a_val) if (h_val.isdigit() or a_val.isdigit()) else 0
        if 'tiros a puerta' in label or 'sot' in label: sot = val
        elif 'ataques peligrosos' in label or 'da' in label: da = val
    return {"sot": sot, "da": da, "efficiency": (sot / da * 100) if da > 0 else 0}

def calculate_ive(score_str, ah_line, is_fav):
    h, a = parse_score(score_str)
    if h is None: return 0
    margin = abs(h - a)
    ah_abs = abs(parse_line(ah_line))
    return margin - ah_abs if is_fav else margin + ah_abs

def analyze_match_bookie_logic(match_data):
    home_name = match_data.get('home_name', 'Local')
    away_name = match_data.get('away_name', 'Visitante')
    home_rank = int(match_data.get('home_rank', 99))
    away_rank = int(match_data.get('away_rank', 99))
    
    odds = match_data.get('main_match_odds', {})
    ah_now = parse_line(odds.get('ah_linea', '0'))
    ou_now = parse_line(odds.get('goals_linea', '2.5'))

    # CONVENCIÓN: MINUS = VISITANTE FAVORITO
    is_away_fav = ah_now < 0
    is_home_fav = ah_now > 0
    fav_name = away_name if is_away_fav else (home_name if is_home_fav else "Ninguno")

    # DATOS PREVIOS
    prev_h = match_data.get('last_home_match', {})
    prev_a = match_data.get('last_away_match', {})
    st_h = get_detailed_stats(prev_h.get('stats_rows', []), home_name, prev_h.get('home_team'))
    st_a = get_detailed_stats(prev_a.get('stats_rows', []), away_name, prev_a.get('home_team'))
    ive_h = calculate_ive(prev_h.get('score', ''), prev_h.get('handicap_line_raw', '0'), True)
    ive_a = calculate_ive(prev_a.get('score', ''), prev_a.get('handicap_line_raw', '0'), True)
    
    # COL3
    comp = match_data.get('comparativas_indirectas', {})
    ind_l = comp.get('left', {})
    ind_r = comp.get('right', {})
    score_l_h, score_l_r = parse_score(ind_l.get('score', ''))
    score_r_h, score_r_r = parse_score(ind_r.get('score', ''))
    f_ind_h = (score_l_h - score_l_r) if score_l_h is not None else -99
    f_ind_a = (score_r_r - score_r_h) if score_r_r is not None else -99
    diff_f = f_ind_h - f_ind_a

    report = {"universe": f"AH {ah_now} | O/U {ou_now}", "ah_actual": ah_now, "labels": [], "justification": [], "recommendation": "Neutral", "confidence": "Baja"}

    # 1. REGLA DE INVERSIÓN (FAVORITO EQUIVOCADO)
    if is_away_fav and diff_f >= 1.0:
        report["labels"].append("FAVORITO EQUIVOCADO (COL3)")
        report["justification"].append(f"El bookie marca al visitante como favorito, pero la Col3 dice que el Local es superior por +{diff_f} goles. Valor masivo en el hándicap positivo del Local.")
        report["recommendation"] = f"Local AH +{abs(ah_now)}"; report["confidence"] = "Extrema"; return report

    if is_home_fav and diff_f <= -1.0:
        report["labels"].append("FAVORITO EQUIVOCADO (COL3)")
        report["justification"].append(f"El bookie marca al local como favorito, pero la Col3 dice que el Visitante es superior por +{abs(diff_f)} goles. Valor masivo en el Visitante.")
        report["recommendation"] = f"Visitante AH {ah_now}"; report["confidence"] = "Extrema"; return report

    # 2. FACTOR SVAY RIENG (LÍNEA ESTANCADA)
    fav_prev_ah = parse_line(prev_a.get('handicap_line_raw', '0')) if is_away_fav else parse_line(prev_h.get('handicap_line_raw', '0'))
    fav_ive = ive_a if is_away_fav else ive_h
    if fav_ive >= 1.5 and abs(ah_now) <= abs(fav_prev_ah):
        report["labels"].append("Infravaloración (Línea Estancada)")
        report["justification"].append(f"El favorito destrozó su hándicap previo, pero la casa no le sube la exigencia. Es un regalo estructural.")
        report["recommendation"] = f"{fav_name} AH {ah_now}"; report["confidence"] = "Extrema"; return report

    # 3. BURBUJA DE PRESTIGIO
    if is_away_fav and away_rank <= 3 and fav_ive <= 0 and abs(ah_now) > abs(fav_prev_ah):
        report["labels"].append("Burbuja de Prestigio (Peligro)")
        report["justification"].append(f"El líder viene de fallar pero el bookie le sube el hándicap para atrapar dinero. No entres al favorito.")
        report["recommendation"] = f"Local AH +{abs(ah_now)}"; report["confidence"] = "Alta"; return report

    # 4. PÓLVORA MOJADA
    if is_home_fav and st_h['sot'] >= 9 and ive_h <= 0:
        report["labels"].append("Pólvora Mojada")
        report["justification"].append(f"El local tira mucho ({st_h['sot']}) pero no marca. El hándicap negativo es una trampa de frustración.")
        report["recommendation"] = "Under o Evitar Local"; report["confidence"] = "Media"; return report

    # 5. DIFERENCIAL DE FUERZA SINCERO
    if abs(diff_f) >= 1.0:
        report["labels"].append("Diferencial de Fuerza Sincero")
        report["justification"].append(f"Superioridad de +{abs(diff_f)} goles validada en Col3. El hándicap actual es una oportunidad real.")
        report["recommendation"] = f"{fav_name} AH {ah_now}"; report["confidence"] = "Alta"; return report

    if not report["labels"]:
        report["labels"].append("Mercado Sincero"); report["recommendation"] = "Neutral"
    return report
