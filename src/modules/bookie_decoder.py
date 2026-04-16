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

def analyze_match_bookie_logic(match_data):
    home_name = match_data.get('home_name', 'Local')
    away_name = match_data.get('away_name', 'Visitante')
    
    odds = match_data.get('main_match_odds', {})
    ah_now = parse_line(odds.get('ah_linea', '0'))
    ou_now = parse_line(odds.get('goals_linea', '2.5'))

    # CONVENCIÓN: MINUS = VISITANTE FAVORITO
    is_away_fav = ah_now < 0
    is_home_fav = ah_now > 0
    
    # Análisis de Col3 Quirúrgico
    comp = match_data.get('comparativas_indirectas', {})
    ind_l = comp.get('left', {})
    ind_r = comp.get('right', {})
    score_l_h, score_l_r = parse_score(ind_l.get('score', ''))
    score_r_h, score_r_r = parse_score(ind_r.get('score', ''))
    
    f_ind_h = (score_l_h - score_l_r) if score_l_h is not None else -99
    f_ind_a = (score_r_r - score_r_h) if score_r_r is not None else -99
    
    # Diferencial Real (Positivo = Local mejor, Negativo = Visitante mejor)
    diff_f = f_ind_h - f_ind_a

    report = {
        "universe": f"AH {ah_now} | O/U {ou_now}",
        "ah_actual": ah_now,
        "labels": [], "justification": [], "recommendation": "Neutral", "confidence": "Baja"
    }

    # ========================================================================
    # REGLA MAESTRA: DETECCIÓN DE FAVORITO EQUIVOCADO (CASO BAKU VS SAFA)
    # ========================================================================
    # El bookie dice que el favorito es el visitante (ah_now < 0), pero la Col3 dice que es el local (diff_f > 1.5)
    if is_away_fav and diff_f >= 1.5:
        report["labels"].append("INVERSIÓN DE PODER: FAVORITO EQUIVOCADO")
        report["justification"].append(f"El bookie marca al visitante como favorito ({ah_now}), pero la Col3 revela que el LOCAL ({home_name}) es estructuralmente superior por +{diff_f} goles. La casa ha puesto el favorito al revés basándose en la tabla. El hándicap positivo del local es un regalo.")
        report["recommendation"] = f"Local AH +{abs(ah_now)} / 0.0"; report["confidence"] = "Extrema"
        return report

    # El bookie dice que el favorito es el local (ah_now > 0), pero la Col3 dice que es el visitante (diff_f < -1.5)
    if is_home_fav and diff_f <= -1.5:
        report["labels"].append("INVERSIÓN DE PODER: FAVORITO EQUIVOCADO")
        report["justification"].append(f"El bookie marca al local como favorito ({ah_now}), pero la Col3 revela que el VISITANTE ({away_name}) es superior por +{abs(diff_f)} goles. El hándicap positivo del visitante es de altísimo valor.")
        report["recommendation"] = f"Visitante AH {ah_now} (Signo Inverso)"; report["confidence"] = "Extrema"
        return report

    # ========================================================================
    # LEY DE SOBRECOMPENSACIÓN POR PÁNICO (Hándicaps inflados)
    # ========================================================================
    ah_col3_h = parse_line(ind_l.get('ah_line', '0'))
    ah_col3_a = parse_line(ind_r.get('ah_line', '0'))
    salto_l = abs(ah_now) - abs(ah_col3_h) if is_home_fav else 0
    salto_v = abs(ah_now) - abs(ah_col3_a) if is_away_fav else 0

    if (salto_l >= 1.25 or salto_v >= 1.25) and abs(diff_f) < 2.0:
        report["labels"].append("Sobrecompensación por Pánico")
        report["justification"].append(f"El bookie ha inflado el hándicap actual ({ah_now}) comparado con lo que pedía antes. Es una línea de miedo, no de fuerza. El favorito sufrirá para cubrirla.")
        report["recommendation"] = f"Apostar CONTRA el favorito {ah_now}"; report["confidence"] = "Alta"
        return report

    # FACTOR GUASTATOYA (Sincero)
    if (is_home_fav and diff_f >= 1.5) or (is_away_fav and diff_f <= -1.5):
        report["labels"].append("Diferencial de Fuerza Sincero")
        report["justification"].append(f"Superioridad estructural de +{abs(diff_f)} goles validada. El hándicap es una oportunidad real.")
        report["recommendation"] = f"Seguir Favorito AH {ah_now}"; report["confidence"] = "Extrema"
        return report

    if not report["labels"]:
        report["labels"].append("Mercado Sincero"); report["recommendation"] = "Neutral"

    return report
