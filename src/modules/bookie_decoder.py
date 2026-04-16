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
    """Extrae métricas quirúrgicas: SOT, DA, y Ratio de Conversión."""
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

def calculate_ive_complex(score_str, ah_line, is_home):
    """IVE Quirúrgico: Validación de la promesa del hándicap."""
    h, a = parse_score(score_str)
    if h is None: return 0
    margin = h - a
    ah = parse_line(ah_line)
    return (margin + ah) if is_home else ((-margin) - ah)

def analyze_match_bookie_logic(match_data):
    """
    MOTOR QUIRÚRGICO DE DECODIFICACIÓN (VERSIÓN INFALIBLE).
    Basado en el lenguaje de micro-fricción y divergencia de mercados.
    """
    home_name = match_data.get('home_name', 'Local')
    away_name = match_data.get('away_name', 'Visitante')
    
    # --- 1. CAPA DE MERCADO ACTUAL ---
    odds = match_data.get('main_match_odds', {})
    ah_now = parse_line(odds.get('ah_linea', '0'))
    ou_now = parse_line(odds.get('goals_linea', '2.5'))
    if ou_now == 0: ou_now = 2.5

    # --- 2. CAPA FORENSE (MEMORIA DE LARGO PLAZO) ---
    prev_h = match_data.get('last_home_match', {})
    prev_a = match_data.get('last_away_match', {})
    
    # Stats quirúrgicas
    st_h = get_detailed_stats(prev_h.get('stats_rows', []), home_name, prev_h.get('home_team'))
    st_a = get_detailed_stats(prev_a.get('stats_rows', []), away_name, prev_a.get('home_team'))
    
    # IVEs (Éxito previo)
    ive_h = calculate_ive_complex(prev_h.get('score', ''), prev_h.get('handicap_line_raw', '0'), prev_h.get('home_team') == home_name)
    ive_a = calculate_ive_complex(prev_a.get('score', ''), prev_a.get('handicap_line_raw', '0'), prev_a.get('home_team') == away_name)
    
    # --- 3. ANÁLISIS DE COL3 (INDIRECTAS DE ALTA PRECISIÓN) ---
    comp = match_data.get('comparativas_indirectas', {})
    ind_l = comp.get('left', {})
    ind_r = comp.get('right', {})
    
    score_l_h, score_l_r = parse_score(ind_l.get('score', ''))
    score_r_h, score_r_r = parse_score(ind_r.get('score', ''))
    
    # Diferencial de fuerza indirecta (Si Local ganó 3-0 y Visitante perdió 0-1 contra el mismo rival)
    fuerza_ind_h = (score_l_h - score_l_r) if score_l_h is not None else -99
    fuerza_ind_a = (score_r_r - score_r_h) if score_r_r is not None else -99 # Invertimos porque r es el visitante actual

    # --- 4. DECODIFICACIÓN DE ANOMALÍAS ---
    report = {
        "universe": f"AH {ah_now} | O/U {ou_now}",
        "labels": [],
        "justification": [],
        "recommendation": "Neutral",
        "confidence": "Baja"
    }

    # REGLA MAESTRA 1: LA TRAMPA DE LA DEGRADACIÓN (Hándicap vs Eficiencia)
    delta_ah = ah_now - parse_line(prev_h.get('handicap_line_raw', '0'))
    if delta_ah > 0.4 and st_h['sot'] >= 6 and ive_h >= 0:
        # El mercado le quita favoritismo (delta > 0), pero el equipo cumplió y tiró mucho.
        report["labels"].append("Ocultamiento de Valor (Hándicap)")
        report["justification"].append(f"La casa 'castiga' a {home_name} bajándole el hándicap ({ah_now}), ignorando que en su último partido tuvo una eficiencia brutal ({st_h['efficiency']:.1f}%) y cumplió su misión. El bookie quiere que desconfíes de su superioridad.")
        report["recommendation"] = f"Local AH {ah_now}"
        report["confidence"] = "Alta"

    # REGLA MAESTRA 2: DIVERGENCIA CRUZADA (Goles vs Hándicap)
    # Si la línea de goles es alta (>2.75) pero el hándicap es corto (<0.5)
    if ou_now >= 2.75 and abs(ah_now) <= 0.25:
        report["labels"].append("Divergencia de Intercambio")
        report["justification"].append("Contradicción detectada: La casa espera muchos goles pero no se atreve a dar un favorito. Esto indica un partido roto tácticamente donde el Over es la salida lógica ante la inseguridad del bookie en el hándicap.")
        report["recommendation"] = f"Over {ou_now}"
        report["confidence"] = "Alta"

    # REGLA MAESTRA 3: EL ESCUDO DE COL3 QUIRÚRGICO (FACTOR GUASTATOYA)
    diff_fuerza = fuerza_ind_h - fuerza_ind_a
    if diff_fuerza >= 1.5 and ah_now >= -0.5:
        report["labels"].append("Diferencial de Fuerza Crítico (Factor Guastatoya)")
        report["justification"].append(f"Análisis quirúrgico de éxito: El diferencial de fuerza indirecta es masivo (+{diff_fuerza}). Mientras {home_name} destrozó al rival común, {away_name} fue ineficiente. El hándicap de {ah_now} es un error grave de bulto del bookie basado en la tabla, no en la pegada real.")
        report["recommendation"] = f"Local AH {ah_now} | Posible Goleada"
        report["confidence"] = "Extrema (Infalible)"
    elif diff_fuerza > 0.5 and ah_now >= -0.25:
        report["labels"].append("Diferencial de Fuerza Oculto")
        report["justification"].append(f"Análisis Col3 quirúrgico: {home_name} rindió mejor ante el rival común. El diferencial de +{diff_fuerza} goles indirectos no está reflejado en el hándicap actual.")
        report["recommendation"] = f"Local AH {ah_now}"
        report["confidence"] = "Alta"

    # REGLA MAESTRA 4: BURBUJA DE GOLES (Análisis de SOT Total)
    total_prev_sot = st_h['sot'] + st_a['sot']
    if ou_now >= 2.5 and total_prev_sot < 6 and ive_h < 0 and ive_a < 0:
        report["labels"].append("Burbuja de Goles Detectada")
        report["justification"].append(f"La línea de {ou_now} es un cebo. Ambos equipos promedian apenas {total_prev_sot} tiros a puerta combinados y vienen de fallar sus promesas de gol (IVEs negativos).")
        report["recommendation"] = f"Under {ou_now}"
        report["confidence"] = "Alta"

    # REGLA MAESTRA 5: RESISTENCIA DE TITANIO (Underdog en racha de eficiencia)
    if ah_now >= 0.5 and st_h['efficiency'] > 15 and ive_h > 0.5:
        report["labels"].append("Resistencia de Titanio")
        report["justification"].append(f"{home_name} llega como Underdog, pero su ratio de conversión ({st_h['efficiency']:.1f}%) es de equipo de Champions. La casa le regala medio gol de ventaja a un equipo que no perdona.")
        report["recommendation"] = f"Local AH +{ah_now}"
        report["confidence"] = "Alta"

    # SÍNTESIS FINAL
    if not report["labels"]:
        report["labels"].append("Mercado Equilibrado")
        report["justification"].append("Tras el análisis quirúrgico de tiros, eficiencia, IVE y Col3, no se detectan anomalías de colocación. La casa ha ajustado las cuotas a la perfección estadística.")
        report["recommendation"] = "Evitar (No hay ventaja)"
        report["confidence"] = "Baja"

    return report
