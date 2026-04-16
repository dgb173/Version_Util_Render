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

def get_stats_value(stats_rows, team_name, home_team_in_match):
    if not stats_rows:
        return 0, 0
    
    is_home = (team_name == home_team_in_match)
    sot = 0
    da = 0
    
    for row in stats_rows:
        label = row.get('label', '').lower()
        h_val = str(row.get('home', '0'))
        a_val = str(row.get('away', '0'))
        
        if 'tiros a puerta' in label or 'sot' in label:
            val = h_val if is_home else a_val
            sot = int(val) if val.isdigit() else 0
        elif 'ataques peligrosos' in label or 'da' in label:
            val = h_val if is_home else a_val
            da = int(val) if val.isdigit() else 0
    return sot, da

def get_total_match_stats(stats_rows):
    """Calcula los tiros a puerta totales (Local + Visitante) en un partido."""
    if not stats_rows:
        return 0, 0
    total_sot = 0
    total_da = 0
    for row in stats_rows:
        label = row.get('label', '').lower()
        h_val = str(row.get('home', '0'))
        a_val = str(row.get('away', '0'))
        if 'tiros a puerta' in label or 'sot' in label:
            total_sot = (int(h_val) if h_val.isdigit() else 0) + (int(a_val) if a_val.isdigit() else 0)
        elif 'ataques peligrosos' in label or 'da' in label:
            total_da = (int(h_val) if h_val.isdigit() else 0) + (int(a_val) if a_val.isdigit() else 0)
    return total_sot, total_da

def calculate_ive(score_str, ah_line, is_home):
    """Calcula el Índice de Validación de Exigencia (IVE) para Hándicap"""
    h, a = parse_score(score_str)
    if h is None: return 0
    
    margin = h - a
    ah = parse_line(ah_line)
    
    if is_home:
        return margin + ah 
    else:
        return (-margin) - ah 

def calculate_ive_ou(score_str, ou_line):
    """Calcula el Índice de Validación de Exigencia (IVE) para Over/Under"""
    h, a = parse_score(score_str)
    if h is None: return 0
    total_goals = h + a
    ou = parse_line(ou_line)
    return total_goals - ou

def analyze_match_bookie_logic(match_data):
    """
    SISTEMA UNIVERSAL INFALIBLE DE DECODIFICACIÓN DE LA MENTE DEL BOOKIE.
    Analiza anomalías en Hándicap Asiático y Over/Under.
    """
    home_name = match_data.get('home_name', 'Local')
    away_name = match_data.get('away_name', 'Visitante')
    
    # 1. Datos del Mercado Actual
    odds = match_data.get('main_match_odds', {})
    ah_actual = parse_line(odds.get('ah_linea', '0'))
    ou_actual = parse_line(odds.get('goals_linea', '2.5'))
    if ou_actual == 0: ou_actual = 2.5 # default de seguridad
    
    # 2. Datos Forenses (Partidos Previos)
    prev_home = match_data.get('last_home_match', {})
    prev_home_ah = parse_line(prev_home.get('handicap_line_raw', '0'))
    prev_home_ou = parse_line(prev_home.get('over_under_line_raw', '2.5'))
    if prev_home_ou == 0: prev_home_ou = 2.5
    prev_home_score = prev_home.get('score', '')
    
    prev_away = match_data.get('last_away_match', {})
    prev_away_ah = parse_line(prev_away.get('handicap_line_raw', '0'))
    prev_away_ou = parse_line(prev_away.get('over_under_line_raw', '2.5'))
    if prev_away_ou == 0: prev_away_ou = 2.5
    prev_away_score = prev_away.get('score', '')

    # 3. Soportes Estructurales Individuales
    ph_sot, ph_da = get_stats_value(prev_home.get('stats_rows', []), home_name, prev_home.get('home_team'))
    pa_sot, pa_da = get_stats_value(prev_away.get('stats_rows', []), away_name, prev_away.get('home_team'))
    
    # Soportes Estructurales Globales (Para O/U)
    ph_total_sot, ph_total_da = get_total_match_stats(prev_home.get('stats_rows', []))
    pa_total_sot, pa_total_da = get_total_match_stats(prev_away.get('stats_rows', []))
    
    ive_h = calculate_ive(prev_home_score, prev_home_ah, prev_home.get('home_team') == home_name)
    ive_a = calculate_ive(prev_away_score, prev_away_ah, prev_away.get('home_team') == away_name)
    
    ive_ou_h = calculate_ive_ou(prev_home_score, prev_home_ou)
    ive_ou_a = calculate_ive_ou(prev_away_score, prev_away_ou)
    
    # Col3 (Comparativas Indirectas)
    comp = match_data.get('comparativas_indirectas', {})
    ind_l = comp.get('left', {})
    ind_r = comp.get('right', {})
    has_ind_l = bool(ind_l.get('score'))
    has_ind_r = bool(ind_r.get('score'))

    # 4. Deltas de Presión (Salto de Estatus)
    delta_estatus = ah_actual - prev_home_ah
    # Media de OU previas
    prev_ou_avg = (prev_home_ou + prev_away_ou) / 2
    delta_ou = ou_actual - prev_ou_avg

    report = {
        "universe": "",
        "ah_actual": ah_actual,
        "labels": [],
        "justification": [],
        "recommendation": "Neutral",
        "confidence": "Baja"
    }

    # DETERMINACIÓN DEL UNIVERSO
    if abs(ah_actual) < 0.25:
        report["universe"] = f"0.0 (Inercia) | O/U: {ou_actual}"
    elif ah_actual < 0:
        report["universe"] = f"{ah_actual} (Autoridad) | O/U: {ou_actual}"
    else:
        report["universe"] = f"+{ah_actual} (Resistencia) | O/U: {ou_actual}"

    # --- LÓGICA INFALIBLE: HÁNDICAP ---
    ah_recommendation = None
    ah_confidence = "Baja"

    # REGLA 1: EL SALTO ILÓGICO (Provocación)
    if ive_h < -0.5 and delta_estatus < -0.25:
        report["labels"].append("Provocación de Mercado (AH)")
        report["justification"].append(f"[AH] La casa le SUBE la exigencia a {home_name} (de {prev_home_ah} a {ah_actual}) a pesar de fallar su hándicap previo (IVE: {ive_h}). Trampa para apostar en su contra.")
        ah_confidence = "Alta"
        ah_recommendation = "Posible Valor Favorito"

    # REGLA 2: EQUILIBRIO FALSO (Disonancia Col3/Tiros)
    elif abs(ah_actual) < 0.5:
        if ph_sot > pa_sot + 3 and (has_ind_l and not has_ind_r):
            report["labels"].append("Equilibrio Falso (AH)")
            report["justification"].append(f"[AH] El mercado vende igualdad ({ah_actual}), pero el soporte de tiros de {home_name} ({ph_sot}) y su Col3 rompen la balanza a su favor.")
            ah_confidence = "Alta"
            ah_recommendation = f"Local AH {ah_actual}"
        elif pa_sot > ph_sot + 3 and (has_ind_r and not has_ind_l):
            report["labels"].append("Equilibrio Falso (AH)")
            report["justification"].append(f"[AH] El mercado vende igualdad ({ah_actual}), pero {away_name} llega con un soporte de fuego ({pa_sot} tiros) ignorado por la línea.")
            ah_confidence = "Alta"
            ah_recommendation = f"Visitante AH {ah_actual}"

    # REGLA 3: ESCUDO DE TITANIO (Underdog Protegido)
    elif ah_actual >= 0.5:
        if ive_h > 0.5 and ph_sot > 4:
            report["labels"].append("Escudo de Titanio (AH)")
            report["justification"].append(f"[AH] {home_name} es 'perro' (+{ah_actual}), pero viene de un IVE alto ({ive_h}) y buen flujo de tiros ({ph_sot}). La línea le da red de seguridad masiva.")
            ah_confidence = "Alta"
            ah_recommendation = f"Local AH +{ah_actual}"

    # REGLA 4: IMPOSTOR INFLADO (Favorito de Papel)
    elif ah_actual <= -0.5:
        if ph_sot < 3 and pa_sot > 5 and ive_h < 0:
            report["labels"].append("Impostor Inflado (AH)")
            report["justification"].append(f"[AH] {home_name} sale de favorito (-{abs(ah_actual)}), pero sus tiros son anémicos ({ph_sot}) vs {away_name} ({pa_sot}). Hándicap de reputación.")
            ah_confidence = "Media"
            ah_recommendation = f"Visitante AH +{abs(ah_actual)}"

    # REGLA 5: AJUSTE DE CASTIGO (Amnesia)
    elif delta_estatus > 0.5 and ive_h > 1.0:
        report["labels"].append("Ajuste de Castigo (AH)")
        report["justification"].append(f"[AH] El mercado le QUITA exigencia a {home_name} a pesar de que destrozó su hándicap previo. Buscan que desconfíes de él.")
        ah_confidence = "Media"
        ah_recommendation = "Local AH"

    # --- LÓGICA INFALIBLE: OVER/UNDER ---
    ou_recommendation = None
    ou_confidence = "Baja"
    
    # El umbral base de Tiros a Puerta totales para un Over suele ser 8-10.
    avg_total_sot = (ph_total_sot + pa_total_sot) / 2

    # REGLA 1 OU: BURBUJA DE GOLES (Over Falso)
    if delta_ou > 0.25 and ive_ou_h < -0.5 and ive_ou_a < -0.5 and avg_total_sot < 7:
        report["labels"].append("Burbuja de Goles (O/U)")
        report["justification"].append(f"[O/U] La casa SUBE la línea a {ou_actual} a pesar de que ambos fallaron sus overs previos (IVEs negativos) y promedian apenas {avg_total_sot} tiros totales. Trampa para apostar a goles.")
        ou_confidence = "Alta"
        ou_recommendation = f"Under {ou_actual}"

    # REGLA 2 OU: PRESIÓN CONTENIDA (Under Falso)
    elif delta_ou < -0.25 and ive_ou_h > 0.5 and ive_ou_a > 0.5 and avg_total_sot > 11:
        report["labels"].append("Presión Contenida (O/U)")
        report["justification"].append(f"[O/U] La casa BAJA la línea a {ou_actual} pese a que ambos destrozaron sus overs previos y generan un volumen brutal de tiros ({avg_total_sot} totales). Oportunidad masiva.")
        ou_confidence = "Alta"
        ou_recommendation = f"Over {ou_actual}"
        
    # REGLA 3 OU: CONVERSIÓN FRUSTRADA (Mucho tiro, poco gol)
    elif avg_total_sot > 12 and ive_ou_h <= 0 and ive_ou_a <= 0:
        report["labels"].append("Conversión Frustrada (O/U)")
        report["justification"].append(f"[O/U] Generan una barbaridad de tiros a puerta ({avg_total_sot} totales) pero sus últimos partidos fueron Under. La regresión estadística empuja al Over.")
        ou_confidence = "Media"
        ou_recommendation = f"Over {ou_actual}"

    # REGLA 4 OU: CERO DEFENSA (Muchos ataques permitidos)
    elif (ph_total_da + pa_total_da) > 200 and ou_actual <= 2.5:
        report["labels"].append("Caos Táctico (O/U)")
        report["justification"].append(f"[O/U] Ambos equipos promedian muchísimo daño (Ataques Peligrosos altos). La línea de {ou_actual} es vulnerable ante tanto volumen de llegadas.")
        ou_confidence = "Media"
        ou_recommendation = f"Over {ou_actual}"

    # REGLA 5 OU: ESPEJISMO DE GOLES (Pocos tiros, muchos goles previos)
    elif avg_total_sot < 6 and ive_ou_h > 1.0 and ive_ou_a > 1.0:
        report["labels"].append("Espejismo de Goles (O/U)")
        report["justification"].append(f"[O/U] Sus últimos partidos fueron muy Over, pero generaron poquísimos tiros a puerta ({avg_total_sot}). Han tenido una efectividad insostenible. La regresión empuja al Under.")
        ou_confidence = "Alta"
        ou_recommendation = f"Under {ou_actual}"


    # --- SÍNTESIS FINAL ---
    if not report["labels"]:
        report["labels"].append("Mercado Sincero")
        report["justification"].append("Las líneas de Hándicap y Goles reflejan fielmente el rendimiento y estadísticas previas sin manipulaciones detectadas.")
        report["recommendation"] = "Neutral (No Apostar)"
        report["confidence"] = "Baja"
    else:
        # Combinar recomendaciones
        recs = []
        confs = []
        if ah_recommendation:
            recs.append(ah_recommendation)
            confs.append(ah_confidence)
        if ou_recommendation:
            recs.append(ou_recommendation)
            confs.append(ou_confidence)
            
        report["recommendation"] = " | ".join(recs)
        
        # Confianza Global
        if "Alta" in confs:
            report["confidence"] = "Alta"
        elif "Media" in confs:
            report["confidence"] = "Media"
        else:
            report["confidence"] = "Baja"

    return report
