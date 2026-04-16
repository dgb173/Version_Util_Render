import math

def parse_ah(ah_str):
    if ah_str is None or ah_str == '' or ah_str == 'N/A':
        return 0.0
    try:
        # Manejar formatos como -0.5/1.0
        if '/' in str(ah_str):
            parts = str(ah_str).split('/')
            return (float(parts[0]) + float(parts[1])) / 2
        return float(ah_str)
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

def calculate_ive(score_str, ah_line, is_home):
    """Calcula el Índice de Validación de Exigencia (IVE)"""
    h, a = parse_score(score_str)
    if h is None: return 0
    
    margin = h - a
    ah = parse_ah(ah_line)
    
    # Si somos el local, el IVE es (Margen Real) - (Exigencia AH)
    # Ejemplo: Gana 2-0 (margen 2), AH era -0.5. IVE = 2 - 0.5 = 1.5 (Excedente)
    # Ejemplo: Gana 1-0 (margen 1), AH era -1.5. IVE = 1 - 1.5 = -0.5 (Déficit)
    if is_home:
        return margin + ah # AH es negativo para el favorito, así que margin - abs(ah)
    else:
        return (-margin) - ah # Para el visitante

def analyze_match_bookie_logic(match_data):
    """
    SISTEMA UNIVERSAL DE DECODIFICACIÓN DE LA MENTE DEL BOOKIE.
    Analiza CUALQUIER partido buscando anomalías estructurales.
    """
    home_name = match_data.get('home_name', 'Local')
    away_name = match_data.get('away_name', 'Visitante')
    
    # 1. Datos del Mercado Actual
    odds = match_data.get('main_match_odds', {})
    ah_actual = parse_ah(odds.get('ah_linea', '0'))
    
    # 2. Datos Forenses (Partidos Previos)
    prev_home = match_data.get('last_home_match', {})
    prev_home_ah = parse_ah(prev_home.get('handicap_line_raw', '0'))
    prev_home_score = prev_home.get('score', '')
    
    prev_away = match_data.get('last_away_match', {})
    prev_away_ah = parse_ah(prev_away.get('handicap_line_raw', '0'))
    prev_away_score = prev_away.get('score', '')

    # 3. Soportes Estructurales
    ph_sot, ph_da = get_stats_value(prev_home.get('stats_rows', []), home_name, prev_home.get('home_team'))
    pa_sot, pa_da = get_stats_value(prev_away.get('stats_rows', []), away_name, prev_away.get('home_team'))
    
    ive_h = calculate_ive(prev_home_score, prev_home_ah, prev_home.get('home_team') == home_name)
    ive_a = calculate_ive(prev_away_score, prev_away_ah, prev_away.get('home_team') == away_name)
    
    # Col3 (Comparativas Indirectas)
    comp = match_data.get('comparativas_indirectas', {})
    ind_l = comp.get('left', {})
    ind_r = comp.get('right', {})
    has_ind_l = bool(ind_l.get('score'))
    has_ind_r = bool(ind_r.get('score'))

    # 4. Deltas de Presión (Salto de Estatus)
    # Si AH Actual es -0.5 y era 0.0 -> Salto de -0.5 (Aumenta exigencia)
    # Usamos la perspectiva del LOCAL para el delta general
    delta_estatus = ah_actual - prev_home_ah

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
        report["universe"] = "0.0 (Duelo de Inercias)"
    elif ah_actual < 0:
        report["universe"] = f"{ah_actual} (Examen de Autoridad)"
    else:
        report["universe"] = f"+{ah_actual} (Trinchera de Resistencia)"

    # --- LÓGICA DE DETECCIÓN AGRESIVA ---

    # REGLA 1: EL SALTO ILÓGICO (Provocación)
    if ive_h < -0.5 and delta_estatus < -0.25:
        report["labels"].append("Provocación de Mercado")
        report["justification"].append(f"La casa de apuestas le SUBE la exigencia a {home_name} (de {prev_home_ah} a {ah_actual}) a pesar de que viene de fallar estrepitosamente su hándicap previo (IVE: {ive_h}). Están tentando al público a ir en su contra.")
        report["confidence"] = "Alta"
        report["recommendation"] = "Posible Valor Favorito (Trampa de Bookie)"

    # REGLA 2: EQUILIBRIO FALSO (Disonancia Col3/Tiros)
    if abs(ah_actual) < 0.5:
        if ph_sot > pa_sot + 3 and (has_ind_l and not has_ind_r):
            report["labels"].append("Equilibrio Falso (Pivote Vencido)")
            report["justification"].append(f"El mercado vende igualdad ({ah_actual}), pero el soporte de tiros de {home_name} ({ph_sot}) y su comparativa indirecta (Col3) vencen la balanza totalmente a su favor.")
            report["confidence"] = "Alta"
            report["recommendation"] = f"Local AH {ah_actual}"
        elif pa_sot > ph_sot + 3 and (has_ind_r and not has_ind_l):
            report["labels"].append("Equilibrio Falso (Pivote Vencido)")
            report["justification"].append(f"El mercado vende igualdad ({ah_actual}), pero {away_name} llega con un soporte de fuego ({pa_sot} tiros) que la línea actual ignora por completo.")
            report["confidence"] = "Alta"
            report["recommendation"] = f"Visitante AH {ah_actual}"

    # REGLA 3: ESCUDO DE TITANIO (Underdog Protegido)
    if ah_actual >= 0.5:
        if ive_h > 0.5 and ph_sot > 4:
            report["labels"].append("Escudo de Titanio")
            report["justification"].append(f"A pesar de ser el 'perro' del partido (+{ah_actual}), {home_name} viene de cumplir con creces su rol previo (IVE: {ive_h}) y mantiene un flujo de tiros saludable ({ph_sot}). La línea le da una red de seguridad excesiva.")
            report["confidence"] = "Alta"
            report["recommendation"] = f"Local AH +{ah_actual}"

    # REGLA 4: IMPOSTOR INFLADO (Favorito de Papel)
    if ah_actual <= -0.5:
        if ph_sot < 3 and pa_sot > 5 and ive_h < 0:
            report["labels"].append("Impostor Inflado")
            report["justification"].append(f"{home_name} sale como favorito (-{abs(ah_actual)}), pero sus tiros son anémicos ({ph_sot}) comparados con el visitante ({pa_sot}). Es un hándicap basado en el nombre, no en el soporte actual.")
            report["confidence"] = "Media"
            report["recommendation"] = f"Visitante AH +{abs(ah_actual)}"

    # REGLA 5: AMNESIA INDUCIDA (Cambio Raro de H2H)
    # (Si tuviéramos acceso fácil a la media histórica de cuotas, compararíamos aquí)
    if delta_estatus > 0.5 and ive_h > 1.0:
        report["labels"].append("Ajuste de Castigo (Ocultamiento)")
        report["justification"].append(f"El mercado le QUITA exigencia a {home_name} (era mucho más favorito antes) a pesar de que cumplió sobradamente su misión anterior. La casa quiere que desconfíes de su superioridad.")
        report["confidence"] = "Media"
        report["recommendation"] = "Local AH (Aprovechar Devaluación)"

    # REGLA 6: INERCIA CHOCANTE
    if ive_h > 0.5 and ive_a > 0.5 and abs(ah_actual) < 0.5:
        report["labels"].append("Inercia Chocante")
        report["justification"].append("Ambos equipos vienen de destrozar sus hándicaps previos. Es un duelo de fuerzas en su punto máximo. La línea 0.0 es la más honesta posible.")
        report["confidence"] = "Baja"
        report["recommendation"] = "No tocar (Mercado Eficiente)"

    # REGLA 7: VÍCTIMA DE TRANSICIÓN
    if delta_estatus > 1.0:
        report["labels"].append("Víctima de Transición")
        report["justification"].append(f"{home_name} ha pasado de ser un favorito claro a un underdog o igualado de forma súbita. El mercado ha detectado un colapso estructural en su juego.")
        report["confidence"] = "Media"
        report["recommendation"] = "Seguir la caída (Visitante)"

    # SI NO SE DETECTA NADA FUERTE, BUSCAR MATICES
    if not report["labels"]:
        if ah_actual < 0:
            report["labels"].append("Autoridad Rutinaria")
            report["justification"].append(f"Favorito estándar sin anomalías de presión. El hándicap -{abs(ah_actual)} se sostiene por inercia estadística básica.")
        else:
            report["labels"].append("Resistencia Estándar")
            report["justification"].append(f"Underdog estándar. La casa le otorga un hándicap de +{ah_actual} reflejando la diferencia de nivel esperada sin trampas detectadas.")

    return report
