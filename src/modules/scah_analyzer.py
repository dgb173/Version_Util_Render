import math
import logging

LOGGER = logging.getLogger(__name__)

def _parse_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default

def analizar_partido_scah(match_data):
    """
    Sistema de Correlaciones y Anomalías de Hándicap (SCAH v10.0)
    Analiza un partido en base a la evolución del hándicap, AP y forma reciente.
    """
    if not isinstance(match_data, dict):
        return {
            "alerta": "DATOS INVÁLIDOS",
            "veredicto": "ERROR",
            "explicacion": "No se recibieron datos de partido válidos.",
            "color": "gray"
        }

    # Extracción del AH actual
    ah_actual_raw = match_data.get("handicap")
    if not ah_actual_raw:
        odds = match_data.get("main_match_odds", {})
        if isinstance(odds, dict):
            ah_actual_raw = odds.get("ah_linea")
    
    ah_actual = _parse_float(ah_actual_raw)

    # Intentar obtener el H2H de estadio (último partido entre ellos en este estadio)
    ah_previo = ah_actual # Default
    h2h_estadio = match_data.get("h2h_col3") or match_data.get("last_home_match") # Ajuste según estructura real
    if isinstance(h2h_estadio, dict):
        ah_previo_raw = h2h_estadio.get("ah") or h2h_estadio.get("handicap_line_raw")
        if ah_previo_raw:
            ah_previo = _parse_float(ah_previo_raw)

    # Ataques peligrosos (AP)
    ap_local = 0
    ap_visita = 0
    last_home = match_data.get("last_home_match")
    last_away = match_data.get("last_away_match")
    
    goles_marcados_local = 0
    goles_recibidos_local = 0
    
    if isinstance(last_home, dict):
        stats = last_home.get("stats_rows", [])
        if isinstance(stats, list):
            for row in stats:
                if "ataques peligrosos" in str(row).lower() or "dangerous attacks" in str(row).lower():
                    # This is very simplified, the actual AP might need robust parsing from stats_rows
                    try:
                        ap_local = int(row.get("home", 0))
                    except Exception:
                        pass
        score = str(last_home.get("score", ""))
        if "-" in score:
            parts = score.split("-")
            if len(parts) == 2:
                goles_marcados_local = _parse_float(parts[0])
                goles_recibidos_local = _parse_float(parts[1])

    if isinstance(last_away, dict):
        stats = last_away.get("stats_rows", [])
        if isinstance(stats, list):
            for row in stats:
                if "ataques peligrosos" in str(row).lower() or "dangerous attacks" in str(row).lower():
                    try:
                        ap_visita = int(row.get("away", 0))
                    except Exception:
                        pass
                        
    # Evaluaciones SCAH v10.0
    
    # 1. EFECTO COLAPSO
    if goles_recibidos_local >= 4 and ah_actual < ah_previo - 1.0:
        return {
            "alerta": "EFECTO COLAPSO",
            "veredicto": "VALOR EN FAVORITO (VISITANTE)",
            "explicacion": "Ajuste de pánico por defensa local rota. Línea gigante real, no hype.",
            "color": "#10b981" # Green
        }

    # 2. INFLACIÓN POR HYPE
    if goles_marcados_local >= 3 and ah_actual < ah_previo:
        if ap_local < 80:
            return {
                "alerta": "TRAMPA DE INFLACIÓN (HYPE)",
                "veredicto": "VALOR EN UNDERDOG",
                "explicacion": f"El mercado cobra caro el hype de la goleada. AP {ap_local} no sostiene la línea.",
                "color": "#dc2626" # Red
            }

    # 3. REBAJA DE ÉLITE
    if ah_previo <= -1.25 and ah_actual >= -0.5:
        if ap_visita > 50 and goles_recibidos_local >= 2:
            return {
                "alerta": "REBAJA DE ÉLITE",
                "veredicto": "VALOR EN FAVORITO",
                "explicacion": "Descuento de la bookie por resultados silenciosos. El favorito despertará.",
                "color": "#10b981"
            }

    # 4. INERCIA / LÍNEA VAGA
    if math.isclose(ah_actual, ah_previo, abs_tol=0.1) and ap_local > 0 and ap_local < 60:
        return {
            "alerta": "TRAMPA DE INERCIA",
            "veredicto": "VALOR EN UNDERDOG",
            "explicacion": f"Línea estancada que atrae dinero fácil. AP local es pobre ({ap_local}).",
            "color": "#dc2626"
        }

    # 5. RESPETO TÉCNICO
    if goles_marcados_local <= 1 and goles_recibidos_local >= 1 and ah_actual <= -0.75:
        if ap_local > 80:
            return {
                "alerta": "RESPETO TÉCNICO",
                "veredicto": "VALOR EN FAVORITO",
                "explicacion": f"Mantiene favoritismo pese a malos resultados. Motor fuerte ({ap_local} AP).",
                "color": "#10b981"
            }

    return {
        "alerta": "LÍNEA ESTÁNDAR",
        "veredicto": "NEUTRAL",
        "explicacion": "El mercado parece equilibrado o faltan datos (AP) para clasificar anomalías.",
        "color": "#6b7280" # Gray
    }
