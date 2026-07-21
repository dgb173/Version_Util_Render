import logging
from modules.handicap_similar_analyzer import analizar_partido_handicap_similar

LOGGER = logging.getLogger(__name__)

def analizar_partido_scah(match_data):
    """
    Sustitución de SCAH v10.0 por el Análisis de Hándicap Similar.
    Detecta si faltan partidos generales del visitante en la caché y fuerza recarga.
    """
    if not isinstance(match_data, dict):
        return {
            "alerta": "DATOS INVÁLIDOS",
            "veredicto": "ERROR",
            "explicacion": "No se recibieron datos de partido válidos.",
            "color": "gray"
        }

    # Si falta la clave de partidos del visitante generales (caché vieja),
    # importamos localmente el scraper y forzamos recarga persistente.
    if 'recent_away_matches_all' not in match_data:
        try:
            from modules.estudio_scraper import analizar_partido_completo
            match_id = match_data.get('match_id')
            if match_id:
                LOGGER.info(f"Falta 'recent_away_matches_all' en caché para el partido {match_id}. Forzando refresco...")
                refreshed = analizar_partido_completo(str(match_id), force_refresh=True)
                if refreshed and 'error' not in refreshed:
                    match_data = refreshed
        except Exception as e:
            LOGGER.error(f"Error recargando caché en scah_analyzer para partido: {e}")

    return analizar_partido_handicap_similar(match_data)
