import asyncio
import os
import re
import sys
from pathlib import Path

# Importamos las funciones de scraping desde el nuevo módulo
from scraping_logic import get_main_page_matches_async, get_main_page_finished_matches_async

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))
from modules import data_manager, sql_store  # noqa: E402

# Patrones de equipos juveniles a excluir
EXCLUDE_PATTERNS = [
    r'\bu19\b', r'sub-19', r'sub 19', r'under 19',
]

# Ligas juveniles que SÍ queremos ver (excepciones)
ALLOWED_YOUTH_LEAGUES = [
    'algeria u20 league',
    # Agregar más ligas aquí si es necesario
]


def cleanup_precacheo_stale(tag):
    """Limpia pendientes antiguos de precacheo para evitar bloat."""
    try:
        pending_days = max(0, int(os.getenv('PRECACHEO_PENDING_MAX_AGE_DAYS', '1')))
    except Exception:
        pending_days = 1
    try:
        removed = data_manager.clean_old_precacheo_matches(
            days_threshold=1,
            pending_days_threshold=pending_days,
        )
        print(
            f"[{tag}] Limpieza precacheo ejecutada: eliminados={removed} "
            f"(pending_max_age_days={pending_days})"
        )
    except Exception as exc:
        print(f"[{tag}] Error en limpieza automática de precacheo: {exc}")

def is_youth_match(match):
    """Verifica si el partido es de equipos juveniles (excepto ligas permitidas)"""
    home = (match.get('home_team') or match.get('home') or match.get('home_name') or '').lower()
    away = (match.get('away_team') or match.get('away') or match.get('away_name') or '').lower()
    league = (match.get('league') or match.get('liga') or '').lower()
    
    # Si la liga está en las permitidas, NO filtrar
    for allowed in ALLOWED_YOUTH_LEAGUES:
        if allowed in league:
            return False
    
    text_to_check = f"{home} {away} {league}"
    
    for pattern in EXCLUDE_PATTERNS:
        if re.search(pattern, text_to_check, re.IGNORECASE):
            return True
    return False

def filter_youth_matches(matches):
    """Filtra partidos de equipos juveniles de una lista"""
    original = len(matches)
    filtered = [m for m in matches if not is_youth_match(m)]
    removed = original - len(filtered)
    if removed > 0:
        print(f"  -> Filtrados {removed} partidos U19/U21")
    return filtered

async def main():
    """
    Función principal que ejecuta ambos scrapers y combina los resultados.
    """
    print("Iniciando el proceso de scraping principal...")
    cleanup_precacheo_stale("PRE")
    try:
        # Obtenemos los partidos próximos y los finalizados en paralelo
        proximos, finalizados = await asyncio.gather(
            get_main_page_matches_async(limit=2000), # Aumentamos el límite para tener más datos
            get_main_page_finished_matches_async(limit=1500)
        )
        
        print(f"Scraping de listas finalizado. {len(proximos)} partidos próximos y {len(finalizados)} finalizados.")
        
        # Filtrar equipos juveniles (U19, U21, etc.)
        print("Filtrando partidos de equipos juveniles...")
        proximos = filter_youth_matches(proximos)
        finalizados = filter_youth_matches(finalizados)
        print(f"Después de filtrar: {len(proximos)} próximos y {len(finalizados)} finalizados.")

        # Creamos un diccionario con todos los datos
        scraped_data = {
            "upcoming_matches": proximos,
            "finished_matches": finalizados
        }
        
        # Guardamos snapshot en SQL para consumo de la app
        sql_store.set_json_state('app_main_page_cache_v1', scraped_data)
        print("Snapshot de partidos guardado en SQL correctamente.")
    finally:
        cleanup_precacheo_stale("POST")

if __name__ == "__main__":
    asyncio.run(main())
