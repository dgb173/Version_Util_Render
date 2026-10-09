import asyncio
import json
import os
import re
import subprocess
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
    """Limpia pendientes antiguos de precacheo para evitar bloat sin borrar historial reciente."""
    try:
        pending_days = max(1, int(os.getenv('PRECACHEO_PENDING_MAX_AGE_DAYS', '7')))
    except Exception:
        pending_days = 7
    try:
        removed = data_manager.clean_old_precacheo_matches(
            days_threshold=3,
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


def sync_cloud_precacheo_to_local():
    """Actualiza SQL local con el precacheo publicado por GitHub/Render."""
    if os.getenv('GITHUB_ACTIONS') or os.getenv('RENDER'):
        return

    print("Sincronizando precacheo cloud con la base local...")
    try:
        subprocess.run(
            ['git', 'fetch', 'origin', 'main'],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        raw = subprocess.check_output(
            ['git', 'show', 'origin/main:data/data_precacheo.json'],
            cwd=PROJECT_ROOT,
            text=True,
            encoding='utf-8',
        )
        rows = json.loads(raw)
        cloud_rows = [
            row for row in rows
            if isinstance(row, dict)
            and (row.get('match_id') or row.get('id')) not in (None, '')
        ]

        conn = sql_store._connect()
        try:
            for row in cloud_rows:
                sql_store._upsert_match(
                    conn,
                    row,
                    bucket='data_precacheo.json',
                    state='precacheo',
                )
            conn.commit()
        finally:
            conn.close()
        print(
            f"Precacheo cloud sincronizado en local: {len(cloud_rows)} partidos integrados."
        )
    except Exception as exc:
        print(f"[AVISO] No se pudo sincronizar el precacheo cloud: {exc}")


def select_upcoming_with_high_ah(matches: list, base_limit: int = 250) -> list:
    """
    Selecciona hasta `base_limit` (250) partidos estándar por orden cronológico,
    MÁS TODOS los partidos que tengan hándicaps significativos (|AH| >= 1.0:
    1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, -1, -1.25, -1.5, -1.75, -2, -2.25, -2.5, etc.),
    garantizando que nunca se queden fuera aunque se supere el límite de 250.
    """
    clean_matches = filter_youth_matches(matches)

    def is_significant_ah(m: dict) -> bool:
        ah_val = m.get("handicap")
        if ah_val in (None, "", "N/A"):
            odds = m.get("main_match_odds") or {}
            ah_val = odds.get("ah_linea")
        if ah_val in (None, "", "N/A"):
            return False
        try:
            val = float(str(ah_val).replace(",", "."))
            return abs(val) >= 0.95
        except (ValueError, TypeError):
            return False

    selected_ids = set()
    result = []

    # 1. Añadir los primeros `base_limit` partidos cronológicos
    for m in clean_matches:
        mid = str(m.get("id") or m.get("match_id") or "")
        if not mid or mid in selected_ids:
            continue
        if len(result) < base_limit:
            result.append(m)
            selected_ids.add(mid)

    # 2. Añadir adicionalmente TODOS los partidos con |AH| >= 1.0 que no estuvieran ya
    extra_ah_count = 0
    for m in clean_matches:
        mid = str(m.get("id") or m.get("match_id") or "")
        if not mid or mid in selected_ids:
            continue
        if is_significant_ah(m):
            result.append(m)
            selected_ids.add(mid)
            extra_ah_count += 1

    print(
        f"Selección final de próximos: {len(result)} partidos "
        f"({min(len(result), base_limit)} base + {extra_ah_count} extra con |AH| >= 1.0)"
    )
    return result


async def main():
    """
    Función principal que ejecuta ambos scrapers y combina los resultados.
    """
    print("Iniciando el proceso de scraping principal...")
    cleanup_precacheo_stale("PRE")
    try:
        # Obtenemos un universo amplio de partidos próximos (500) y finalizados
        proximos, finalizados = await asyncio.gather(
            get_main_page_matches_async(limit=None),
            get_main_page_finished_matches_async(limit=1500)
        )
        
        print(f"Scraping de listas finalizado. {len(proximos)} partidos próximos y {len(finalizados)} finalizados.")
        
        # Filtrar y seleccionar 250 base + TODOS los de hándicap >= 1.0 / <= -1.0
        print("Filtrando partidos de equipos juveniles y seleccionando cuotas AH objetivo...")
        proximos = proximos  # No extraction cap; Render window is applied separately.
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
    sync_cloud_precacheo_to_local()
    asyncio.run(main())
