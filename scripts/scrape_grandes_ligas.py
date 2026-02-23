#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Scraper automático de Grandes Ligas
Scrapea partidos de las 5 grandes ligas europeas (próximos 7 días)
Usa 10 workers en paralelo y detecta si ya están scrapeados
"""
import sys
import os
from pathlib import Path

# Añadir src al path - buscar desde la ubicación del script
script_dir = Path(__file__).resolve().parent
project_dir = script_dir.parent
src_dir = project_dir / 'src'

# Añadir al path
sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(project_dir))

# Cambiar al directorio del proyecto para que las rutas relativas funcionen
os.chdir(project_dir)

import requests
import re
import datetime
import concurrent.futures

# Importar funciones del proyecto
try:
    from modules import data_manager
    from modules.estudio_scraper import analizar_partido_completo
    from modules.specialist_validator import validator  # Importar validador
    print(f"✅ Módulos importados correctamente desde: {src_dir}")
except ImportError as e:
    print(f"❌ Error importando módulos: {e}")
    print(f"   Script dir: {script_dir}")
    print(f"   Project dir: {project_dir}")
    print(f"   Src dir: {src_dir}")
    print(f"   sys.path: {sys.path[:3]}")
    sys.exit(1)


# Configuración de ligas
LIGAS_URLS = {
    'Premier League': {
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s36_en.js',
        'id': 36, 'short': 'PL'
    },
    'La Liga': {
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s31_en.js',
        'id': 31, 'short': 'LL'
    },
    'Serie A': {
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s34_2948_en.js',
        'id': 34, 'short': 'SA'
    },
    'Ligue 1': {
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s11_en.js',
        'id': 11, 'short': 'L1'
    },
    'Bundesliga': {
        'url': 'https://football.nowgoal26.com/jsData/matchResult/2025-2026/s8_en.js',
        'id': 8, 'short': 'BL'
    },
}


def get_upcoming_matches():
    """Obtiene todos los partidos de las grandes ligas en los próximos 7 días."""
    all_matches = []
    now = datetime.datetime.now()
    max_date = now + datetime.timedelta(days=7)
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    for liga_name, liga_info in LIGAS_URLS.items():
        try:
            print(f"🌍 Descargando {liga_name}...")
            response = session.get(liga_info['url'], timeout=10)
            if response.status_code != 200:
                print(f"❌ Error {response.status_code} para {liga_name}")
                continue
            
            js_content = response.text
            
            # Parsear equipos
            teams = {}
            team_match = re.search(r'var arrTeam = \[(.*?)\];', js_content, re.DOTALL)
            if team_match:
                team_str = team_match.group(1)
                team_entries = re.findall(r'\[(\d+),\'[^\']*\',\'[^\']*\',\'([^\']+)\'', team_str)
                for tid, tname in team_entries:
                    teams[int(tid)] = tname
            
            # Parsear partidos
            round_matches = re.findall(r'jh\["R_\d+"\]\s*=\s*\[(.*?)\];', js_content, re.DOTALL)
            
            match_count = 0
            for round_data in round_matches:
                matches_raw = re.findall(r'\[([^\[\]]+)\]', round_data)
                
                for match_raw in matches_raw:
                    parts = match_raw.split(',')
                    if len(parts) < 14:
                        continue
                    
                    try:
                        match_id = parts[0].strip()
                        date_str = parts[3].strip().strip("'")
                        home_id = int(parts[4].strip())
                        away_id = int(parts[5].strip())
                        score = parts[6].strip().strip("'")
                        
                        # Parsear fecha
                        try:
                            match_time = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                        except:
                            continue
                        
                        # Solo partidos futuros
                        if match_time < now or match_time > max_date:
                            continue
                        
                        # Verificar que no tenga resultado
                        clean_score = score.replace("'", "").strip()
                        if clean_score and clean_score != '-' and ':' not in clean_score and '-' in clean_score:
                            continue
                        
                        home_name = teams.get(home_id, f"Team {home_id}")
                        away_name = teams.get(away_id, f"Team {away_id}")
                        
                        all_matches.append({
                            'id': match_id,
                            'league': liga_name,
                            'home_team': home_name,
                            'away_team': away_name,
                            'match_time': match_time
                        })
                        match_count += 1
                        
                    except Exception as e:
                        continue
            
            print(f"   ✅ {liga_name}: {match_count} partidos encontrados en rango (Próximos 7 días)")
            
        except Exception as e:
            print(f"❌ Error con {liga_name}: {e}")
            continue
    
    # Ordenar por fecha
    all_matches.sort(key=lambda x: x['match_time'])
    return all_matches


def scrape_single_match(match_id):
    """Scrapea un partido individual."""
    try:
        # Verificar si ya existe en precacheo (SQL + compat legacy)
        existing = data_manager.get_precacheo_match(str(match_id))
        
        # Verificar también si NO está pendiente en precacheo
        # Porque get_precacheo_match puede devolver false si no está,
        # pero queremos re-scrapear si está incompleto?
        # El user quiere "si ya tiene en caso de que ya tenga que no haga falta"
        
        if existing and existing.get('final_score') and existing.get('final_score') != '??':
             # Ya tiene resultado final o está completo?
             # Para precacheo lo importante es tener los prev_home/away
             if existing.get('prev_home') and existing.get('prev_away'):
                 return {'id': match_id, 'status': 'exists'}
        
        # Scrapear
        result = analizar_partido_completo(str(match_id), check_odds_early=True)
        
        if result and result.get('skipped'):
             return {'id': match_id, 'status': 'skipped'}

        if result and not result.get('error'):
            # Data Manager se encarga de guardar
            # pero analizar_partido_completo ya guarda el preview en caché?
            # No, analizar_partido_completo devuelve el dict.
            # data_manager lo guarda si llamamos a save...
            
            # En app.py api_preview llama a analizar_partido_completo y devuelve JSON
            # Pero NO guarda en precacheo explícitamente a menos que...
            # Espera, analizar_partido_completo hace todo el trabajo pesado.
            # Pero necesitamos guardarlo en el sistema de precacheo.
            
            # data_manager.save_precacheo_match(result) # Esta función existe?
            # Vamos a asumir que sí, o usar save_match_to_json
            
            # Revisando app.py: process_single_precache_worker usa save_match_to_json_thread_safe
            # tras obtener datos.
            
            # Guardar en precacheo usando la función correcta de data_manager
            # Añadir match_id al result si no existe
            if 'match_id' not in result:
                result['match_id'] = str(match_id)
            
            # --- VALIDACIÓN DE REGLAS ---
            try:
                picks = validator.evaluate_match(result)
                result['specialist_picks'] = picks
                if picks:
                    print(f"   🎯 {len(picks)} Picks encontrados para {match_id}")
            except Exception as e:
                print(f"   ⚠️ Error validando reglas para {match_id}: {e}")
            # ---------------------------

            data_manager.save_precacheo_match(result)
            
            return {'id': match_id, 'status': 'scraped'}
        else:
            return {'id': match_id, 'status': 'error'}
            
    except Exception as e:
        print(f"   ⚠️ Error scrapeando {match_id}: {e}")
        return {'id': match_id, 'status': 'error', 'error': str(e)}


def main():
    print("=" * 60)
    print("  SCRAPER AUTOMÁTICO GRANDES LIGAS")
    print("  Próximos 7 días - 10 Workers paralelos")
    print("=" * 60)
    print()
    
    # Obtener partidos
    matches = get_upcoming_matches()
    print(f"\n📊 Total: {len(matches)} partidos encontrados")
    
    if not matches:
        print("⚠️ No hay partidos para scrapear")
        return
    
    # Filtrar los que ya están scrapeados
    to_scrape = []
    already_done = 0
    
    for m in matches:
        existing = data_manager.get_precacheo_match(str(m['id']))
        if existing:
            already_done += 1
        else:
            to_scrape.append(m['id'])
    
    print(f"   ✅ Ya scrapeados: {already_done}")
    print(f"   🔄 Pendientes: {len(to_scrape)}")
    
    if not to_scrape:
        print("\n✅ Todos los partidos ya están scrapeados!")
        return
    
    # Scrapear con 10 workers
    print(f"\n🚀 Iniciando scraping con 10 workers...")
    
    completed = 0
    errors = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_single_match, mid): mid for mid in to_scrape}
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result['status'] == 'scraped':
                completed += 1
                print(f"   ✅ [{completed}/{len(to_scrape)}] {result['id']}")
            elif result['status'] == 'skipped':
                print(f"   ⏭️ {result['id']} Skipped (No AH)")
            elif result['status'] == 'exists':
                completed += 1
            else:
                errors += 1
                print(f"   ❌ {result['id']}: {result.get('error', 'Error desconocido')}")
    
    print()
    print("=" * 60)
    print(f"  RESULTADOS:")
    print(f"  ✅ Scrapeados: {completed}")
    print(f"  ❌ Errores: {errors}")
    print("=" * 60)


if __name__ == '__main__':
    main()
