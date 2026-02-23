#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de Backtest para los 200 Patrones de Oro en una liga específica.
Uso: py scripts/backtest_league_gold.py
"""

import sys
import os
import re
import json
import requests
import datetime
import concurrent.futures
from pathlib import Path

# Configuración de Rutas
script_dir = Path(__file__).resolve().parent
project_dir = script_dir.parent
src_dir = project_dir / 'src'

sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(project_dir))
os.chdir(project_dir)

from modules.estudio_scraper import analizar_partido_completo
from scripts.rule_applier import get_rule_applier

# CONFIGURACIÓN DE LA LIGA (User: Revisar en la Portugal 2)
LIGA_URL = "https://football.nowgoal26.com/jsData/matchResult/2024-2025/s157_1787_en.js"
LIGA_NAME = "Liga Portugal 2 (ID 157)"
MAX_MATCHES = 150 # Ampliamos muestra para mayor robustez

def calculate_roi_backtest(match_data, pick_side, odds=1.8):
    """Calcula el profit de un pick basado en el resultado real."""
    final_score = match_data.get('final_score')
    if not final_score or final_score == '??':
        return 0, 'PENDING'
    
    try:
        parts = re.findall(r'\d+', final_score)
        if len(parts) < 2: return 0, 'ERROR'
        gh, ga = int(parts[0]), int(parts[1])
        
        # Handicap actual
        ah_str = match_data.get('main_match_odds', {}).get('ah_linea')
        ah = float(ah_str) if ah_str and ah_str != '-' else 0
        
        # Perspective of Home
        diff = gh - ga - ah
        
        # Outcome for Home
        if diff >= 0.5: outcome_home = 'WIN'
        elif diff == 0.25: outcome_home = 'HALF_WIN'
        elif diff == 0: outcome_home = 'PUSH'
        elif diff == -0.25: outcome_home = 'HALF_LOSS'
        else: outcome_home = 'LOSS'
        
        # Map to pick
        res = outcome_home
        if pick_side == 'AWAY':
            if outcome_home == 'WIN': res = 'LOSS'
            elif outcome_home == 'LOSS': res = 'WIN'
            elif outcome_home == 'HALF_WIN': res = 'HALF_LOSS'
            elif outcome_home == 'HALF_LOSS': res = 'HALF_WIN'
            # PUSH remains PUSH
            
        # Calculate Profit
        if res == 'WIN': return odds - 1, 'WIN'
        elif res == 'HALF_WIN': return (odds - 1) / 2, 'HALF_WIN'
        elif res == 'PUSH': return 0, 'PUSH'
        elif res == 'HALF_LOSS': return -0.5, 'HALF_LOSS'
        elif res == 'LOSS': return -1, 'LOSS'
        
        return 0, 'ERROR'
    except Exception as e:
        return 0, f'ERROR: {e}'

def get_league_match_ids(url):
    """Extrae IDs de partidos jugados desde el JS de la liga."""
    try:
        print(f"📥 Descargando datos de la liga desde {url}...")
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Referer': 'https://football.nowgoal26.com/',
            'Connection': 'keep-alive'
        })
        
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"❌ Error al descargar league JS: {resp.status_code}")
            return []
        
        js_content = resp.text
        # Encontrar matches en jh["R_X"]
        matches_raw = re.findall(r'jh\["R_\d+"\]\s*=\s*\[(.*?)\];', js_content, re.DOTALL)
        
        ids = []
        for round_data in matches_raw:
            m_list = re.findall(r'\[([^\[\]]+)\]', round_data)
            for m_raw in m_list:
                parts = m_raw.split(',')
                if len(parts) < 10: continue
                
                mid = parts[0].strip()
                score = parts[6].strip().strip("'")
                
                # Solo si tiene score final (partido ya jugado)
                if score and '-' in score and score != '?-?':
                    ids.append(mid)
        
        print(f"✅ Encontrados {len(ids)} partidos ya jugados.")
        return ids
    except Exception as e:
        print(f"❌ Error extrayendo IDs: {e}")
        return []

def worker(match_id):
    """Procesa un partido individual: Scrapea -> Pick -> Evalua."""
    try:
        # 1. Scrapea (analizar_partido_completo)
        data = analizar_partido_completo(str(match_id))
        if not data or data.get('error'):
            return None
        
        # 2. Aplica Patrones
        applier = get_rule_applier()
        pick_data = applier.get_best_pick(data)
        
        if not pick_data:
            return {'id': match_id, 'pick': None}
        
        # 3. Evalúa Resultado
        profit, result_type = calculate_roi_backtest(data, pick_data['pick'])
        
        return {
            'id': match_id,
            'home': data.get('home_name'),
            'away': data.get('away_name'),
            'score': data.get('final_score'),
            'ah': data.get('main_match_odds', {}).get('ah_linea'),
            'pick': pick_data['pick'],
            'rule': pick_data['rule_name'],
            'roi_pattern': pick_data.get('samples', 0), # Using samples as proxy or just info
            'samples': pick_data.get('samples', 0),
            'profit': profit,
            'result': result_type,
            'reason': pick_data.get('reason', '')
        }
    except Exception as e:
        print(f"Error procesando {match_id}: {e}")
        return None

def main():
    print("="*50)
    print(f"🚀 Iniciando Backtest: {LIGA_NAME}")
    print("="*50)
    
    match_ids = get_league_match_ids(LIGA_URL)
    if not match_ids:
        return
    
    # Invertimos para empezar por los más recientes si se desea, 
    # o simplemente limitamos.
    match_ids = match_ids[::-1][:MAX_MATCHES]
    print(f"🔄 Procesando los {len(match_ids)} partidos más recientes...")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(worker, mid): mid for mid in match_ids}
        
        count = 0
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            count += 1
            if res and res.get('pick'):
                results.append(res)
                print(f"   [{count}/{len(match_ids)}] 🎯 PICK: {res['home']} vs {res['away']} -> {res['pick']} ({res['result']})")
            else:
                if count % 10 == 0:
                    print(f"   [{count}/{len(match_ids)}] Sin pick...")

    # Generar Informe
    if not results:
        print("\n❌ No se activó ningún patrón en los partidos analizados.")
        return

    total_bets = len(results)
    total_profit = sum(r['profit'] for r in results)
    roi = (total_profit / total_bets) * 100 if total_bets > 0 else 0
    wins = len([r for r in results if r['result'] in ['WIN', 'HALF_WIN']])
    losses = len([r for r in results if r['result'] in ['LOSS', 'HALF_LOSS']])
    pushes = len([r for r in results if r['result'] == 'PUSH'])

    report_path = project_dir / "scripts" / "backtest_informe_ligue1.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(f"# Informe de Backtest: {LIGA_NAME}\n\n")
        f.write(f"- **Picks Totales**: {total_bets}\n")
        f.write(f"- **ROI Final**: {roi:.2f}%\n")
        f.write(f"- **Balance**: {total_profit:+.2f} unidades\n")
        f.write(f"- **W/D/L**: {wins} / {pushes} / {losses}\n\n")
        
        f.write("## Detalle de Picks\n\n")
        f.write("| Partido | AH | Pick | Regla | Resultado | Profit |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
        for r in results:
            f.write(f"| {r['home']} vs {r['away']} | {r['ah']} | {r['pick']} | {r['rule']} | {r['result']} | {r['profit']:+.2f} |\n")
        
        f.write("\n\n### Análisis de ROI por Patrón\n")
        f.write("- Se consideraron cuotas fijas de 1.8.\n")
        f.write("- Los resultados parciales (HALF_WIN/LOSS) se calcularon proporcionalmente.\n")

    print(f"\n✅ Backtest completado. Informe generado en: {report_path}")
    print(f"📊 ROI: {roi:.2f}% en {total_bets} partidos.")

if __name__ == '__main__':
    main()
