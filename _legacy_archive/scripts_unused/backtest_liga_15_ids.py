#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Backtest Liga 15 (K League 1) con match IDs conocidos
"""
import sys
import os
from pathlib import Path

script_dir = Path(__file__).resolve().parent
project_dir = script_dir.parent
src_dir = project_dir / 'src'
sys.path.insert(0, str(src_dir))
sys.path.insert(0, str(project_dir))
os.chdir(project_dir)

import time
import json

try:
    from modules.estudio_scraper import analizar_partido_completo
    print("Modulos importados")
except ImportError as e:
    print(f"Error: {e}")
    sys.exit(1)

from scripts.rule_applier import RuleApplier


# Match IDs de liga 15 (K League 1) obtenidos del navegador
MATCH_IDS = [
    "2522931", "2522932", "2522933", "2522934", "2522935", "2522936",
    "2522937", "2522938", "2522939", "2522940", "2522941", "2522942",
    "2522943", "2522944", "2522945", "2522946", "2522947", "2522948",
    "2523117", "2523118", "2523119", "2523120", "2523121", "2523122"
]


def parse_score(score_str):
    if not score_str or score_str in ['-', '?:?', '??']:
        return None, None
    score_str = str(score_str).replace('-', ':')
    parts = score_str.split(':')
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except:
        return None, None

def settle_ah(home_g, away_g, ah_line, bet_side):
    """
    Calcula resultado de apuesta AH.
    
    En Nowgoal:
    - AH positivo (+0.5): LOCAL recibe handicap, VISITANTE es favorito
    - AH negativo (-0.5): LOCAL da handicap, LOCAL es favorito
    
    La fórmula de settlement siempre suma el AH al score del LOCAL:
    - diff = (home_goals - away_goals) + ah_line
    - Si diff > 0: LOCAL cubre
    - Si diff < 0: VISITANTE cubre
    """
    diff = (home_g - away_g) + ah_line
    
    # Determinar si el bet_side gana
    if bet_side in ['HOME', 'LOCAL']:
        # Apostamos al LOCAL a cubrir
        if diff > 0.25:
            return 'WIN'
        elif diff > 0:
            return 'HALF_WIN'
        elif diff == 0:
            return 'PUSH'
        elif diff >= -0.25:
            return 'HALF_LOSS'
        else:
            return 'LOSS'
    else:
        # Apostamos al VISITANTE a cubrir (inverso del diff)
        if diff < -0.25:
            return 'WIN'
        elif diff < 0:
            return 'HALF_WIN'
        elif diff == 0:
            return 'PUSH'
        elif diff <= 0.25:
            return 'HALF_LOSS'
        else:
            return 'LOSS'



def main():
    print("=" * 70)
    print("BACKTEST LIGA 15 (K League 1)")
    print("=" * 70)
    
    # Cargar reglas
    print("\nCargando reglas...")
    applier = RuleApplier()
    print(f"Reglas: {len(applier.rules)}")
    
    # Scrapear partidos
    print(f"\nScrapeando {len(MATCH_IDS)} partidos...")
    
    matches = []
    for i, mid in enumerate(MATCH_IDS):
        print(f"  [{i+1}/{len(MATCH_IDS)}] {mid}...", end='', flush=True)
        try:
            result = analizar_partido_completo(mid)
            if result and not result.get('error'):
                matches.append(result)
                print(f" OK - {result.get('home_name', '?')} vs {result.get('away_name', '?')}")
            else:
                print(" skip")
        except Exception as e:
            print(f" error: {e}")
        time.sleep(0.3)
    
    print(f"\nPartidos scrapeados: {len(matches)}")
    
    if not matches:
        print("No se pudieron scrapear partidos")
        return
    
    # Backtest
    print("\nEjecutando backtest...")
    
    STAKE = 5.0
    ODDS = 1.85
    
    picks = 0
    wins = 0
    losses = 0
    staked = 0.0
    returned = 0.0
    details = []
    
    for m in matches:
        odds = m.get('main_match_odds', {})
        ah_str = odds.get('ah_linea')
        if not ah_str or ah_str == '-':
            continue
        
        try:
            ah = float(ah_str)
        except:
            continue
        
        score = m.get('final_score', '')
        home_g, away_g = parse_score(score)
        if home_g is None:
            continue
        
        pick = applier.get_best_pick(m)
        if not pick:
            continue
        
        picks += 1
        staked += STAKE
        
        outcome = settle_ah(home_g, away_g, ah, pick['pick'])
        
        if outcome == 'WIN':
            wins += 1
            returned += STAKE * ODDS
        elif outcome == 'HALF_WIN':
            wins += 1
            returned += STAKE + (STAKE * (ODDS - 1) / 2)
        elif outcome == 'PUSH':
            returned += STAKE
        elif outcome == 'HALF_LOSS':
            losses += 1
            returned += STAKE / 2
        else:
            losses += 1
        
        details.append({
            'match': f"{m.get('home_name', '?')[:15]} vs {m.get('away_name', '?')[:15]}",
            'score': score,
            'ah': ah,
            'pick': pick['pick'],
            'outcome': outcome
        })
    
    # Resultados
    profit = returned - staked
    roi = (profit / staked * 100) if staked > 0 else 0
    win_rate = (wins / picks * 100) if picks > 0 else 0
    
    print("\n" + "=" * 70)
    print("RESULTADOS LIGA 15 (K League 1)")
    print("=" * 70)
    print(f"Partidos scrapeados: {len(matches)}")
    print(f"Picks realizados: {picks}")
    print(f"Ganadas: {wins}")
    print(f"Perdidas: {losses}")
    print()
    print(f"Total apostado: EUR{staked:.2f}")
    print(f"Total retornado: EUR{returned:.2f}")
    print(f"PROFIT/LOSS: EUR{profit:+.2f}")
    print()
    print(f"WIN RATE: {win_rate:.1f}%")
    print(f"ROI: {roi:+.1f}%")
    
    # Detalles
    if details:
        print("\n" + "=" * 70)
        print("DETALLE APUESTAS:")
        print("=" * 70)
        for d in details:
            emoji = "[OK]" if d['outcome'] in ['WIN', 'HALF_WIN'] else ("[XX]" if d['outcome'] == 'LOSS' else "[ ]")
            print(f"{emoji} {d['match']:32} | {d['score']:5} | AH={d['ah']:+.2f} | {d['pick']:5} | {d['outcome']}")


if __name__ == '__main__':
    main()
