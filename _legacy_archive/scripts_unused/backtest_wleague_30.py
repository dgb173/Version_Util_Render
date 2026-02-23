"""
BACKTEST AUSTRALIA W-LEAGUE 2024-2025
30 partidos - Usando reglas minadas con ROI >= 15%
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.apply_mined_rules import apply_rules_to_match, extract_features

# 30 partidos de la Australia W-League 2024-2025 (scraped de nowgoal)
matches = [
    # Jornada 1
    {"home": "Western United (W)", "away": "Wellington Phoenix (W)", "score": "4:2", "ah": 1.0},
    {"home": "Central Coast Mariners (W)", "away": "Sydney FC (W)", "score": "3:1", "ah": -1.25},
    {"home": "Canberra United (W)", "away": "Brisbane Roar (W)", "score": "3:2", "ah": 0.5},
    {"home": "Adelaide United (W)", "away": "Melbourne Victory (W)", "score": "2:3", "ah": -1.25},
    {"home": "Melbourne City (W)", "away": "Perth Glory (W)", "score": "5:2", "ah": 1.5},
    {"home": "Newcastle Jets (W)", "away": "WS Wanderers (W)", "score": "2:2", "ah": 0.25},
    # Jornada 2
    {"home": "Melbourne Victory (W)", "away": "Melbourne City (W)", "score": "2:3", "ah": -0.25},
    {"home": "Brisbane Roar (W)", "away": "Sydney FC (W)", "score": "1:0", "ah": -0.75},
    {"home": "Wellington Phoenix (W)", "away": "Canberra United (W)", "score": "0:1", "ah": 0.75},
    {"home": "WS Wanderers (W)", "away": "Adelaide United (W)", "score": "0:2", "ah": 1.0},
    {"home": "Perth Glory (W)", "away": "Newcastle Jets (W)", "score": "3:2", "ah": -0.5},
    {"home": "Central Coast Mariners (W)", "away": "Western United (W)", "score": "2:1", "ah": 0.5},
    # Jornada 3
    {"home": "Brisbane Roar (W)", "away": "Perth Glory (W)", "score": "3:0", "ah": 1.25},
    {"home": "Sydney FC (W)", "away": "WS Wanderers (W)", "score": "1:0", "ah": 1.0},
    {"home": "Canberra United (W)", "away": "Adelaide United (W)", "score": "0:2", "ah": -0.25},
    {"home": "Melbourne City (W)", "away": "Central Coast Mariners (W)", "score": "2:2", "ah": 0.75},
    {"home": "Newcastle Jets (W)", "away": "Wellington Phoenix (W)", "score": "0:3", "ah": 0.5},
    {"home": "Melbourne Victory (W)", "away": "Western United (W)", "score": "3:2", "ah": 0.75},
    # Jornada 4+
    {"home": "Perth Glory (W)", "away": "Brisbane Roar (W)", "score": "1:2", "ah": -0.25},
    {"home": "Adelaide United (W)", "away": "Newcastle Jets (W)", "score": "2:0", "ah": 0.75},
    {"home": "Western United (W)", "away": "Sydney FC (W)", "score": "1:3", "ah": -0.5},
    {"home": "WS Wanderers (W)", "away": "Melbourne Victory (W)", "score": "0:2", "ah": -0.5},
    {"home": "Central Coast Mariners (W)", "away": "Canberra United (W)", "score": "1:1", "ah": 0.25},
    {"home": "Wellington Phoenix (W)", "away": "Melbourne City (W)", "score": "1:3", "ah": -1.0},
    # Jornada 23 (más reciente)
    {"home": "Adelaide United (W)", "away": "Sydney FC (W)", "score": "2:1", "ah": 0},
    {"home": "Perth Glory (W)", "away": "Melbourne City (W)", "score": "1:5", "ah": -1.25},
    {"home": "Melbourne Victory (W)", "away": "Brisbane Roar (W)", "score": "2:0", "ah": 0.75},
    {"home": "Newcastle Jets (W)", "away": "Central Coast Mariners (W)", "score": "1:2", "ah": -0.5},
    {"home": "WS Wanderers (W)", "away": "Canberra United (W)", "score": "1:3", "ah": 0},
    {"home": "Wellington Phoenix (W)", "away": "Western United (W)", "score": "1:1", "ah": 0},
]

def calculate_ah_result(home_goals, away_goals, ah_line):
    """Calcula resultado desde perspectiva del FAVORITO."""
    is_home_fav = ah_line >= 0
    
    if is_home_fav:
        fav_diff = home_goals - away_goals
        handicap = ah_line
    else:
        fav_diff = away_goals - home_goals
        handicap = abs(ah_line)
    
    adjusted = fav_diff - handicap
    
    if abs(handicap % 0.5) == 0.25:
        line1 = handicap - 0.25
        line2 = handicap + 0.25
        adj1 = fav_diff - line1
        adj2 = fav_diff - line2
        r1 = 1 if adj1 > 0 else (-1 if adj1 < 0 else 0)
        r2 = 1 if adj2 > 0 else (-1 if adj2 < 0 else 0)
        return (r1 + r2) / 2
    else:
        if adjusted > 0: return 1
        elif adjusted < 0: return -1
        else: return 0

def run_backtest():
    stake = 5
    odds = 1.85
    
    print("=" * 80)
    print("BACKTEST: Australia W-League 2024-2025 (30 partidos)")
    print("=" * 80)
    print(f"Stake: {stake}€ | Cuotas: {odds}")
    print()
    
    total_profit = 0
    total_bets = 0
    wins = 0
    losses = 0
    pushes = 0
    no_pick = 0
    
    print(f"{'#':>2} | {'Local':<22} | {'Score':^5} | {'Visitante':<22} | {'AH':>5} | {'Pick':>6} | {'Res':>10} | {'P/L':>7}")
    print("-" * 95)
    
    for i, m in enumerate(matches, 1):
        parts = m['score'].split(':')
        hg, ag = int(parts[0]), int(parts[1])
        ah = m['ah']
        is_home_fav = ah >= 0
        
        # Simular match de precacheo (sin datos H2H, solo AH)
        simulated_match = {
            'home_name': m['home'],
            'away_name': m['away'],
            'handicap': ah,
            'main_match_odds': {'ah_linea': ah}
        }
        
        # Aplicar reglas
        pick_result = apply_rules_to_match(simulated_match)
        
        if pick_result is None:
            print(f"{i:>2} | {m['home'][:22]:<22} | {m['score']:^5} | {m['away'][:22]:<22} | {ah:>5} | {'---':>6} | {'NO PICK':>10} | {'---':>7}")
            no_pick += 1
            continue
        
        pick = pick_result['pick']  # HOME o AWAY
        rule_name = pick_result['rule_name']
        
        # Calcular resultado de la apuesta
        if pick == 'HOME':
            if is_home_fav:
                result = calculate_ah_result(hg, ag, ah)
            else:
                # Apostamos al HOME pero es el DOG
                result = -calculate_ah_result(hg, ag, ah)
        else:  # AWAY
            if not is_home_fav:
                result = calculate_ah_result(hg, ag, ah)
            else:
                # Apostamos al AWAY pero es el DOG
                result = -calculate_ah_result(hg, ag, ah)
        
        # Calcular profit
        if result > 0:
            if result == 1:
                profit = stake * (odds - 1)
                res_str = "WIN"
                wins += 1
            else:
                profit = stake * (odds - 1) / 2
                res_str = "HALF WIN"
                wins += 0.5
        elif result < 0:
            if result == -1:
                profit = -stake
                res_str = "LOSS"
                losses += 1
            else:
                profit = -stake / 2
                res_str = "HALF LOSS"
                losses += 0.5
        else:
            profit = 0
            res_str = "PUSH"
            pushes += 1
        
        total_profit += profit
        total_bets += 1
        
        fav_team = m['home'][:10] if is_home_fav else m['away'][:10]
        pick_team = m['home'][:10] if pick == 'HOME' else m['away'][:10]
        
        emoji = "✅" if profit > 0 else ("❌" if profit < 0 else "🔄")
        
        print(f"{i:>2} | {m['home'][:22]:<22} | {m['score']:^5} | {m['away'][:22]:<22} | {ah:>5} | {pick:>6} | {res_str:>10} | {emoji}{profit:>6.2f}€")
    
    print("-" * 95)
    print()
    print("=" * 50)
    print("RESUMEN")
    print("=" * 50)
    print(f"Partidos analizados: {len(matches)}")
    print(f"Con pick generado: {total_bets}")
    print(f"Sin pick (no match): {no_pick}")
    print()
    if total_bets > 0:
        print(f"Ganadas: {wins} | Perdidas: {losses} | Push: {pushes}")
        print(f"Win Rate: {wins/total_bets*100:.1f}%")
        print()
        print(f"💰 PROFIT TOTAL: {total_profit:.2f}€")
        print(f"📊 ROI: {total_profit/(total_bets*stake)*100:.1f}%")
    else:
        print("No se generaron picks para estos partidos.")

if __name__ == "__main__":
    run_backtest()
