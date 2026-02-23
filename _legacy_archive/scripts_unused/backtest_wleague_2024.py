"""
Backtest Australia W-League 2024-2025
Datos extraídos de nowgoal26.com/subleague/2024-2025/729
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.apply_mined_rules import apply_rules_to_match

# Partidos de la temporada 2024-2025 (extraídos de nowgoal)
matches_2024_2025 = [
    # Jornada 1 (Nov 2024)
    {"date": "2024-11-01", "home": "Western United (W)", "away": "Wellington Phoenix (W)", "score": "4:2", "ah": 1.0},
    {"date": "2024-11-02", "home": "Central Coast Mariners (W)", "away": "Sydney FC (W)", "score": "3:1", "ah": -1.25},
    {"date": "2024-11-02", "home": "Canberra United (W)", "away": "Brisbane Roar (W)", "score": "3:2", "ah": 0.5},
    {"date": "2024-11-02", "home": "Adelaide United (W)", "away": "Melbourne Victory (W)", "score": "2:3", "ah": -1.25},
    {"date": "2024-11-03", "home": "Melbourne City (W)", "away": "Perth Glory (W)", "score": "5:2", "ah": 1.5},
    {"date": "2024-11-03", "home": "Newcastle Jets (W)", "away": "WS Wanderers (W)", "score": "2:2", "ah": 0.25},
    
    # Jornada 2 (Nov 2024)
    {"date": "2024-11-09", "home": "Melbourne Victory (W)", "away": "Melbourne City (W)", "score": "2:3", "ah": -0.25},
    {"date": "2024-11-09", "home": "Brisbane Roar (W)", "away": "Sydney FC (W)", "score": "1:0", "ah": -0.75},
    {"date": "2024-11-10", "home": "Wellington Phoenix (W)", "away": "Canberra United (W)", "score": "0:1", "ah": 0.75},
    {"date": "2024-11-10", "home": "WS Wanderers (W)", "away": "Adelaide United (W)", "score": "0:2", "ah": 1.0},
    {"date": "2024-11-10", "home": "Perth Glory (W)", "away": "Newcastle Jets (W)", "score": "3:2", "ah": -0.5},
    
    # Jornada 3 (Nov 2024)
    {"date": "2024-11-15", "home": "Brisbane Roar (W)", "away": "Perth Glory (W)", "score": "3:0", "ah": 1.25},
    {"date": "2024-11-16", "home": "Sydney FC (W)", "away": "WS Wanderers (W)", "score": "1:0", "ah": 1.0},
    {"date": "2024-11-16", "home": "Canberra United (W)", "away": "Adelaide United (W)", "score": "0:2", "ah": -0.25},
    {"date": "2024-11-16", "home": "Melbourne City (W)", "away": "Central Coast Mariners (W)", "score": "2:2", "ah": 0.75},
    
    # Jornada 23 (Abril 2025 - Más reciente)
    {"date": "2025-04-18", "home": "Adelaide United (W)", "away": "Sydney FC (W)", "score": "2:1", "ah": 0},
    {"date": "2025-04-18", "home": "Perth Glory (W)", "away": "Melbourne City (W)", "score": "1:5", "ah": -1.25},
    {"date": "2025-04-19", "home": "Melbourne Victory (W)", "away": "Brisbane Roar (W)", "score": "2:0", "ah": 0.75},
    {"date": "2025-04-19", "home": "Newcastle Jets (W)", "away": "Central Coast Mariners (W)", "score": "1:2", "ah": -0.5},
    {"date": "2025-04-19", "home": "WS Wanderers (W)", "away": "Canberra United (W)", "score": "1:3", "ah": 0},
    {"date": "2025-04-20", "home": "Wellington Phoenix (W)", "away": "Western United (W)", "score": "1:1", "ah": 0},
]

def asian_result(team_goals, opp_goals, ah_line):
    """Calcula resultado AH."""
    diff = team_goals - opp_goals
    line = float(ah_line)
    
    lines = []
    if abs(line % 0.5) == 0.25:
        if line > 0: lines = [line - 0.25, line + 0.25]
        else: lines = [line + 0.25, line - 0.25]
    else:
        lines = [line]
    
    results = []
    for l in lines:
        val = diff + l
        if val > 0: results.append(1)
        elif val < 0: results.append(-1)
        else: results.append(0)
    
    return sum(results) / len(results)

def get_movement_direction(mov_str):
    return None  # No tenemos datos de movimiento para estos partidos

def get_da_diff(stats_rows):
    return None  # No tenemos datos de DA

def extract_simple_features(ah):
    """Extrae features básicas para aplicar reglas."""
    is_home_fav = ah >= 0
    
    features = {
        'h2h_stadium_res': None,
        'h2h_stadium_mov': None,
        'h2h_general_res': None,
        'h2h_general_mov': None,
        'fav_da_diff': None,
        'fav_da_cat': None,
        'ah_delta': None,
        'is_home_fav': is_home_fav
    }
    return features

def run_backtest():
    stake = 5
    odds = 1.85
    
    print("=" * 60)
    print("BACKTEST: Australia W-League 2024-2025")
    print("=" * 60)
    print(f"Stake: €{stake} | Cuotas AH: {odds}")
    print(f"Total partidos: {len(matches_2024_2025)}")
    print()
    
    # Para este backtest, como no tenemos datos completos de H2H/movimientos,
    # vamos a simular qué habría pasado si el sistema hubiera recomendado
    # apostar al FAVORITO en cada partido (que es lo que hacen las reglas)
    
    total_profit = 0
    wins = 0
    losses = 0
    pushes = 0
    
    print("DETALLE DE APUESTAS:")
    print("-" * 80)
    
    for m in matches_2024_2025:
        parts = m['score'].split(':')
        hg, ag = int(parts[0]), int(parts[1])
        ah = m['ah']
        
        # Determinar favorito
        is_home_fav = ah >= 0
        pick = 'HOME' if is_home_fav else 'AWAY'
        pick_team = m['home'] if is_home_fav else m['away']
        
        # Calcular resultado desde perspectiva del favorito
        if is_home_fav:
            result = asian_result(hg, ag, ah)
        else:
            result = asian_result(ag, hg, -ah)
        
        # Calcular profit
        if result > 0:
            if result == 1:
                profit = stake * (odds - 1)
                wins += 1
                emoji = "✅"
            else:
                profit = stake * (odds - 1) / 2
                wins += 0.5
                emoji = "✅½"
        elif result < 0:
            if result == -1:
                profit = -stake
                losses += 1
                emoji = "❌"
            else:
                profit = -stake / 2
                losses += 0.5
                emoji = "❌½"
        else:
            profit = 0
            pushes += 1
            emoji = "🔄"
        
        total_profit += profit
        
        print(f"{emoji} {m['date']} | {m['home'][:20]:<20} {m['score']:^5} {m['away'][:20]:<20} | AH:{ah:>5} | {pick:>4} | €{profit:>6.2f}")
    
    print("-" * 80)
    print()
    print("=" * 60)
    print("RESUMEN")
    print("=" * 60)
    print(f"Total apuestas: {len(matches_2024_2025)}")
    print(f"Ganadas: {wins} | Perdidas: {losses} | Push: {pushes}")
    print(f"Win Rate: {wins/len(matches_2024_2025)*100:.1f}%")
    print()
    print(f"💰 PROFIT TOTAL: €{total_profit:.2f}")
    print(f"📊 ROI: {total_profit/(len(matches_2024_2025)*stake)*100:.1f}%")

if __name__ == "__main__":
    run_backtest()
