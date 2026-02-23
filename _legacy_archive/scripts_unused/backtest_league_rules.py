"""
=== BACKTEST DE LIGA CON REGLAS MINADAS ===
Evalúa el rendimiento de las reglas minadas en una liga específica.
"""

import json
import sys
from pathlib import Path

# Añadir path para importar módulos
sys.path.insert(0, str(Path(__file__).parent.parent))

DATA_DIR = Path(__file__).parent.parent / 'data'

def parse_score(score_str):
    if not score_str: return None, None
    s = str(score_str).replace(':', '-').replace(' ', '')
    if '-' not in s or '?' in s: return None, None
    try:
        parts = s.split('-')
        return int(parts[0]), int(parts[1])
    except:
        return None, None

def asian_result(team_goals, opp_goals, ah_line):
    """Calcula resultado del Asian Handicap. Retorna profit/loss."""
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

def load_league_matches(league_name):
    """Carga todos los partidos de una liga específica."""
    matches = []
    
    all_files = list(DATA_DIR.glob('data_ah_*.json')) + list(DATA_DIR.glob('data_minus_ah_*.json'))
    
    for f in all_files:
        try:
            with open(f, 'r', encoding='utf-8') as file:
                data = json.load(file)
                for m in data:
                    if league_name.lower() in (m.get('league_name') or '').lower():
                        matches.append(m)
        except:
            pass
    
    return matches

def run_backtest(league_name, stake=5, odds=1.85):
    """Ejecuta backtest en una liga."""
    from scripts.apply_mined_rules import apply_rules_to_match
    
    print(f"=== BACKTEST: {league_name} ===")
    print(f"Stake por apuesta: €{stake}")
    print(f"Cuotas AH: {odds}")
    print()
    
    # Cargar partidos
    matches = load_league_matches(league_name)
    print(f"Partidos encontrados: {len(matches)}")
    
    if not matches:
        print("No se encontraron partidos.")
        return
    
    # Aplicar reglas y calcular resultados
    total_bets = 0
    total_profit = 0
    wins = 0
    losses = 0
    pushes = 0
    
    results = []
    
    for m in matches:
        # Verificar que el partido tenga resultado
        score = m.get('final_score')
        hg, ag = parse_score(score)
        if hg is None:
            continue
            
        # Aplicar reglas
        pick = apply_rules_to_match(m)
        if not pick:
            continue
        
        # Obtener hándicap
        odds_data = m.get('main_match_odds') or {}
        ah_raw = odds_data.get('ah_linea') or m.get('handicap')
        if ah_raw is None:
            continue
        try:
            ah = float(ah_raw)
        except:
            continue
        
        # Calcular resultado de la apuesta
        if pick['pick'] == 'HOME':
            result = asian_result(hg, ag, ah)
        else:  # AWAY
            result = asian_result(ag, hg, -ah)
        
        # Calcular profit/loss
        if result > 0:
            if result == 1:
                profit = stake * (odds - 1)
                wins += 1
            else:  # Half win
                profit = stake * (odds - 1) / 2
                wins += 0.5
        elif result < 0:
            if result == -1:
                profit = -stake
                losses += 1
            else:  # Half loss
                profit = -stake / 2
                losses += 0.5
        else:
            profit = 0
            pushes += 1
        
        total_bets += 1
        total_profit += profit
        
        results.append({
            'match': f"{m.get('home_name')} vs {m.get('away_name')}",
            'score': score,
            'ah': ah,
            'pick': pick['pick'],
            'rule': pick['rule_name'],
            'roi_rule': pick['roi'],
            'result': 'WIN' if result > 0 else ('LOSS' if result < 0 else 'PUSH'),
            'profit': round(profit, 2)
        })
    
    # Mostrar resultados
    print(f"\n=== RESULTADOS ===")
    print(f"Total apuestas: {total_bets}")
    print(f"Ganadas: {wins} | Perdidas: {losses} | Push: {pushes}")
    print(f"Win Rate: {wins/total_bets*100:.1f}%" if total_bets > 0 else "N/A")
    print(f"\nProfit/Loss total: €{total_profit:.2f}")
    print(f"ROI: {total_profit/(total_bets*stake)*100:.1f}%" if total_bets > 0 else "N/A")
    
    # Mostrar detalle de últimos 20 partidos
    print(f"\n=== ÚLTIMOS 20 PARTIDOS ===")
    for r in results[-20:]:
        emoji = '✅' if r['result'] == 'WIN' else ('❌' if r['result'] == 'LOSS' else '🔄')
        print(f"{emoji} {r['match'][:40]:<40} | {r['score']:>5} | AH: {r['ah']:>4} | Pick: {r['pick']:>4} | €{r['profit']:>6}")
    
    return {
        'bets': total_bets,
        'wins': wins,
        'losses': losses,
        'pushes': pushes,
        'profit': total_profit,
        'roi': total_profit/(total_bets*stake)*100 if total_bets > 0 else 0
    }

if __name__ == "__main__":
    # Liga: Australia W-League (ID 729)
    run_backtest("Australia W-League", stake=5, odds=1.85)
