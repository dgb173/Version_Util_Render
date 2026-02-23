
import json
import warnings
from pathlib import Path
import sys

# Suppress warnings
warnings.filterwarnings('ignore')

# Add src to path just in case
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'src'))

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / 'data'
INPUTS_FILE = DATA_DIR / 'testing_inputs.json'
RESULTS_FILE = DATA_DIR / 'testing_results.json'

# Try to find rules file
RULES_FILES = [
    DATA_DIR / 'models' / 'top_rules.json',
    DATA_DIR / 'mined_rules.json'
]

def load_json(path):
    if not path.exists():
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return None

def parse_score(score_str):
    if not score_str: return None, None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except:
        return None, None

def settle_bet(bet_side, home_goals, away_goals, handicap):
    """
    Settle AH bet.
    Returns: Payout multiplier (-1.0, -0.5, 0.0, 0.5, 1.0)
    """
    # AH Formula: (Home - Away) - Line
    diff = (home_goals - away_goals) - handicap
    
    # Perspective correction
    # If we bet AWAY, we just invert the perspective or the comparison?
    # Actually, standard AH logic:
    # If bet HOME: Payout based on diff > 0
    # If bet AWAY: Payout based on diff < 0 (or -diff > 0)
    
    # Let's standardize payout for HOME, then invert if bet is AWAY
    
    payout = 0.0
    if diff > 0.25:
        payout = 1.0
    elif diff > 0: # 0.25
        payout = 0.5
    elif diff == 0:
        payout = 0.0
    elif diff >= -0.25:
        payout = -0.5
    else:
        payout = -1.0
        
    if bet_side == 'AWAY':
        # Invert rules roughly:
        # If Home Win (1.0) -> Away Loss (-1.0)
        # If Home Half Win (0.5) -> Away Half Loss (-0.5)
        # If Push (0.0) -> Push (0.0)
        # If Home Half Loss (-0.5) -> Away Half Win (0.5)
        # If Home Loss (-1.0) -> Away Win (1.0)
        # Note: logic holds for standard AH lines
        payout = -payout
        
# Import rule application logic directly from script
sys.path.insert(0, str(BASE_DIR / 'scripts'))
try:
    from apply_mined_rules import apply_rules_to_match
except ImportError:
    print("Error: Could not import apply_mined_rules. logic missing.")
    sys.exit(1)

def run_backtest():
    print("--- Iniciando Backtest (Cuota 1.8) ---")
    
    # 1. Load Data
    inputs = load_json(INPUTS_FILE)
    results = load_json(RESULTS_FILE)
    
    if not inputs or not results:
        print("Error: No se encontraron los archivos de testing generated.")
        return

    print(f"Inputs disponibles: {len(inputs)}")
    print(f"Resultados disponibles: {len(results)}")
    
    # 3. Simulate
    stake = 1.0 # Una unidad
    fixed_odds = 1.80
    
    total_bets = 0
    total_wins = 0
    total_profit = 0.0
    
    bets_log = []
    
    for match in inputs:
        mid = match.get('match_id')
        final_score_str = results.get(mid)
        
        # Skip if no result available for validation
        if not final_score_str:
            continue
            
        home_g, away_g = parse_score(final_score_str)
        if home_g is None: continue
        
        # Get Match AH for Settlement (needed for ROI calc)
        odds = match.get('main_match_odds', {})
        try:
            ah_line = float(odds.get('ah_linea', 0))
        except:
            continue

        # Get Prediction using production logic
        prediction = apply_rules_to_match(match)
        
        if prediction:
            side = prediction['pick'] # 'HOME' or 'AWAY' (already translated)
            rname = prediction['rule_name']
            
            payout_mult = settle_bet(side, home_g, away_g, ah_line)
            
            # Profit Calculation (Standard Asian Settlement)
            profit = 0.0
            if payout_mult == 1.0:
                profit = stake * (fixed_odds - 1)
                total_wins += 1
            elif payout_mult == 0.5:
                profit = (stake / 2) * (fixed_odds - 1)
                total_wins += 0.5
            elif payout_mult == 0.0:
                profit = 0.0
            elif payout_mult == -0.5:
                profit = -stake / 2
            elif payout_mult == -1.0:
                profit = -stake
            
            total_profit += profit
            total_bets += 1
            
            bets_log.append({
                'match': f"{match.get('home_team')} vs {match.get('away_team')}",
                'result': final_score_str,
                'bet': f"{side} (AH {ah_line})",
                'rule': rname,
                'outcome': payout_mult,
                'profit': profit
            })

    # 4. Report
    print("\n" + "="*50)
    print("RESULTADOS DEL BACKTEST")
    print("="*50)
    print(f"Partidos procesados: {len(bets_log)}")
    print(f"Apuestas realizadas: {total_bets}")
    
    if total_bets > 0:
        roi = (total_profit / total_bets) * 100
        win_rate = (total_wins / total_bets) * 100
        print(f"Win Rate: {win_rate:.2f}%")
        print(f"Net Profit (Unidades): {total_profit:.2f}u")
        print(f"ROI: {roi:.2f}%")
    else:
        print("No se encontraron apuestas que cumplieran las reglas.")
        
    # Show last few bets
    if bets_log:
        print("\nÚltimas 5 apuestas:")
        for b in bets_log[-5:]:
            res = "WIN" if b['profit'] > 0 else ("PUSH" if b['profit'] == 0 else "LOSS")
            print(f"[{res}] {b['match']} -> {b['bet']} (Profit: {b['profit']:.2f})")

    # 4. Report
    print("\n" + "="*50)
    print("RESULTADOS DEL BACKTEST")
    print("="*50)
    print(f"Partidos procesados: {len(bets_log)}")
    print(f"Apuestas realizadas: {total_bets}")
    
    if total_bets > 0:
        roi = (total_profit / total_bets) * 100
        win_rate = (total_wins / total_bets) * 100
        print(f"Win Rate: {win_rate:.2f}%")
        print(f"Net Profit (Unidades): {total_profit:.2f}u")
        print(f"ROI: {roi:.2f}%")
    else:
        print("No se encontraron apuestas que cumplieran las reglas.")
        
    # Show last few bets
    if bets_log:
        print("\nÚltimas 5 apuestas:")
        for b in bets_log[-5:]:
            res = "WIN" if b['profit'] > 0 else ("PUSH" if b['profit'] == 0 else "LOSS")
            print(f"[{res}] {b['match']} -> {b['bet']} (Profit: {b['profit']:.2f})")

if __name__ == "__main__":
    run_backtest()
