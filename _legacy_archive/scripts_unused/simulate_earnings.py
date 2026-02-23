import json
import os
import sys
from mega_trainer import extract_mega_features

def normalize_match(m):
    """Fallback to final_score if home_score is missing"""
    if m.get('home_score') is None and m.get('final_score'):
        try:
            parts = str(m['final_score']).replace('-', ':').split(':')
            if len(parts) == 2:
                m['home_score'] = int(parts[0])
                m['away_score'] = int(parts[1])
        except: pass
    return m

def load_data():
    matches = []
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    if not os.path.exists(data_dir):
        print(f"Data dir not found: {data_dir}")
        return []

    for f in os.listdir(data_dir):
        if f.endswith('.json'):
            try:
                with open(os.path.join(data_dir, f), 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    if isinstance(data, list):
                        matches.extend(data)
                    elif isinstance(data, dict):
                        if 'partidos' in data: matches.extend(data['partidos'])
            except: pass
    return matches

def simulate():
    # 1. Load Patterns
    if not os.path.exists('scripts/mega_patterns.json'):
        print("Error: No patterns found. Run mega_trainer.py first.")
        return

    with open('scripts/mega_patterns.json', 'r', encoding='utf-8') as f:
        patterns = json.load(f)

    # 4. Extract Pattern Lists
    print(f"Tipo de patrones: {type(patterns)}")
    if isinstance(patterns, dict):
        ah = patterns.get('ah_patterns', [])
        ou = patterns.get('ou_patterns', [])
        patterns = ah + ou
        print(f"Patrones cargados: {len(patterns)} (AH: {len(ah)}, OU: {len(ou)})")

    # 2. Load Data
    print("Cargando datos históricos...")
    matches = load_data()
    print(f"Total datos brutos: {len(matches)}")
    
    # Pre-process
    matches = [normalize_match(m) for m in matches]
    matches.sort(key=lambda x: x.get('date', ''))
    
    # Filter finished
    finished_matches = [m for m in matches if m.get('home_score') is not None]
    print(f"Total partidos finalizados disponibles: {len(finished_matches)}")
    
    # Take last 1000 finished
    test_data = finished_matches[-1000:]
    print(f"Simulando apuestas en los últimos {len(test_data)} partidos finalizados...")

    # 3. Simulation Config
    STAKE = 5
    ODDS = 1.80
    
    total_bets = 0
    total_won = 0
    total_loss = 0
    balance = 0
    
    matches_with_bets = []

    for i, m in enumerate(test_data):
        if not isinstance(m, dict): continue
        
        f = extract_mega_features(m)
        
        # Check AH Patterns
        best_ah_pick = None
        best_ah_conf = 0
        pick_type = ''
        
        # Logic: We might have AH and OU patterns.
        # Let's check AH patterns first.
        # Filter patterns by target
        
        for p in patterns:
            if not isinstance(p, dict): continue
            
            # Simple check
            match_features = True
            for feat in p['features']:
                if not f.get(feat, False):
                    match_features = False
                    break
            
            if match_features:
                # Prioritize by accuracy
                if p['accuracy'] > best_ah_conf:
                    best_ah_pick = p
                    best_ah_conf = p['accuracy']
        
        if best_ah_pick:
            pick = best_ah_pick['target']
            
            # Result Logic
            home_goals = int(m.get('home_score', 0))
            away_goals = int(m.get('away_score', 0))
            ah = float(m.get('ah', 0)) # Main match AH
            
            win = False
            void = False
            
            if pick == 'LOCAL':
                if (home_goals + ah) > away_goals: win = True
                elif abs((home_goals + ah) - away_goals) < 0.01: void = True
            elif pick == 'VISITA':
                if (home_goals + ah) < away_goals: win = True
                elif abs((home_goals + ah) - away_goals) < 0.01: void = True
            elif pick == 'OVER':
                if (home_goals + away_goals) > 2.5: win = True
            elif pick == 'UNDER':
                if (home_goals + away_goals) < 2.5: win = True
            elif 'OVER' in str(pick).upper() and '2.5' in str(pick): # Handle other OU lines if needed
                 if (home_goals + away_goals) > 2.5: win = True
            
            # Record
            total_bets += 1
            if void:
                res_str = "PUSH"
                # Stake returned, no profit/loss
            elif win:
                total_won += 1
                profit = (STAKE * ODDS) - STAKE
                balance += profit
                res_str = "GANADA"
            else:
                total_loss += 1
                balance -= STAKE
                res_str = "PERDIDA"
            
            matches_with_bets.append({
                'date': m.get('date'),
                'match': f"{m.get('home_name')} vs {m.get('away_name')}",
                'pick': pick,
                'ah': ah,
                'result': res_str,
                'score': f"{home_goals}-{away_goals}",
                'rule': best_ah_pick.get('group') or best_ah_pick.get('type')
            })

    # 5. Report
    print("\n" + "="*60)
    print(f"REPORTE DE SIMULACION (Backtest Ultimos {len(test_data)} Partidos)")
    print("="*60)
    
    # Calculate real ROI excluding voids from count? Or including?
    # Usually ROI = Profit / TotalStake. Voids return stake.
    # TotalStake = total_bets * STAKE.
    
    print(f"Apuestas Realizadas: {total_bets}")
    print(f"Aciertos:            {total_won} ({total_won/total_bets*100:.1f}%)" if total_bets else "Aciertos: 0")
    print(f"Fallos:              {total_loss}")
    pushes = total_bets - total_won - total_loss
    print(f"Nulos (Push):        {pushes}")
    print("-" * 30)
    print(f"BALANCE FINAL:       {balance:+.2f} €")
    
    roi = (balance / (total_bets * STAKE)) * 100 if total_bets else 0
    print(f"ROI FINAL:           {roi:+.1f}%")
    print("="*60)

    print("\nÚltimas 15 apuestas:")
    for b in matches_with_bets[-15:]:
        print(f"[{b['date']}] {b['match']:40} | {b['pick']:6} (AH {b['ah']}) | {b['score']} | {b['result']} [{b['rule']}]")

if __name__ == "__main__":
    simulate()
