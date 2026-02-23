"""
BACKTEST AUSTRALIA W-LEAGUE - USANDO DATOS REALES
Busca partidos de la liga en los JSON y aplica las reglas minadas.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts.apply_mined_rules import apply_rules_to_match

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

def calculate_ah_result_for_pick(home_goals, away_goals, ah_line, pick):
    """
    Calcula resultado de la apuesta según el pick (HOME o AWAY).
    """
    is_home_fav = ah_line >= 0
    
    if is_home_fav:
        fav_diff = home_goals - away_goals
        handicap = ah_line
    else:
        fav_diff = away_goals - home_goals
        handicap = abs(ah_line)
    
    adjusted = fav_diff - handicap
    
    # Resultado desde perspectiva del FAV
    if abs(handicap % 0.5) == 0.25:
        line1 = handicap - 0.25
        line2 = handicap + 0.25
        adj1 = fav_diff - line1
        adj2 = fav_diff - line2
        r1 = 1 if adj1 > 0 else (-1 if adj1 < 0 else 0)
        r2 = 1 if adj2 > 0 else (-1 if adj2 < 0 else 0)
        fav_result = (r1 + r2) / 2
    else:
        if adjusted > 0: fav_result = 1
        elif adjusted < 0: fav_result = -1
        else: fav_result = 0
    
    # Determinar si apostamos al FAV o DOG
    if pick == 'HOME':
        if is_home_fav:
            return fav_result  # Apostamos al FAV
        else:
            return -fav_result  # Apostamos al DOG
    else:  # AWAY
        if not is_home_fav:
            return fav_result  # Apostamos al FAV
        else:
            return -fav_result  # Apostamos al DOG

def load_league_matches(league_name, limit=30):
    """Carga partidos de una liga desde los JSON."""
    matches = []
    all_files = list(DATA_DIR.glob('data_ah_*.json')) + list(DATA_DIR.glob('data_minus_ah_*.json'))
    
    for f in all_files:
        try:
            with open(f, 'r', encoding='utf-8') as file:
                data = json.load(file)
                for m in data:
                    if league_name.lower() in (m.get('league_name') or '').lower():
                        # Verificar que tiene score y handicap
                        score = m.get('final_score')
                        hg, ag = parse_score(score)
                        if hg is None: continue
                        
                        odds = m.get('main_match_odds') or {}
                        ah_raw = odds.get('ah_linea') or m.get('handicap')
                        if ah_raw is None: continue
                        
                        matches.append(m)
                        if len(matches) >= limit:
                            return matches
        except:
            pass
    
    return matches

def run_backtest(league_name, limit=30):
    stake = 5
    odds = 1.85
    
    print("=" * 90)
    print(f"BACKTEST: {league_name} ({limit} partidos)")
    print("=" * 90)
    print(f"Stake: {stake}€ | Cuotas: {odds}")
    print()
    
    matches = load_league_matches(league_name, limit)
    print(f"Partidos encontrados: {len(matches)}")
    print()
    
    total_profit = 0
    total_bets = 0
    wins = 0
    losses = 0
    pushes = 0
    no_pick = 0
    
    print(f"{'#':>2} | {'Local':<20} | {'Score':^5} | {'Visitante':<20} | {'AH':>5} | {'Pick':>4} | {'Regla':<25} | {'Res':>10} | {'P/L':>7}")
    print("-" * 110)
    
    for i, m in enumerate(matches, 1):
        home = m.get('home_name') or m.get('home_team') or 'Local'
        away = m.get('away_name') or m.get('away_team') or 'Visitante'
        score = m.get('final_score')
        hg, ag = parse_score(score)
        
        odds_data = m.get('main_match_odds') or {}
        ah = float(odds_data.get('ah_linea') or m.get('handicap'))
        
        # Aplicar reglas
        pick_result = apply_rules_to_match(m)
        
        if pick_result is None:
            print(f"{i:>2} | {home[:20]:<20} | {score:^5} | {away[:20]:<20} | {ah:>5} | {'--':>4} | {'NO MATCH':<25} | {'---':>10} | {'---':>7}")
            no_pick += 1
            continue
        
        pick = pick_result['pick']
        rule_name = pick_result['rule_name'][:25]
        
        # Calcular resultado
        result = calculate_ah_result_for_pick(hg, ag, ah, pick)
        
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
        
        emoji = "+" if profit > 0 else ("-" if profit < 0 else "=")
        print(f"{i:>2} | {home[:20]:<20} | {score:^5} | {away[:20]:<20} | {ah:>5} | {pick:>4} | {rule_name:<25} | {res_str:>10} | {emoji}{abs(profit):>6.2f}")
    
    print("-" * 110)
    print()
    print("=" * 50)
    print("RESUMEN")
    print("=" * 50)
    print(f"Partidos con pick: {total_bets}/{len(matches)}")
    print(f"Sin match de regla: {no_pick}")
    print()
    if total_bets > 0:
        print(f"Ganadas: {wins} | Perdidas: {losses} | Push: {pushes}")
        print(f"Win Rate: {wins/total_bets*100:.1f}%")
        print()
        print(f"PROFIT TOTAL: {total_profit:.2f}€")
        print(f"ROI: {total_profit/(total_bets*stake)*100:.1f}%")

if __name__ == "__main__":
    run_backtest("Israel Premier League", limit=100)
