import json
import os
import re
from collections import defaultdict

def parse_score(score_str):
    if not score_str or not isinstance(score_str, str):
        return None, None
    try:
        match = re.search(r'(\d+)\s*[:\-]\s*(\d+)', score_str)
        if match:
            return int(match.group(1)), int(match.group(2))
    except:
        pass
    return None, None

def get_outcomes(home_g, away_g, ah, is_home_fav):
    if home_g is None or away_g is None:
        return None, None
    
    if is_home_fav:
        # Home fav gives 'ah' goals
        # diff > 0 means Home covered
        diff = home_g - away_g - ah
    else:
        # Away fav gives abs(ah) goals
        diff = away_g - home_g - abs(ah)
    
    # Fav Outcome
    if diff >= 0.5: fav_res = "WIN"
    elif diff == 0.25: fav_res = "HALF_WIN"
    elif diff == 0: fav_res = "PUSH"
    elif diff == -0.25: fav_res = "HALF_LOSS"
    else: fav_res = "LOSS"
    
    # Underdog Outcome (the opposite)
    if diff <= -0.5: und_res = "WIN"
    elif diff == -0.25: und_res = "HALF_WIN"
    elif diff == 0: und_res = "PUSH"
    elif diff == 0.25: und_res = "HALF_LOSS"
    else: und_res = "LOSS"
    
    return fav_res, und_res

def calculate_profit(outcome, odds=1.8):
    if outcome == "WIN": return odds - 1.0
    if outcome == "HALF_WIN": return (odds - 1.0) / 2.0
    if outcome == "PUSH": return 0.0
    if outcome == "HALF_LOSS": return -0.5
    if outcome == "LOSS": return -1.0
    return 0.0

def process_match(match):
    ah_str = match.get("main_match_odds", {}).get("ah_linea")
    if not ah_str or ah_str == "-": return None
    try:
        curr_ah_val = float(ah_str)
    except: return None
    if curr_ah_val == 0: return None
    
    is_home_fav = curr_ah_val > 0
    curr_ah_abs = abs(curr_ah_val)

    # Filter: Fav LOST last match
    prev_key = "last_home_match" if is_home_fav else "last_away_match"
    prev_match_gen = match.get(prev_key)
    if not prev_match_gen or not prev_match_gen.get("score"): return None
    
    pg_h, pg_a = parse_score(prev_match_gen.get("score"))
    pg_ah_str = prev_match_gen.get("handicap_line_raw")
    try:
        pg_ah = float(pg_ah_str) if pg_ah_str and pg_ah_str != "-" else 0
    except: pg_ah = 0
    
    # gen_outcome for the current team
    f_res, _ = get_outcomes(pg_h, pg_a, pg_ah, pg_ah >= 0) if is_home_fav else (None, None)
    if not is_home_fav:
        _, f_res = get_outcomes(pg_h, pg_a, pg_ah, pg_ah >= 0)
        if pg_ah < 0: # they were fav away
            f_res, _ = get_outcomes(pg_h, pg_a, pg_ah, False)

    # Simplified re-check for "Fav LOST last"
    # (Checking the fav team of the CURRENT match in their PREVIOUS match)
    if is_home_fav:
        f_res, _ = get_outcomes(pg_h, pg_a, pg_ah, pg_ah >= 0)
    else:
        # If Curr Fav is Away, last_away_match is their result.
        # But get_outcomes is (Home, Away, AH, is_home_fav)
        # We need to know if they were Home or Away in that prev match.
        # The JSON 'last_away_match' implies they were Away.
        # So we need to evaluate Away result.
        _, f_res = get_outcomes(pg_h, pg_a, pg_ah, pg_ah >= 0)

    if f_res not in ["LOSS", "HALF_LOSS"]: return None

    # Filter: Won in stadium
    h2h_s = match.get("h2h_stadium")
    if not h2h_s or not h2h_s.get("res1"): return None
    ps_h, ps_a = parse_score(h2h_s.get("res1"))
    ps_ah_str = h2h_s.get("ah1")
    try:
        ps_ah = float(ps_ah_str) if ps_ah_str and ps_ah_str != "-" else 0
    except: ps_ah = 0
    
    if is_home_fav:
        s_res, _ = get_outcomes(ps_h, ps_a, ps_ah, ps_ah >= 0)
    else:
        _, s_res = get_outcomes(ps_h, ps_a, ps_ah, ps_ah >= 0)
    
    if s_res not in ["WIN", "HALF_WIN"]: return None

    # Movement
    if is_home_fav: prev_fav_ah_abs = ps_ah
    else: prev_fav_ah_abs = -ps_ah if ps_ah >= 0 else abs(ps_ah)
            
    if curr_ah_abs > prev_fav_ah_abs: move = "UP"
    elif curr_ah_abs < prev_fav_ah_abs: move = "DOWN"
    else: move = "SAME"

    # Current Result
    final_score = match.get("final_score")
    ch, ca = parse_score(final_score)
    if ch is None: return None
    
    fav_res, und_res = get_outcomes(ch, ca, curr_ah_val, is_home_fav)
    
    return {
        "move": move,
        "und_outcome": und_res,
        "ah": curr_ah_abs
    }

def main():
    data_dir = r"c:\Users\Usuario\Desktop\Version_Util_Render\data"
    files = ["data_ah_0.json", "data_ah_0.5.json", "data_ah_1.5.json", "data_ah_2_plus.json", 
             "data_minus_ah_0.5.json", "data_minus_ah_1.5.json", "data_minus_ah_2_plus.json"]
    
    all_data = []
    for f in files:
        p = os.path.join(data_dir, f); 
        if os.path.exists(p):
            with open(p, 'r', encoding='utf-8') as j: all_data.extend(json.load(j))
    
    results = []
    for m in all_data:
        res = process_match(m)
        if res: results.append(res)

    print(f"Total partidos analizados: {len(results)}")
    
    # 1. Answer user specific question about 'DOWN' (38.14 fav win rate)
    down_results = [r for r in results if r["move"] == "DOWN"]
    total_down = len(down_results)
    und_profit_down = sum(calculate_profit(r["und_outcome"], 1.8) for r in down_results)
    und_wins_down = sum(1 for r in down_results if r["und_outcome"] in ["WIN", "HALF_WIN"])
    
    print("\n--- CASO 'BAJA' (FAV GANÓ 38.14%) ---")
    print(f"Total partidos: {total_down}")
    print(f"Ganancia acumulada apostando al Underdog: {und_profit_down:.2f} unidades")
    print(f"Win % del Underdog: {(und_wins_down/total_down*100):.2f}%")
    print(f"ROI % del Underdog: {(und_profit_down/total_down*100):.2f}%")

    # 2. Search for 20% ROI scenario for Underdog
    print("\n--- BÚSQUEDA DE ESCENARIO ÓPTIMO PARA EL UNDERDOG (ROI > 10%) ---")
    
    segments = defaultdict(list)
    for r in results:
        segments[(r["move"], r["ah"])].append(r)
        segments[(r["move"], "TOTAL")].append(r)
        segments[("ANY", r["ah"])].append(r)

    print(f"{'Caso (Mov, AH)':<20} | {'Total':<6} | {'ROI % Underdog':<15}")
    print("-" * 50)
    
    matches = []
    for key, data in segments.items():
        if len(data) < 20: continue # Min sample
        p = sum(calculate_profit(r["und_outcome"], 1.8) for r in data)
        roi = p / len(data) * 100
        matches.append((key, len(data), roi))
    
    # Sort by ROI
    matches.sort(key=lambda x: x[2], reverse=True)
    for m in matches[:10]:
        print(f"{str(m[0]):<20} | {m[1]:<6} | {m[2]:>14.2f}%")

if __name__ == "__main__":
    main()
