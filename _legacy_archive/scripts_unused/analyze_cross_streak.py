import json
import os
import re
from collections import defaultdict

def parse_score(score_str):
    if not score_str or not isinstance(score_str, str): return None, None
    try:
        match = re.search(r'(\d+)\s*[:\-]\s*(\d+)', score_str)
        if match: return int(match.group(1)), int(match.group(2))
    except: pass
    return None, None

def get_outcomes(home_g, away_g, ah, is_home_fav):
    if home_g is None or away_g is None: return None, None
    diff = (home_g - away_g - ah) if is_home_fav else (away_g - home_g - abs(ah))
    
    if diff >= 0.5: fav_res = "WIN"
    elif diff == 0.25: fav_res = "HALF_WIN"
    elif diff == 0: fav_res = "PUSH"
    elif diff == -0.25: fav_res = "HALF_LOSS"
    else: fav_res = "LOSS"
    
    # Underdog is the opposite
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
    try: curr_ah_val = float(ah_str)
    except: return None
    if curr_ah_val == 0: return None
    
    is_home_fav = curr_ah_val > 0
    curr_ah_abs = abs(curr_ah_val)

    # 1. Fav (Home) LOST last match
    fav_prev = match.get("last_home_match" if is_home_fav else "last_away_match")
    if not fav_prev or not fav_prev.get("score"): return None
    f_h, f_a = parse_score(fav_prev.get("score"))
    f_ah = float(fav_prev["handicap_line_raw"]) if fav_prev.get("handicap_line_raw") and fav_prev["handicap_line_raw"] != "-" else 0
    
    # Was the Fav team successful in their prev match? (Evaluating as Home/Away as stored)
    if is_home_fav: f_res, _ = get_outcomes(f_h, f_a, f_ah, f_ah >= 0)
    else: _, f_res = get_outcomes(f_h, f_a, f_ah, f_ah >= 0)
    
    if f_res not in ["LOSS", "HALF_LOSS"]: return None

    # 2. Underdog (Away) PREVIOUS match result
    und_prev = match.get("last_away_match" if is_home_fav else "last_home_match")
    if not und_prev or not und_prev.get("score"): return None
    u_h, u_a = parse_score(und_prev.get("score"))
    u_ah = float(und_prev["handicap_line_raw"]) if und_prev.get("handicap_line_raw") and und_prev["handicap_line_raw"] != "-" else 0

    # Was the Underdog successful in their prev match?
    if is_home_fav: _, u_res = get_outcomes(u_h, u_a, u_ah, u_ah >= 0) # Away result
    else: u_res, _ = get_outcomes(u_h, u_a, u_ah, u_ah >= 0) # Home result

    # 3. Won in stadium previously
    h2h_s = match.get("h2h_stadium")
    if not h2h_s or not h2h_s.get("res1"): return None
    s_h, s_a = parse_score(h2h_s.get("res1"))
    s_ah = float(h2h_s["ah1"]) if h2h_s.get("ah1") and h2h_s["ah1"] != "-" else 0
    if is_home_fav: s_res, _ = get_outcomes(s_h, s_a, s_ah, s_ah >= 0)
    else: _, s_res = get_outcomes(s_h, s_a, s_ah, s_ah >= 0)
    if s_res not in ["WIN", "HALF_WIN"]: return None

    # 4. Line Movement
    if is_home_fav: prev_f_ah = s_ah
    else: prev_f_ah = -s_ah if s_ah >= 0 else abs(s_ah)
    if curr_ah_abs > prev_f_ah: move = "UP"
    elif curr_ah_abs < prev_f_ah: move = "DOWN"
    else: move = "SAME"

    # Current Result
    ch, ca = parse_score(match.get("final_score"))
    if ch is None: return None
    _, und_outcome = get_outcomes(ch, ca, curr_ah_val, is_home_fav)
    
    return {
        "move": move,
        "ah": curr_ah_abs,
        "und_prev_won": u_res in ["WIN", "HALF_WIN"],
        "und_outcome": und_outcome
    }

def main():
    data_dir = r"c:\Users\Usuario\Desktop\Version_Util_Render\data"
    files = ["data_ah_0.json", "data_ah_0.5.json", "data_ah_1.5.json", "data_ah_2_plus.json", 
             "data_minus_ah_0.5.json", "data_minus_ah_1.5.json", "data_minus_ah_2_plus.json"]
    
    all_res = []
    for f in files:
        p = os.path.join(data_dir, f)
        if os.path.exists(p):
            with open(p, 'r', encoding='utf-8') as j:
                for m in json.load(j):
                    r = process_match(m)
                    if r: all_res.append(r)

    print(f"Total partidos filtrados (Fav Perdió, Ganó Estadio): {len(all_res)}")
    
    # Analysis Table
    print("\n--- IMPACTO DE LA RACHA DEL UNDERDOG (VISITANTE) ---")
    print(f"{'AH':<5} | {'Mov':<5} | {'Underdog Prev':<15} | {'Total':<6} | {'ROI %'}")
    print("-" * 55)
    
    # We focus on the previously "optimal" case: AH 0.5 + DOWN
    scenarios = [
        {"ah": 0.5, "move": "DOWN", "und_won": None},
        {"ah": 0.5, "move": "DOWN", "und_won": True},
        {"ah": 0.5, "move": "DOWN", "und_won": False},
        {"ah": "TOTAL", "move": "ANY", "und_won": True}
    ]
    
    for scen in scenarios:
        filtered = [r for r in all_res if 
                    (scen["ah"] == "TOTAL" or r["ah"] == scen["ah"]) and 
                    (scen["move"] == "ANY" or r["move"] == scen["move"]) and 
                    (scen["und_won"] is None or r["und_prev_won"] == scen["und_won"])]
        
        if not filtered: continue
        profit = sum(calculate_profit(r["und_outcome"], 1.8) for r in filtered)
        roi = profit / len(filtered) * 100
        label = "GANÓ (Cruzada)" if scen["und_won"] is True else "PERDIÓ" if scen["und_won"] is False else "CUALQUIERA"
        print(f"{scen['ah']:<5} | {scen['move']:<5} | {label:<15} | {len(filtered):<6} | {roi:>7.2f}%")

if __name__ == "__main__":
    main()
