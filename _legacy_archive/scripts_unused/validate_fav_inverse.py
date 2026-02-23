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
    if diff >= 0.5: f, u = "WIN", "LOSS"
    elif diff == 0.25: f, u = "HALF_WIN", "HALF_LOSS"
    elif diff == 0: f, u = "PUSH", "PUSH"
    elif diff == -0.25: f, u = "HALF_LOSS", "HALF_WIN"
    else: f, u = "LOSS", "WIN"
    return f, u

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

    # 1. Fav LOST last match
    fav_prev_match = match.get("last_home_match" if is_home_fav else "last_away_match")
    if not fav_prev_match or not fav_prev_match.get("score"): return None
    fh, fa = parse_score(fav_prev_match.get("score"))
    fah = float(fav_prev_match["handicap_line_raw"]) if fav_prev_match.get("handicap_line_raw") and fav_prev_match["handicap_line_raw"] != "-" else 0
    if is_home_fav: f_prev_res, _ = get_outcomes(fh, fa, fah, fah >= 0)
    else: _, f_prev_res = get_outcomes(fh, fa, fah, fah >= 0)
    if f_prev_res not in ["LOSS", "HALF_LOSS"]: return None

    # 2. Fav WON in stadium (H2H Stadium)
    h2h_s = match.get("h2h_stadium")
    if not h2h_s or not h2h_s.get("res1"): return None
    sh, sa = parse_score(h2h_s.get("res1"))
    sah = float(h2h_s["ah1"]) if h2h_s.get("ah1") and h2h_s["ah1"] != "-" else 0
    if is_home_fav: s_res, _ = get_outcomes(sh, sa, sah, sah >= 0)
    else: _, s_res = get_outcomes(sh, sa, sah, sah >= 0)
    if s_res not in ["WIN", "HALF_WIN"]: return None

    # 3. Line Movement
    if is_home_fav: prev_f_ah = sah
    else: prev_f_ah = -sah if sah >= 0 else abs(sah)
    if curr_ah_abs > prev_f_ah: move = "UP"
    elif curr_ah_abs < prev_f_ah: move = "DOWN"
    else: move = "SAME"

    # Current Result
    ch, ca = parse_score(match.get("final_score"))
    if ch is None: return None
    _, und_outcome = get_outcomes(ch, ca, curr_ah_val, is_home_fav)
    
    return {
        "fav_loc": "HOME" if is_home_fav else "AWAY",
        "move": move,
        "ah": curr_ah_abs,
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

    print("\n--- REGLA DE ORO: CASA VS VISITANTE (Fav perdio + Estadio OK) ---")
    print(f"{'Fav Loc':<8} | {'AH':<5} | {'Mov':<5} | {'Total':<6} | {'ROI Und %'}")
    print("-" * 50)
    
    for loc in ["HOME", "AWAY"]:
        for ah in [0.5, "ANY"]:
            for move in ["DOWN", "ANY"]:
                filtered = [r for r in all_res if 
                            r["fav_loc"] == loc and 
                            (ah == "ANY" or r["ah"] == ah) and 
                            (move == "ANY" or r["move"] == move)]
                if len(filtered) < 10: continue
                profit = sum(calculate_profit(r["und_outcome"], 1.8) for r in filtered)
                roi = profit / len(filtered) * 100
                print(f"{loc:<8} | {ah:<5} | {move:<5} | {len(filtered):<6} | {roi:>7.2f}%")

if __name__ == "__main__":
    main()
