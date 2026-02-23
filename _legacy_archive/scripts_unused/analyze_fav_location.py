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
    
    # Favorite's previous match result
    prev_key = "last_home_match" if is_home_fav else "last_away_match"
    prev_match = match.get(prev_key)
    if not prev_match or not prev_match.get("score"): return None
    
    ph, pa = parse_score(prev_match.get("score"))
    pah_str = prev_match.get("handicap_line_raw")
    try: pah = float(pah_str) if pah_str and pah_str != "-" else 0
    except: pah = 0
    
    # Was our current favorite successful in their last match?
    if is_home_fav: f_prev_res, _ = get_outcomes(ph, pa, pah, pah >= 0)
    else: _, f_prev_res = get_outcomes(ph, pa, pah, pah >= 0)
    
    if f_prev_res in ["WIN", "HALF_WIN"]: f_prev_cat = "WON_PREV"
    elif f_prev_res in ["LOSS", "HALF_LOSS"]: f_prev_cat = "LOST_PREV"
    else: return None

    # Current result
    ch, ca = parse_score(match.get("final_score"))
    if ch is None: return None
    fav_res, und_res = get_outcomes(ch, ca, curr_ah_val, is_home_fav)
    
    return {
        "fav_loc": "HOME" if is_home_fav else "AWAY",
        "prev_cat": f_prev_cat,
        "fav_outcome": fav_res,
        "und_outcome": und_res,
        "ah": abs(curr_ah_val)
    }

def main():
    data_dir = r"c:\Users\Usuario\Desktop\Version_Util_Render\data"
    files = ["data_ah_0.json", "data_ah_0.5.json", "data_ah_1.5.json", "data_ah_2_plus.json", 
             "data_minus_ah_0.5.json", "data_minus_ah_1.5.json", "data_minus_ah_2_plus.json"]
    
    results = []
    for f in files:
        p = os.path.join(data_dir, f)
        if os.path.exists(p):
            with open(p, 'r', encoding='utf-8') as j:
                for m in json.load(j):
                    res = process_match(m)
                    if res: results.append(res)

    print(f"Total analizados: {len(results)}")
    
    stats = defaultdict(lambda: {"total": 0, "fav_profit": 0.0, "und_profit": 0.0, "fav_wins": 0})
    
    for r in results:
        keys = [
            (r["fav_loc"], r["prev_cat"]), # Segment by Location and Streak
            (r["fav_loc"], "ALL"),
            ("BOTH", r["prev_cat"]),
            ("BOTH", "ALL")
        ]
        # AH 0.5 filter specifically
        if r["ah"] == 0.5:
            keys.extend([
                (r["fav_loc"], r["prev_cat"], "AH05"),
                ("BOTH", r["prev_cat"], "AH05")
            ])
            
        for k in keys:
            s = stats[k]
            s["total"] += 1
            s["fav_profit"] += calculate_profit(r["fav_outcome"], 1.8)
            s["und_profit"] += calculate_profit(r["und_outcome"], 1.8)
            if r["fav_outcome"] in ["WIN", "HALF_WIN"]: s["fav_wins"] += 1

    print("\n" + "="*70)
    print("COMPARATIVA: FAVORITO CASA VS FAVORITO FUERA")
    print("="*70)
    print(f"{'Ubicación':<10} | {'Racha Fav':<10} | {'Filtro':<6} | {'Total':<6} | {'ROI Fav %':<10} | {'ROI Und %'}")
    print("-" * 75)
    
    sorted_keys = sorted(stats.keys(), key=lambda x: (str(x[0]), str(x[1])))
    for k in sorted_keys:
        s = stats[k]
        if s["total"] < 50: continue
        roi_f = s["fav_profit"] / s["total"] * 100
        roi_u = s["und_profit"] / s["total"] * 100
        loc = k[0]
        streak = k[1]
        filt = k[2] if len(k) > 2 else "-"
        print(f"{loc:<10} | {streak:<10} | {filt:<6} | {s['total']:<6} | {roi_f:>10.2f}% | {roi_u:>10.2f}%")

if __name__ == "__main__":
    main()
