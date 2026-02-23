import json
import os
import re
from collections import defaultdict
import itertools

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

def get_match_features(match):
    ah_str = match.get("main_match_odds", {}).get("ah_linea")
    if not ah_str or ah_str == "-": return None
    try: curr_ah_val = float(ah_str)
    except: return None
    if curr_ah_val == 0: return None
    
    is_home_fav = curr_ah_val > 0
    ah_abs = abs(curr_ah_val)

    # 1. Fav Gen Result
    f_gen_match = match.get("last_home_match" if is_home_fav else "last_away_match")
    if not f_gen_match or not f_gen_match.get("score"): return None
    gh, ga = parse_score(f_gen_match.get("score"))
    gah = float(f_gen_match["handicap_line_raw"]) if f_gen_match.get("handicap_line_raw") and f_gen_match["handicap_line_raw"] != "-" else 0
    if is_home_fav: f_gen_res, _ = get_outcomes(gh, ga, gah, gah >= 0)
    else: _, f_gen_res = get_outcomes(gh, ga, gah, gah >= 0)
    f_gen = "F_GEN_WIN" if f_gen_res in ["WIN", "HALF_WIN"] else "F_GEN_LOSS" if f_gen_res in ["LOSS", "HALF_LOSS"] else "F_GEN_PUSH"

    # 2. Underdog Gen Result
    u_gen_match = match.get("last_away_match" if is_home_fav else "last_home_match")
    if not u_gen_match or not u_gen_match.get("score"): return None
    ugh, uga = parse_score(u_gen_match.get("score"))
    ugah = float(u_gen_match["handicap_line_raw"]) if u_gen_match.get("handicap_line_raw") and u_gen_match["handicap_line_raw"] != "-" else 0
    if is_home_fav: _, u_gen_res = get_outcomes(ugh, uga, ugah, ugah >= 0)
    else: u_gen_res, _ = get_outcomes(ugh, uga, ugah, ugah >= 0)
    u_gen = "U_GEN_WIN" if u_gen_res in ["WIN", "HALF_WIN"] else "U_GEN_LOSS" if u_gen_res in ["LOSS", "HALF_LOSS"] else "U_GEN_PUSH"

    # 3. H2H Stadium Result
    h2h_s = match.get("h2h_stadium")
    if not h2h_s or not h2h_s.get("res1"): return None
    sh, sa = parse_score(h2h_s.get("res1"))
    sah = float(h2h_s["ah1"]) if h2h_s.get("ah1") and h2h_s["ah1"] != "-" else 0
    if is_home_fav: s_res, _ = get_outcomes(sh, sa, sah, sah >= 0)
    else: _, s_res = get_outcomes(sh, sa, sah, sah >= 0)
    f_stad = "F_STAD_WIN" if s_res in ["WIN", "HALF_WIN"] else "F_STAD_LOSS" if s_res in ["LOSS", "HALF_LOSS"] else "F_STAD_PUSH"

    # 4. Movement
    if is_home_fav: prev_f_ah = sah
    else: prev_f_ah = -sah if sah >= 0 else abs(sah)
    move = "UP" if ah_abs > prev_f_ah else "DOWN" if ah_abs < prev_f_ah else "SAME"

    # Current Result
    ch, ca = parse_score(match.get("final_score"))
    if ch is None: return None
    fav_res, und_res = get_outcomes(ch, ca, curr_ah_val, is_home_fav)
    
    return {
        "AH": ah_abs,
        "LOC": "HOME" if is_home_fav else "AWAY",
        "FGEN": f_gen,
        "UGEN": u_gen,
        "FSTAD": f_stad,
        "MOVE": move,
        "fav_prof": calculate_profit(fav_res),
        "und_prof": calculate_profit(und_res)
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
                    feat = get_match_features(m)
                    if feat: all_res.append(feat)

    print(f"Partidos válidos: {len(all_res)}")
    
    dimensions = ["AH", "LOC", "FGEN", "UGEN", "FSTAD", "MOVE"]
    
    # We will try combinations of 3, 4, 5 features
    megastats = defaultdict(lambda: {"total": 0, "fav_p": 0.0, "und_p": 0.0})
    
    for r in all_res:
        for r_len in range(3, 7):
            for combo in itertools.combinations(dimensions, r_len):
                key = tuple((c, r[c]) for c in combo)
                s = megastats[key]
                s["total"] += 1
                s["fav_p"] += r["fav_prof"]
                s["und_p"] += r["und_prof"]

    # Filter patterns
    gold_mines = []
    for key, s in megastats.items():
        if s["total"] < 30: continue
        
        roi_f = s["fav_p"] / s["total"] * 100
        roi_u = s["und_p"] / s["total"] * 100
        
        if roi_f >= 20:
            gold_mines.append({"type": "FAVORITO", "key": key, "total": s["total"], "roi": roi_f})
        if roi_u >= 20:
            gold_mines.append({"type": "UNDERDOG", "key": key, "total": s["total"], "roi": roi_u})

    # Sort and print
    gold_mines.sort(key=lambda x: x["roi"], reverse=True)
    
    print("\n" + "="*80)
    print("PATRONES DE ORO ENCONTRADOS (ROI > 20%, N >= 30)")
    print("="*80)
    
    seen_combinations = set()
    count = 0
    for g in gold_mines:
        # Simplify key for printing
        desc = ", ".join([f"{k}:{v}" for k, v in g["key"]])
        
        # Avoid redundant sub-patterns (if a more specific pattern exists with same ROI, or vice versa)
        # For simplicity, just show top 15 unique descriptions
        if desc in seen_combinations: continue
        seen_combinations.add(desc)
        
        print(f"[{g['type']}] ROI: {g['roi']:>6.2f}% | N: {g['total']:<4} | Combo: {desc}")
        count += 1
        if count >= 30: break

if __name__ == "__main__":
    main()
