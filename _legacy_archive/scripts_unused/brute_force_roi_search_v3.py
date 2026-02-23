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
    try: curr_ah_val = float(ah_str); ah_abs = abs(curr_ah_val)
    except: return None
    if curr_ah_val == 0: return None
    is_home_fav = curr_ah_val > 0

    # Fav Previous
    f_p_m = match.get("last_home_match" if is_home_fav else "last_away_match")
    if not f_p_m or not f_p_m.get("score"): return None
    gh, ga = parse_score(f_p_m["score"])
    gah = float(f_p_m["handicap_line_raw"]) if f_p_m.get("handicap_line_raw") and f_p_m["handicap_line_raw"] != "-" else 0
    fr, _ = get_outcomes(gh, ga, gah, gah >= 0) if is_home_fav else (None, None)
    if not is_home_fav: _, fr = get_outcomes(gh, ga, gah, gah >= 0)
    f_gen = "F_GEN_WIN" if fr in ["WIN", "HALF_WIN"] else "F_GEN_LOSS" if fr in ["LOSS", "HALF_LOSS"] else "F_GEN_PUSH"
    
    # FWDL
    if is_home_fav: fwdl = "W" if gh > ga else "D" if gh == ga else "L"
    else: fwdl = "W" if ga > gh else "D" if gh == ga else "L"

    # Und Previous
    u_p_m = match.get("last_away_match" if is_home_fav else "last_home_match")
    if not u_p_m or not u_p_m.get("score"): return None
    ugh, uga = parse_score(u_p_m["score"])
    ugah = float(u_p_m["handicap_line_raw"]) if u_p_m.get("handicap_line_raw") and u_p_m["handicap_line_raw"] != "-" else 0
    _, ur = get_outcomes(ugh, uga, ugah, ugah >= 0) if is_home_fav else (None, None)
    if not is_home_fav: ur, _ = get_outcomes(ugh, uga, ugah, ugah >= 0)
    u_gen = "U_GEN_WIN" if ur in ["WIN", "HALF_WIN"] else "U_GEN_LOSS" if ur in ["LOSS", "HALF_LOSS"] else "U_GEN_PUSH"
    
    # UWDL
    if is_home_fav: uwdl = "W" if uga > ugh else "D" if uga == ugh else "L"
    else: uwdl = "W" if ugh > uga else "D" if uga == ugh else "L"

    # H2H Stadium
    h_s = match.get("h2h_stadium")
    if not h_s or not h_s.get("res1"): return None
    sh, sa = parse_score(h_s["res1"])
    sah = float(h_s["ah1"]) if h_s.get("ah1") and h_s["ah1"] != "-" else 0
    sr, _ = get_outcomes(sh, sa, sah, sah >= 0) if is_home_fav else (None, None)
    if not is_home_fav: _, sr = get_outcomes(sh, sa, sah, sah >= 0)
    f_stad = "F_STAD_WIN" if sr in ["WIN", "HALF_WIN"] else "F_STAD_LOSS" if sr in ["LOSS", "HALF_LOSS"] else "F_STAD_PUSH"

    # Move
    p_f_ah = sah if is_home_fav else (-sah if sah >= 0 else abs(sah))
    move = "UP" if ah_abs > p_f_ah else "DOWN" if ah_abs < p_f_ah else "SAME"

    # Current Res
    ch, ca = parse_score(match.get("final_score"))
    if ch is None: return None
    fav_r, und_r = get_outcomes(ch, ca, curr_ah_val, is_home_fav)
    
    return {
        "AH": ah_abs, "LOC": "HOME" if is_home_fav else "AWAY",
        "FGEN": f_gen, "UGEN": u_gen, "FSTAD": f_stad, "MOVE": move,
        "FWDL": fwdl, "UWDL": uwdl,
        "fav_p": calculate_profit(fav_r), "und_p": calculate_profit(und_r)
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

    dimensions = ["AH", "LOC", "FGEN", "UGEN", "FSTAD", "MOVE", "FWDL", "UWDL"]
    megastats = defaultdict(lambda: {"total": 0, "fav_p": 0.0, "und_p": 0.0})
    
    for r in all_res:
        # Optimization: only look at some combination lengths to avoid memory blowout
        for r_len in [2, 3, 4]: 
            for combo in itertools.combinations(dimensions, r_len):
                key = tuple((c, r[c]) for c in combo)
                s = megastats[key]
                s["total"] += 1
                s["fav_p"] += r["fav_p"]; s["und_p"] += r["und_p"]

    gold = []
    for k, s in megastats.items():
        if s["total"] < 20: continue # Lowering to 20 to find more, but will prioritize 30+
        roi_f, roi_u = s["fav_p"]/s["total"]*100, s["und_p"]/s["total"]*100
        if roi_f >= 20: gold.append({"type": "FAVORITO", "key": k, "total": s["total"], "roi": roi_f})
        if roi_u >= 20: gold.append({"type": "UNDERDOG", "key": k, "total": s["total"], "roi": roi_u})

    gold.sort(key=lambda x: (x["total"] >= 30, x["roi"]), reverse=True)
    
    final_list = []
    seen = set()
    for g in gold:
        desc = ", ".join([f"{k}:{v}" for k, v in g["key"]])
        if desc not in seen:
            final_list.append(g)
            seen.add(desc)
        if len(final_list) >= 200: break

    print(f"Patrones de Oro finales: {len(final_list)}")
    
    # Save to JSON
    out_path = r'c:\Users\Usuario\Desktop\Version_Util_Render\scripts\200_gold_patterns.json'
    with open(out_path, 'w') as f:
        json.dump(final_list, f, indent=2)

if __name__ == "__main__":
    main()
