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

def get_outcome(home_g, away_g, ah, is_home_fav):
    if home_g is None or away_g is None:
        return None
    
    if is_home_fav:
        # Home fav gives 'ah' goals
        # diff > 0 means Home covered
        diff = home_g - away_g - ah
    else:
        # Away fav gives abs(ah) goals
        diff = away_g - home_g - abs(ah)
    
    if diff >= 0.25:
        if diff >= 0.5: return "WIN"
        return "HALF_WIN"
    if diff == 0: return "PUSH"
    if diff == -0.25: return "HALF_LOSS"
    if diff <= -0.5: return "LOSS"
    return "LOSS"

def calculate_profit(outcome, odds=1.8):
    if outcome == "WIN": return odds - 1.0
    if outcome == "HALF_WIN": return (odds - 1.0) / 2.0
    if outcome == "PUSH": return 0.0
    if outcome == "HALF_LOSS": return -0.5
    if outcome == "LOSS": return -1.0
    return 0.0

def process_match(match):
    # 1. Fav Info
    ah_str = match.get("main_match_odds", {}).get("ah_linea")
    if not ah_str or ah_str == "-": return None
    try:
        curr_ah_val = float(ah_str)
    except: return None
    
    if curr_ah_val == 0: return None
    
    is_home_fav = curr_ah_val > 0
    fav_team_name = match.get("home_name") if is_home_fav else match.get("away_name")
    curr_ah_abs = abs(curr_ah_val)

    # 2. Filter: Fav LOST last match (general)
    prev_key = "last_home_match" if is_home_fav else "last_away_match"
    prev_match_gen = match.get(prev_key)
    if not prev_match_gen or not prev_match_gen.get("score"): return None
    
    pg_h, pg_a = parse_score(prev_match_gen.get("score"))
    pg_ah_str = prev_match_gen.get("handicap_line_raw")
    try:
        pg_ah = float(pg_ah_str) if pg_ah_str and pg_ah_str != "-" else 0
    except: pg_ah = 0
    
    # Check if OUR fav team covered that AH
    # Note: last_home_match means they were Home there.
    if is_home_fav:
        gen_outcome = get_outcome(pg_h, pg_a, pg_ah, pg_ah >= 0)
    else:
        # they were Away. get_outcome(..., pg_ah, pg_ah >= 0) gives outcome for the favority of that match.
        # We need outcome for Away.
        temp_outcome = get_outcome(pg_h, pg_a, pg_ah, pg_ah >= 0)
        # Flip outcome if Home was fav
        if pg_ah >= 0: # Home was fav
            if temp_outcome == "WIN": gen_outcome = "LOSS"
            elif temp_outcome == "LOSS": gen_outcome = "WIN"
            elif temp_outcome == "HALF_WIN": gen_outcome = "HALF_LOSS"
            elif temp_outcome == "HALF_LOSS": gen_outcome = "HALF_WIN"
            else: gen_outcome = temp_outcome
        else: # Away was fav, outcome is already for Away
            gen_outcome = temp_outcome
            
    if gen_outcome not in ["LOSS", "HALF_LOSS"]: return None

    # 3. Filter: Won last match in THIS STADIUM
    h2h_s = match.get("h2h_stadium")
    if not h2h_s or not h2h_s.get("res1"): return None
    
    ps_h, ps_a = parse_score(h2h_s.get("res1"))
    ps_ah_str = h2h_s.get("ah1")
    try:
        ps_ah = float(ps_ah_str) if ps_ah_str and ps_ah_str != "-" else 0
    except: ps_ah = 0
    
    # In H2H Stadium, the "Home" is always the current Home.
    # So if our current Fav is Home, they were Home then.
    # If our current Fav is Away, they were Away then.
    if is_home_fav:
        stad_outcome = get_outcome(ps_h, ps_a, ps_ah, ps_ah >= 0)
    else:
        temp_outcome = get_outcome(ps_h, ps_a, ps_ah, ps_ah >= 0)
        if ps_ah >= 0:
            if temp_outcome == "WIN": stad_outcome = "LOSS"
            elif temp_outcome == "LOSS": stad_outcome = "WIN"
            elif temp_outcome == "HALF_WIN": stad_outcome = "HALF_LOSS"
            elif temp_outcome == "HALF_LOSS": stad_outcome = "HALF_WIN"
            else: stad_outcome = temp_outcome
        else:
            stad_outcome = temp_outcome
            
    if stad_outcome not in ["WIN", "HALF_WIN"]: return None

    # 4. Movement: current AH vs previous stadium AH
    # We compare the "Favoritism level".
    # Previous Fav AH in stadium:
    if is_home_fav:
        prev_fav_ah_abs = ps_ah # We assume ps_ah refers to Home
    else:
        # If Home was fav (ps_ah > 0), Away fav ah was -ps_ah? No.
        # Favoritism is relative. Line UP = "I am MORE favorite now than I was then".
        if ps_ah >= 0: # Home was fav
            # Away was underdog. Fav AH was negative for Away?
            prev_fav_ah_abs = -ps_ah
        else: # Away was fav
            prev_fav_ah_abs = abs(ps_ah)
            
    # Line UP = current_ah_abs > prev_fav_ah_abs
    if curr_ah_abs > prev_fav_ah_abs:
        move = "UP"
    elif curr_ah_abs < prev_fav_ah_abs:
        move = "DOWN"
    else:
        move = "SAME"

    # 5. Current Result
    final_score = match.get("final_score")
    ch, ca = parse_score(final_score)
    if ch is None: return None
    
    curr_outcome = get_outcome(ch, ca, curr_ah_val, is_home_fav)
    
    return {
        "move": move,
        "curr_outcome": curr_outcome,
        "ah": curr_ah_abs
    }

def main():
    data_dir = r"c:\Users\Usuario\Desktop\Version_Util_Render\data"
    files = [
        "data_ah_0.json", "data_ah_0.5.json", "data_ah_1.5.json", "data_ah_2_plus.json",
        "data_minus_ah_0.5.json", "data_minus_ah_1.5.json", "data_minus_ah_2_plus.json"
    ]
    
    results = []
    for f in files:
        path = os.path.join(data_dir, f)
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as jf:
                data = json.load(jf)
                for m in data:
                    res = process_match(m)
                    if res: results.append(res)
    
    print(f"Total matches matching criteria: {len(results)}")
    
    stats = defaultdict(lambda: {"total": 0, "wins": 0, "half_wins": 0, "pushes": 0, "half_losses": 0, "losses": 0, "profit": 0.0})
    
    for r in results:
        for key in [r["move"], "TOTAL"]:
            s = stats[key]
            s["total"] += 1
            if r["curr_outcome"] == "WIN": s["wins"] += 1
            elif r["curr_outcome"] == "HALF_WIN": s["half_wins"] += 1
            elif r["curr_outcome"] == "PUSH": s["pushes"] += 1
            elif r["curr_outcome"] == "HALF_LOSS": s["half_losses"] += 1
            elif r["curr_outcome"] == "LOSS": s["losses"] += 1
            s["profit"] += calculate_profit(r["curr_outcome"], odds=1.8)

    print("\n" + "="*60)
    print("ANÁLISIS ESPECÍFICO: FAVORITO PERDIÓ ÚLTIMO Y GANÓ EN ESTADIO")
    print("Segmentado por Movimiento de Línea (Cuota 1.8)")
    print("="*60)
    print(f"{'Movimiento':<12} | {'Total':<6} | {'Win %':<8} | {'ROI %':<8}")
    print("-" * 50)
    
    for k in ["UP", "DOWN", "SAME", "TOTAL"]:
        s = stats[k]
        if s["total"] == 0: continue
        wr = (s["wins"] + s["half_wins"] * 0.5) / s["total"] * 100
        roi = (s["profit"] / s["total"]) * 100
        print(f"{k:<12} | {s['total']:<6} | {wr:>7.2f}% | {roi:>7.2f}%")

if __name__ == "__main__":
    main()
