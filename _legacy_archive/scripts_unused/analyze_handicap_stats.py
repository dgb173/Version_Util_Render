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
        diff = home_g - away_g - ah
    else:
        # Away fav gives abs(ah) goals
        diff = away_g - home_g - abs(ah)
    
    if diff >= 0.5: return "WIN"
    if diff == 0.25: return "HALF_WIN"
    if diff == 0: return "PUSH"
    if diff == -0.25: return "HALF_LOSS"
    if diff <= -0.5: return "LOSS"
    return "LOSS" # default

def calculate_profit(outcome, odds=1.8):
    if outcome == "WIN": return odds - 1.0
    if outcome == "HALF_WIN": return (odds - 1.0) / 2.0
    if outcome == "PUSH": return 0.0
    if outcome == "HALF_LOSS": return -0.5
    if outcome == "LOSS": return -1.0
    return 0.0

def process_file(filepath):
    results = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if not isinstance(data, list):
                return []
            
            for match in data:
                # Current match AH
                ah_str = match.get("main_match_odds", {}).get("ah_linea")
                if not ah_str or ah_str == "-":
                    continue
                try:
                    ah_linea = float(ah_str)
                except:
                    continue
                
                if ah_linea == 0:
                    continue
                
                is_home_fav = ah_linea > 0
                
                # Previous match
                prev_key = "last_home_match" if is_home_fav else "last_away_match"
                prev_match = match.get(prev_key)
                if not prev_match or not prev_match.get("score"):
                    continue
                
                prev_score = prev_match.get("score")
                prev_ah_str = prev_match.get("handicap_line_raw")
                if not prev_ah_str or prev_ah_str == "-":
                    prev_ah = 0
                else:
                    try:
                        prev_ah = float(prev_ah_str)
                    except:
                        prev_ah = 0
                
                ph, pa = parse_score(prev_score)
                if ph is None:
                    continue
                
                # Did the favorite cover their previous specific match?
                # Note: last_home_match means the current team was home there too.
                # last_away_match means they were away.
                if is_home_fav:
                    # Favorite was Home in previous match
                    prev_outcome = get_outcome(ph, pa, prev_ah, prev_ah >= 0)
                else:
                    # Favorite was Away in previous match
                    # get_outcome takes (home, away, ah, is_home_fav)
                    # We want to know result for Away.
                    prev_outcome = get_outcome(ph, pa, prev_ah, prev_ah >= 0)
                    # Wait, get_outcome returns result for the FAVORITE in that match.
                    # We need the result for OUR team (which is Away).
                    if prev_ah >= 0: # Home was favorite
                        # If get_outcome is WIN, Home covered. So Away LOST.
                        if prev_outcome == "WIN": prev_outcome = "LOSS"
                        elif prev_outcome == "LOSS": prev_outcome = "WIN"
                        elif prev_outcome == "HALF_WIN": prev_outcome = "HALF_LOSS"
                        elif prev_outcome == "HALF_LOSS": prev_outcome = "HALF_WIN"
                    else: # Away was favorite
                        # Already correct for Away
                        pass
                
                # Current result
                final_score = match.get("final_score")
                ch, ca = parse_score(final_score)
                if ch is None:
                    continue
                
                curr_outcome = get_outcome(ch, ca, ah_linea, is_home_fav)
                
                results.append({
                    "prev_outcome": prev_outcome,
                    "curr_outcome": curr_outcome,
                    "ah": abs(ah_linea)
                })
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
    
    return results

def main():
    data_dir = r"c:\Users\Usuario\Desktop\Version_Util_Render\data"
    files = [
        "data_ah_0.json", "data_ah_0.5.json", "data_ah_1.5.json", "data_ah_2_plus.json",
        "data_minus_ah_0.5.json", "data_minus_ah_1.5.json", "data_minus_ah_2_plus.json"
    ]
    
    all_results = []
    for f in files:
        path = os.path.join(data_dir, f)
        if os.path.exists(path):
            print(f"Processing {f}...")
            all_results.extend(process_file(path))
    
    print(f"Total valid matches found: {len(all_results)}")
    
    stats = {
        "LOST_PREV": defaultdict(lambda: {"total": 0, "wins": 0, "half_wins": 0, "pushes": 0, "half_losses": 0, "losses": 0, "profit": 0.0}),
        "WON_PREV": defaultdict(lambda: {"total": 0, "wins": 0, "half_wins": 0, "pushes": 0, "half_losses": 0, "losses": 0, "profit": 0.0}),
        "ALL": defaultdict(lambda: {"total": 0, "wins": 0, "half_wins": 0, "pushes": 0, "half_losses": 0, "losses": 0, "profit": 0.0})
    }
    
    for r in all_results:
        # Group prev_outcome into LOST (including half-loss) and WON (including half-win)
        # Push is excluded from this specific logic or treated as other.
        if r["prev_outcome"] in ["LOSS", "HALF_LOSS"]:
            group = "LOST_PREV"
        elif r["prev_outcome"] in ["WIN", "HALF_WIN"]:
            group = "WON_PREV"
        else:
            continue
            
        ah_key = str(r["ah"])
        
        for g in [group, "ALL"]:
            for k in [ah_key, "TOTAL"]:
                s = stats[g][k]
                s["total"] += 1
                if r["curr_outcome"] == "WIN": s["wins"] += 1
                elif r["curr_outcome"] == "HALF_WIN": s["half_wins"] += 1
                elif r["curr_outcome"] == "PUSH": s["pushes"] += 1
                elif r["curr_outcome"] == "HALF_LOSS": s["half_losses"] += 1
                elif r["curr_outcome"] == "LOSS": s["losses"] += 1
                s["profit"] += calculate_profit(r["curr_outcome"], odds=1.8)

    # Print Report
    print("\n" + "="*50)
    print("ESTADÍSTICAS DE HANDICAP (CUOTA 1.8)")
    print("="*50)
    
    for group in ["LOST_PREV", "WON_PREV"]:
        print(f"\n>>> FAVORITO VIENE DE: {group.replace('_PREV', '')} <<<")
        print(f"{'AH':<10} | {'Total':<6} | {'Win %':<8} | {'ROI %':<8}")
        print("-" * 40)
        
        # Sort AH keys
        ah_keys = sorted([k for k in stats[group].keys() if k != "TOTAL"], key=lambda x: float(x))
        ah_keys.append("TOTAL")
        
        for k in ah_keys:
            s = stats[group][k]
            if s["total"] == 0: continue
            
            # Win % calculated as (Wins + HalfWins*0.5) / (Total - Pushes)
            # Actually, standard way is (Wins + HalfWins*0.5) / Total
            win_rate = (s["wins"] + s["half_wins"] * 0.5) / s["total"] * 100
            roi = (s["profit"] / s["total"]) * 100
            
            print(f"{k:<10} | {s['total']:<6} | {win_rate:>7.2f}% | {roi:>7.2f}%")

if __name__ == "__main__":
    main()
