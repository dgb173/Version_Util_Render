import json
import itertools
import os
from pathlib import Path

# Configuración
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
MIN_N = 15
TARGET_ROI = 30.0
TARGET_LEAGUE = "Liga Portugal 1"

def safe_float(val):
    if val is None: return 0.0
    try:
        # Limpiar posibles caracteres extraños
        clean_val = str(val).strip().replace("'", "").replace("\u2192", "->")
        if clean_val == "N/A" or clean_val == "-" or not clean_val:
            return 0.0
        return float(clean_val)
    except:
        return 0.0

def parse_score(score_str):
    if not score_str or ":" not in score_str: return None, None
    try:
        p = score_str.split(":")
        return int(p[0]), int(p[1])
    except: return None, None

def get_outcomes(gh, ga, ah, is_home_fav):
    if gh is None or ga is None: return None, None
    # Perspectiva del favorito
    diff = (gh - ga - ah) if is_home_fav else (ga - gh - abs(ah))
    if diff >= 0.5: f, u = "WIN", "LOSS"
    elif diff == 0.25: f, u = "HALF_WIN", "HALF_LOSS"
    elif diff == 0: f, u = "PUSH", "PUSH"
    elif diff == -0.25: f, u = "HALF_LOSS", "HALF_WIN"
    else: f, u = "LOSS", "WIN"
    return f, u

def calculate_profit(res, odds=1.8):
    if res == "WIN": return odds - 1
    if res == "HALF_WIN": return (odds - 1) / 2
    if res == "HALF_LOSS": return -0.5
    if res == "LOSS": return -1
    return 0

def get_match_features(match):
    ah_str = match.get("main_match_odds", {f"ah_linea"}).get("ah_linea")
    curr_ah_val = safe_float(ah_str)
    ah_abs = abs(curr_ah_val)
    
    is_home_fav = curr_ah_val >= 0

    # Fav Previous
    f_p_m = match.get("last_home_match" if is_home_fav else "last_away_match")
    if not f_p_m or not f_p_m.get("score"): return None
    gh, ga = parse_score(f_p_m["score"])
    gah = safe_float(f_p_m.get("handicap_line_raw"))
    fr, _ = get_outcomes(gh, ga, gah, gah >= 0) if is_home_fav else (None, None)
    if not is_home_fav: _, fr = get_outcomes(gh, ga, gah, gah >= 0)
    f_gen = "F_GEN_WIN" if fr in ["WIN", "HALF_WIN"] else "F_GEN_LOSS" if fr in ["LOSS", "HALF_LOSS"] else "F_GEN_PUSH"
    
    if is_home_fav: fwdl = "W" if gh > ga else "D" if gh == ga else "L"
    else: fwdl = "W" if ga > gh else "D" if gh == ga else "L"

    # Und Previous
    u_p_m = match.get("last_away_match" if is_home_fav else "last_home_match")
    if not u_p_m or not u_p_m.get("score"): return None
    ugh, uga = parse_score(u_p_m["score"])
    ugah = safe_float(u_p_m.get("handicap_line_raw"))
    _, ur = get_outcomes(ugh, uga, ugah, ugah >= 0) if is_home_fav else (None, None)
    if not is_home_fav: ur, _ = get_outcomes(ugh, uga, ugah, ugah >= 0)
    u_gen = "U_GEN_WIN" if ur in ["WIN", "HALF_WIN"] else "U_GEN_LOSS" if ur in ["LOSS", "HALF_LOSS"] else "U_GEN_PUSH"
    
    if is_home_fav: uwdl = "W" if uga > ugh else "D" if uga == ugh else "L"
    else: uwdl = "W" if ugh > uga else "D" if ugh == uga else "L"

    # Stadium
    h_s = match.get("market_analysis_data", {}).get("stadium", {})
    if not h_s or not h_s.get("result"): return None
    sh, sa = parse_score(h_s["result"].replace("-", ":"))
    sah = safe_float(h_s.get("movement", "").split("->")[0].split("\u2192")[0].strip())
    sr, _ = get_outcomes(sh, sa, sah, sah >= 0) if is_home_fav else (None, None)
    if not is_home_fav: _, sr = get_outcomes(sh, sa, sah, sah >= 0)
    f_stad = "F_STAD_WIN" if sr in ["WIN", "HALF_WIN"] else "F_STAD_LOSS" if sr in ["LOSS", "HALF_LOSS"] else "F_STAD_PUSH"

    p_f_ah = sah if is_home_fav else (-sah if sah >= 0 else abs(sah))
    move = "UP" if ah_abs > p_f_ah else "DOWN" if ah_abs < p_f_ah else "SAME"

    ch, ca = parse_score(match.get("final_score"))
    if ch is None: return None
    fav_r, und_r = get_outcomes(ch, ca, curr_ah_val, is_home_fav)
    
    return {
        "AH": ah_abs, "LOC": "HOME" if is_home_fav else "AWAY",
        "FGEN": f_gen, "UGEN": u_gen, "FSTAD": f_stad, "MOVE": move,
        "FWDL": fwdl, "UWDL": uwdl,
        "fav_p": calculate_profit(fav_r), "und_p": calculate_profit(und_r)
    }

def mine():
    print(f"Buscando partidos de {TARGET_LEAGUE}...")
    league_matches = []
    for f in DATA_DIR.glob("data_ah_*.json"):
        with open(f, "r", encoding="utf-8") as file:
            data = json.load(file)
            for m in data:
                if m.get("league_name") == TARGET_LEAGUE:
                    feat = get_match_features(m)
                    if feat: league_matches.append(feat)
    
    print(f"Encontrados {len(league_matches)} partidos con caracter\u00edsticas v\u00e1lidas.")
    
    dims = ["AH", "LOC", "FGEN", "UGEN", "FSTAD", "MOVE", "FWDL", "UWDL"]
    gold_patterns = []

    for length in range(2, 6):
        print(f"Probando combinaciones de longitud {length}...")
        for combo_dims in itertools.combinations(dims, length):
            counts = {}
            for m in league_matches:
                key = tuple((d, m[d]) for d in combo_dims)
                if key not in counts: counts[key] = {"f": [], "u": []}
                counts[key]["f"].append(m["fav_p"])
                counts[key]["u"].append(m["und_p"])
            
            for key, stats in counts.items():
                # Favorite
                n_f = len(stats["f"])
                if n_f >= MIN_N:
                    roi_f = (sum(stats["f"]) / n_f) * 100
                    if roi_f >= TARGET_ROI:
                        gold_patterns.append({"tipo": "FAVORITO", "key": key, "total": n_f, "roi": round(roi_f, 2)})
                
                # Underdog
                n_u = len(stats["u"])
                if n_u >= MIN_N:
                    roi_u = (sum(stats["u"]) / n_u) * 100
                    if roi_u >= TARGET_ROI:
                        gold_patterns.append({"tipo": "UNDERDOG", "key": key, "total": n_u, "roi": round(roi_u, 2)})

    # Limpiar duplicados y ordenar
    unique_patterns = []
    seen = set()
    for p in gold_patterns:
        k_str = str(sorted(p["key"])) + p["tipo"]
        if k_str not in seen:
            unique_patterns.append(p)
            seen.add(k_str)

    unique_patterns.sort(key=lambda x: (x["roi"], x["total"]), reverse=True)
    
    # Tomar los 200 mejores
    final_200 = unique_patterns[:200]
    
    output_path = PROJECT_ROOT / "scripts" / "200_gold_patterns.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_200, f, indent=2, ensure_ascii=False)
    
    print(f"¡Hecho! Guardados {len(final_200)} patrones en {output_path}")
    if final_200:
        print(f"Mejor ROI: {final_200[0]['roi']}% con N={final_200[0]['total']}")

if __name__ == "__main__":
    mine()
