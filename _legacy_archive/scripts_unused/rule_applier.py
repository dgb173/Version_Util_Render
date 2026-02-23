"""
Rule Applier - Aplica los 200 Patrones de Oro generados
para mostrar predicciones en la columna Pick de precacheo.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

class RuleApplier:
    def __init__(self, patterns_path: str = None):
        self.patterns = []
        # Look for the patterns file in the current directory (scripts/) or models/ as fallback
        base_path = Path(__file__).parent
        self.patterns_path = patterns_path or str(base_path / '200_gold_patterns.json')
        self.load_patterns()

    def load_patterns(self):
        try:
            p = Path(self.patterns_path)
            if p.exists():
                with open(p, 'r', encoding='utf-8') as f:
                    self.patterns = json.load(f)
                print(f"[RuleApplier] Loaded {len(self.patterns)} gold patterns")
            else:
                # Try fallback to project root scripts/
                fallback = Path(__file__).parent.parent / 'scripts' / '200_gold_patterns.json'
                if fallback.exists():
                    with open(fallback, 'r', encoding='utf-8') as f:
                        self.patterns = json.load(f)
                    print(f"[RuleApplier] Loaded {len(self.patterns)} gold patterns from fallback")
                else:
                    print(f"[RuleApplier] Warning: 200_gold_patterns.json not found")
        except Exception as e:
            print(f"[RuleApplier] Error loading patterns: {e}")
            self.patterns = []

    def _parse_score(self, score_str: str) -> Tuple[Optional[int], Optional[int]]:
        if not score_str or not isinstance(score_str, str): return None, None
        try:
            # Handle both "1-0" and "1:0"
            match = re.search(r'(\d+)\s*[:\-]\s*(\d+)', score_str)
            if match: return int(match.group(1)), int(match.group(2))
        except: pass
        return None, None

    def _get_outcomes(self, home_g, away_g, ah, is_home_fav):
        if home_g is None or away_g is None: return None, None
        # Perspective of favorite (ah is positive if Home is fav, negative if Away is fav)
        # But here we pass is_home_fav separately.
        
        # Consistent with brute force: 
        # diff = (home_g - away_g - ah) if is_home_fav else (away_g - home_g - abs(ah))
        # Note: ah passed here should be the absolute AH value for the math to match.
        ah_abs = abs(ah)
        diff = (home_g - away_g - ah_abs) if is_home_fav else (away_g - home_g - ah_abs)
        
        if diff >= 0.5: f, u = "WIN", "LOSS"
        elif diff == 0.25: f, u = "HALF_WIN", "HALF_LOSS"
        elif diff == 0: f, u = "PUSH", "PUSH"
        elif diff == -0.25: f, u = "HALF_LOSS", "HALF_WIN"
        else: f, u = "LOSS", "WIN"
        return f, u

    def _safe_float(self, val):
        if val is None: return 0.0
        try:
            clean_val = str(val).strip().replace("'", "").replace("\u2192", "->")
            if clean_val == "N/A" or clean_val == "-" or not clean_val:
                return 0.0
            return float(clean_val)
        except:
            return 0.0

    def _get_rank_diff_cat(self, fav_rank, und_rank):
        try:
            fr = int(fav_rank)
            ur = int(und_rank)
            diff = fr - ur
            if diff <= -10: return "R_SUPERIOR"
            if diff <= -3: return "R_BETTER"
            if diff < 3: return "R_EVEN"
            return "R_INFERIOR"
        except:
            return "R_UNKNOWN"

    def get_match_features(self, match):
        """Extract features matching the brute force script logic exactly."""
        try:
            odds = match.get("main_match_odds", {})
            ah_str = odds.get("ah_linea")
            curr_ah_val = self._safe_float(ah_str)
            ah_abs = abs(curr_ah_val)
            is_home_fav = curr_ah_val >= 0

            # Rankings
            f_stand = match.get("home_standings" if is_home_fav else "away_standings", {})
            u_stand = match.get("away_standings" if is_home_fav else "home_standings", {})
            rdif_cat = self._get_rank_diff_cat(f_stand.get("ranking"), u_stand.get("ranking"))
            
            # AH Category
            ah_cat = "AH_SMALL" if ah_abs <= 0.5 else "AH_LARGE"

            # Fav Previous
            f_p_m = match.get("last_home_match" if is_home_fav else "last_away_match")
            if not f_p_m or not f_p_m.get("score"): return None
            gh, ga = self._parse_score(f_p_m["score"])
            gah = self._safe_float(f_p_m.get("handicap_line_raw"))
            fr, _ = self._get_outcomes(gh, ga, gah, gah >= 0) if is_home_fav else (None, None)
            if not is_home_fav: _, fr = self._get_outcomes(gh, ga, gah, gah >= 0)
            f_gen = "F_GEN_WIN" if fr in ["WIN", "HALF_WIN"] else "F_GEN_LOSS" if fr in ["LOSS", "HALF_LOSS"] else "F_GEN_PUSH"
            
            if is_home_fav: fwdl = "W" if gh > ga else "D" if gh == ga else "L"
            else: fwdl = "W" if ga > gh else "D" if gh == ga else "L"

            # Und Previous
            u_p_m = match.get("last_away_match" if is_home_fav else "last_home_match")
            if not u_p_m or not u_p_m.get("score"): return None
            ugh, uga = self._parse_score(u_p_m["score"])
            ugah = self._safe_float(u_p_m.get("handicap_line_raw"))
            _, ur = self._get_outcomes(ugh, uga, ugah, ugah >= 0) if is_home_fav else (None, None)
            if not is_home_fav: ur, _ = self._get_outcomes(ugh, uga, ugah, ugah >= 0)
            u_gen = "U_GEN_WIN" if ur in ["WIN", "HALF_WIN"] else "U_GEN_LOSS" if ur in ["LOSS", "HALF_LOSS"] else "U_GEN_PUSH"
            
            if is_home_fav: uwdl = "W" if uga > ugh else "D" if uga == ugh else "L"
            else: uwdl = "W" if ugh > uga else "D" if uga == ugh else "L"

            # Stadium (H2H Estadio)
            h_s = match.get("h2h_stadium")
            if not h_s or not h_s.get("res1_raw"): 
                mkt = match.get("market_analysis_data", {}).get("stadium", {})
                if not mkt or not mkt.get("result"): return None
                sh, sa = self._parse_score(mkt.get("result", ""))
                sah = self._safe_float(mkt.get("movement", "").split("->")[0].split("\u2192")[0].strip())
            else:
                sh, sa = self._parse_score(h_s["res1_raw"])
                sah = self._safe_float(h_s.get("ah1"))

            sr, _ = self._get_outcomes(sh, sa, sah, sah >= 0) if is_home_fav else (None, None)
            if not is_home_fav: _, sr = self._get_outcomes(sh, sa, sah, sah >= 0)
            f_stad = "F_STAD_WIN" if sr in ["WIN", "HALF_WIN"] else "F_STAD_LOSS" if sr in ["LOSS", "HALF_LOSS"] else "F_STAD_PUSH"

            p_f_ah = sah if is_home_fav else (-sah if sah >= 0 else abs(sah))
            move = "UP" if ah_abs > p_f_ah else "DOWN" if ah_abs < p_f_ah else "SAME"

            return {
                "AH": ah_abs, "AHC": ah_cat, "LOC": "HOME" if is_home_fav else "AWAY",
                "RDIF": rdif_cat, "FGEN": f_gen, "UGEN": u_gen, "FSTAD": f_stad, 
                "MOVE": move, "FWDL": fwdl, "UWDL": uwdl
            }
        except Exception as e:
            # print(f"[RuleApplier] Feature Extraction Error: {e}")
            return None

    def apply_patterns(self, match_data: Dict) -> List[Dict]:
        features = self.get_match_features(match_data)
        if not features: return []
        
        matches = []
        for p in self.patterns:
            conditions = p.get('key', [])
            is_match = True
            for feat, val in conditions:
                if features.get(feat) != val:
                    is_match = False
                    break
            
            if is_match:
                matches.append(p)
        
        # Sort by ROI descending
        matches.sort(key=lambda x: x.get('roi', 0), reverse=True)
        return matches

    def get_best_pick(self, match_data: Dict) -> Optional[Dict]:
        matches = self.apply_patterns(match_data)
        if not matches: return None
        
        best = matches[0]
        tipo = best.get('tipo', 'FAVORITO')
        roi = best.get('roi', 0)
        samples = best.get('total', 0)
        
        # Determine the side to bet
        ah_str = match_data.get("main_match_odds", {}).get("ah_linea")
        try:
            ah_val = float(ah_str)
            is_home_fav = ah_val >= 0
        except:
            return None
            
        if tipo == 'FAVORITO':
            pick = 'HOME' if is_home_fav else 'AWAY'
        else: # UNDERDOG
            pick = 'AWAY' if is_home_fav else 'HOME'
            
        # Context note for the reason
        cond_text = ", ".join([f"{k}={v}" for k, v in best.get('key', [])])
        
        return {
            'pick': pick,
            'probability': int(roi + 50), # Dummy probability (>50 for profitable)
            'rule_name': f"Gold Pattern {tipo}",
            'samples': samples,
            'reason': f"ROI: {roi}% | Muestra: {samples}. Factores: {cond_text}"
        }

# Singleton
_rule_applier = None
def get_rule_applier() -> RuleApplier:
    global _rule_applier
    if _rule_applier is None: _rule_applier = RuleApplier()
    return _rule_applier

def apply_rules_to_match(match_data: Dict) -> Optional[Dict]:
    return get_rule_applier().get_best_pick(match_data)
