
import json
import re
import sys
from pathlib import Path
from datetime import datetime

# Constants for Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
DATA_DIR = PROJECT_ROOT / 'data'

# Add project root to path for scripts/ imports
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def parse_score(score_str):
    if not score_str: return None
    s = str(score_str).replace(':', '-').replace(' ', '')
    if '-' not in s: return None
    try:
        p = s.split('-')
        return int(p[0]), int(p[1])
    except:
        return None

def parse_stats_rows(rows):
    res = {}
    if not rows: return res
    for r in rows:
        lbl = (r.get('label') or '').strip()
        try:
            res[lbl] = {'h': float(r.get('home', 0)), 'a': float(r.get('away', 0))}
        except: pass
    return res

def days_between(d1_str, d2_str):
    # Formats: YYYY-MM-DD or MM/DD/YYYY
    def parse_dt(d):
        for fmt in ["%Y-%m-%d", "%m/%d/%Y"]:
            try:
                return datetime.strptime(d, fmt)
            except: pass
        return None

    dt1 = parse_dt(d1_str)
    dt2 = parse_dt(d2_str)
    if dt1 and dt2:
        return abs((dt1 - dt2).days)
    return 9999 # Return high if parse fails

class SpecialistValidator:
    def __init__(self):
        self.rules = []
        self.load_rules()

    def load_rules(self):
        self.rules = []
        if not RESULTS_DIR.exists():
            return

        # Load ALL specialist rules
        for f in RESULTS_DIR.glob('specialist_*.json'):
            try:
                with open(f, 'r', encoding='utf-8') as fh:
                    file_rules = json.load(fh)
                    
                    # Check if new format (has 'model' key) or old format
                    for r in file_rules:
                        # New simple format already has 'type' set
                        if 'type' not in r:
                            # Try to infer from filename for old format
                            name = f.stem
                            parts = name.split('_')
                            if len(parts) >= 3:
                                r['type'] = parts[1].upper()
                                try:
                                    r['target_line'] = float(parts[2])
                                except:
                                    r['target_line'] = 0.0
                        
                        # Ensure type is set
                        if 'type' not in r:
                            r['type'] = 'AH'
                        if 'target_line' not in r:
                            r['target_line'] = 0.0
                            
                        self.rules.append(r)
            except Exception as e:
                print(f"Error loading {f}: {e}")

        # 2. Load ADVANCED rules (new)
        adv_file = RESULTS_DIR / 'advanced_rules_col3.json'
        if adv_file.exists():
            try:
                with open(adv_file, 'r', encoding='utf-8') as fh:
                    adv_rules = json.load(fh)
                    for r in adv_rules:
                        if 'type' not in r: r['type'] = 'AH'
                        if 'algorithm' not in r: r['algorithm'] = 'ADVANCED'
                        if 'target_line' not in r: r['target_line'] = 0.0
                        self.rules.append(r)
            except Exception as e:
                print(f"Error loading advanced rules: {e}")
                
        print(f"SpecialistValidator: Loaded {len(self.rules)} rules.")

    # --- HELPER FUNCTIONS FOR ADVANCED FEATURES ---
    def get_asian_result_category(self, team_goals, opp_goals, ah):
        diff = (team_goals - opp_goals) - ah
        if diff > 0.25: return 'WIN'
        if diff < -0.25: return 'LOSS'
        return 'PUSH'

    def get_wdl_rank(self, score_str, is_home):
        s = parse_score(score_str)
        if not s: return None
        hg, ag = s
        team_goals = hg if is_home else ag
        opp_goals = ag if is_home else hg
        if team_goals > opp_goals: return 2
        if team_goals == opp_goals: return 1
        return 0

    def get_movement_direction(self, mov_str):
        if not mov_str or '->' not in mov_str: return 'N/A'
        try:
            parts = mov_str.split('->')
            start = float(parts[0])
            end = float(parts[1])
            if end > start: return 'UP'
            if end < start: return 'DOWN'
            return 'EQUAL'
        except: return 'N/A'

    def extract_advanced_features(self, m):
        f = {}
        odds = m.get('main_match_odds', {})
        try:
            # Fallback for data_precacheo.json/history.json structure
            current_ah = float(odds.get('ah_linea') or m.get('handicap') or m.get('ah') or 0.0)
            current_ou = float(odds.get('goals_linea') or m.get('ou') or m.get('goals_linea') or 2.5)
        except:
            current_ah = 0.0
            current_ou = 2.5
        
        f['current_ah'] = current_ah
        f['current_ou'] = current_ou
        
        # Context
        fav_is_local = current_ah > 0
        fav_is_visitor = current_ah <= 0
        f['fav_is_home'] = fav_is_local
        f['is_heavy_fav'] = abs(current_ah) >= 1.0

        # Prev Home
        pc_home = m.get('last_home_match')
        f['ph_exists'] = bool(pc_home and pc_home.get('score'))
        if f['ph_exists']:
            ph_score_str = pc_home.get('score')
            parsed_ph = parse_score(ph_score_str)
            if parsed_ph:
                ph_cover = self.get_asian_result_category(parsed_ph[0], parsed_ph[1], current_ah)
                f['ph_covers_current'] = (ph_cover == 'WIN')
                f['ph_loses_current'] = (ph_cover == 'LOSS')
            else:
                f['ph_covers_current'] = False
                f['ph_loses_current'] = False
        else:
             f['ph_covers_current'] = False
             f['ph_loses_current'] = False

        # Prev Away
        pc_away = m.get('last_away_match')
        f['pa_exists'] = bool(pc_away and pc_away.get('score'))
        if f['pa_exists']:
            pa_score_str = pc_away.get('score')
            parsed_pa = parse_score(pa_score_str)
            if parsed_pa:
                # Away perspective: (Away - Home) - (-current_ah)
                pa_diff = (parsed_pa[1] - parsed_pa[0]) - (-current_ah)
                if pa_diff > 0.25: pa_res = 'WIN'
                elif pa_diff < -0.25: pa_res = 'LOSS'
                else: pa_res = 'PUSH'
                f['pa_covers_current'] = (pa_res == 'WIN')
                f['pa_loses_current'] = (pa_res == 'LOSS')
            else:
                f['pa_covers_current'] = False
                f['pa_loses_current'] = False
        else:
            f['pa_covers_current'] = False
            f['pa_loses_current'] = False

        # H2H Col 3
        col3 = m.get('h2h_col3', {})
        f['has_col3'] = (col3.get('status') == 'found')
        if f['has_col3']:
            home_team = (m.get('home_team') or '').lower().strip()
            away_team = (m.get('away_team') or '').lower().strip()
            
            fav_wdl = None
            mirror_team = None
            
            if fav_is_local and f['ph_exists']:
                 prev_h_home = (pc_home.get('home_team') or '').lower().strip()
                 fav_played_home = home_team in prev_h_home
                 fav_wdl = self.get_wdl_rank(pc_home.get('score'), fav_played_home)
                 if f['pa_exists']:
                     prev_a_home = (pc_away.get('home_team') or '').lower().strip()
                     prev_a_away = (pc_away.get('away_team') or '').lower().strip()
                     if away_team in prev_a_home: mirror_team = prev_a_away
                     else: mirror_team = prev_a_home
            elif fav_is_visitor and f['pa_exists']:
                 prev_a_home = (pc_away.get('home_team') or '').lower().strip()
                 fav_played_home_in_prev = away_team in prev_a_home
                 fav_wdl = self.get_wdl_rank(pc_away.get('score'), fav_played_home_in_prev)
                 if f['ph_exists']:
                     prev_h_home = (pc_home.get('home_team') or '').lower().strip()
                     prev_h_away = (pc_home.get('away_team') or '').lower().strip()
                     if home_team in prev_h_home: mirror_team = prev_h_away
                     else: mirror_team = prev_h_home
            
            mirror_wdl = None
            if mirror_team:
                h2h_home = (col3.get('h2h_home_team_name') or '').lower().strip()
                mirror_played_home = mirror_team in h2h_home
                h2h_score = f"{col3.get('goles_home', 0)}-{col3.get('goles_away', 0)}"
                mirror_wdl = self.get_wdl_rank(h2h_score, mirror_played_home)
            
            if fav_wdl is not None and mirror_wdl is not None:
                if fav_wdl > mirror_wdl: f['col3_mejora'] = True
                elif fav_wdl == mirror_wdl: f['col3_iguala'] = True
                elif fav_wdl < mirror_wdl: f['col3_empeora'] = True

            mirror_is_home_h2h = mirror_team and (mirror_team in (col3.get('h2h_home_team_name') or '').lower().strip())
            base_type = 'directa' if mirror_is_home_h2h else 'inversa'
            if fav_is_visitor:
                final_type = 'inversa' if base_type == 'directa' else 'directa'
            else:
                final_type = base_type
            f['col3_directa'] = (final_type == 'directa')
            f['col3_inversa'] = (final_type == 'inversa')

        # Movements
        stadium = m.get('market_analysis_data', {}).get('stadium', {})
        general = m.get('market_analysis_data', {}).get('general', {})
        st_mov = self.get_movement_direction(stadium.get('movement'))
        gen_mov = self.get_movement_direction(general.get('movement'))
        
        f['mov_stadium_up'] = (st_mov == 'UP')
        f['mov_stadium_down'] = (st_mov == 'DOWN')
        f['mov_general_up'] = (gen_mov == 'UP')
        f['mov_general_down'] = (gen_mov == 'DOWN')

        # NEW: Advanced Mined Features (Stats from previous matches)
        lhm = m.get('last_home_match') or {}
        lam = m.get('last_away_match') or {}
        sh = self._parse_stats(lhm.get('stats_rows'))
        sa = self._parse_stats(lam.get('stats_rows'))
        
        # Guard: Ensure we actually have stats for both teams
        f['has_stats_h'] = bool(sh)
        f['has_stats_a'] = bool(sa)
        f['has_advanced_stats'] = f['has_stats_h'] and f['has_stats_a']

        # AH Patterns metrics
        h_da_d = sh.get('Ataques Peligrosos', {}).get('h', 0) - sh.get('Ataques Peligrosos', {}).get('a', 0)
        v_da_d = sa.get('Ataques Peligrosos', {}).get('a', 0) - sa.get('Ataques Peligrosos', {}).get('h', 0)
        f['h_da_d'] = h_da_d
        f['v_da_d'] = v_da_d
        f['da_g'] = h_da_d - v_da_d
        f['h_sot_r'] = sh.get('Tiros a Puerta', {}).get('h', 0) / max(1, sh.get('Tiros', {}).get('h', 0))
        f['v_sot_r'] = sa.get('Tiros a Puerta', {}).get('a', 0) / max(1, sa.get('Tiros', {}).get('a', 0))
        f['ind'] = 1 if (m.get('comparativas_indirectas', {}).get('left') or m.get('comparativas_indirectas', {}).get('right')) else 0
        f['col3'] = 1 if m.get('h2h_col3', {}).get('status') == 'found' else 0

        # O/U Patterns metrics
        h_da = sh.get('Ataques Peligrosos', {}).get('h', 0)
        a_da = sa.get('Ataques Peligrosos', {}).get('a', 0)
        h_sot = sh.get('Tiros a Puerta', {}).get('h', 0)
        a_sot = sa.get('Tiros a Puerta', {}).get('a', 0)
        
        f['da_total'] = h_da + a_da
        f['sot_total'] = h_sot + a_sot
        f['da_diff_h'] = h_da - a_da
        f['da_diff_v'] = a_da - h_da
        f['sot_diff_h'] = h_sot - a_sot
        f['sot_diff_v'] = a_sot - h_sot
        f['da_ratio_h'] = h_da / max(1, a_da)
        f['da_ratio_v'] = a_da / max(1, h_da)
        
        # Goals trend from scores (more reliable than stats_rows)
        h_goles_total = 0
        if f['ph_exists']:
            s_ph = parse_score(pc_home.get('score'))
            if s_ph: h_goles_total = s_ph[0] + s_ph[1]
            
        a_goles_total = 0
        if f['pa_exists']:
            s_pa = parse_score(pc_away.get('score'))
            if s_pa: a_goles_total = s_pa[0] + s_pa[1]
            
        f['trend'] = h_goles_total + a_goles_total
        
        f['ah_abs'] = abs(current_ah)
        f['fav_is_local'] = current_ah > 0
        col3_data = m.get('h2h_col3', {})
        f['col3_perf'] = col3_data.get('performance') or col3_data.get('h2h_result') or 'N/A'
        
        return f

    def _parse_stats(self, rows):
        r = {}
        if not rows: return r
        for row in rows:
            try:
                label = row.get('label')
                h = int(str(row.get('home', '0')).replace('%',''))
                a = int(str(row.get('away', '0')).replace('%',''))
                r[label] = {'h': h, 'a': a}
            except: pass
        return r

    def extract_features(self, match, target_line, line_type='AH'):
        # Updated to capture "Contextual Inertia", "Dominance", and "20 Days Rule"
        f = {}
        
        # 1. Odds & Handicaps
        main_odds = match.get('main_match_odds') or {}
        try:
            current_ah = float(main_odds.get('ah_linea') or match.get('handicap') or match.get('ah') or 0.0)
            current_ou = float(main_odds.get('goals_linea') or match.get('ou') or match.get('goals_linea') or 2.5)
        except:
            return {} # Minimal data missing

        f['current_ah'] = current_ah
        f['line_value'] = current_ah if line_type == 'AH' else current_ou
        f['current_ah_val'] = current_ah # Compatibility
        f['current_ou_val'] = current_ou # Compatibility
        
        # Match Date (for 20 days check)
        match_date = match.get('match_date') or match.get('date') or datetime.now().strftime("%Y-%m-%d")

        # 2. Ranks & Standings
        hs = match.get('home_standings') or {}
        as_ = match.get('away_standings') or {}
        try:
            hr = int(hs.get('ranking', 0) or 0)
            ar = int(as_.get('ranking', 0) or 0)
            if hr > 0 and ar > 0:
                f['rank_diff'] = ar - hr # Positive if Home (Rank 2) vs Away (Rank 18) -> 16
                f['home_better_rank'] = (hr < ar)
                f['context_fav_home'] = (current_ah > 0) # Positive HA = Local favorite
            else:
                f['rank_diff'] = 0
                f['home_better_rank'] = False
                f['context_fav_home'] = False
        except: 
            f['rank_diff'] = 0
            f['home_better_rank'] = False
            f['context_fav_home'] = False

        # Simple model features
        # Simple model features (MATCHING train_simple_model.py)
        f['fav_home_strong'] = current_ah >= 1.0  # LOCAL muy favorito
        f['fav_away_strong'] = current_ah <= -1.0  # VISITANTE muy favorito
        f['is_heavy_fav'] = abs(current_ah) >= 1.0
        f['underdog_home'] = current_ah <= -1.0 # Local es underdog (Visita muy fav)
        f['underdog_away'] = current_ah >= 1.0 # Visita es underdog (Local muy fav)
        f['rank_close'] = abs(f['rank_diff']) <= 3

        # 3. Market Inertia (Movement)
        # Parse "1.5 -> 2" from market_analysis_data
        md = match.get('market_analysis_data') or {}
        stad = md.get('stadium') or {}
        gen = md.get('general') or {}
        
        def parse_movement(mov_str):
            if not mov_str or '→' not in mov_str: return 0.0
            try:
                parts = mov_str.split('→')
                start = float(parts[0].strip())
                end = float(parts[1].strip())
                # If AH is negative, line moving -1.5 -> -2 is "Strengthening Fav".
                # Standardize: End - Start.
                return end - start
            except: return 0.0

        f['market_inertia_stadium'] = parse_movement(stad.get('movement'))
        f['market_inertia_general'] = parse_movement(gen.get('movement'))

        # 4. H2H Col3 (Triangulation)
        # data_precacheo.json uses 'h2h_col3' dict
        col3 = match.get('h2h_col3') or {}
        # We don't have numeric attributes in JSON easily, but look for 'goles_home'
        # Logic: MEJORA/IGUALA not directly in JSON unless calculated.
        # But we can check goals advantage from the proxy match.
        try:
             gh = float(col3.get('goles_home') or 0)
             ga = float(col3.get('goles_away') or 0)
             f['col3_home_advantage'] = gh - ga
             f['has_col3'] = (col3.get('status') == 'found')
        except:
             f['col3_home_advantage'] = 0
             f['has_col3'] = False

        # 5. Dominance (Previous Match Stats) with 20 DAYS RULE
        def get_dominance_and_recency(last_match, ref_date):
            if not last_match: return 0, False
            
            # Check Recency
            lm_date = last_match.get('date')
            days = days_between(ref_date, lm_date)
            is_recent = days <= 20
            
            if not is_recent:
                # User says: "no uses esos partidos como datos medibles"
                return 0, False
                
            rows = parse_stats_rows(last_match.get('stats_rows'))
            if not rows: return 0, True # Recent but no stats
            
            # Extract DA
            da = rows.get('Ataques Peligrosos', {'h':0, 'a':0})
            
            # Difference: Home - Away (in THAT match context)
            # We assume stats are mapped to 'home' and 'away' of THAT match.
            pressure_diff = (da['h'] - da['a']) 
            return pressure_diff, True

        lhm = match.get('last_home_match')
        lam = match.get('last_away_match')
        
        dom_h, valid_h = get_dominance_and_recency(lhm, match_date)
        dom_a, valid_a = get_dominance_and_recency(lam, match_date)
        
        f['home_prev_dominance'] = dom_h
        f['away_prev_dominance'] = dom_a
        f['valid_recent_data'] = (valid_h and valid_a)
        
        # Unlucky Loss? (Dominated but lost > 20 days ago? Ignored)
        score_home = parse_score(lhm.get('score')) if lhm else None
        if score_home and valid_h:
            # If Home lost (h < a) but dominance > 15 => Unlucky
            f['home_unlucky_loss'] = (score_home[0] < score_home[1]) and (dom_h > 15)
        else:
            f['home_unlucky_loss'] = False
        
        # === SIMPLE MODEL FEATURES ===
        # prev_home_won/lost
        if score_home:
            f['prev_home_won'] = score_home[0] > score_home[1]
            f['prev_home_lost'] = score_home[0] < score_home[1]
            f['prev_home_draw'] = score_home[0] == score_home[1]
        else:
            f['prev_home_won'] = False
            f['prev_home_lost'] = False
            f['prev_home_draw'] = False
        
        # prev_away_won/lost
        score_away = parse_score(lam.get('score')) if lam else None
        if score_away:
            # In away match, the visiting team's perspective
            f['prev_away_won'] = score_away[1] > score_away[0]
            f['prev_away_lost'] = score_away[1] < score_away[0]
            f['prev_away_draw'] = score_away[1] == score_away[0]
        else:
            f['prev_away_won'] = False
            f['prev_away_lost'] = False
            f['prev_away_draw'] = False
        
        # Momentum features
        f['home_momentum'] = f['prev_home_won'] and (dom_h > 10)
        f['away_momentum'] = f['prev_away_won'] and (dom_a > 10)
        f['both_won'] = f['prev_home_won'] and f['prev_away_won']
        f['both_lost'] = f['prev_home_lost'] and f['prev_away_lost']
            
        return f

    def evaluate_match(self, match):
        results = []
        
        # Evaluate for AH
        try:
            ah_target = float(match.get('main_match_odds', {}).get('ah_linea', 0) or 0)
            feats_ah = self.extract_features(match, ah_target, 'AH')
            
            for r in self.rules:
                if r.get('type') != 'AH' or r.get('algorithm'): continue
                
                # Context Safety Check: Don't apply Home Fav rules to Away Fav matches and vice-versa
                rule_target = r.get('target_line', 0)
                # If Signs Differ (and not 0), skip.
                if (rule_target < 0 and ah_target > 0) or (rule_target > 0 and ah_target < 0):
                    continue
                
                if self.check_rule(r, feats_ah):
                    # Clone to avoid mutating original
                    r_out = r.copy()
                    # Add Source Tag as requested
                    origin = r.get('target_line')
                    r_out['name'] = f"[AH {origin}] {r['name']}"
                    r_out['match_id'] = match.get('match_id')
                    results.append(r_out)
        except Exception as e:
            # print(f"Error AH: {e}")
            pass

        # 2. ADVANCED RULES Eval
        try:
            feats_adv = self.extract_advanced_features(match)
            for r in self.rules:
                if r.get('algorithm') not in ['ADVANCED', 'ADVANCED_MINED']: continue
                
                # Context check removed for advanced rules as they should handle it in conditions
                
                # Skip mined rules if we lack advanced stats (prev matches data)
                if r.get('algorithm') == 'ADVANCED_MINED' and not feats_adv.get('has_advanced_stats'):
                    continue
                    
                if self.check_rule(r, feats_adv):
                    r_out = r.copy()
                    r_out['match_id'] = match.get('match_id')
                    results.append(r_out)
                    
        except Exception as e:
            # print(f"Error Advanced: {e}")
            pass

        # Evaluate for OU
        try:
            ou_target = float(match.get('main_match_odds', {}).get('goals_linea', 2.5) or 2.5)
            feats_ou = self.extract_features(match, ou_target, 'OU')
            
            for r in self.rules:
                if r.get('type') != 'OU' or r.get('algorithm'): continue
                
                # Universal O/U Check
                if self.check_rule(r, feats_ou):
                    r_out = r.copy()
                    origin = r.get('target_line')
                    r_out['name'] = f"[OU {origin}] {r['name']}"
                    r_out['match_id'] = match.get('match_id')
                    results.append(r_out)
        except: pass
        
        # 3. PATTERNS V2 Eval (new PatternMiner v2 system)
        try:
            from scripts.pattern_miner_v2.precacheo_loader import get_loader
            loader = get_loader()
            if loader.loaded and (loader.ah_patterns or loader.ou_patterns):
                v2_result = loader.evaluate_match(match)
                # v2_result = {'ah_picks': [...], 'ou_picks': [...]}
                for pick in v2_result.get('ah_picks', []):
                    results.append({
                        'name': f"[v2] {pick.get('pattern_name', 'Pattern')}",
                        'pick': pick.get('pick'),
                        'type': 'AH',
                        'match_id': match.get('match_id'),
                        'accuracy': pick.get('accuracy_test'),
                        'roi': pick.get('roi_test'),
                        'n_train': pick.get('n_train'),
                        'explanation': pick.get('explanation'),
                        'algorithm': 'PATTERN_V2'
                    })
                for pick in v2_result.get('ou_picks', []):
                    results.append({
                        'name': f"[v2] {pick.get('pattern_name', 'Pattern')}",
                        'pick': pick.get('pick'),
                        'type': 'OU',
                        'match_id': match.get('match_id'),
                        'accuracy': pick.get('accuracy_test'),
                        'roi': pick.get('roi_test'),
                        'n_train': pick.get('n_train'),
                        'explanation': pick.get('explanation'),
                        'algorithm': 'PATTERN_V2'
                    })
        except Exception as e:
            # Silently fail if v2 patterns not available
            pass
        
        # Dedup by name
        seen = set()
        unique_results = []
        for res in results:
            if res['name'] not in seen:
                seen.add(res['name'])
                unique_results.append(res)
            
        return unique_results

    def check_rule(self, rule, features):
        # Universal Validator logic
        # condition: [feature, op, value]
        for cond in rule['conditions']:
            if isinstance(cond, list): # Legacy format
                feat, op, val = cond
                curr = features.get(feat)
                if curr is None: return False
                try:
                    if op == '>':
                        if not (curr > val): return False
                    elif op == '<':
                        if not (curr < val): return False
                    elif op == '==':
                        if not (curr == val): return False
                    elif op == '>=':
                        if not (curr >= val): return False
                    elif op == '<=':
                        if not (curr <= val): return False
                except: return False
            elif isinstance(rule['conditions'], dict): # Advanced format
                 # We are iterating keys of dict if we do 'for cond in rule[conditions]' BUT
                 # usually rule['conditions'] is the dict. 
                 # Let's fix the outer loop or handle dict access
                 pass
        
        if isinstance(rule['conditions'], dict):
             for feat, val in rule['conditions'].items():
                 curr = features.get(feat)
                 if curr != val: return False
             return True
             
        return True

# Singleton instance
validator = SpecialistValidator()
