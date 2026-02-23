import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.rule_applier import apply_rules_to_match, get_rule_applier

# Dummy match data
dummy_match = {
    'main_match_odds': {'ah_linea': '-0.5'},
    'market_analysis_data': {'stadium': {'movement': '1.9 -> 1.8'}},
    'last_home_match': {'score': '1-0', 'stats_rows': [{'label': 'Fulltime Dangerous Attacks', 'home': '50', 'away': '30'}]},
    'last_away_match': {'score': '0-1', 'stats_rows': [{'label': 'Fulltime Dangerous Attacks', 'home': '40', 'away': '60'}]}
}

# Test load
applier = get_rule_applier()
print(f"Loaded rules: {len(applier.rules)}")

# Test match
pick = apply_rules_to_match(dummy_match)
print(f"Pick for dummy match: {pick}")
