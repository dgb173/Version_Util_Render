
import json
import sys
from scripts.pattern_miner_v2.features_builder_v2 import build_match_features

def test_feature_extraction():
    # Cargar 1 partido que tenga HTML
    try:
        with open('data/data_ah_0.5_pl_only_backup.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        print(f"Loaded {len(data)} matches.")
        
        # Buscar uno con HTML y AH
        match = None
        for m in data:
            if m.get('historical_matches_html') and m.get('main_match_odds', {}).get('ah_linea'):
                match = m
                break
                
        if not match:
            print("No match found with HTML and AH")
            return

        print(f"Testing match: {match['home_name']} vs {match['away_name']}")
        print(f"AH: {match['main_match_odds'].get('ah_linea')}")
        
        # Check Indirectas
        print("\n--- INDIRECTAS RAW ---")
        ind = match.get('comparativas_indirectas')
        if ind:
            print(json.dumps(ind, indent=2))
        else:
            print("No comparativas_indirectas found")
            
        features = build_match_features(match)
        
        print("\n--- EXTRACTED HIST FEATURES ---")
        for k, v in features.items():
            if 'HIST' in k:
                print(f"{k}: {v}")
                
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_feature_extraction()
