
import json
import re

file_path = r"c:\Users\Usuario\Desktop\Version-00-nueva\data\data_minus_ah_1.5.json"

def test_regex():
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    regex_pattern = r'movimiento:.*?>\s*([+-]?\d*\.?\d+)\s*[→->]\s*([+-]?\d*\.?\d+)'
    
    count = 0
    for match in data:
        html = match.get('market_analysis_html')
        if not html: continue
        
        found = re.search(regex_pattern, html)
        if found:
            print(f"Match {match.get('match_id')}: Found {found.group(1)} -> {found.group(2)}")
            count += 1
            if count > 5: break
        else:
             # Print fail sample
             if "movimiento" in html:
                 print(f"Match {match.get('match_id')}: Failed to match regex in: {html[:200]}...")

if __name__ == "__main__":
    test_regex()
