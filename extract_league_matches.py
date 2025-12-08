import requests
import re
import json
import sys
import os

def extract_league_matches(league_id):
    # Default to current season 2025-2026 as per user context
    season = "2025-2026"
    url = f"https://football.nowgoal26.com/jsData/matchResult/{season}/s{league_id}_en.js"
    
    print(f"Fetching data from: {url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": f"https://football.nowgoal26.com/league/{league_id}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        content = response.text
        
        # Extract Match IDs using regex
        # Pattern looks for: [MatchID, LeagueID, ...
        # Example: [2789129,36,
        # We use the league_id in the regex to be extra safe
        pattern = r'\[' + r'(\d+),' + str(league_id) + r','
        
        match_ids = re.findall(pattern, content)
        
        # Remove duplicates if any (though unlikely in this structure)
        unique_ids = sorted(list(set(match_ids)))
        
        print(f"Found {len(unique_ids)} unique matches for league {league_id}.")
        
        output_file = f"league_{league_id}_matches.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(unique_ids, f, indent=2)
            
        print(f"Successfully saved match IDs to {output_file}")
        return unique_ids
        
    except Exception as e:
        print(f"Error extracting matches: {e}")
        return []

if __name__ == "__main__":
    if len(sys.argv) > 1:
        lid = sys.argv[1]
    else:
        lid = "36" # Default to Premier League
        
    extract_league_matches(lid)
