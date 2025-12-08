import json

def inspect_match():
    try:
        with open('studied_matches/history.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print("history.json not found.")
        return

    target_home = "Centro Olimpico U20 (W)"
    target_away = "Santos FC U20 (W)"
    
    found = False
    for match in data:
        # Check main match details
        if target_home in match.get('home_team', '') and target_away in match.get('away_team', ''):
            print("Found Main Match:")
            print(json.dumps(match, indent=2))
            found = True
            break
            
        # Check if it's in the history of another match?
        # The screenshot shows "Historial de Partidos (Casa vs Fuera)"
        # And the match is listed there.
        # So it might be a historical match inside a main match object?
        # Or history.json contains a flat list of matches?
        
        # Let's assume history.json is a list of matches that have been studied.
        # But the screenshot shows a list of *historical* matches for a team.
        # These historical matches might be stored within the studied match object, 
        # or fetched dynamically.
        
        # If history.json stores the *result* of the study, it might contain the lists of historical matches.
        
    if not found:
        print("Match not found as a main entry. Searching inside historical lists...")
        for match in data:
            # Check home_last_matches
            for m in match.get('home_last_matches', []):
                if target_home in m.get('home', '') and target_away in m.get('away', ''):
                    print(f"Found in home_last_matches of match {match.get('match_id')}:")
                    print(json.dumps(m, indent=2))
                    found = True
            
            # Check away_last_matches
            for m in match.get('away_last_matches', []):
                if target_home in m.get('home', '') and target_away in m.get('away', ''):
                    print(f"Found in away_last_matches of match {match.get('match_id')}:")
                    print(json.dumps(m, indent=2))
                    found = True
                    
    if not found:
        print("Match not found in history.json. Checking data.json...")
        try:
            with open('data.json', 'r', encoding='utf-8') as f:
                data_cache = json.load(f)
                # data.json might be a dict or list
                if isinstance(data_cache, dict):
                    # It might be keyed by match ID
                    for mid, m in data_cache.items():
                        if isinstance(m, dict):
                             if target_home in m.get('home', '') and target_away in m.get('away', ''):
                                print(f"Found in data.json (Match ID {mid}):")
                                print(json.dumps(m, indent=2))
                                found = True
                        elif isinstance(m, list):
                            # Maybe it's a list of matches? Or a list of something else?
                            # Let's check the first item if it's a dict
                            if m and isinstance(m[0], dict):
                                for item in m:
                                    if target_home in item.get('home', '') and target_away in item.get('away', ''):
                                        print(f"Found in data.json (Match ID {mid}, list item):")
                                        print(json.dumps(item, indent=2))
                                        found = True
                            else:
                                # print(f"Skipping list for key {mid}, type: {type(m)}")
                                pass
                        else:
                             # print(f"Skipping key {mid}, type: {type(m)}")
                             pass
                elif isinstance(data_cache, list):
                    for m in data_cache:
                         if target_home in m.get('home', '') and target_away in m.get('away', ''):
                            print(f"Found in data.json:")
                            print(json.dumps(m, indent=2))
                            found = True
        except FileNotFoundError:
            print("data.json not found.")

    print("Searching for any match with AH -3 or '-3'...")
    found_ah = False
    for match in data:
        for m in match.get('home_last_matches', []) + match.get('away_last_matches', []):
            ah = m.get('ahLine')
            if ah == '-3' or ah == -3:
                print("Found match with AH -3:")
                print(json.dumps(m, indent=2))
                found_ah = True
                break
        if found_ah:
            break
            
    if not found_ah:
        print("No match with AH -3 found in history.json.")


if __name__ == "__main__":
    inspect_match()
