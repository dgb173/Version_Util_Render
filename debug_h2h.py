import json
from collections import defaultdict
import datetime

def load_data():
    with open('data.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data.get('finished_matches', [])

def find_h2h_pairs(matches):
    pairs = defaultdict(list)
    for m in matches:
        h = m.get('home_team', '').strip()
        a = m.get('away_team', '').strip()
        date_str = m.get('time_obj') or m.get('match_date') or m.get('date')
        
        if not h or not a or not date_str:
            continue
            
        # Sort to make key unique regardless of home/away
        key = tuple(sorted([h, a]))
        pairs[key].append({
            'date': date_str,
            'home': h,
            'away': a,
            'score': m.get('score') or m.get('final_score')
        })
        
    # Filter for pairs with > 1 match
    multi_h2h = {k: v for k, v in pairs.items() if len(v) > 1}
    return multi_h2h

def test_get_h2h(matches):
    pairs = find_h2h_pairs(matches)
    print(f"Found {len(pairs)} pairs with multiple matches.")
    
    # Pick one
    if not pairs:
        print("No pairs found.")
        return

    # Sort matches by date for the first pair
    first_pair = list(pairs.keys())[0]
    match_list = pairs[first_pair]
    match_list.sort(key=lambda x: x['date'])
    
    print(f"Testing pair: {first_pair}")
    for m in match_list:
        print(f" - {m['date']}: {m['home']} vs {m['away']} ({m['score']})")
        
    # Test get_h2h_history logic
    # We want to find the H2H for the *last* match, which should be the *second to last* match.
    latest_match = match_list[-1]
    target_date = latest_match['date']
    h_team = latest_match['home']
    a_team = latest_match['away']
    
    print(f"\nSearching for H2H before {target_date} for {h_team} vs {a_team}...")
    
    # Simulate get_h2h_history logic
    candidates = []
    h_norm = h_team.strip().lower()
    a_norm = a_team.strip().lower()
    
    try:
        if 'T' in target_date:
             t_date = datetime.datetime.strptime(target_date, "%Y-%m-%dT%H:%M:%S")
        elif ' ' in target_date:
            t_date = datetime.datetime.strptime(target_date, "%Y-%m-%d %H:%M:%S")
        else:
            t_date = datetime.datetime.strptime(target_date, "%Y-%m-%d")
    except Exception as e:
        print(f"Date parse error: {e}")
        return

    for m in matches:
        m_date_val = m.get('time_obj') or m.get('match_date') or m.get('date')
        if not m_date_val: continue
        
        try:
            if 'T' in m_date_val:
                 m_date = datetime.datetime.strptime(m_date_val, "%Y-%m-%dT%H:%M:%S")
            elif ' ' in m_date_val:
                m_date = datetime.datetime.strptime(m_date_val, "%Y-%m-%d %H:%M:%S")
            else:
                m_date = datetime.datetime.strptime(m_date_val, "%Y-%m-%d")
        except:
            continue
            
        if m_date >= t_date:
            continue
            
        mh = m.get('home_team', '').strip().lower()
        ma = m.get('away_team', '').strip().lower()
        
        if (mh == h_norm and ma == a_norm) or (mh == a_norm and ma == h_norm):
            candidates.append({
                'match': m,
                'date': m_date
            })
            
    candidates.sort(key=lambda x: x['date'], reverse=True)
    
    if candidates:
        print("FOUND H2H!")
        print(candidates[0]['match'])
    else:
        print("NO H2H FOUND via logic.")

if __name__ == "__main__":
    matches = load_data()
    test_get_h2h(matches)
