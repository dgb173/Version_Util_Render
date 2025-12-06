import json
import datetime

def get_previous_match_debug(team_name, current_date_str, all_matches):
    print(f"DEBUG: Searching for {team_name} before {current_date_str}")
    
    try:
        if 'T' in current_date_str:
            current_date = datetime.datetime.strptime(current_date_str, "%Y-%m-%dT%H:%M:%S")
        else:
            current_date = datetime.datetime.strptime(current_date_str, "%Y-%m-%d")
    except Exception as e:
        print(f"DEBUG: Date parse error: {e}")
        return None

    candidates = []
    
    for m in all_matches:
        m_date_val = m.get('time_obj')
        if not m_date_val: continue
            
        try:
            if 'T' in m_date_val:
                m_date = datetime.datetime.strptime(m_date_val, "%Y-%m-%dT%H:%M:%S")
            else:
                continue
        except:
            continue
            
        if m_date < current_date:
            h_name = m.get('home_team', '').strip()
            a_name = m.get('away_team', '').strip()
            
            if h_name == team_name or a_name == team_name:
                print(f"DEBUG: Found candidate: {h_name} vs {a_name} on {m_date}")
                candidates.append(m)
                
    print(f"DEBUG: Total candidates found: {len(candidates)}")
    return candidates

try:
    data = json.load(open('data.json', encoding='utf-8'))
    finished = data.get('finished_matches', [])
    print(f"Total finished matches: {len(finished)}")
    
    dates = []
    for m in finished:
        d_str = m.get('time_obj')
        if d_str:
            try:
                if 'T' in d_str:
                    dates.append(datetime.datetime.strptime(d_str, "%Y-%m-%dT%H:%M:%S"))
            except:
                pass
                
    if dates:
        print(f"Min date: {min(dates)}")
        print(f"Max date: {max(dates)}")
    else:
        print("No valid dates found.")
        
except Exception as e:
    print(f"Error: {e}")
