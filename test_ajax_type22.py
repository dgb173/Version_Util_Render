import requests
import json
import sys

# Fix encoding
sys.stdout.reconfigure(encoding='utf-8')

match_ids = ['2850521', '2807413']
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.nowgoal.com/'
}

for mid in match_ids:
    print(f"\n--- Testing Match ID: {mid} ---")
    # Type 22 found in script sameOddsCount
    # cid=8 is Bet365, ocid=281 is also Bet365 alternative
    url = f"https://www.nowgoal.com/ajax/soccerajax?type=22&cid=8&ocid=281&id={mid}"
    print(f"URL: {url}")
    
    try:
        r = requests.get(url, headers=headers, timeout=10)
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            print("Response:", r.text[:500])
            try:
                data = r.json()
                if "Data" in data:
                    print("Data found in JSON!")
                    # Examine first few keys of data['Data']
                    data_obj = data['Data']
                    print("Keys in Data:", list(data_obj.keys())[:10])
                    if "FirstOdds" in data_obj:
                        print("FirstOdds:", data_obj["FirstOdds"])
                else:
                    print("No 'Data' key in JSON")
            except:
                print("Failed to parse JSON")
    except Exception as e:
        print(f"Error: {e}")
