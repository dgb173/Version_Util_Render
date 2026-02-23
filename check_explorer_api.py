import requests
import json

url = 'http://localhost:5000/api/explorer_search'
headers = {'Content-Type': 'application/json'}
payload = {
    'filters': {
        'limit': 50
    }
}

try:
    print(f"Testing URL: {url}")
    response = requests.post(url, json=payload, headers=headers)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        results = data.get('results', [])
        print(f"Success! Received {len(results)} results.")
        
        movements = []
        for r in results:
            if r.get('h2h_general') and r['h2h_general'].get('movement'):
                movements.append(r['h2h_general']['movement'])
        
        print("H2H General Movements found:")
        for m in sorted(list(set(movements))):
            print(f"  '{m}'")

        if len(results) > 0:
            print("Sample match data:")
            print(json.dumps(results[0], indent=2))
        else:
            print("Warning: Received 0 results.")
    else:
        print(f"Error: {response.text}")
        
except Exception as e:
    print(f"Exception: {e}")
