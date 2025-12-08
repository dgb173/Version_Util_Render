import requests
import re

def inspect_js_structure():
    # Premier League 2024-2025
    url = "https://football.nowgoal26.com/jsData/matchResult/2024-2025/s36_en.js"
    print(f"Fetching {url}...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://football.nowgoal26.com/league/2024-2025/36"
    }
    
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            # Find the start of the array
            # Usually var A = [[...],[...]];
            # Let's print the first few matches
            content = r.text
            # Matches are usually in arrays like [2587775,36,...]
            # Let's find all arrays starting with a match ID
            # We'll just print the first 2000 chars to see the structure
            try:
                print(content[:2000])
            except:
                print(content[:2000].encode('utf-8', errors='replace'))
            
            # Try to find a specific match array to analyze indices
            # Example match ID from previous steps: 2587775 (though that was Belgium, let's look for any)
            match = re.search(r'\[(\d+),36,', content)
            if match:
                mid = match.group(1)
                print(f"\nFound match ID: {mid}")
                # Extract the full array for this match
                # It starts with [ and ends with ]
                # This is a bit hacky with regex, but let's try to grab the line or segment
                start = content.find(f"[{mid}")
                end = content.find("]", start)
                print(f"Match Array: {content[start:end+1]}")
        else:
            print(f"Failed: {r.status_code}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_js_structure()
