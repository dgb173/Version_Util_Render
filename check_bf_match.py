import requests
import re

url = "https://live2.nowgoal26.com/gf/data/bf_en-idn.js"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://live2.nowgoal26.com"
}

try:
    print(f"Fetching {url}...")
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    content = resp.text
    
    match_id = "2789604"
    if match_id in content:
        print(f"Match {match_id} FOUND in BF data.")
        match_pattern = re.compile(r"A\[\d+\]=\[(.*?)\];")
        for match in match_pattern.finditer(content):
            if match_id in match.group(1):
                print(f"Data: {match.group(1)[:100]}...")
    else:
        print(f"Match {match_id} NOT found in BF data.")

except Exception as e:
    print(f"Error: {e}")
