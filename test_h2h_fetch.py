import requests
from bs4 import BeautifulSoup
import re

BASE_URL_MAIN = "https://live20.nowgoal25.com"
BASE_URL_H2H = "https://live18.nowgoal25.com" # Keep this as per scraper, or try live20

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

def get_match_id():
    try:
        print(f"Fetching main page: {BASE_URL_MAIN}")
        resp = requests.get(BASE_URL_MAIN, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        print(f"Main page response length: {len(resp.text)}")
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Find a match ID
        match_rows = soup.find_all('tr', id=lambda x: x and x.startswith('tr1_'))
        print(f"Found {len(match_rows)} match rows.")
        
        if match_rows:
            return match_rows[0]['id'].replace('tr1_', '')
    except Exception as e:
        print(f"Error fetching main page: {e}")
    return None

def test_h2h(match_id):
    url = f"{BASE_URL_H2H}/match/h2h-{match_id}"
    print(f"Fetching H2H page: {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        print(f"H2H page response length: {len(resp.text)}")
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        table_v1 = soup.find("table", id="table_v1")
        table_v2 = soup.find("table", id="table_v2")
        
        print(f"Table v1 found: {bool(table_v1)}")
        print(f"Table v2 found: {bool(table_v2)}")
        
        if table_v1:
            rows = table_v1.find_all("tr")
            print(f"Table v1 rows: {len(rows)}")
            
        script_tag = soup.find("script", string=re.compile(r"var _matchInfo = "))
        print(f"Match info script found: {bool(script_tag)}")
        
    except Exception as e:
        print(f"Error fetching H2H page: {e}")

if __name__ == "__main__":
    mid = get_match_id()
    if mid:
        print(f"Found match ID: {mid}")
        test_h2h(mid)
    else:
        print("Could not find a match ID to test.")
