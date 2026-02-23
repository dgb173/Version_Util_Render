import requests
from bs4 import BeautifulSoup
import sys
import os
import json
import re

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from modules.estudio_scraper import extract_bet365_initial_odds_of, fetch_odds_from_ajax, fetch_odds_from_bf_data, get_requests_session_of

MATCH_ID = "2905991"
URL = f"https://live2.nowgoal26.com/match/h2h-{MATCH_ID}"

def debug_scraping():
    print(f"Fetching {URL}...")
    session = get_requests_session_of()
    try:
        response = session.get(URL, timeout=10)
        response.raise_for_status()
        html = response.text
        soup = BeautifulSoup(html, 'lxml')
        
        print("\n--- HTML Scraping ---")
        odds = extract_bet365_initial_odds_of(soup, MATCH_ID)
        print(f"Extracted Odds: {json.dumps(odds, indent=2)}")
        
        # Check specific elements
        bet365_cell = soup.find("td", string=lambda text: text and "Bet365" in text)
        if bet365_cell:
            print("Found Bet365 cell by text.")
            row = bet365_cell.find_parent("tr")
            print(f"Row HTML: {row.prettify()[:200]}...")
            tds = row.find_all("td")
            if len(tds) > 3:
                print(f"AH Cell (td[3]): {tds[3].get('data-o')} / Text: {tds[3].text}")
        else:
            print("Bet365 cell NOT found by text.")
            
        print("\n--- AJAX Fallback ---")
        # Fetch raw AJAX to inspect
        ajax_url = f"https://live2.nowgoal26.com/Ajax/SoccerAjax/?type=1&id={MATCH_ID}"
        print(f"Fetching AJAX: {ajax_url}")
        ajax_resp = session.get(ajax_url, timeout=10)
        if ajax_resp.status_code == 200:
            data_json = ajax_resp.json()
            raw_data = data_json.get("Data", "")
            print(f"Raw AJAX Data (first 500 chars): {raw_data[:500]}")
            # Split and find Bet365 (ID 8)
            companies = raw_data.split('^')
            for c in companies:
                if c.startswith('8;'):
                    print(f"Bet365 Raw Data: {c}")
                    parts = c.split(';')
                    for i, p in enumerate(parts):
                        print(f"  Part {i}: {p}")
        
        print("\n--- BF Data Fallback ---")
        bf_url = "https://live2.nowgoal26.com/gf/data/bf_en-idn.js"
        print(f"Fetching BF Data: {bf_url}")
        bf_resp = session.get(bf_url, timeout=10)
        if bf_resp.status_code == 200:
            content = bf_resp.text
            # Search for match ID
            if MATCH_ID in content:
                print(f"Match ID {MATCH_ID} found in BF Data!")
                # Extract the line
                match = re.search(r"A\[\d+\]=\[(.*?)\];", content)
                # This regex might match the wrong line if we don't look for the specific ID
                # Iterate lines
                for line in content.splitlines():
                    if MATCH_ID in line:
                        print(f"BF Line: {line[:200]}...")
                        # Parse it roughly
                        try:
                            # A[123]=[...]
                            bracket_content = line.split('=[')[1].rstrip('];')
                            # Clean
                            bracket_content = bracket_content.replace("'", '"').replace(',,', ',null,')
                            if bracket_content.endswith(','): bracket_content += 'null'
                            # items = json.loads(f"[{bracket_content}]") # Might fail
                            items = bracket_content.split(',')
                            print(f"  Index 21 (AH): {items[21] if len(items)>21 else 'N/A'}")
                            print(f"  Index 25 (Goals): {items[25] if len(items)>25 else 'N/A'}")
                        except Exception as e:
                            print(f"  Error parsing BF line: {e}")
            else:
                print(f"Match ID {MATCH_ID} NOT found in BF Data.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_scraping()
