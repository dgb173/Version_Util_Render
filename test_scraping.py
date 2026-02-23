
import requests
from bs4 import BeautifulSoup
import re

URL = "https://live2.nowgoal26.com/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive"
}

def test_fetch():
    print("Fetching URL:", URL)
    try:
        resp = requests.get(URL, headers=HEADERS, timeout=15)
        print("Status Code:", resp.status_code)
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Check for any tr
        all_tr = soup.find_all('tr')
        print(f"Total <tr> tags found: {len(all_tr)}")
        
        # Check for match rows
        match_rows = [tr for tr in all_tr if tr.get('id', '').startswith('tr1_')]
        print(f"Match rows (id^='tr1_') found: {len(match_rows)}")
        
        # Check for league titles
        league_rows = [tr for tr in all_tr if 'Leaguestitle' in tr.get('class', [])]
        print(f"League rows (class='Leaguestitle') found: {len(league_rows)}")
        
        if match_rows:
            first_match = match_rows[0]
            print("\nFirst Match Row HTML snippet:")
            print(first_match.prettify()[:500])
            
            # Check ID
            mid = first_match.get('id')
            print("Match ID:", mid)
            
            # Check Rank in Home Team
            home_link = first_match.find('a', id=lambda x: x and x.startswith('team1_'))
            if home_link:
                print("Home Team Text:", home_link.get_text(strip=True))
                # Check regex
                m = re.search(r'^(.*?)\s*[\[\()](\d+)[\]\)]$', home_link.get_text(strip=True))
                if m:
                    print("Extracted Rank:", m.group(2))
                else:
                    print("No rank matched in regex.")
            else:
                print("No home link found.")

    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    test_fetch()
