
import requests
import datetime
from bs4 import BeautifulSoup

URL_NOWGOAL = "https://live20.nowgoal25.com/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
}

def inspect():
    try:
        resp = requests.get(URL_NOWGOAL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        html = resp.text
        
        soup = BeautifulSoup(html, 'html.parser')
        # Find a match row
        rows = soup.find_all('tr', id=lambda x: x and x.startswith('tr1_'))
        print(f"Found {len(rows)} match rows.")
        
        if rows:
            row = rows[0]
            match_id = row.get('id').replace('tr1_', '')
            print(f"Inspecting Match {match_id}:")
            
            # Home Team
            home_cell = row.find('td', align='right') # Usually home team cell
            if home_cell:
                print("Home Cell HTML:")
                print(home_cell.prettify())
                
                # Check for rank
                # Usually in a span or text like Team[1]
                print("Text content:", home_cell.get_text(separator='|', strip=True))
            
            # Try to find specific rank elements
            # Common patterns: <span class="lp">[1]</span>
    except Exception as e:
        print(e)

if __name__ == "__main__":
    inspect()
