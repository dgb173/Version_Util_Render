from bs4 import BeautifulSoup
import re

def extract_bet365_initial_odds_of(soup):
    odds_info = {
        "ah_home_cuota": "N/A", "ah_linea_raw": "N/A", "ah_away_cuota": "N/A",
        "goals_over_cuota": "N/A", "goals_linea_raw": "N/A", "goals_under_cuota": "N/A"
    }
    
    if soup:
        # Logic from current estudio_scraper.py
        bet365_row = soup.select_one("tr#tr_o_1_8[name='earlyOdds'], tr#tr_o_1_31[name='earlyOdds']")
        if bet365_row:
            print(f"Found row: {bet365_row.attrs}")
            tds = bet365_row.find_all("td")
            print(f"Number of tds: {len(tds)}")
            for i, td in enumerate(tds):
                print(f"td[{i}]: text='{td.text.strip()}', data-o='{td.get('data-o')}'")
                
            if len(tds) >= 11:
                odds_info["ah_home_cuota"] = tds[2].get("data-o", tds[2].text).strip()
                odds_info["ah_linea_raw"] = tds[3].get("data-o", tds[3].text).strip()
                odds_info["ah_away_cuota"] = tds[4].get("data-o", tds[4].text).strip()
                odds_info["goals_over_cuota"] = tds[8].get("data-o", tds[8].text).strip()
                odds_info["goals_linea_raw"] = tds[9].get("data-o", tds[9].text).strip()
                odds_info["goals_under_cuota"] = tds[10].get("data-o", tds[10].text).strip()
        else:
            print("Bet365 row NOT found")
            
    return odds_info

# Read the reference file
try:
    with open(r"C:\Users\Usuario\Desktop\V_buena\reference_code\analisis.txt", "r", encoding="utf-8") as f:
        html_content = f.read()
        
    soup = BeautifulSoup(html_content, "lxml")
    result = extract_bet365_initial_odds_of(soup)
    print("\nExtracted Result:")
    print(result)

except Exception as e:
    print(f"Error: {e}")
