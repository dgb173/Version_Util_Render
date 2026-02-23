
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from modules.estudio_scraper import fetch_odds_from_bf_data

def test_fetch():
    # Match ID from previous test: 2898709 (San Diego FC vs Minnesota United FC)
    match_id = "2898709"
    print(f"Testing fetch_odds_from_bf_data with match_id={match_id}...")
    
    data = fetch_odds_from_bf_data(match_id)
    
    if data:
        print("Success! Data found:")
        print(data)
        if data.get('ah_linea_raw') != 'N/A':
            print("Handicap line is present.")
        else:
            print("Handicap line is N/A.")
    else:
        print("Failed. No data returned.")

if __name__ == "__main__":
    test_fetch()
