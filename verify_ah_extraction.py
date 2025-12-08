from src.modules import league_scraper
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_ah_extraction():
    # Premier League 2024-2025, League ID 36
    print("Testing AH Extraction for League 36...")
    res = league_scraper.extract_ids_by_params("2025", "36")
    
    if "error" in res:
        print(f"Error: {res['error']}")
        return

    match_data = res.get('match_data', [])
    print(f"Found {len(match_data)} matches.")
    
    if not match_data:
        print("No matches found.")
        return

    # Print first 5 matches with AH
    print("First 5 matches:")
    for m in match_data[:5]:
        print(f"ID: {m['id']}, AH: {m['ah']}")
        
    # Check if we have valid AH values (not empty)
    valid_ah = [m for m in match_data if m['ah']]
    print(f"Matches with non-empty AH: {len(valid_ah)}")

if __name__ == "__main__":
    test_ah_extraction()
