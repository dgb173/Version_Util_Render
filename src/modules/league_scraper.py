import requests
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_ids_by_params(season, league_id, ah_filter=None):
    """
    Extracts match IDs using season and league_id directly.
    Tries multiple season formats (YYYY, YYYY-YYYY) to find data.
    ah_filter: Optional string of comma-separated AH values to filter by (e.g. "0.5, -0.5").
    """
    
    # Parse AH filter if present
    target_ahs = []
    if ah_filter:
        try:
            # Split by comma and normalize/clean
            parts = [x.strip() for x in ah_filter.split(',')]
            for p in parts:
                if p:
                    target_ahs.append(float(p))
            logger.info(f"Filtering by AH: {target_ahs}")
        except ValueError:
            logger.warning(f"Invalid AH filter format: {ah_filter}")
    
    # Generate potential season formats
    season_formats = [season]
    if "-" not in season and len(season) == 4 and season.isdigit():
        year = int(season)
        season_formats.append(f"{year-1}-{year}") # e.g. 2025 -> 2024-2025
        season_formats.append(f"{year}-{year+1}") # e.g. 2025 -> 2025-2026

    # Remove duplicates while preserving order
    season_formats = list(dict.fromkeys(season_formats))
    
    last_error = None
    
    for current_season in season_formats:
        try:
            # First, fetch the league page to check for SubSclassID
            league_url = f"https://football.nowgoal26.com/league/{current_season}/{league_id}"
            logger.info(f"Checking league page for SubSclassID: {league_url}")
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Referer": "https://football.nowgoal26.com/"
            }
            
            # Default JS URL pattern
            js_url = f"https://football.nowgoal26.com/jsData/matchResult/{current_season}/s{league_id}_en.js"
            
            try:
                page_response = requests.get(league_url, headers=headers)
                if page_response.status_code == 200:
                    # Look for SubSclassID
                    # Pattern: var SubSclassID = 114;
                    sub_id_match = re.search(r'var\s+SubSclassID\s*=\s*(\d+);', page_response.text)
                    if sub_id_match:
                        sub_id = sub_id_match.group(1)
                        if sub_id and sub_id != "0":
                            logger.info(f"Found SubSclassID: {sub_id}")
                            # Construct URL with SubID: .../s{league_id}_{sub_id}_en.js
                            js_url = f"https://football.nowgoal26.com/jsData/matchResult/{current_season}/s{league_id}_{sub_id}_en.js"
            except Exception as e:
                logger.warning(f"Failed to fetch league page to check SubSclassID: {e}")

            logger.info(f"Trying to fetch data from: {js_url}")
            
            response = requests.get(js_url, headers=headers)
            
            if response.status_code == 404:
                logger.warning(f"Data not found for season {current_season} at {js_url}")
                continue
                
            response.raise_for_status()
            content = response.text
            
            # Extract Match IDs and AH using regex
            # Pattern: [MatchID, LeagueID, ..., AH(index 11), ...]
            # The array is comma separated. We need to skip 9 items after LeagueID to get to AH.
            # Example: [2590898,36,-1,'2024-08-17 03:00',27,29,'1-0','0-0','8','13',1,0.25,...]
            # Items can be numbers or strings '...'.
            # Regex strategy: Match [ID, LeagueID, then match next 9 items, then capture AH.
            # Note: Items might be empty or null in JS (e.g. ,,), but usually not for these fields.
            
            # Let's try a more robust parsing approach for the array content
            # Find all array starts
            matches_found = []
            
            # Regex to find the start of a match array: [12345, 36,
            start_pattern = r'\[' + r'(\d+),' + str(league_id) + r','
            for match in re.finditer(start_pattern, content):
                match_id = match.group(1)
                start_index = match.start()
                
                # Find the end of this array ']'
                end_index = content.find(']', start_index)
                if end_index != -1:
                    array_str = content[start_index:end_index+1]
                    
                    # Simple CSV parsing of the array string
                    # Remove [ and ]
                    inner = array_str[1:-1]
                    
                    # Split by comma, respecting quotes is hard with split, but let's assume simple structure for now
                    # or use a regex to split.
                    # Actually, the AH value is usually a number or string.
                    # Let's try to split by comma.
                    parts = inner.split(',')
                    
                    # Index 11 is AH (0-based)
                    # 0: ID, 1: LeagueID, ... 11: AH
                    if len(parts) > 11:
                        ah_raw = parts[11].strip().replace("'", "")
                        
                        # Apply Filter if set
                        if target_ahs:
                            try:
                                ah_val = float(ah_raw)
                                # Check if matches any target (with tolerance)
                                match_filter = False
                                for target in target_ahs:
                                    if abs(ah_val - target) < 0.01:
                                        match_filter = True
                                        break
                                
                                # Also check bucket logic? 
                                # User said "siguiendo siempre las reglas...".
                                # If user asks for 0.5, they might mean the 0.5 bucket (which includes 0.25/0.75).
                                # But usually "filter by AH" implies specific line.
                                # However, the user said "reglas de handicaps de ,25 i 0,5 i ,75".
                                # If they input "0.5", maybe they want EXACTLY 0.5?
                                # Let's assume exact match for now unless they list multiple.
                                # Or I can implement bucket matching if I import data_manager logic.
                                # For now, exact match against the list provided by user.
                                
                                if not match_filter:
                                    continue
                            except ValueError:
                                # If AH is not a number (e.g. empty), skip if filtering is on
                                continue
                                
                        matches_found.append({'id': match_id, 'ah': ah_raw})
            
            logger.info(f"Found {len(matches_found)} unique matches for league {league_id} in season {current_season}.")
            
            if not matches_found:
                logger.warning(f"No matches found in {js_url}")
                continue

            return {
                "season": current_season,
                "league_id": league_id,
                "match_data": matches_found
            }
            
        except requests.RequestException as e:
            logger.error(f"Error fetching JS data for {current_season}: {e}")
            last_error = e
        except Exception as e:
            logger.error(f"Error extracting matches for {current_season}: {e}")
            last_error = e
            
    # If we reach here, no data was found in any format
    return {"error": f"No matches found for League {league_id} in season {season} (tried: {season_formats}). Last error: {str(last_error)}"}

def extract_ids_from_league(url):
    """
    Extracts match IDs from a NowGoal league URL.
    URL format: https://football.nowgoal26.com/league/{season}/{leagueId}
    """
    try:
        match = re.search(r"league/([\d-]+)/(\d+)", url)
        if not match:
            return {"error": "Invalid URL format. Expected .../league/{season}/{leagueId}"}
        
        season = match.group(1)
        league_id = match.group(2)
        
        return extract_ids_by_params(season, league_id)
        
    except Exception as e:
        return {"error": str(e)}
