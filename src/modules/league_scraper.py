import requests
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _walk_schedule_matches(node, sub_id="0", round_name=""):
    if isinstance(node, dict):
        for key, val in node.items():
            key_str = str(key)
            new_sub_id = sub_id
            new_round = round_name
            if key_str.startswith("sub_"):
                new_sub_id = key_str.removeprefix("sub_")
            elif key_str.startswith("R_"):
                new_round = key_str.removeprefix("R_")
            elif key_str.startswith("G"):
                new_round = key_str
            yield from _walk_schedule_matches(val, new_sub_id, new_round)
    elif isinstance(node, list):
        if len(node) >= 8 and str(node[0]).isdigit() and (isinstance(node[1], int) or str(node[1]).isdigit()):
            yield sub_id, round_name, node
        else:
            for item in node:
                yield from _walk_schedule_matches(item, sub_id, round_name)


def _parse_json_or_js(text: str) -> dict:
    text = text.lstrip("\ufeff").strip()
    if text.startswith("{") and text.endswith("}"):
        return json.loads(text)
    match = re.search(r'(?:var\s+\w+\s*=\s*|^\s*)({.*?});?$', text, re.DOTALL)
    if match:
        return json.loads(match.group(1))
    return json.loads(text)


def extract_ids_by_params(season, league_id, ah_filter=None):
    """
    Extracts match IDs using season and league_id directly.
    Tries multiple season formats (YYYY, YYYY-YYYY) to find data.
    ah_filter: Optional string of comma-separated AH values to filter by (e.g. "0.5, -0.5").
    """
    
    target_ahs = []
    if ah_filter:
        try:
            normalized_filter = str(ah_filter).replace(',', '.')
            matches = re.findall(r'[-+]?\d*\.?\d+', normalized_filter)
            for m in matches:
                if m in ['.', '-', '+', '']: continue
                try:
                    target_ahs.append(float(m))
                except ValueError:
                    continue
            logger.info(f"Filtering by AH: {target_ahs} (Raw input: '{ah_filter}')")
        except Exception as e:
            logger.warning(f"Error parsing AH filter '{ah_filter}': {e}")
    
    season_formats = [season]
    if "-" not in season and len(season) == 4 and season.isdigit():
        year = int(season)
        season_formats.append(f"{year-1}-{year}")
        season_formats.append(f"{year}-{year+1}")

    season_formats = list(dict.fromkeys(season_formats))
    last_error = None
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://football.nowgoal26.com/"
    }

    session = requests.Session()
    session.headers.update(headers)
    
    for current_season in season_formats:
        try:
            league_url = f"https://football.nowgoal26.com/league/{current_season}/{league_id}" if current_season else f"https://football.nowgoal26.com/league/{league_id}"
            logger.info(f"Checking league page: {league_url}")
            
            page_response = session.get(league_url, timeout=30)
            if page_response.status_code != 200:
                continue
                
            html = page_response.text
            path_match = re.search(r'const\s+_dataPath\s*=\s*"([^"]+)"', html)
            if not path_match:
                logger.warning(f"No _dataPath found in HTML for {league_url}")
                continue
                
            data_url = requests.compat.urljoin("https://football.nowgoal26.com", path_match.group(1))
            logger.info(f"Fetching schedule data from: {data_url}")
            
            data_response = session.get(data_url, timeout=30)
            data_response.raise_for_status()
            data = _parse_json_or_js(data_response.text)
            
            matches_found = []
            seen = set()
            for sub_id, round_name, row in _walk_schedule_matches(data.get("ScheduleList") or {}):
                if not isinstance(row, list) or len(row) < 8:
                    continue
                match_id = str(row[0])
                if match_id in seen:
                    continue
                seen.add(match_id)
                
                ah_raw = str(row[8]).strip() if len(row) > 8 and row[8] is not None else ""
                
                if target_ahs:
                    try:
                        ah_val = float(ah_raw)
                        match_filter = any(abs(ah_val - target) < 0.01 for target in target_ahs)
                        if not match_filter:
                            continue
                    except ValueError:
                        continue
                        
                matches_found.append({'id': match_id, 'ah': ah_raw})
            
            logger.info(f"Found {len(matches_found)} unique matches for league {league_id} in season {current_season}.")
            
            if not matches_found:
                logger.warning(f"No matches found in {data_url}")
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
