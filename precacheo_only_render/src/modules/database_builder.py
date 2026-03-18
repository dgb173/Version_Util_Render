
import os
import json
import pandas as pd
from bs4 import BeautifulSoup
import re

def parse_html_stat(html_content):
    """
    Parses HTML content from a stat string to extract the numerical value.
    Example: <span style="color: red;">7</span> -> 7
    """
    if not isinstance(html_content, str) or not html_content:
        return None
    
    # Use BeautifulSoup to parse the HTML and get the text
    soup = BeautifulSoup(html_content, 'html.parser')
    text = soup.get_text(strip=True)
    
    # Extract numbers from the text
    numbers = re.findall(r'\d+', text)
    if numbers:
        return int(numbers[0])
    return None

def parse_score(score_str):
    """
    Parses a score string like "1 : 0" into home and away goals.
    """
    if not isinstance(score_str, str) or ':' not in score_str:
        return None, None
    
    parts = score_str.split(':')
    try:
        home_goals = int(parts[0].strip())
        away_goals = int(parts[1].strip())
        return home_goals, away_goals
    except (ValueError, IndexError):
        return None, None

def build_database(previews_dir='src/static/cached_previews', output_path='data/historical_matches.csv'):
    """
    Reads all JSON previews, cleans the data, and saves it as a consolidated CSV file.
    """
    all_matches = []
    
    if not os.path.exists(previews_dir):
        print(f"Directory not found: {previews_dir}")
        return

    json_files = [f for f in os.listdir(previews_dir) if f.endswith('.json')]
    
    for file_name in json_files:
        file_path = os.path.join(previews_dir, file_name)
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON file: {file_name}")
                continue

            # The main match this preview is for
            main_match_id = data.get('match_id')

            # Process all nested match objects
            for key, match_data in data.get('recent_indirect_full', {}).items():
                if not isinstance(match_data, dict) or 'home' not in match_data:
                    continue

                home_goals, away_goals = parse_score(match_data.get('score'))
                
                processed_match = {
                    'main_match_id': main_match_id,
                    'source_key': key,
                    'date': match_data.get('date'),
                    'home_team': match_data.get('home'),
                    'away_team': match_data.get('away'),
                    'home_goals': home_goals,
                    'away_goals': away_goals,
                    'ah': match_data.get('ah'),
                    'cover_status': match_data.get('cover_status')
                }
                
                # Process stats rows
                for stat_row in match_data.get('stats_rows', []):
                    label = stat_row.get('label', '').lower().replace(' ', '_')
                    if label:
                        processed_match[f'home_{label}'] = parse_html_stat(stat_row.get('home'))
                        processed_match[f'away_{label}'] = parse_html_stat(stat_row.get('away'))
                
                all_matches.append(processed_match)

    if not all_matches:
        print("No match data found to process.")
        return

    # Create DataFrame and save to CSV
    df = pd.DataFrame(all_matches)
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Database built successfully! Saved to {output_path}")
    print(f"Total matches processed: {len(df)}")

if __name__ == '__main__':
    # This allows the script to be run directly
    build_database()
