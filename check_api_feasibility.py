
import requests
import json
import re

MATCH_ID = "2590957"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Referer": "https://www.nowgoal.com/"
}

def check_url(url, description):
    print(f"\n--- Checking {description} ---")
    print(f"URL: {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            content = response.text[:500] + "..." if len(response.text) > 500 else response.text
            print(f"Content Preview: {content}")
            
            # Check if it looks like JSON or JS Data
            if "{" in content and "}" in content:
                print(">> Potentially JSON/Structural Data")
            
            # Check for specific keywords
            if "Vs_hOdds" in response.text:
                print(">> Contains 'Vs_hOdds' (Historical Odds)")
            if "h2h" in response.text.lower():
                print(">> Contains 'h2h' data")
                
            return response.text
        else:
            print(">> Request failed")
    except Exception as e:
        print(f"Error: {e}")
    return None


# Try strictly with the domain known to work: football.nowgoal26.com
BASE_DOMAIN = "https://football.nowgoal26.com"

# 1. Ajax Odds (Standard)
check_url(f"{BASE_DOMAIN}/Ajax/SoccerAjax/?type=1&id={MATCH_ID}", "Ajax Odds (type=1)")

# 2. Analysis Data (Probe likely endpoints)
# Often used for analysis panel
check_url(f"{BASE_DOMAIN}/Ajax/SoccerAjax/?type=2&id={MATCH_ID}", "Ajax type=2") 
check_url(f"{BASE_DOMAIN}/Ajax/SoccerAjax/?type=3&id={MATCH_ID}", "Ajax type=3 (H2H?)")
check_url(f"{BASE_DOMAIN}/Ajax/SoccerAjax/?type=14&id={MATCH_ID}", "Ajax type=14 (Stats?)")

# 3. JS Data Probes
# Standard pattern for match info in some versions
check_url(f"{BASE_DOMAIN}/jsData/matchResult/2024-2025/s36_en.js", "League Data Probe (Check connectivity)")
check_url(f"{BASE_DOMAIN}/jsData/analysis/{MATCH_ID}.js", "JS Analysis File")

# 4. Standard H2H Page (HTTPS)
html_content = check_url(f"{BASE_DOMAIN}/match/h2h-{MATCH_ID}", "Standard H2H Page (HTTPS)")

# Analyze HTML for Hidden API Paths
if html_content:
    print("\n--- Analyzing HTML for Hidden API Paths ---")
    # Javascript variable pointers often contain the data path
    # var analysisData = ...
    # var h2hData = ...
    
    # Look for .js references
    js_files = re.findall(r'src="([^"]+\.js[^"]*)"', html_content)
    for js in js_files:
        if "data" in js.lower() or "ajax" in js.lower() or "analysis" in js.lower():
            print(f"Found interesting JS src: {js}")

