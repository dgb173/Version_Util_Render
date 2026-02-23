
import requests
import re

MATCH_ID = "2590957"
DOMAINS = [
    "https://football.nowgoal26.com",
    "https://data.nowgoal26.com",
    "https://info.nowgoal26.com",
    "https://www.nowgoal26.com"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://football.nowgoal26.com/"
}

print(f"--- Probing Data Sources for Match {MATCH_ID} ---\n")

# 1. Search for JS Data Files
print("1. Checking Static JS Data Files:")
formats = [
    "/jsData/analysis/{id}.js",
    "/jsData/match/{id}.js",
    "/jsData/team/{id}.js",
    "/jsData/h2h/{id}.js",
    "/analysis/{id}.js"
]

found_js = False
for d in DOMAINS:
    for fmt in formats:
        url = f"{d}{fmt.format(id=MATCH_ID)}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=5)
            if r.status_code == 200 and "Page Not Found" not in r.text and "<html" not in r.text[:50]:
                print(f"[FOUND] {url} (Size: {len(r.text)} bytes)")
                print(f"Content Start: {r.text[:100]}...")
                found_js = True
            # else:
            #     print(f"[404/Bad] {url}")
        except:
             pass

if not found_js:
    print("No static JS files found.")

# 2. Analyze Main Page Source for Data Variables
print("\n2. Analyzing HTML Source for Data Variables:")
main_url = f"https://football.nowgoal26.com/match/h2h-{MATCH_ID}"
try:
    r = requests.get(main_url, headers=HEADERS, timeout=10)
    if r.status_code == 200:
        html = r.text
        # Look for typical NowGoal data variables
        vars_to_check = [
            "var h2h_data", "var analysisData", "var headToHead", 
            "var _matchInfo", "var Vs_hOdds", "var matchData"
        ]
        
        found_vars = []
        for v in vars_to_check:
            if v in html:
                found_vars.append(v)
                # Print a snippet
                match = re.search(re.escape(v) + r'\s*=\s*(.*?);', html, re.DOTALL)
                if match:
                    val = match.group(1)[:200].replace('\n', ' ')
                    print(f"[FOUND] {v} = {val}...")
                else:
                    print(f"[FOUND] {v} (but parse failed)")
        
        if not found_vars:
            print("No known data variables found in HTML.")
            
        # Check for Ajax calls in the source
        print("\nChecking for internal Ajax calls:")
        ajax_urls = re.findall(r'["\']([^"\']+\.php[^"\']*)["\']', html) + re.findall(r'["\']([^"\']+\.aspx[^"\']*)["\']', html)
        if ajax_urls:
            print(f"Potential dynamic calls: {ajax_urls[:5]}")
            
    else:
        print(f"Failed to load main page: {r.status_code}")
except Exception as e:
    print(f"Error loading main page: {e}")
