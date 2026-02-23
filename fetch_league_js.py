import requests

url = "https://football.nowgoal26.com/jsData/matchResult/2025-2026/s36_en.js"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://football.nowgoal26.com/league/36"
}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    with open("league_36_data.js", "w", encoding="utf-8") as f:
        f.write(response.text)
    print("Successfully saved league_36_data.js")
except Exception as e:
    print(f"Error fetching URL: {e}")
