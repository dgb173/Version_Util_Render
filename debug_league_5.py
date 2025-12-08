import requests

def fetch_raw_data():
    # League 5 (Spain), Season 2024-2025
    url = "https://football.nowgoal26.com/jsData/matchResult/2024-2025/s5_en.js"
    print(f"Fetching {url}...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://football.nowgoal26.com/league/2024-2025/5"
    }
    
    try:
        r = requests.get(url, headers=headers)
        print(f"Status Code: {r.status_code}")
        if r.status_code == 200:
            print("First 2000 characters:")
            print(r.text[:2000])
        else:
            print("Failed to fetch data.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_raw_data()
