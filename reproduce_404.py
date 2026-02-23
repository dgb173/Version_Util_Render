import requests
import time

BASE_URL_OF = "https://live2.nowgoal26.com"
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Referer": BASE_URL_OF,
}

url = "https://live2.nowgoal26.com/match/h2h-2722917"

print(f"Testing URL: {url}")
try:
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=10)
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        print("Success! Page content length:", len(response.text))
        print("Title snippet:", response.text[:200])
    else:
        print("Failed with status:", response.status_code)
except Exception as e:
    print(f"Exception: {e}")
