import os
import time
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 950})
    page.goto("https://nowgoal-app-v8fw.onrender.com/precacheo", timeout=45000)
    page.wait_for_selector("tr[data-match-id]", timeout=45000)

    row = page.locator("tr[data-match-id]").filter(has_text="Roasso Kumamoto").first
    btns = row.locator("button").all()
    print("Found buttons in row:", len(btns))
    for i, b in enumerate(btns):
        html = b.evaluate("el => el.outerHTML")
        print(f"btn {i}: {html}")
    browser.close()
