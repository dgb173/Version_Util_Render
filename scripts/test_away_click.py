import os
import time
from playwright.sync_api import sync_playwright

artifacts_dir = r"C:\Users\Usuario\.gemini\antigravity\brain\81a441db-c9dc-4454-a0aa-0d280304c2e8"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 950})
    print("Navigating to Render /precacheo...")
    page.goto("https://nowgoal-app-v8fw.onrender.com/precacheo", timeout=45000)
    page.wait_for_load_state("networkidle", timeout=30000)

    # Buscar la fila de Roasso Kumamoto vs Ehime FC
    row = page.locator("tr").filter(has_text="Roasso Kumamoto").first
    print("Row found. Looking for btn-outline-success button...")

    btn = row.locator("button.btn-outline-success").first
    btn.click()
    print("Clicked btn-outline-success button.")

    # Esperar a que los paneles de contexto sean visibles
    page.locator(".pre-context-team").first.wait_for(timeout=10000)
    time.sleep(1)

    # 1. Comprobar que .pre-context-ah-analysis no existe
    ah_count = page.locator(".pre-context-ah-analysis").count()
    print(f"COUNT .pre-context-ah-analysis: {ah_count}")

    # 2. Localizar el panel visitante (Ehime FC, role=away)
    away_panel = page.locator(".pre-context-team[data-default-role='away']").first
    away_name = away_panel.locator(".pre-context-team-name").inner_text()
    print(f"Away panel team: {away_name}")

    matsumoto_link = away_panel.locator(".pre-context-rival-name-link").filter(has_text="Matsumoto").first
    print(f"Matsumoto link in away panel: {matsumoto_link.inner_text()}")
    print(f"Onclick: {matsumoto_link.get_attribute('onclick')}")

    matsumoto_link.click()
    time.sleep(4)

    page.screenshot(path=os.path.join(artifacts_dir, "render_matsumoto_away_single.png"))
    print("Saved render_matsumoto_away_single.png")

    badge = page.locator("#teamHistoryModalBody .badge").first.inner_text()
    print("Single modal badge:", badge)

    # Click en Comparar con Rival
    comp_btn = page.locator("#teamHistoryCompareBtn")
    if comp_btn.count() > 0:
        print("Clicking Comparar con Rival...")
        comp_btn.click()
        time.sleep(5)

        page.screenshot(path=os.path.join(artifacts_dir, "render_matsumoto_away_compare.png"))
        print("Saved render_matsumoto_away_compare.png")

    browser.close()
    print("Done testing!")
