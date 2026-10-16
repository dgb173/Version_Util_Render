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

    # Buscar la fila principal de Roasso Kumamoto en la tabla de escritorio
    row = page.locator("tr[data-match-id]").filter(has_text="Roasso Kumamoto").first
    print("Row found. Clicking stats button with force=True...")

    # El botón verde en td:last-child
    stats_btn = row.locator("td:last-child button:not(.mobile-detail-back)").first
    stats_btn.click(force=True)
    print("Clicked stats button.")

    time.sleep(2)

    # 1. Comprobar que .pre-context-ah-analysis no existe
    ah_count = page.locator(".pre-context-ah-analysis").count()
    print(f"COUNT .pre-context-ah-analysis: {ah_count}")

    page.screenshot(path=os.path.join(artifacts_dir, "render_final_context.png"))
    print("Saved render_final_context.png")

    # 2. Localizar enlace de rival Matsumoto Yamaga dentro del contexto abierto
    matsumoto_links = page.locator(".pre-context-rival-name-link").filter(has_text="Matsumoto")
    print(f"Matsumoto links found: {matsumoto_links.count()}")

    if matsumoto_links.count() > 0:
        target_link = matsumoto_links.first
        print(f"Clicking link: {target_link.inner_text()}")
        target_link.click(force=True)
        time.sleep(3)

        page.screenshot(path=os.path.join(artifacts_dir, "render_final_single.png"))
        print("Saved render_final_single.png")

        # 3. Pulsar Comparar con Rival
        comp_btn = page.locator("#teamHistoryCompareBtn")
        if comp_btn.count() > 0:
            print("Clicking Comparar con Rival...")
            comp_btn.click(force=True)
            time.sleep(5)

            page.screenshot(path=os.path.join(artifacts_dir, "render_final_comparison.png"))
            print("Saved render_final_comparison.png")

    browser.close()
    print("Done testing successfully!")
