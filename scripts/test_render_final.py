import os
import time
from playwright.sync_api import sync_playwright

artifacts_dir = r"C:\Users\Usuario\.gemini\antigravity\brain\81a441db-c9dc-4454-a0aa-0d280304c2e8"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 950})
    print("Navigating to Render /precacheo...")
    page.goto("https://nowgoal-app-v8fw.onrender.com/precacheo", timeout=45000)

    print("Waiting for match rows to render...")
    page.wait_for_selector("tr[data-match-id]", timeout=45000)
    print("Match rows loaded! Total count:", page.locator("tr[data-match-id]").count())

    row = page.locator("tr[data-match-id]").filter(has_text="Roasso Kumamoto").first
    print("Found Roasso Kumamoto row!")

    # Expandir contexto: el boton verde con icono fa-table-cells
    stats_btn = row.locator("button[data-bs-target*='global-stats']").first
    print("Stats button title:", stats_btn.get_attribute("title"), "class:", stats_btn.get_attribute("class"))
    stats_btn.click(force=True)
    print("Clicked stats button to expand context.")

    # Esperar a que aparezca .pre-context-team
    page.wait_for_selector(".pre-context-team", timeout=15000)
    time.sleep(2)

    # 1. Verificar ausencia de sección eliminada
    ah_count = page.locator(".pre-context-ah-analysis").count()
    print(f"Verificación .pre-context-ah-analysis: {ah_count}")
    page.screenshot(path=os.path.join(artifacts_dir, "render_final_context_no_similar_ah.png"))
    print("Saved render_final_context_no_similar_ah.png")

    # 2. Localizar Matsumoto en la tabla
    matsumoto_links = page.locator(".pre-context-rival-name-link").filter(has_text="Matsumoto")
    print("Matsumoto links count:", matsumoto_links.count())
    if matsumoto_links.count() > 0:
        matsumoto_links.first.click(force=True)
        time.sleep(3)
        page.screenshot(path=os.path.join(artifacts_dir, "render_final_single_matsumoto.png"))
        print("Saved render_final_single_matsumoto.png")

        # 3. Comparar con rival
        comp_btn = page.locator("#teamHistoryCompareBtn")
        comp_btn.click(force=True)
        time.sleep(4)
        page.screenshot(path=os.path.join(artifacts_dir, "render_final_modal_comparison.png"))
        print("Saved render_final_modal_comparison.png")

    browser.close()
    print("DONE EVERYTHING SUCCESSFULLY!")
