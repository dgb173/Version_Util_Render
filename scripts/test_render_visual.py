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
    print("Row found. Looking for context toggle button...")

    # El botón verde con ícono de tabla en la columna Acciones
    action_btns = row.locator("td:last-child button")
    if action_btns.count() > 0:
        action_btns.first.click()
        print("Clicked first action button to expand context.")
    else:
        # Alternativa: click en la fila
        row.click()
        print("Clicked row to expand.")

    time.sleep(2)

    # 1. Verificar si la sección de hándicaps similares está presente
    ah_count = page.locator(".pre-context-ah-analysis").count()
    print(f"COUNT .pre-context-ah-analysis: {ah_count}")

    page.screenshot(path=os.path.join(artifacts_dir, "render_context_expanded.png"))
    print("Saved render_context_expanded.png")

    # 2. Localizar enlace de rival Matsumoto Yamaga dentro del contexto abierto
    matsumoto_links = page.locator(".pre-context-rival-name-link").filter(has_text="Matsumoto")
    print(f"Matsumoto links found: {matsumoto_links.count()}")

    if matsumoto_links.count() > 0:
        target_link = matsumoto_links.first
        print(f"Clicking link: {target_link.inner_text()}")
        target_link.click()
        time.sleep(3)

        # Captura del modal individual
        page.screenshot(path=os.path.join(artifacts_dir, "render_modal_single_matsumoto.png"))
        print("Saved render_modal_single_matsumoto.png")

        # 3. Pulsar botón 'Comparar con Rival'
        comp_btn = page.locator("#teamHistoryCompareBtn")
        if comp_btn.count() > 0:
            print("Clicking Comparar con Rival...")
            comp_btn.click()
            time.sleep(4)

            # Captura de la comparativa
            page.screenshot(path=os.path.join(artifacts_dir, "render_modal_comparison.png"))
            print("Saved render_modal_comparison.png")
    else:
        # Si no hay Matsumoto, buscar el enlace de Ehime FC
        ehime_link = page.locator(".pre-context-team-name").filter(has_text="Ehime").first
        if ehime_link.count() > 0:
            print("Clicking Ehime team link...")
            ehime_link.click()
            time.sleep(3)
            page.screenshot(path=os.path.join(artifacts_dir, "render_modal_single_ehime.png"))
            print("Saved render_modal_single_ehime.png")

            comp_btn = page.locator("#teamHistoryCompareBtn")
            if comp_btn.count() > 0:
                print("Clicking Comparar con Rival from Ehime...")
                comp_btn.click()
                time.sleep(4)
                page.screenshot(path=os.path.join(artifacts_dir, "render_modal_comparison.png"))
                print("Saved render_modal_comparison.png")

    browser.close()
    print("Done testing!")
