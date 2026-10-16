import os
import time
import requests
import urllib3
from playwright.sync_api import sync_playwright

urllib3.disable_warnings()

artifacts_dir = r"C:\Users\Usuario\.gemini\antigravity\brain\81a441db-c9dc-4454-a0aa-0d280304c2e8"
url = "https://nowgoal-app-v8fw.onrender.com/precacheo"

print("Comprobando despliegue en Render...")
deployed = False
for attempt in range(1, 40):
    try:
        r = requests.get(url, verify=False, timeout=15)
        if "Math.floor((window.innerWidth - targetW) / 2)" in r.text:
            print(f"Intento {attempt}: ¡Despliegue activo en Render!")
            deployed = True
            break
        else:
            print(f"Intento {attempt}: Render compilando/desplegando...")
    except Exception as e:
        print(f"Intento {attempt}: Conectando: {e}")
    time.sleep(5)

if not deployed:
    print("Aviso: Procediendo a probar...")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 950})
    print("Navegando a Render /precacheo con Playwright...")
    page.goto(url, timeout=45000)
    page.wait_for_load_state("networkidle", timeout=30000)

    # 1. Expandir contexto del primer partido
    row = page.locator("tr").filter(has_text="Roasso Kumamoto").first
    btn = row.locator("button.table-action-btn[data-bs-toggle='collapse'], button[data-bs-target*='global-stats']").first
    if btn.count() > 0:
        btn.click()
    else:
        row.locator("td").nth(1).click()

    time.sleep(2)

    # Verificar ausencia de hándicaps similares
    ah_count = page.locator(".pre-context-ah-analysis").count()
    print(f"Verificación sección eliminada (.pre-context-ah-analysis): {ah_count}")

    page.screenshot(path=os.path.join(artifacts_dir, "render_final_context_no_similar_ah.png"))
    print("Guardada render_final_context_no_similar_ah.png")

    # 2. Localizar enlace de Matsumoto Yamaga en la tabla del rival
    matsumoto_links = page.locator(".pre-context-rival-name-link").filter(has_text="Matsumoto")
    print(f"Enlaces Matsumoto encontrados: {matsumoto_links.count()}")

    if matsumoto_links.count() > 0:
        target_link = matsumoto_links.first
        print(f"Haciendo clic en: {target_link.inner_text()}")
        target_link.click()
        time.sleep(3)

        page.screenshot(path=os.path.join(artifacts_dir, "render_final_single_matsumoto.png"))
        print("Guardada render_final_single_matsumoto.png")

        # 3. Pulsar Comparar con Rival
        comp_btn = page.locator("#teamHistoryCompareBtn")
        if comp_btn.count() > 0:
            print("Pulsando botón 'Comparar con Rival'...")
            comp_btn.click()
            time.sleep(5)

            # Captura completa donde ambas columnas deben ser visibles
            page.screenshot(path=os.path.join(artifacts_dir, "render_final_modal_comparison.png"))
            print("Guardada render_final_modal_comparison.png")

    browser.close()
    print("¡Verificación visual completada con éxito!")
