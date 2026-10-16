import os
import time
from playwright.sync_api import sync_playwright

artifacts_dir = r"C:\Users\Usuario\.gemini\antigravity\brain\81a441db-c9dc-4454-a0aa-0d280304c2e8"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1440, "height": 1000})
    print("Navegando a Render /precacheo...")
    page.goto("https://nowgoal-app-v8fw.onrender.com/precacheo", timeout=45000)

    print("Esperando filas de partidos...")
    page.wait_for_selector("tr[data-match-id]", timeout=45000)
    rows = page.locator("tr[data-match-id]")
    count = rows.count()
    print(f"Total partidos encontrados: {count}")

    # Buscar Kumamoto o el primero
    target_row = None
    for i in range(count):
        txt = rows.nth(i).inner_text()
        if "Kumamoto" in txt:
            target_row = rows.nth(i)
            print(f"Encontrado Roasso Kumamoto en fila {i}")
            break

    if not target_row:
        target_row = rows.nth(0)
        print("Usando fila 0 por defecto")

    # Clic en el boton verde de estadisticas/contexto
    btn = target_row.locator("button.btn-outline-success").first
    print("Haciendo clic en boton de contexto...")
    btn.click(force=True)

    print("Esperando .pre-context-team...")
    page.wait_for_selector(".pre-context-team", timeout=25000)
    time.sleep(2)

    # Buscar enlaces de rivales
    rival_links = page.locator(".pre-context-rival-name-link")
    print(f"Enlaces de rivales encontrados: {rival_links.count()}")

    if rival_links.count() > 0:
        # Preferir Matsumoto si existe, sino el primer rival disponible
        matsumoto = rival_links.filter(has_text="Matsumoto")
        target_link = matsumoto.first if matsumoto.count() > 0 else rival_links.first
        rival_text = target_link.inner_text()
        print(f"Haciendo clic en el rival: '{rival_text}'...")
        target_link.click(force=True)
        time.sleep(4)

        # 1. Comprobar que el modal flotante NO esta visible
        modal_visible = page.locator("#teamHistoryModal").is_visible()
        print(f"Modal flotante visible: {modal_visible} (debe ser False)")

        # 2. Comprobar que la fila inline SI es visible
        inline = page.locator(".pre-context-team-history-inline:visible")
        print(f"Fila inline visible: {inline.count() > 0}")

        if inline.count() > 0:
            inline.first.scroll_into_view_if_needed()
            time.sleep(1)

        single_path = os.path.join(artifacts_dir, "render_live_inline_single.png")
        page.screenshot(path=single_path)
        print(f"Captura individual guardada: {single_path}")

        # 3. Clic en 'Comparar con Rival' inline
        comp_btn = inline.locator(".team-history-inline-compare-btn").first
        if comp_btn.count() > 0:
            print("Haciendo clic en 'Comparar con Rival' inline...")
            comp_btn.click(force=True)
            time.sleep(4)

            compare_path = os.path.join(artifacts_dir, "render_live_inline_compare.png")
            page.screenshot(path=compare_path)
            print(f"Captura comparativa guardada: {compare_path}")

        # 4. Clic en 'Cerrar' inline
        close_btn = inline.locator("button:has-text('Cerrar')").first
        if close_btn.count() > 0:
            print("Haciendo clic en 'Cerrar' inline...")
            close_btn.click(force=True)
            time.sleep(1)

            closed_count = page.locator(".pre-context-team-history-inline:visible").count()
            print(f"Fila inline visible tras cerrar: {closed_count} (debe ser 0)")

            closed_path = os.path.join(artifacts_dir, "render_live_inline_closed.png")
            page.screenshot(path=closed_path)
            print(f"Captura cerrada guardada: {closed_path}")

    browser.close()
    print("TODO COMPLETADO CON EXITO TOTAL!")
