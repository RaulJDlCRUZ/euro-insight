import json
from datetime import date, timedelta
from playwright.sync_api import sync_playwright

def fetch_euromillones_rango(page, fecha_inicio: str, fecha_fin: str) -> list:
    """Reutiliza la misma página/sesión para todas las peticiones"""
    resultados = []

    def handle_response(response):
        if "buscadorSorteos" in response.url and "EMIL" in response.url:
            if response.status == 200:
                try:
                    data = response.json()
                    resultados.extend(data)
                except Exception as e:
                    print(f"  Error parseando: {e}")

    page.on("response", handle_response)

    campo_desde = page.locator("input[id*='desde'], input[placeholder*='esde'], input[name*='esde']").first
    campo_desde.click()
    page.keyboard.press("Control+a")
    page.keyboard.press("Delete")
    page.keyboard.type(fecha_inicio, delay=150)
    page.keyboard.press("Tab")

    page.wait_for_timeout(500)

    campo_hasta = page.locator("input[id*='hasta'], input[placeholder*='asta'], input[name*='asta']").first
    campo_hasta.click()
    page.keyboard.press("Control+a")
    page.keyboard.press("Delete")
    page.keyboard.type(fecha_fin, delay=150)
    page.keyboard.press("Tab")

    page.wait_for_timeout(500)

    boton = page.locator("a:has-text('Buscar'), button:has-text('Buscar')").first
    boton.click()

    page.wait_for_timeout(4000)

    # Eliminar el listener para que no acumule en la siguiente iteración
    page.remove_listener("response", handle_response)

    return resultados


def generar_anios(inicio: date, fin: date):
    tramos = []
    cursor = inicio
    while cursor <= fin:
        fin_tramo = date(cursor.year, 12, 31)
        if fin_tramo > fin:
            fin_tramo = fin
        tramos.append((cursor, fin_tramo))
        cursor = date(cursor.year + 1, 1, 1)
    return tramos


with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    page = context.new_page()
    page.goto("https://www.loteriasyapuestas.es/es/resultados/euromillones")
    page.wait_for_timeout(4000)

    historico = []
    anios = generar_anios(date(2004, 2, 13), date.today())

    for inicio, fin in anios:
        inicio_str = inicio.strftime("%d/%m/%Y")
        fin_str = fin.strftime("%d/%m/%Y")
        print(f"Descargando {inicio_str} → {fin_str}...", end=" ")
        tramo = fetch_euromillones_rango(page, inicio_str, fin_str)
        print(f"{len(tramo)} sorteos")
        historico.extend(tramo)

    browser.close()

print(f"\nTotal histórico: {len(historico)}")

with open("euromillones_historico_completo.json", "w", encoding="utf-8") as f:
    json.dump(historico, f, ensure_ascii=False, indent=2)