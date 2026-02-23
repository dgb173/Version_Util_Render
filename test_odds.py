"""Verificar que las cuotas históricas se extraen correctamente"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, 'src')

from modules.estudio_scraper import get_requests_session_of, extract_vs_odds
from bs4 import BeautifulSoup

session = get_requests_session_of()
r = session.get('https://www.nowgoal.com/match/h2h-2850521', timeout=15)
soup = BeautifulSoup(r.text, 'lxml')

# Extraer odds map (cuotas históricas)
odds_map = extract_vs_odds(soup)
print(f"Odds map tiene {len(odds_map)} entradas")
if odds_map:
    print("Ejemplos:", list(odds_map.items())[:5])

# Verificar tabla de historial con cuotas
table = soup.find('table', id='table_v1')
if table:
    rows = table.find_all('tr', id=lambda x: x and x.startswith('tr1_'))[:3]
    print(f"\nTabla table_v1 encontrada con {len(rows)} filas ejemplo")
    for row in rows:
        tds = row.find_all('td')
        if len(tds) > 11:
            ah_line = tds[11].get('data-o', 'N/A')
            score_td = tds[3]
            score = score_td.get_text(strip=True) if score_td else 'N/A'
            print(f"  Fila {row.get('id')}: AH={ah_line}, Score={score}")
else:
    print("No se encontró table_v1")
