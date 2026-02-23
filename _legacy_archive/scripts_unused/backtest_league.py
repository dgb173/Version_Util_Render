"""
Backtest de Reglas en una Liga Específica
Calcula el profit/loss aplicando las reglas minadas a partidos históricos.
"""

import sys
from pathlib import Path
import json
import requests
import re

# Setup path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.rule_applier import RuleApplier


def fetch_league_matches(league_id: int, season: str = "2024-2025") -> list:
    """Descarga partidos de una liga desde Nowgoal JS."""
    
    # URL del archivo JS con los datos
    url = f"https://football.nowgoal26.com/jsData/matchResult/{season}/s{league_id}_en.js"
    
    print(f"Descargando datos de: {url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
        "Accept": "*/*",
        "Referer": "https://football.nowgoal26.com/"
    }
    
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            js_content = resp.text
            break
        except Exception as e:
            print(f"Intento {attempt+1} fallido: {e}")
            if attempt == 2:
                return []
            import time
            time.sleep(2)
    
    # Parsear el JS (formato: arrMatch = [[...], [...], ...])
    # Buscar el array arrMatch
    match = re.search(r'var\s+arrMatch\s*=\s*(\[.*?\]);', js_content, re.DOTALL)
    if not match:
        print("No se encontró arrMatch en el JS")
        return []
    
    # Limpiar y parsear
    arr_str = match.group(1)
    # Reemplazar comillas simples por dobles
    arr_str = arr_str.replace("'", '"')
    # Manejar valores vacíos
    arr_str = re.sub(r',\s*,', ',null,', arr_str)
    arr_str = re.sub(r'\[\s*,', '[null,', arr_str)
    arr_str = re.sub(r',\s*\]', ',null]', arr_str)
    
    try:
        matches_raw = json.loads(arr_str)
    except json.JSONDecodeError as e:
        print(f"Error parseando JSON: {e}")
        return []
    
    # Convertir a formato de diccionario
    # Formato típico: [id, date, time, homeId, homeName, awayId, awayName, homeGoals, awayGoals, ...]
    matches = []
    for row in matches_raw:
        if len(row) < 10:
            continue
        
        try:
            match_data = {
                'match_id': str(row[0]),
                'date': str(row[1]),
                'time': str(row[2]) if len(row) > 2 else '',
                'home_name': str(row[4]) if len(row) > 4 else '',
                'away_name': str(row[6]) if len(row) > 6 else '',
                'home_goals': int(row[7]) if row[7] is not None else None,
                'away_goals': int(row[8]) if row[8] is not None else None,
            }
            
            # Solo incluir partidos con resultado
            if match_data['home_goals'] is not None and match_data['away_goals'] is not None:
                matches.append(match_data)
        except:
            continue
    
    print(f"Encontrados {len(matches)} partidos con resultado")
    return matches


def fetch_match_odds(match_id: str) -> dict:
    """Obtiene odds AH para un partido específico."""
    # Esta función requeriría scrapear la página de cada partido
    # Por ahora retornamos datos mock para el ejemplo
    return {
        'ah_linea': None  # No disponible sin scrapeo individual
    }


def simulate_betting(matches: list, applier: RuleApplier, stake: float = 5.0, odds: float = 1.8) -> dict:
    """Simula apuestas basadas en las reglas."""
    
    results = {
        'total_bets': 0,
        'wins': 0,
        'losses': 0,
        'pushes': 0,
        'total_staked': 0.0,
        'total_returned': 0.0,
        'profit': 0.0,
        'details': []
    }
    
    for m in matches:
        # Necesitamos datos completos para aplicar reglas
        # Construir un objeto de partido simulado
        match_data = {
            'match_id': m['match_id'],
            'home_name': m['home_name'],
            'away_name': m['away_name'],
            'final_score': f"{m['home_goals']}:{m['away_goals']}",
            'main_match_odds': {
                'ah_linea': None  # Sin datos de odds
            }
        }
        
        # Intentar aplicar reglas
        pick = applier.get_best_pick(match_data)
        
        if not pick:
            continue
        
        # Tenemos una recomendación
        results['total_bets'] += 1
        results['total_staked'] += stake
        
        # Determinar si ganó o perdió
        home_goals = m['home_goals']
        away_goals = m['away_goals']
        
        bet_side = pick['pick']  # 'HOME' o 'AWAY'
        
        # Sin handicap, simplemente vemos quién ganó
        if bet_side == 'HOME':
            won = home_goals > away_goals
            push = home_goals == away_goals
        else:  # AWAY
            won = away_goals > home_goals
            push = home_goals == away_goals
        
        if won:
            results['wins'] += 1
            returned = stake * odds
            results['total_returned'] += returned
            outcome = 'WIN'
        elif push:
            results['pushes'] += 1
            results['total_returned'] += stake  # Devuelven la apuesta
            outcome = 'PUSH'
        else:
            results['losses'] += 1
            outcome = 'LOSS'
        
        results['details'].append({
            'match': f"{m['home_name']} vs {m['away_name']}",
            'score': f"{home_goals}:{away_goals}",
            'pick': bet_side,
            'rule': pick.get('rule_name', 'N/A'),
            'outcome': outcome
        })
    
    results['profit'] = results['total_returned'] - results['total_staked']
    
    return results


def main():
    print("=" * 60)
    print("BACKTEST DE REGLAS - A-League Australia 2024-2025")
    print("=" * 60)
    
    # Parámetros
    LEAGUE_ID = 273
    STAKE = 5.0
    ODDS = 1.8
    
    # Cargar reglas
    applier = RuleApplier()
    print(f"\nReglas cargadas: {len(applier.rules)}")
    
    if len(applier.rules) == 0:
        print("ERROR: No hay reglas cargadas. Ejecuta primero massive_pattern_trainer.py")
        return
    
    # Obtener partidos
    matches = fetch_league_matches(LEAGUE_ID)
    
    if not matches:
        print("ERROR: No se pudieron obtener partidos")
        return
    
    print(f"\nPartidos para backtest: {len(matches)}")
    
    # La limitación principal es que no tenemos datos de AH para cada partido
    # Las reglas minadas requieren ah_linea para funcionar
    print("\n" + "=" * 60)
    print("LIMITACION: Las reglas minadas requieren datos de handicap (AH)")
    print("que no están disponibles en el archivo JS de resultados.")
    print("Para un backtest real, necesitaríamos:")
    print("1. Scrapear cada partido individualmente para obtener AH")
    print("2. O usar datos de training_data con AH ya incluido")
    print("=" * 60)
    
    # Mostrar primeros partidos como ejemplo
    print("\nPrimeros 10 partidos encontrados:")
    for m in matches[:10]:
        print(f"  {m['date']} | {m['home_name']} {m['home_goals']}:{m['away_goals']} {m['away_name']}")
    
    # Intentar backtest básico (sin AH, las reglas no aplicarán)
    print("\n\nIntentando aplicar reglas (sin datos de AH)...")
    results = simulate_betting(matches, applier, STAKE, ODDS)
    
    print(f"\n{'='*60}")
    print("RESULTADOS DEL BACKTEST")
    print(f"{'='*60}")
    print(f"Apuestas realizadas: {results['total_bets']}")
    print(f"Ganadas: {results['wins']}")
    print(f"Perdidas: {results['losses']}")
    print(f"Push: {results['pushes']}")
    print(f"Total apostado: €{results['total_staked']:.2f}")
    print(f"Total retornado: €{results['total_returned']:.2f}")
    print(f"PROFIT/LOSS: €{results['profit']:.2f}")
    
    if results['total_bets'] > 0:
        print(f"\nROI: {(results['profit']/results['total_staked'])*100:.1f}%")
        print(f"Win Rate: {(results['wins']/results['total_bets'])*100:.1f}%")


if __name__ == '__main__':
    main()
