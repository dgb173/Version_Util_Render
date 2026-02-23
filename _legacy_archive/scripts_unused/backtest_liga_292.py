"""
Backtest en Liga 292 - Usando datos de validation_detailed_292.json
Testea las reglas mega-entrenadas en una liga específica.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
from scripts.rule_applier import RuleApplier


def parse_score(score_str):
    """Parsea score en formato X-Y."""
    if not score_str or score_str in ['-', '?:?']:
        return None, None
    score_str = str(score_str).replace(':', '-')
    parts = score_str.split('-')
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except:
        return None, None

def settle_ah(home_g, away_g, ah_line, bet_side):
    """
    Calcula resultado de apuesta AH según convención Nowgoal.
    
    En Nowgoal:
    - AH negativo (-1.75): VISITANTE es favorito, debe ganar por ese margen
      Ejemplo: 0-0 con AH -1.75 -> AWAY tiene que ganar por 2+, no lo hizo -> AWAY PIERDE
    - AH positivo (+1.75): LOCAL es favorito, recibe ese handicap
      Ejemplo: 0-0 con AH +1.75 -> LOCAL tiene +1.75 virtual -> LOCAL CUBRE
    
    Fórmula:
    - Para saber si LOCAL cubre: (home - away) + ah_line > 0
      (esto funciona porque AH positivo favorece al home, negativo lo perjudica)
    - Para saber si AWAY cubre: (away - home) - ah_line > 0
      (el AWAY gana si el home NO cubre)
    """
    # Calcular desde perspectiva del HOME
    # FORMULA CORRECTA: restar ah_line (si es positivo, local es favorito y se le resta)
    home_diff = (home_g - away_g) - ah_line
    
    # HOME gana si home_diff > 0
    # AWAY gana si home_diff < 0
    
    if bet_side == 'HOME' or bet_side == 'LOCAL':
        diff = home_diff
    else:  # AWAY/VISITA
        diff = -home_diff  # Invertimos para ver desde perspectiva AWAY
    
    # Ahora evaluamos si el bet_side gana
    if diff > 0.25:
        return 'WIN', 1.0
    elif diff > 0:
        return 'HALF_WIN', 0.5
    elif diff == 0:
        return 'PUSH', 0.0
    elif diff >= -0.25:
        return 'HALF_LOSS', -0.5
    else:
        return 'LOSS', -1.0


def main():
    print("=" * 70)
    print("BACKTEST EN LIGA 292 - Validacion Real")
    print("=" * 70)
    
    base_path = Path(__file__).parent.parent
    
    # Cargar datos de validacion
    data_path = base_path / 'validation_detailed_292.json'
    if not data_path.exists():
        print(f"ERROR: No existe {data_path}")
        return
    
    with open(data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    details = data.get('details', [])
    print(f"Partidos en liga 292: {len(details)}")
    print(f"Temporada: {data.get('season', 'N/A')}")
    
    # Cargar reglas
    print("\nCargando reglas mega-entrenadas...")
    applier = RuleApplier()
    print(f"Reglas cargadas: {len(applier.rules)}")
    
    if len(applier.rules) == 0:
        print("ERROR: No hay reglas")
        return
    
    # Parametros
    STAKE = 5.0
    ODDS = 1.85
    
    # Backtest
    print(f"\nEjecutando backtest (stake={STAKE}, odds={ODDS})...")
    
    results = {
        'total': len(details),
        'picks': 0,
        'wins': 0,
        'half_wins': 0,
        'pushes': 0,
        'half_losses': 0,
        'losses': 0,
        'staked': 0.0,
        'returned': 0.0
    }
    
    bet_details = []
    
    for match in details:
        score = match.get('score', '')
        ah = match.get('ah')
        
        if ah is None:
            continue
        
        home_g, away_g = parse_score(score)
        if home_g is None:
            continue
        
        # Construir objeto de partido para las reglas
        match_data = {
            'match_id': 'unknown',
            'home_name': match.get('home', 'Unknown'),
            'away_name': match.get('away', 'Unknown'),
            'final_score': score,
            'main_match_odds': {
                'ah_linea': str(ah)
            },
            # Add empty values to avoid NoneType errors
            'last_home_match': None,
            'last_away_match': None,
            'h2h_stadium': {},
            'h2h_general': {},
        }
        
        # Aplicar reglas
        pick = applier.get_best_pick(match_data)
        
        if not pick:
            continue
        
        results['picks'] += 1
        results['staked'] += STAKE
        
        bet_side = pick['pick']  # 'HOME' o 'AWAY'
        
        # Calcular resultado
        outcome, payout = settle_ah(home_g, away_g, ah, bet_side)
        
        if outcome == 'WIN':
            results['wins'] += 1
            returned = STAKE * ODDS
        elif outcome == 'HALF_WIN':
            results['half_wins'] += 1
            returned = STAKE + (STAKE * (ODDS - 1) / 2)
        elif outcome == 'PUSH':
            results['pushes'] += 1
            returned = STAKE
        elif outcome == 'HALF_LOSS':
            results['half_losses'] += 1
            returned = STAKE / 2
        else:
            results['losses'] += 1
            returned = 0.0
        
        results['returned'] += returned
        
        bet_details.append({
            'score': score,
            'ah': ah,
            'pick': bet_side,
            'rule': pick.get('rule_name', 'N/A')[:25],
            'outcome': outcome,
            'profit': returned - STAKE
        })
    
    # Resultados
    profit = results['returned'] - results['staked']
    roi = (profit / results['staked'] * 100) if results['staked'] > 0 else 0
    win_count = results['wins'] + results['half_wins']
    win_rate = (win_count / results['picks'] * 100) if results['picks'] > 0 else 0
    
    print("\n" + "=" * 70)
    print("RESULTADOS EN LIGA 292")
    print("=" * 70)
    print(f"Total partidos: {results['total']}")
    print(f"Picks realizados: {results['picks']}")
    print(f"  - Ganadas: {results['wins']}")
    print(f"  - Media Ganada: {results['half_wins']}")
    print(f"  - Push: {results['pushes']}")
    print(f"  - Media Perdida: {results['half_losses']}")
    print(f"  - Perdidas: {results['losses']}")
    print()
    print(f"Total apostado: EUR{results['staked']:.2f}")
    print(f"Total retornado: EUR{results['returned']:.2f}")
    print(f"PROFIT/LOSS: EUR{profit:+.2f}")
    print()
    print(f"WIN RATE: {win_rate:.1f}%")
    print(f"ROI: {roi:+.1f}%")
    
    # Primeras apuestas
    print("\n" + "=" * 70)
    print("PRIMERAS 20 APUESTAS:")
    print("=" * 70)
    for d in bet_details[:20]:
        emoji = "[OK]" if d['outcome'] in ['WIN', 'HALF_WIN'] else ("[ ]" if d['outcome'] == 'PUSH' else "[XX]")
        print(f"{emoji} {d['score']:6} | AH={d['ah']:+.2f} | {d['pick']:5} | {d['outcome']:10} | EUR{d['profit']:+.2f}")
    
    # Guardar detalle
    output = base_path / 'backtest_liga_292.json'
    with open(output, 'w', encoding='utf-8') as f:
        json.dump({
            'league_id': 292,
            'total_matches': results['total'],
            'total_picks': results['picks'],
            'wins': results['wins'],
            'half_wins': results['half_wins'],
            'losses': results['losses'],
            'win_rate': win_rate,
            'roi': roi,
            'profit': profit,
            'details': bet_details
        }, f, indent=2)
    
    print(f"\nGuardado: {output}")


if __name__ == '__main__':
    main()
