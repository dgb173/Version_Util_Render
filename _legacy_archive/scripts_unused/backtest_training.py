"""
Backtest de Reglas usando Training Data con Handicaps
Calcula el profit/loss simulando apuestas en partidos históricos.
"""

import sys
from pathlib import Path
import json

# Setup path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.rule_applier import RuleApplier


def load_training_data(json_path: str) -> list:
    """Carga partidos del archivo de training."""
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def parse_score(score_str: str):
    """Parsea score en formato X:Y."""
    if not score_str or score_str in ['-', '?:?', '']:
        return None, None
    score_str = score_str.replace('-', ':')
    parts = score_str.split(':')
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except:
        return None, None


def settle_ah_bet(home_goals: int, away_goals: int, ah_line: float, bet_side: str) -> str:
    """
    Determina el resultado de una apuesta AH.
    
    ah_line es desde la perspectiva del HOME.
    Si ah_line = -0.5, HOME debe ganar por 1+ para cubrir.
    Si ah_line = 0.5, AWAY debe ganar o empatar para cubrir desde su perspectiva.
    
    Retorna: 'WIN', 'HALF_WIN', 'PUSH', 'HALF_LOSS', 'LOSS'
    """
    # Ajustar goles según la línea
    adjusted_diff = (home_goals - away_goals) + ah_line
    
    if bet_side == 'HOME':
        if adjusted_diff > 0.5:
            return 'WIN'
        elif adjusted_diff == 0.5:
            return 'HALF_WIN'  # Gana mitad
        elif adjusted_diff == 0:
            return 'PUSH'
        elif adjusted_diff == -0.5:
            return 'HALF_LOSS'
        else:
            return 'LOSS'
    else:  # AWAY
        # Invertir la lógica
        if adjusted_diff < -0.5:
            return 'WIN'
        elif adjusted_diff == -0.5:
            return 'HALF_WIN'
        elif adjusted_diff == 0:
            return 'PUSH'
        elif adjusted_diff == 0.5:
            return 'HALF_LOSS'
        else:
            return 'LOSS'


def calculate_return(outcome: str, stake: float, odds: float) -> float:
    """Calcula el retorno según el resultado."""
    if outcome == 'WIN':
        return stake * odds  # Ganancia completa
    elif outcome == 'HALF_WIN':
        return stake + (stake * (odds - 1) / 2)  # Mitad de ganancia
    elif outcome == 'PUSH':
        return stake  # Devuelven apuesta
    elif outcome == 'HALF_LOSS':
        return stake / 2  # Pierdes mitad
    else:  # LOSS
        return 0.0


def run_backtest(matches: list, applier: RuleApplier, stake: float = 5.0, odds: float = 1.8):
    """Ejecuta el backtest."""
    
    results = {
        'total_matches': len(matches),
        'matches_with_picks': 0,
        'wins': 0,
        'half_wins': 0,
        'pushes': 0,
        'half_losses': 0,
        'losses': 0,
        'total_staked': 0.0,
        'total_returned': 0.0,
        'details': []
    }
    
    for m in matches:
        # Verificar que tiene datos necesarios
        odds_data = m.get('main_match_odds', {})
        ah_line_str = odds_data.get('ah_linea')
        
        if not ah_line_str or ah_line_str == '-':
            continue
        
        try:
            ah_line = float(ah_line_str)
        except:
            continue
        
        # Parsear score
        score = m.get('final_score', '')
        home_goals, away_goals = parse_score(score)
        
        if home_goals is None:
            continue
        
        # Aplicar reglas
        pick = applier.get_best_pick(m)
        
        if not pick:
            continue
        
        # Tenemos un pick!
        results['matches_with_picks'] += 1
        results['total_staked'] += stake
        
        bet_side = pick['pick']  # 'HOME' o 'AWAY'
        
        # Calcular resultado
        outcome = settle_ah_bet(home_goals, away_goals, ah_line, bet_side)
        returned = calculate_return(outcome, stake, odds)
        results['total_returned'] += returned
        
        # Contabilizar
        if outcome == 'WIN':
            results['wins'] += 1
        elif outcome == 'HALF_WIN':
            results['half_wins'] += 1
        elif outcome == 'PUSH':
            results['pushes'] += 1
        elif outcome == 'HALF_LOSS':
            results['half_losses'] += 1
        else:
            results['losses'] += 1
        
        # Guardar detalle
        profit = returned - stake
        results['details'].append({
            'match': f"{m.get('home_name', '?')} vs {m.get('away_name', '?')}",
            'score': score,
            'ah': ah_line,
            'pick': bet_side,
            'rule': pick.get('rule_name', 'N/A')[:30],
            'outcome': outcome,
            'profit': profit
        })
    
    results['profit'] = results['total_returned'] - results['total_staked']
    
    return results


def main():
    print("=" * 70)
    print("BACKTEST DE REGLAS MINADAS - Training Data Completo")
    print("=" * 70)
    
    # Parámetros
    STAKE = 5.0
    ODDS = 1.85  # Cuota promedio realista
    
    # Paths
    base_path = Path(__file__).parent.parent
    training_path = base_path / 'training_data_1465.json'
    
    if not training_path.exists():
        print(f"ERROR: No existe {training_path}")
        return
    
    # Cargar reglas
    print("\nCargando reglas...")
    applier = RuleApplier()
    print(f"Reglas cargadas: {len(applier.rules)}")
    
    if len(applier.rules) == 0:
        print("ERROR: No hay reglas. Ejecuta massive_pattern_trainer.py primero.")
        return
    
    # Cargar datos
    print("\nCargando datos de entrenamiento...")
    matches = load_training_data(str(training_path))
    print(f"Partidos cargados: {len(matches)}")
    
    # Ejecutar backtest
    print(f"\nEjecutando backtest con stake=€{STAKE}, odds={ODDS}...")
    results = run_backtest(matches, applier, STAKE, ODDS)
    
    # Resultados
    print("\n" + "=" * 70)
    print("RESULTADOS DEL BACKTEST")
    print("=" * 70)
    print(f"Total partidos analizados: {results['total_matches']}")
    print(f"Partidos con picks: {results['matches_with_picks']}")
    print(f"  - Ganadas: {results['wins']}")
    print(f"  - Media Ganada: {results['half_wins']}")
    print(f"  - Push: {results['pushes']}")
    print(f"  - Media Perdida: {results['half_losses']}")
    print(f"  - Perdidas: {results['losses']}")
    print()
    print(f"Total apostado: €{results['total_staked']:.2f}")
    print(f"Total retornado: €{results['total_returned']:.2f}")
    print(f"PROFIT/LOSS: €{results['profit']:.2f}")
    
    if results['matches_with_picks'] > 0:
        roi = (results['profit'] / results['total_staked']) * 100
        win_rate = ((results['wins'] + results['half_wins']) / results['matches_with_picks']) * 100
        print()
        print(f"ROI: {roi:+.1f}%")
        print(f"Win Rate (full+half): {win_rate:.1f}%")
    
    # Mostrar primeras apuestas
    print("\n" + "=" * 70)
    print("PRIMERAS 20 APUESTAS:")
    print("=" * 70)
    for d in results['details'][:20]:
        emoji = "✅" if d['outcome'] in ['WIN', 'HALF_WIN'] else ("⬜" if d['outcome'] == 'PUSH' else "❌")
        print(f"{emoji} {d['match'][:35]:35} | {d['score']:5} | AH={d['ah']:+.2f} | {d['pick']:4} | {d['outcome']:9} | €{d['profit']:+.2f}")
    
    # Guardar detalle completo
    detail_path = base_path / 'backtest_results.json'
    with open(detail_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nDetalle completo guardado en: {detail_path}")


if __name__ == '__main__':
    main()
