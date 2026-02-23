"""
Backtest en Liga 15 (German Bundesliga)
Filtra partidos de Bundesliga de los datos existentes y hace backtest.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
from scripts.rule_applier import RuleApplier


def load_all_data(data_dir):
    """Carga todos los archivos de datos."""
    all_matches = []
    
    json_files = [
        'data_ah_0.5.json',
        'data_ah_0.json',
        'data_minus_ah_0.5.json',
        'data_ah_1.5.json',
        'data_minus_ah_1.5.json',
        'data_ah_2_plus.json',
        'data_minus_ah_2_plus.json',
    ]
    
    for fname in json_files:
        fpath = data_dir / fname
        if fpath.exists():
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_matches.extend(data)
            except:
                pass
    
    return all_matches


def filter_league(matches, league_keywords):
    """Filtra partidos por nombre de liga."""
    filtered = []
    for m in matches:
        league = m.get('league_name', '').lower()
        for kw in league_keywords:
            if kw.lower() in league:
                filtered.append(m)
                break
    return filtered


def parse_score(score_str):
    if not score_str or score_str in ['-', '?:?']:
        return None, None
    score_str = str(score_str).replace('-', ':')
    parts = score_str.split(':')
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except:
        return None, None


def settle_ah(home_g, away_g, ah_line, bet_side):
    diff = (home_g - away_g) + ah_line
    
    if bet_side in ['HOME', 'LOCAL']:
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
    else:
        diff = -diff
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


def run_backtest(matches, applier, stake=5.0, odds=1.85):
    """Ejecuta backtest."""
    
    results = {
        'total': len(matches),
        'picked': 0,
        'wins': 0,
        'half_wins': 0,
        'pushes': 0,
        'half_losses': 0,
        'losses': 0,
        'staked': 0.0,
        'returned': 0.0
    }
    
    for m in matches:
        score = m.get('final_score', '')
        odds_data = m.get('main_match_odds', {})
        ah_str = odds_data.get('ah_linea')
        
        if not ah_str or ah_str == '-':
            continue
        
        try:
            ah = float(ah_str)
        except:
            continue
        
        home_g, away_g = parse_score(score)
        if home_g is None:
            continue
        
        # Aplicar reglas
        pick = applier.get_best_pick(m)
        if not pick:
            continue
        
        results['picked'] += 1
        results['staked'] += stake
        
        bet_side = pick['pick']
        outcome, _ = settle_ah(home_g, away_g, ah, bet_side)
        
        if outcome == 'WIN':
            results['wins'] += 1
            results['returned'] += stake * odds
        elif outcome == 'HALF_WIN':
            results['half_wins'] += 1
            results['returned'] += stake + (stake * (odds - 1) / 2)
        elif outcome == 'PUSH':
            results['pushes'] += 1
            results['returned'] += stake
        elif outcome == 'HALF_LOSS':
            results['half_losses'] += 1
            results['returned'] += stake / 2
        else:
            results['losses'] += 1
    
    return results


def main():
    print("=" * 70)
    print("BACKTEST EN LIGA 15 (German Bundesliga)")
    print("=" * 70)
    
    base_path = Path(__file__).parent.parent
    data_dir = base_path / 'data'
    
    # Cargar todos los datos
    print("\nCargando datos...")
    all_matches = load_all_data(data_dir)
    print(f"Total partidos cargados: {len(all_matches)}")
    
    # Filtrar Bundesliga (liga 15)
    # Keywords para German Bundesliga
    keywords = ['bundesliga', 'german bundesliga', 'germany bundesliga']
    bundesliga_matches = filter_league(all_matches, keywords)
    print(f"Partidos de Bundesliga: {len(bundesliga_matches)}")
    
    if not bundesliga_matches:
        # Si no hay Bundesliga, mostrar ligas disponibles
        print("\nNo se encontraron partidos de Bundesliga.")
        leagues = set()
        for m in all_matches[:1000]:
            leagues.add(m.get('league_name', 'Unknown'))
        print("\nLigas disponibles (muestra):")
        for l in sorted(leagues)[:20]:
            print(f"  - {l}")
        return
    
    # Cargar reglas
    print("\nCargando reglas...")
    applier = RuleApplier()
    print(f"Reglas: {len(applier.rules)}")
    
    # Backtest
    STAKE = 5.0
    ODDS = 1.85
    
    print(f"\nEjecutando backtest (stake={STAKE}, odds={ODDS})...")
    results = run_backtest(bundesliga_matches, applier, STAKE, ODDS)
    
    # Resultados
    profit = results['returned'] - results['staked']
    roi = (profit / results['staked'] * 100) if results['staked'] > 0 else 0
    win_count = results['wins'] + results['half_wins']
    win_rate = (win_count / results['picked'] * 100) if results['picked'] > 0 else 0
    
    print("\n" + "=" * 70)
    print("RESULTADOS EN BUNDESLIGA (Liga 15)")
    print("=" * 70)
    print(f"Total partidos: {results['total']}")
    print(f"Picks realizados: {results['picked']}")
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


if __name__ == '__main__':
    main()
