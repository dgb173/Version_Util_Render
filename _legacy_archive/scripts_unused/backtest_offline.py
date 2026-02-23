#!/usr/bin/env python
"""
Backtest Offline - Usa datos locales de data_ah_*.json
No hace requests externos
"""
import sys
import json
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.precacheo_loader import get_loader
from scripts.pattern_miner_v2.features_builder_v2 import build_match_features, discretize_features
from scripts.pattern_miner_v2.settle_asian import settle_ah, get_ah_family

STAKE = 5.0
ODDS = 1.80

def load_all_matches():
    """Carga todos los partidos de data_ah_*.json"""
    data_dir = PROJECT_ROOT / 'data'
    all_matches = []
    
    for f in data_dir.glob('data_ah_*.json'):
        try:
            with open(f, 'r', encoding='utf-8') as file:
                data = json.load(file)
                matches = data if isinstance(data, list) else data.get('matches', [])
                all_matches.extend(matches)
                print(f"  {f.name}: {len(matches)} partidos")
        except Exception as e:
            print(f"  Error {f.name}: {e}")
    
    return all_matches

def main():
    print("=" * 60)
    print("BACKTEST OFFLINE DE PATRONES v2")
    print(f"Stake: {STAKE}€ | Cuota: {ODDS}")
    print("=" * 60)
    
    # 1. Cargar partidos
    print("\n[1] Cargando partidos locales...")
    matches = load_all_matches()
    print(f"    Total: {len(matches)} partidos")
    
    # Filtrar solo los que tienen resultado
    matches_with_result = [m for m in matches if m.get('final_score') and ':' in str(m.get('final_score', ''))]
    print(f"    Con resultado: {len(matches_with_result)}")
    
    # 2. Cargar patrones
    print("\n[2] Cargando patrones...")
    loader = get_loader()
    if not loader.loaded:
        loader.load_all_patterns(str(PROJECT_ROOT / 'data' / 'patterns_v2'))
    print(f"    Patrones AH: {len(loader.ah_patterns)}, OU: {len(loader.ou_patterns)}")
    
    # 3. Backtest
    print("\n[3] Ejecutando backtest...")
    
    results = {
        'total_evaluated': 0,
        'with_picks': 0,
        'total_picks': 0,
        'wins': 0,
        'losses': 0,
        'pushes': 0,
        'profit': 0.0,
        'staked': 0.0,
        'by_family': defaultdict(lambda: {'wins': 0, 'losses': 0, 'profit': 0}),
        'by_target': {'HOME': {'wins': 0, 'losses': 0, 'profit': 0}, 'AWAY': {'wins': 0, 'losses': 0, 'profit': 0}}
    }
    
    for i, match in enumerate(matches_with_result[:500]):  # Limitar a 500 para velocidad
        try:
            # Parsear resultado
            score = match.get('final_score', '')
            if ':' in score:
                parts = score.split(':')
                home_goals = int(parts[0].strip())
                away_goals = int(parts[1].strip())
            else:
                continue
            
            # Parsear AH
            ah_str = match.get('main_match_odds', {}).get('ah_linea') or match.get('ah', 0)
            try:
                ah_line = float(ah_str) if ah_str else 0
            except:
                continue
            
            if ah_line == 0:
                continue
            
            results['total_evaluated'] += 1
            
            # Evaluar patrones
            picks_result = loader.evaluate_match(match)
            ah_picks = picks_result.get('ah_picks', [])
            
            if not ah_picks:
                continue
            
            results['with_picks'] += 1
            
            # Tomar el mejor pick por ROI
            best_pick = max(ah_picks, key=lambda x: x.get('roi_test', 0))
            target = best_pick.get('target', '').upper()
            family = get_ah_family(ah_line)
            
            results['total_picks'] += 1
            results['staked'] += STAKE
            
            # Calcular settlement
            outcome, payout = settle_ah(home_goals, away_goals, ah_line)
            
            # Si apostamos HOME, payout positivo = ganamos
            # Si apostamos AWAY, payout negativo = ganamos
            if target == 'HOME':
                profit = payout * STAKE * (ODDS - 1) if payout > 0 else payout * STAKE
            else:  # AWAY
                profit = -payout * STAKE * (ODDS - 1) if payout < 0 else -payout * STAKE
            
            results['profit'] += profit
            results['by_family'][family]['profit'] += profit
            results['by_target'][target]['profit'] += profit
            
            if profit > 0:
                results['wins'] += 1
                results['by_family'][family]['wins'] += 1
                results['by_target'][target]['wins'] += 1
            elif profit < 0:
                results['losses'] += 1
                results['by_family'][family]['losses'] += 1
                results['by_target'][target]['losses'] += 1
            else:
                results['pushes'] += 1
                
        except Exception as e:
            pass
        
        if (i + 1) % 100 == 0:
            print(f"    Procesados {i+1}...")
    
    # 4. Resultados
    print("\n" + "=" * 60)
    print("📊 RESULTADOS DEL BACKTEST")
    print("=" * 60)
    
    print(f"\n🎯 RESUMEN:")
    print(f"  Partidos evaluados: {results['total_evaluated']}")
    print(f"  Partidos con picks: {results['with_picks']}")
    print(f"  Total picks: {results['total_picks']}")
    print(f"  ✅ Aciertos: {results['wins']}")
    print(f"  ❌ Fallos: {results['losses']}")
    print(f"  ➖ Push: {results['pushes']}")
    
    if results['total_picks'] > 0:
        win_rate = results['wins'] / (results['wins'] + results['losses']) * 100 if (results['wins'] + results['losses']) > 0 else 0
        print(f"  📈 Win Rate: {win_rate:.1f}%")
    
    print(f"\n💰 BALANCE FINANCIERO:")
    print(f"  Total apostado: {results['staked']:.2f}€")
    print(f"  Profit/Loss: {results['profit']:+.2f}€")
    
    if results['staked'] > 0:
        roi = results['profit'] / results['staked'] * 100
        print(f"  ROI: {roi:+.1f}%")
    
    print(f"\n📊 POR TARGET:")
    for target, stats in results['by_target'].items():
        total = stats['wins'] + stats['losses']
        if total > 0:
            wr = stats['wins'] / total * 100
            print(f"  {target}: {stats['wins']}W/{stats['losses']}L ({wr:.0f}%) | P:{stats['profit']:+.2f}€")
    
    print(f"\n📊 POR FAMILIA AH:")
    for family, stats in sorted(results['by_family'].items()):
        total = stats['wins'] + stats['losses']
        if total > 0:
            wr = stats['wins'] / total * 100
            print(f"  {family}: {stats['wins']}W/{stats['losses']}L ({wr:.0f}%) | P:{stats['profit']:+.2f}€")
    
    # Guardar
    output = PROJECT_ROOT / 'backtest_results' / 'backtest_offline_report.json'
    with open(output, 'w', encoding='utf-8') as f:
        json.dump(dict(results), f, indent=2, default=str)
    print(f"\n[✓] Reporte guardado: {output}")

if __name__ == "__main__":
    main()
