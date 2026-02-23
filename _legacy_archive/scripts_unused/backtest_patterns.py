#!/usr/bin/env python
"""
Backtest de Patrones v2 con data.json
Evalúa los patrones actuales contra partidos históricos
"""
import sys
import json
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.precacheo_loader import get_loader
from scripts.pattern_miner_v2.features_builder_v2 import build_match_features, discretize_features
from scripts.pattern_miner_v2.settle_asian import settle_ah, settle_ou, get_ah_family

# Configuración
STAKE = 5.0  # Euros por apuesta
ODDS = 1.80  # Cuota típica AH

def load_backtest_data():
    """Carga data.json de backtest_results"""
    data_file = PROJECT_ROOT / 'backtest_results' / 'data.json'
    if not data_file.exists():
        print(f"[ERROR] No se encontró {data_file}")
        return []
    
    with open(data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data.get('upcoming_matches', [])

def get_match_result(match_id):
    """
    Obtiene el resultado de un partido de NowGoal.
    Retorna (home_goals, away_goals) o (None, None) si no hay resultado.
    """
    import requests
    
    url = f"https://live15.nowgoal25.com/detail/data?id={match_id}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://live.nowgoal25.com/'
    }
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        content = resp.text
        
        # Parsear el JS para obtener scores
        import re
        
        # Buscar home y away scores
        home_match = re.search(r'"home":\s*(\d+)', content)
        away_match = re.search(r'"away":\s*(\d+)', content)
        
        if home_match and away_match:
            return int(home_match.group(1)), int(away_match.group(1))
        
        # Formato alternativo
        score_match = re.search(r'"score":\s*"(\d+)-(\d+)"', content)
        if score_match:
            return int(score_match.group(1)), int(score_match.group(2))
            
    except Exception as e:
        pass
    
    return None, None

def evaluate_pick(pick, home_goals, away_goals, ah_line, ou_line):
    """
    Evalúa si un pick fue acertado.
    Retorna (resultado, profit)
    """
    if pick['type'] == 'AH':
        outcome, payout = settle_ah(home_goals, away_goals, ah_line)
        target = pick.get('normalizedPick', pick.get('pick', ''))
        
        # Si apostamos al favorito (HOME cuando ah < 0, AWAY cuando ah > 0)
        if 'LOCAL' in target.upper() or 'HOME' in target.upper():
            # Apostamos a HOME
            if payout > 0:
                return 'WIN', STAKE * (ODDS - 1) * payout
            elif payout < 0:
                return 'LOSS', STAKE * payout
            else:
                return 'PUSH', 0
        else:
            # Apostamos a AWAY (invertido)
            if payout < 0:
                return 'WIN', STAKE * (ODDS - 1) * abs(payout)
            elif payout > 0:
                return 'LOSS', -STAKE * payout
            else:
                return 'PUSH', 0
    
    elif pick['type'] == 'OU':
        total = home_goals + away_goals
        outcome, payout = settle_ou(total, ou_line, 'OVER')
        target = pick.get('normalizedPick', pick.get('pick', ''))
        
        if 'OVER' in target.upper():
            if payout > 0:
                return 'WIN', STAKE * (ODDS - 1) * payout
            elif payout < 0:
                return 'LOSS', STAKE * payout
            else:
                return 'PUSH', 0
        else:  # UNDER
            if payout < 0:
                return 'WIN', STAKE * (ODDS - 1) * abs(payout)
            elif payout > 0:
                return 'LOSS', -STAKE * payout
            else:
                return 'PUSH', 0
    
    return 'UNKNOWN', 0

def main():
    print("=" * 60)
    print("BACKTEST DE PATRONES v2")
    print(f"Stake: {STAKE}€ | Cuota: {ODDS}")
    print("=" * 60)
    
    # 1. Cargar datos
    matches = load_backtest_data()
    print(f"\n[1] Partidos cargados: {len(matches)}")
    
    if not matches:
        print("[ERROR] No hay partidos para testear")
        return
    
    # 2. Cargar patrones
    loader = get_loader()
    if not loader.loaded:
        loader.load_all_patterns(str(PROJECT_ROOT / 'data' / 'patterns_v2'))
    
    print(f"[2] Patrones AH: {len(loader.ah_patterns)}, OU: {len(loader.ou_patterns)}")
    
    # 3. Resultados
    results = {
        'total_matches': 0,
        'matches_with_picks': 0,
        'total_picks': 0,
        'wins': 0,
        'losses': 0,
        'pushes': 0,
        'total_profit': 0,
        'total_staked': 0,
        'by_pattern': {},
        'by_type': {'AH': {'wins': 0, 'losses': 0, 'profit': 0}, 'OU': {'wins': 0, 'losses': 0, 'profit': 0}},
        'picks_detail': []
    }
    
    print("\n[3] Obteniendo resultados y evaluando picks...")
    
    for i, match in enumerate(matches):
        match_id = match.get('id')
        home_team = match.get('home_team', 'Home')
        away_team = match.get('away_team', 'Away')
        ah_line = float(match.get('handicap', 0) or 0)
        ou_line = float(match.get('goal_line', 2.5) or 2.5)
        
        # Obtener resultado
        home_goals, away_goals = get_match_result(match_id)
        
        if home_goals is None:
            continue
        
        results['total_matches'] += 1
        
        # Crear match_data para el loader
        match_data = {
            'match_id': match_id,
            'home_name': home_team,
            'away_name': away_team,
            'main_match_odds': {
                'ah_linea': str(ah_line),
                'goals_linea': str(ou_line)
            },
            'final_score': f"{home_goals}:{away_goals}"
        }
        
        # Evaluar patrones
        try:
            picks_result = loader.evaluate_match(match_data)
            ah_picks = picks_result.get('ah_picks', [])
            ou_picks = picks_result.get('ou_picks', [])
            
            all_picks = ah_picks + ou_picks
            
            if all_picks:
                results['matches_with_picks'] += 1
                
                # Tomar el mejor pick de cada tipo
                best_ah = sorted(ah_picks, key=lambda x: x.get('roi_test', 0), reverse=True)[:1]
                best_ou = sorted(ou_picks, key=lambda x: x.get('roi_test', 0), reverse=True)[:1]
                
                for pick in best_ah + best_ou:
                    results['total_picks'] += 1
                    results['total_staked'] += STAKE
                    
                    pick_type = pick.get('market', 'AH')
                    pick['type'] = pick_type
                    
                    outcome, profit = evaluate_pick(pick, home_goals, away_goals, ah_line, ou_line)
                    
                    results['total_profit'] += profit
                    
                    pattern_name = pick.get('pattern_name', 'Unknown')
                    if pattern_name not in results['by_pattern']:
                        results['by_pattern'][pattern_name] = {'wins': 0, 'losses': 0, 'pushes': 0, 'profit': 0}
                    
                    if outcome == 'WIN':
                        results['wins'] += 1
                        results['by_pattern'][pattern_name]['wins'] += 1
                        results['by_type'][pick_type]['wins'] += 1
                    elif outcome == 'LOSS':
                        results['losses'] += 1
                        results['by_pattern'][pattern_name]['losses'] += 1
                        results['by_type'][pick_type]['losses'] += 1
                    else:
                        results['pushes'] += 1
                        results['by_pattern'][pattern_name]['pushes'] += 1
                    
                    results['by_pattern'][pattern_name]['profit'] += profit
                    results['by_type'][pick_type]['profit'] += profit
                    
                    results['picks_detail'].append({
                        'match': f"{home_team} vs {away_team}",
                        'score': f"{home_goals}-{away_goals}",
                        'pick': pick.get('pick'),
                        'type': pick_type,
                        'pattern': pattern_name,
                        'outcome': outcome,
                        'profit': round(profit, 2)
                    })
        except Exception as e:
            pass
        
        if (i + 1) % 10 == 0:
            print(f"  Procesados {i+1}/{len(matches)}...")
    
    # 4. Mostrar resultados
    print("\n" + "=" * 60)
    print("RESULTADOS DEL BACKTEST")
    print("=" * 60)
    
    print(f"\n📊 RESUMEN GENERAL:")
    print(f"  • Partidos analizados: {results['total_matches']}")
    print(f"  • Partidos con picks: {results['matches_with_picks']}")
    print(f"  • Total picks realizados: {results['total_picks']}")
    print(f"  • Aciertos: {results['wins']}")
    print(f"  • Fallos: {results['losses']}")
    print(f"  • Empates: {results['pushes']}")
    
    if results['total_picks'] > 0:
        win_rate = results['wins'] / results['total_picks'] * 100
        print(f"  • Win Rate: {win_rate:.1f}%")
    
    print(f"\n💰 BALANCE FINANCIERO (Stake: {STAKE}€):")
    print(f"  • Total apostado: {results['total_staked']:.2f}€")
    print(f"  • Profit/Loss: {results['total_profit']:+.2f}€")
    
    if results['total_staked'] > 0:
        roi = results['total_profit'] / results['total_staked'] * 100
        print(f"  • ROI: {roi:+.1f}%")
    
    print(f"\n📈 POR TIPO DE MERCADO:")
    for mtype, stats in results['by_type'].items():
        total = stats['wins'] + stats['losses']
        if total > 0:
            wr = stats['wins'] / total * 100
            print(f"  • {mtype}: {stats['wins']}W/{stats['losses']}L ({wr:.1f}%) | Profit: {stats['profit']:+.2f}€")
    
    print(f"\n🎯 TOP 10 PATRONES (por profit):")
    sorted_patterns = sorted(results['by_pattern'].items(), key=lambda x: x[1]['profit'], reverse=True)[:10]
    for pattern, stats in sorted_patterns:
        total = stats['wins'] + stats['losses']
        wr = stats['wins'] / total * 100 if total > 0 else 0
        print(f"  • {pattern[:40]}: {stats['wins']}W/{stats['losses']}L ({wr:.0f}%) P:{stats['profit']:+.2f}€")
    
    print(f"\n❌ PEORES 5 PATRONES:")
    worst = sorted(results['by_pattern'].items(), key=lambda x: x[1]['profit'])[:5]
    for pattern, stats in worst:
        total = stats['wins'] + stats['losses']
        wr = stats['wins'] / total * 100 if total > 0 else 0
        print(f"  • {pattern[:40]}: {stats['wins']}W/{stats['losses']}L ({wr:.0f}%) P:{stats['profit']:+.2f}€")
    
    # Guardar resultados
    output_file = PROJECT_ROOT / 'backtest_results' / 'backtest_report.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n[✓] Reporte guardado en: {output_file}")

if __name__ == "__main__":
    main()
