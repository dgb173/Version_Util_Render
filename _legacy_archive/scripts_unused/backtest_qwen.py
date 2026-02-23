"""
Backtest Sistema Qwen v6 - Con logica corregida
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.features_builder_v2 import build_match_features, discretize_features
from scripts.pattern_miner_v2.precacheo_loader import PrecacheoLoaderV2
from scripts.pattern_miner_v2.settle_asian import settle_ah, calculate_profit

FIXED_ODDS = 1.8


def settle_match_ah(home_goals, away_goals, ah_line, target):
    """
    Calcula resultado usando settle_ah oficial.
    NowGoal: AH positivo = Local favorito
    settle_ah: AH negativo = Local favorito
    Por tanto, negamos ah_line
    """
    if home_goals is None or away_goals is None or ah_line is None:
        return None, 0.0
    
    ah_for_settle = -ah_line
    outcome_home, _ = settle_ah(home_goals, away_goals, ah_for_settle)
    profit_home = calculate_profit(FIXED_ODDS, outcome_home)
    
    if target == 'HOME':
        return outcome_home, profit_home
    else:
        if outcome_home in ['W', 'HW']:
            outcome_away = 'L' if outcome_home == 'W' else 'HL'
        elif outcome_home in ['L', 'HL']:
            outcome_away = 'W' if outcome_home == 'L' else 'HW'
        else:
            outcome_away = 'P'
        profit_away = calculate_profit(FIXED_ODDS, outcome_away)
        return outcome_away, profit_away


def main():
    data_dir = PROJECT_ROOT / 'data'
    patterns_dir = data_dir / 'patterns_v2'
    
    print("=" * 80)
    print("[BACKTEST] Sistema Qwen v6 - Con Logica Corregida")
    print("=" * 80)
    
    # Cargar patrones
    print("\n[1] Cargando patrones Qwen...")
    loader = PrecacheoLoaderV2(str(patterns_dir))
    qwen_patterns = [p for p in loader.ah_patterns if p.get('algorithm') == 'QWEN_ML']
    print(f"    Patrones Qwen: {len(qwen_patterns)}")
    
    # Cargar datos
    print("\n[2] Cargando datos de testing...")
    with open(data_dir / 'testing_inputs.json', 'r', encoding='utf-8') as f:
        test_matches = json.load(f)
    
    with open(data_dir / 'testing_results.json', 'r', encoding='utf-8') as f:
        results_dict = json.load(f)
    
    print(f"    Partidos: {len(test_matches)}")
    print(f"    Resultados: {len(results_dict)}")
    
    # Backtest
    print("\n[3] Ejecutando backtest...")
    print("-" * 80)
    
    all_picks = []
    total_profit = 0.0
    total_bets = 0
    wins = 0
    losses = 0
    pushes = 0
    no_result = 0
    
    for match in test_matches:
        match_id = match.get('match_id', match.get('id', ''))
        home = match.get('home_name', 'Local')
        away = match.get('away_name', 'Visitante')
        
        # Resultado real
        final_score = results_dict.get(str(match_id), results_dict.get(match_id, ''))
        
        # Features
        features = build_match_features(match)
        if not features:
            continue
        
        features = discretize_features(features)
        current_ah = features.get('current_ah')
        
        if current_ah is None:
            continue
        
        # Evaluar patrones
        picks = loader.evaluate_match(match)
        ah_picks = [p for p in picks.get('ah_picks', []) if p.get('algorithm') == 'QWEN_ML']
        
        if not ah_picks:
            continue
        
        # Mejor pick
        best_pick = max(ah_picks, key=lambda p: p.get('roi', 0))
        pick_target = best_pick.get('target', '')
        perspective = best_pick.get('perspective', '')
        expected_roi = best_pick.get('roi', 0)
        
        # Calcular resultado
        if final_score and ':' in str(final_score):
            try:
                parts = str(final_score).replace('-', ':').split(':')
                home_goals = int(parts[0])
                away_goals = int(parts[1])
                outcome, profit = settle_match_ah(home_goals, away_goals, current_ah, pick_target)
            except:
                outcome, profit = None, 0.0
        else:
            outcome, profit = None, 0.0
        
        # Estadisticas
        if outcome is None:
            result_str = 'NO_RESULT'
            no_result += 1
        elif outcome == 'P':
            result_str = 'PUSH'
            pushes += 1
        elif profit > 0:
            result_str = 'WIN'
            wins += 1
            total_profit += profit
            total_bets += 1
        else:
            result_str = 'LOSS'
            losses += 1
            total_profit += profit
            total_bets += 1
        
        # Log
        emoji = {'WIN': '[OK]', 'LOSS': '[X]', 'PUSH': '[=]', 'NO_RESULT': '[?]'}[result_str]
        print(f"{emoji} {home} vs {away}")
        print(f"    AH: {current_ah:+.2f} | Pick: {pick_target} ({perspective}) | Score: {final_score or '??'} | Profit: {profit:+.2f}")
        
        all_picks.append({
            'match': f"{home} vs {away}",
            'ah': current_ah,
            'pick': pick_target,
            'perspective': perspective,
            'expected_roi': f"{expected_roi*100:.0f}%",
            'score': final_score or '??',
            'outcome': outcome,
            'profit': round(profit, 3),
            'result': result_str
        })
    
    # Resumen
    print("\n" + "=" * 80)
    print("[RESUMEN]")
    print("=" * 80)
    
    if total_bets > 0:
        roi = total_profit / total_bets * 100
        win_rate = wins / total_bets * 100
    else:
        roi = 0
        win_rate = 0
    
    print(f"   Total picks con resultado: {total_bets}")
    print(f"   Wins: {wins}")
    print(f"   Losses: {losses}")
    print(f"   Push: {pushes}")
    print(f"   Sin resultado: {no_result}")
    print(f"\n   Win Rate: {win_rate:.1f}%")
    print(f"   ROI Real (cuota {FIXED_ODDS}): {roi:.1f}%")
    print(f"   Profit Total: {total_profit:+.2f} unidades")
    
    # Guardar
    output_file = PROJECT_ROOT / 'backtest_qwen_results.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'total_bets': total_bets,
            'wins': wins,
            'losses': losses,
            'pushes': pushes,
            'win_rate': round(win_rate, 2),
            'roi': round(roi, 2),
            'total_profit': round(total_profit, 2),
            'picks': all_picks
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\n   Guardado: {output_file}")


if __name__ == '__main__':
    main()
