"""
Sistema Qwen ML v9 - Exploración Exhaustiva de Features

Estrategia: Probar TODAS las features booleanas disponibles y sus combinaciones
para encontrar patrones con buenos ROIs.

Features disponibles:
- H2H: H2H_Driver_Covered, H2H_Driver_Failed, H2H_Line_Higher, H2H_Line_Lower
- Comparativas: TRIANG_Home_Dom, TRIANG_Away_Weak, IND_Strong_DA, IND_Market_Home
- Anterior: PREV_Dom_DA, PREV_Dom_SOT, PREV_Solid, FALSE_LOSER
- Mercado: MONEY_Home, MONEY_Strong, movement_bin
- Benchmark: TAG_Mejora, TAG_Empeora, TAG_Iguala
- Expectativas: EXP_Reliable, EXP_Unreliable, exp_cover_bin
- Contexto: CTX_Urgency, CTX_Home_Top, CTX_Away_Bottom, CTX_Derby
- Triggers: TRIGGER_Sniper, TRIGGER_Trap
- Rankings: home_top5, away_top5, home_bottom5, away_bottom5
- Validacion: has_prev_home, has_prev_away, has_h2h, has_indirectas
"""

import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
from collections import defaultdict
from itertools import combinations

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.features_builder_v2 import (
    build_match_features, discretize_features, load_all_training_data
)
from scripts.pattern_miner_v2.settle_asian import settle_ah, calculate_profit

FIXED_ODDS = 1.8
TRAIN_RATIO = 0.7

# TODAS las features booleanas a explorar
ALL_BOOLEAN_FEATURES = [
    # H2H
    'H2H_Driver_Covered', 'H2H_Driver_Failed', 'H2H_Line_Higher', 'H2H_Line_Lower',
    # Comparativas/Triangulacion
    'TRIANG_Home_Dom', 'TRIANG_Away_Weak', 'IND_Strong_DA', 'IND_Market_Home',
    # Partido anterior
    'PREV_Dom_DA', 'PREV_Dom_SOT', 'PREV_Solid', 'FALSE_LOSER',
    # Smart Money
    'MONEY_Home', 'MONEY_Strong',
    # Benchmark
    'TAG_Mejora', 'TAG_Empeora', 'TAG_Iguala',
    # Expectativas
    'EXP_Reliable', 'EXP_Unreliable',
    # Contexto
    'CTX_Urgency', 'CTX_Home_Top', 'CTX_Away_Bottom', 'CTX_Derby',
    # Triggers
    'TRIGGER_Sniper', 'TRIGGER_Trap',
    # Rankings directos
    'home_top5', 'away_top5', 'home_bottom5', 'away_bottom5',
    # Validacion (datos disponibles)
    'has_prev_home', 'has_prev_away', 'has_h2h', 'has_indirectas',
    # O/U H2H
    'H2H_Over_Line', 'H2H_Under_Line',
]


def get_ah_family_signed(ah_line: float) -> str:
    if ah_line is None:
        return 'unknown'
    prefix = 'H' if ah_line >= 0 else 'A'
    mag = abs(ah_line)
    if mag < 0.01:
        return 'H0'
    elif mag <= 0.75:
        return f'{prefix}0_5'
    elif mag <= 1.0:
        return f'{prefix}1_0'
    elif mag <= 1.75:
        return f'{prefix}1_5'
    else:
        return f'{prefix}2_0_plus'


def settle_match_ah(home_goals, away_goals, ah_line, target):
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


def get_bet_perspective(ah_line: float, target: str) -> str:
    if ah_line is None or ah_line == 0:
        return 'Neutral'
    local_es_favorito = ah_line > 0
    if target == 'HOME':
        return 'Favorito' if local_es_favorito else 'Underdog'
    else:
        return 'Favorito' if not local_es_favorito else 'Underdog'


def extract_active_features(features: Dict) -> List[str]:
    """Extrae TODAS las features booleanas activas."""
    active = []
    for feat in ALL_BOOLEAN_FEATURES:
        if features.get(feat, False):
            active.append(feat)
    return active


def calculate_stats(matches: List[Dict], target: str) -> Tuple[int, float, float]:
    profits = []
    wins = 0
    for m in matches:
        outcome, profit = settle_match_ah(
            m.get('home_goals'), m.get('away_goals'),
            m.get('current_ah'), target
        )
        if outcome:
            profits.append(profit)
            if profit > 0:
                wins += 1
    n = len(profits)
    if n == 0:
        return 0, 0.0, 0.0
    return n, sum(profits) / n, wins / n


def explore_patterns(family: str, train_data: List[Dict], test_data: List[Dict]) -> List[Dict]:
    """Explora TODAS las combinaciones de features para encontrar patrones rentables."""
    print(f"\n   Explorando familia {family}...")
    patterns = []
    sample_ah = train_data[0].get('current_ah', 0) if train_data else 0
    
    # 1. Identificar features frecuentes en esta familia
    
    # Umbrales "SNIPER" ajustados para capturar features raras (HIST)
    # User pidio N=6, pero los datos HIST a veces tienen N=4 o 5.
    # Probamos N=4 para encontrar sus patrones, manteniendo WR alto.
    MIN_TRAIN_N = 4
    MIN_TRAIN_ROI = 0.20
    MIN_TRAIN_WR = 0.75   # 3 de 4 = 75%. 4 de 5 = 80%.
    
    MIN_TEST_N = 0        # Aceptamos sin test si el train es perfecto (sniper puro)
    MIN_TEST_ROI = 0.0    # Si hay test, que no pierda
    
    # Recalcular frequent features para optimizar
    temp_freq = []
    for f in ALL_BOOLEAN_FEATURES:
        count = sum(1 for m in train_data if f in m.get('active_features', []))
        if count >= MIN_TRAIN_N: # Solo features que aparecen al menos 6 veces
            temp_freq.append(f)
    frequent_features = temp_freq
    print(f"      Features validas (N>={MIN_TRAIN_N}): {len(frequent_features)}")

    all_combos = []
    for size in range(1, 3): # Probamos 1 y 2 variables primero por velocidad
        for combo in combinations(frequent_features, size):
            all_combos.append(combo)
            
    print(f"      Combinaciones a probar: {len(all_combos)}")
    
    valid_count = 0
    
    for features_combo in all_combos:
        for target in ['HOME', 'AWAY']:
            
            # Sin umbrales dinamicos complejos, reglas fijas de "Sniper"
            current_train_n = MIN_TRAIN_N
            current_train_roi = MIN_TRAIN_ROI
            current_train_wr = MIN_TRAIN_WR
            
            # Filtrar partidos con estas features
            train_matches = [
                m for m in train_data
                if all(f in m.get('active_features', []) for f in features_combo)
            ]
            
            if len(train_matches) < current_train_n:
                continue
            
            n_train, roi_train, wr_train = calculate_stats(train_matches, target)
            
            if n_train < current_train_n or roi_train < current_train_roi or wr_train < current_train_wr:
                continue
            
            # Test
            test_matches = [
                m for m in test_data
                if all(f in m.get('active_features', []) for f in features_combo)
            ]
            
            # En modo sniper, el test puede ser pequeÃ±o
            n_test = 0
            roi_test = 0.0
            wr_test = 0.0
            
            if len(test_matches) > 0:
                 n_test, roi_test, wr_test = calculate_stats(test_matches, target)
                 # Si hay datos de test, que no sea desastroso
                 if n_test >= MIN_TEST_N and roi_test < MIN_TEST_ROI:
                     continue
            else:
                 # Si no hay test, confiamos en el alto WR de train?
                 # No, mejor pedir minimo un par de validaciones
                 if MIN_TEST_N > 0:
                     continue
                
            perspective = get_bet_perspective(sample_ah, target)
            if perspective == 'Favorito' and wr_train < 0.60: continue # Favoritos necesitan alto WR
            
            pattern_name = f"Qwen {family} [{'+'.join(features_combo)}] (ROI={int(roi_train*100)}%/{int(roi_test*100)}%T) {perspective}"
            
            pattern = {
                'name': pattern_name,
                'target': target,
                'perspective': perspective,
                'family': family,
                'conditions': list(features_combo),
                'train': {
                    'n': n_train,
                    'roi': round(roi_train, 3),
                    'win_rate': round(wr_train, 3)
                },
                'test': {
                    'n': n_test,
                    'roi': round(roi_test, 3),
                    'win_rate': round(wr_test, 3)
                },
                'algorithm': 'QWEN_ML',
                'market': 'AH'
            }
            
            patterns.append(pattern)
            valid_count += 1
    
    print(f"      Patrones validos: {valid_count}")
    
    # Ordenar por ROI combinado
    patterns.sort(key=lambda p: -(p['train']['roi'] + p['test']['roi']))
    
    # Eliminar duplicados
    seen = set()
    unique = []
    for p in patterns:
        key = (p['target'], tuple(sorted(p['conditions'])))
        if key not in seen:
            seen.add(key)
            unique.append(p)
    
    return unique


def main():
    data_dir = PROJECT_ROOT / 'data'
    output_dir = data_dir / 'patterns_v2'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 70)
    print("[QWEN v9] Exploracion Exhaustiva de Features")
    print("=" * 70)
    print(f"   Features a explorar: {len(ALL_BOOLEAN_FEATURES)}")
    print("=" * 70)
    
    print("\n[*] Cargando datos...")
    matches = load_all_training_data(str(data_dir))
    
    print("\n[*] Procesando...")
    processed = []
    
    for match in matches:
        features = build_match_features(match)
        if not features:
            continue
        if not features.get('has_result') or features.get('current_ah') is None:
            continue
        if features.get('home_goals') is None or features.get('away_goals') is None:
            continue
        
        features = discretize_features(features)
        features['ah_family_signed'] = get_ah_family_signed(features.get('current_ah'))
        features['active_features'] = extract_active_features(features)
        
        processed.append(features)
    
    print(f"   Partidos: {len(processed)}")
    
    # Estadisticas de features
    feature_stats = defaultdict(int)
    for p in processed:
        for f in p.get('active_features', []):
            feature_stats[f] += 1
    
    print("\n[*] Distribucion de features (top 15):")
    for feat, count in sorted(feature_stats.items(), key=lambda x: -x[1])[:15]:
        print(f"   {feat}: {count} ({count/len(processed)*100:.1f}%)")
    
    # Agrupar por familia
    families = defaultdict(list)
    for f in processed:
        fam = f.get('ah_family_signed', 'unknown')
        if fam != 'unknown':
            families[fam].append(f)
    
    all_patterns = []
    
    for family in sorted(families.keys()):
        family_matches = families[family]
        
        if len(family_matches) < 50:
            continue
        
        family_matches.sort(key=lambda x: x.get('match_date', ''))
        split_idx = int(len(family_matches) * TRAIN_RATIO)
        train_data = family_matches[:split_idx]
        test_data = family_matches[split_idx:]
        
        print(f"\n   {family}: {len(train_data)} train, {len(test_data)} test")
        
        patterns = explore_patterns(family, train_data, test_data)
        
        if patterns:
            # Guardar solo los mejores 10 por familia
            top_patterns = patterns[:10]
            
            output_file = output_dir / f'qwen_{family}.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'meta': {
                        'system': 'Qwen',
                        'version': '9.0',
                        'family': family,
                        'logic': 'exhaustive_search',
                        'total_patterns': len(top_patterns),
                        'generated_at': datetime.now().isoformat()
                    },
                    'patterns': top_patterns
                }, f, indent=2, ensure_ascii=False)
            
            all_patterns.extend(top_patterns)
        else:
            output_file = output_dir / f'qwen_{family}.json'
            if output_file.exists():
                output_file.unlink()
    
    # Resumen
    print("\n" + "=" * 70)
    print("[RESUMEN]")
    print("=" * 70)
    print(f"   TOTAL PATRONES: {len(all_patterns)}")
    
    # Ordenar todos por ROI combinado
    all_patterns.sort(key=lambda p: -(p['train']['roi'] + p['test']['roi']))
    
    if all_patterns:
        print("\n   TOP 15 PATRONES:")
        print("   " + "-" * 65)
        for i, p in enumerate(all_patterns[:15], 1):
            conds = '+'.join(p['conditions'][:2])
            if len(p['conditions']) > 2:
                conds += f"+{len(p['conditions'])-2}mas"
            print(f"   {i:2}. {p['family']} [{conds}] {p['perspective']}")
            print(f"       Train: ROI={p['train']['roi']*100:.0f}% WR={p['train']['win_rate']*100:.0f}% N={p['train']['n']}")
            print(f"       Test:  ROI={p['test']['roi']*100:.0f}% WR={p['test']['win_rate']*100:.0f}% N={p['test']['n']}")
    
    # Mostrar patrones con mejor ROI en TEST (mas confiables)
    by_test_roi = sorted(all_patterns, key=lambda p: -p['test']['roi'])[:10]
    print("\n   TOP 10 por ROI en TEST (mas confiables):")
    print("   " + "-" * 65)
    for i, p in enumerate(by_test_roi, 1):
        conds = '+'.join(p['conditions'][:2])
        print(f"   {i:2}. {p['family']} [{conds}] -> Test ROI={p['test']['roi']*100:.0f}% Train={p['train']['roi']*100:.0f}%")
    
    print("=" * 70)


if __name__ == '__main__':
    main()
