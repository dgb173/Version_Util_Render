#!/usr/bin/env python
"""
Train AH Specialists Simple - Version simplificada de entrenamiento
Busca patrones directamente sin doble filtro de familia
"""
import sys
import json
import random
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.features_builder_v2 import (
    load_all_training_data, 
    build_training_dataframe
)

# Configuracion
MIN_SAMPLES = 15
MIN_ROI = 0.20
GENERATIONS = 3000

def mine_patterns_simple(df, target_col, target_values, generations=3000):
    """
    Mina patrones de forma simple sin doble filtro.
    """
    # Obtener features booleanas simples
    features = []
    for col in df.columns:
        if col in ['match_id', 'home_name', 'away_name', 'league', 'ah_covered', 
                   'ah_outcome', 'ah_profit', 'ou_outcome', 'ou_profit', 'match_date']:
            continue
        
        if df[col].dtype == 'bool':
            if df[col].sum() > 20 and df[col].sum() < len(df) - 20:
                features.append(col)
        elif df[col].dtype == 'object' and col.endswith('_bin'):
            for val in df[col].dropna().unique():
                if (df[col] == val).sum() > 20:
                    features.append(f"{col}=={val}")
    
    print(f"[INFO] Features booleanas: {len(features)}")
    
    # Split temporal
    split_idx = int(len(df) * 0.8)
    df_sorted = df.sort_values('match_date') if 'match_date' in df.columns else df
    train = df_sorted.iloc[:split_idx]
    test = df_sorted.iloc[split_idx:]
    print(f"[INFO] Split: Train={len(train)}, Test={len(test)}")
    
    patterns = []
    
    for target in target_values:
        print(f"  Buscando patrones para {target}...")
        
        for gen in range(generations):
            # Seleccionar 2-4 features aleatorias
            n_feat = random.randint(2, min(4, len(features)))
            selected = random.sample(features, n_feat)
            
            # Evaluar en train
            mask_train = eval_conditions(train, selected)
            matching_train = train[mask_train]
            
            if len(matching_train) < MIN_SAMPLES:
                continue
            
            # Filtrar por target (HOME o AWAY)
            # ah_covered es string: 'HOME', 'AWAY', 'PUSH'
            if target == 'HOME':
                target_mask = matching_train['ah_covered'] == 'HOME'
            else:  # AWAY
                target_mask = matching_train['ah_covered'] == 'AWAY'
            
            wins_train = target_mask.sum()
            acc_train = wins_train / len(matching_train)
            
            # Calcular ROI REAL usando ah_profit (payout score: -1 a +1)
            # Para HOME: profit positivo = ganamos
            # Para AWAY: profit negativo = ganamos (inverted)
            if target == 'HOME':
                total_profit_train = matching_train['ah_profit'].sum()
            else:
                total_profit_train = -matching_train['ah_profit'].sum()
            
            # ROI = profit total / stake total (stake = N partidos * 1 unidad)
            roi_train = total_profit_train / len(matching_train)
            
            if roi_train < 0.10:  # Minimo 10% ROI real en train
                continue
            
            # Evaluar en test
            mask_test = eval_conditions(test, selected)
            matching_test = test[mask_test]
            
            if len(matching_test) < MIN_SAMPLES:
                continue
            
            if target == 'HOME':
                target_mask_test = matching_test['ah_covered'] == 'HOME'
                total_profit_test = matching_test['ah_profit'].sum()
            else:
                target_mask_test = matching_test['ah_covered'] == 'AWAY'
                total_profit_test = -matching_test['ah_profit'].sum()
            
            wins_test = target_mask_test.sum()
            acc_test = wins_test / len(matching_test)
            roi_test = total_profit_test / len(matching_test)

            
            if roi_test >= MIN_ROI:
                # Verificar duplicados
                is_dup = any(set(p['conditions']) == set(selected) and p['target'] == target for p in patterns)
                if not is_dup:
                    patterns.append({
                        'target': target,
                        'conditions': sorted(selected),
                        'train': {'n': len(matching_train), 'accuracy': round(acc_train, 3), 'roi': round(roi_train, 3)},
                        'test': {'n': len(matching_test), 'accuracy': round(acc_test, 3), 'roi': round(roi_test, 3)}
                    })
                    print(f"    [OK] {target}: ROI test={roi_test*100:.1f}% (N={len(matching_test)})")
            
            if (gen + 1) % 1000 == 0:
                print(f"    Gen {gen+1}/{generations}... ({len(patterns)} patrones)")
    
    return patterns


def eval_conditions(df, conditions):
    """Evalua condiciones y retorna mascara."""
    import pandas as pd
    mask = pd.Series([True] * len(df), index=df.index)
    
    for cond in conditions:
        try:
            if '==' in cond:
                col, val = cond.split('==')
                mask = mask & (df[col] == val)
            else:
                mask = mask & df[cond].fillna(False).astype(bool)
        except:
            mask = pd.Series([False] * len(df), index=df.index)
            break
    
    return mask


def main():
    print("="*60)
    print("ENTRENAMIENTO SIMPLIFICADO DE ESPECIALISTAS AH")
    print("="*60)
    
    data_dir = str(PROJECT_ROOT / 'data')
    output_dir = PROJECT_ROOT / 'data' / 'patterns_v2'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Cargar datos
    print("\n[STEP 1] Cargando datos...")
    matches = load_all_training_data(data_dir)
    df = build_training_dataframe(matches)
    print(f"[OK] {len(df)} partidos con resultado")
    
    # 2. Familias AH
    if 'ah_family' not in df.columns:
        print("[ERROR] No hay columna ah_family")
        return 1
    
    families = df['ah_family'].value_counts()
    print(f"\nFamilias AH:")
    print(families)
    
    total_patterns = 0
    
    # 3. Entrenar por cada familia
    for family in sorted(families.index):
        if families[family] < 100:
            print(f"\n[SKIP] {family}: muy pocos datos ({families[family]})")
            continue
        
        print(f"\n{'='*50}")
        print(f"[TARGET] {family} ({families[family]} partidos)")
        print(f"{'='*50}")
        
        family_df = df[df['ah_family'] == family].copy()
        
        patterns = mine_patterns_simple(family_df, 'ah_covered', ['HOME', 'AWAY'], GENERATIONS)
        
        print(f"\n[RESULT] {len(patterns)} patrones validos")
        
        if patterns:
            # Guardar inmediatamente
            family_key = family.replace('.', '_')
            filepath = output_dir / f"specialist_ah_{family_key}.json"
            
            # Determinar una línea representativa para el archivo
            representative_line = 0
            if family_key == 'H0_5': representative_line = 0.5
            elif family_key == 'H1_0': representative_line = 1.0
            elif family_key == 'H1_25_1_75': representative_line = 1.5
            elif family_key == 'H2_0_plus': representative_line = 2.5
            elif '0.25' in family: representative_line = 0.25
            elif '0.75' in family: representative_line = 0.75
            
            output_data = {
                'id': f"specialist_ah_{family_key}",
                'market': 'AH',
                'family': family,
                'line': representative_line,
                'criteria': f"ROI>={MIN_ROI*100:.0f}%, N>={MIN_SAMPLES}",
                'patterns': patterns,
                'count': len(patterns),
                'generated': datetime.now().isoformat()
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, default=str, ensure_ascii=False)
            
            print(f"[SAVED] {filepath.name}")
            total_patterns += len(patterns)
    
    print(f"\n{'='*60}")
    print(f"[OK] TOTAL: {total_patterns} patrones AH")
    print(f"{'='*60}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
