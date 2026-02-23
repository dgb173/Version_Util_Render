#!/usr/bin/env python
"""
Entrenador Especialista H0.5 (Local Favorito)
Version: Ultra-Precisa (Strict ROI & Features Específicas)

Features de Foco:
- Dominio Real: prev_home_dDA, prev_home_dSOT
- Falso Perdedor: prev_home_exp_failed + Dominio
- Forma Engañosa Rival: bridge_D_gap_DA_mean
- Dinero Inteligente: movement_dir
"""
import sys
import json
import random
import numpy as np
import pandas as pd
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.features_builder_v2 import load_all_training_data, build_training_dataframe
from scripts.pattern_miner_v2.settle_asian import get_ah_family

# Configuración Estricta
MIN_SAMPLES = 15
MIN_ROI = 0.20
GENERATIONS = 50000

# Features de Interés (White-box approach)
CORE_FEATURES = [
    # Dominio
    'prev_home_dDA', 'prev_home_dSOT', 'dom_avg_dDA', 
    # Expectativa / Momentum
    'prev_home_exp_failed', 'prev_home_beat_expectation',
    'exp_cover_own_rate',
    # Dinero
    'movement_dir', 'ah_delta',
    # Puentes (Indirectas)
    'bridge_D_gap_DA_mean', 'bridge_D_gap_SOT_mean',
    'bridge_contradiction_rate',
    # Contexto
    'home_bottom5', 'away_mid_table', 'goal_diff_last5'
]

def train_h05_specialist():
    print("="*60)
    print("ENTRENAMIENTO ESPECIALISTA H0.5 (LOCAL FAVORITO)")
    print(f"Criterios: ROI >={MIN_ROI*100}%, N >={MIN_SAMPLES} (Cálculo Estricto)")
    print("="*60)
    
    # 1. Cargar Datos
    print("\n[1] Cargando datos...")
    data_dir = str(PROJECT_ROOT / 'data')
    matches = load_all_training_data(data_dir)
    df = build_training_dataframe(matches)
    print(f"    Total partidos: {len(df)}")
    
    # 2. Filtrar para H0.5 Local Favorito
    # H0.5 Family incluye 0.5, 0.25, 0.75
    # Local Favorito significa AH < 0 (ej: -0.5, -0.25, -0.75)
    
    # Crear columna de familia si no existe
    if 'ah_family' not in df.columns:
        df['ah_family'] = df['current_ah'].apply(lambda x: get_ah_family(x) if pd.notnull(x) else None)
    
    # Filtro maestro
    mask_h05 = (df['ah_family'] == 'H0.5') & (df['current_ah'] < 0)
    df_target = df[mask_h05].copy()
    
    # Calcular 'ah_line_gap' (diferencia con línea esperada previa)
    # Si no existe last_line, asumimos 0 diff
    df_target['ah_line_gap'] = df_target.apply(
        lambda row: abs(row['current_ah'] - (row.get('prev_home_exp_line') or row['current_ah'])), 
        axis=1
    )
    
    print(f"    Partidos H0.5 Local Favorito: {len(df_target)}")
    
    if len(df_target) < 100:
        print("[ERROR] Pocos datos para entrenar.")
        return

    # 3. Preparar Features
    # Convertir numéricas a booleanas/bins para el algoritmo genético
    print("\n[2] Preparando features...")
    bool_features = {}
    
    # Binarización inteligente
    # -- Dominio
    bool_features['dom_high'] = df_target['prev_home_dDA'] > 15
    bool_features['dom_positive'] = df_target['prev_home_dDA'] > 0
    bool_features['sot_dom_high'] = df_target['prev_home_dSOT'] > 3
    
    # -- Falso Perdedor
    bool_features['false_loser'] = (df_target['prev_home_exp_failed'] == True) & (df_target['prev_home_dDA'] > 10)
    
    # -- Handicap Repetido
    bool_features['line_repeated'] = df_target['ah_line_gap'] < 0.25
    
    # -- Puentes
    # Si el gap de dominio indirecto favorece al local (> 5)
    bool_features['indirect_dom_home'] = df_target['bridge_D_gap_DA_mean'] > 5
    
    # -- Dinero
    bool_features['money_with_home'] = df_target['movement_dir'] == 'DOWN'  # Bajó cuota/línea (favorece local)
    bool_features['money_against_away'] = df_target['movement_dir'] == 'UP'
    
    # -- Expectativa
    bool_features['strong_cover_rate'] = df_target['exp_cover_own_rate'] > 0.6
    
    # Agregar las numéricas discretizadas
    for col in CORE_FEATURES:
        if col in df_target.columns:
            if df_target[col].dtype == 'bool':
                bool_features[col] = df_target[col]
            elif df_target[col].dtype in ['float64', 'int64']:
                # Crear cuartiles o bins simples
                med = df_target[col].median()
                bool_features[f"{col}>med"] = df_target[col] > med
                bool_features[f"{col}<med"] = df_target[col] < med

    # Crear DataFrame de features booleanas
    df_bool = pd.DataFrame(bool_features, index=df_target.index)
    feature_names = list(df_bool.columns)
    print(f"    Features generadas: {len(feature_names)}")
    
    # Split Train/Test
    split_idx = int(len(df_target) * 0.8)
    train_idx = df_target.index[:split_idx]
    test_idx = df_target.index[split_idx:]
    
    df_train = df_target.loc[train_idx]
    df_test = df_target.loc[test_idx]
    feat_train = df_bool.loc[train_idx]
    feat_test = df_bool.loc[test_idx]
    
    # 4. Entrenamiento Genético
    print("\n[3] Buscando patrones...")
    patterns = []
    
    for gen in range(GENERATIONS):
        # Seleccionar 2-4 condiciones
        n_conds = random.randint(2, 4)
        conds = random.sample(feature_names, n_conds)
        
        # Evaluar en Train
        mask = np.ones(len(df_train), dtype=bool)
        for c in conds:
            mask = mask & feat_train[c].values
            
        n_matches = mask.sum()
        if n_matches < MIN_SAMPLES:
            continue
            
        # Calcular ROI Estricto
        # ah_profit: W=+1(odds-1), HW=..., P=0, L=-1
        # OJO: ah_profit ya calculado en features_builder considera half wins
        # Si apostamos HOME, necesitamos profit positivo
        
        subset = df_train[mask]
        profit_train = subset['ah_profit'].sum() # Asumiendo bet HOME
        roi_train = profit_train / n_matches
        
        if roi_train < 0.15: # 15% minimo en train
            continue
            
        # Evaluar en Test
        mask_test = np.ones(len(df_test), dtype=bool)
        for c in conds:
            mask_test = mask_test & feat_test[c].values
            
        n_test = mask_test.sum()
        if n_test < MIN_SAMPLES: # Tambien minimo en test
            continue
            
        subset_test = df_test[mask_test]
        profit_test = subset_test['ah_profit'].sum()
        roi_test = profit_test / n_test
        
        if roi_test >= MIN_ROI:
            # Validar duplicados
            if not any(set(p['conditions']) == set(conds) for p in patterns):
                patterns.append({
                    'name': f"H0.5 Specialist {len(patterns)+1}",
                    'target': 'HOME',
                    'conditions': sorted(conds),
                    'train': {'n': int(n_matches), 'roi': round(roi_train, 3)},
                    'test': {'n': int(n_test), 'roi': round(roi_test, 3)}
                })
                print(f"  [FOUND] ROI Test: {roi_test*100:.1f}% (N={n_test}) | {conds}")
    
    # 5. Guardar
    if patterns:
        patterns.sort(key=lambda x: x['test']['roi'], reverse=True)
        output_file = PROJECT_ROOT / 'data' / 'patterns_v2' / 'specialist_ah_H0_5_improved.json'
        
        output_data = {
            'generated_at': str(pd.Timestamp.now()),
            'count': len(patterns),
            'market': 'AH',
            'family': 'H0.5',
            'patterns': patterns
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)
            
        print(f"\n[SUCCESS] Guardados {len(patterns)} patrones en {output_file.name}")
    else:
        print("\n[FAIL] No se encontraron patrones que cumplan criterios.")

if __name__ == "__main__":
    train_h05_specialist()
