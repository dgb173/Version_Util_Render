#!/usr/bin/env python
"""
============================================================
ENTRENADOR COMPLETO DE PATRONES AH
============================================================
Entrena TODOS los patrones AH:
- Positivos (Local Favorito): H0.5, H1.0, H1.5, H2.0+
- Negativos (Visitante Favorito): A0.5, A1.0, A1.5, A2.0+

Para CADA familia entrena AMBOS lados:
- Favorito cubre
- Underdog cubre (X2)
============================================================
"""
import sys
import json
import random
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.features_builder_v2 import load_all_training_data, build_training_dataframe
from scripts.pattern_miner_v2.gates import safe_float

# ============ CONFIGURACIÓN ============
# ============ CONFIGURACIÓN ESTRICTA (USER REQUEST) ============
MIN_SAMPLES = 20
MIN_ROI = 0.25  # ROI ALTO requerido
GENERATIONS = 120000
MIN_CONDITIONS = 3
MAX_CONDITIONS = 8  # Más condiciones para combinaciones complejas

# Familias POSITIVAS (Local Favorito)
HOME_FAV_FAMILIES = {
    'H0_5': {'min': 0.25, 'max': 0.75},
    'H1_0': {'min': 0.75, 'max': 1.25},
    'H1_5': {'min': 1.25, 'max': 1.75},
    'H2_0_plus': {'min': 1.75, 'max': 10},
}

# Familias NEGATIVAS (Visitante Favorito)  
AWAY_FAV_FAMILIES = {
    'A0_5': {'min': -0.75, 'max': -0.25},
    'A1_0': {'min': -1.25, 'max': -0.75},
    'A1_5': {'min': -1.75, 'max': -1.25},
    'A2_0_plus': {'min': -10, 'max': -1.75},
}

def safe_col(df, col, default=False):
    if col in df.columns:
        return df[col].fillna(default)
    return pd.Series(default, index=df.index)

def build_home_features(df):
    """Features para apostar por LOCAL."""
    bool_df = pd.DataFrame(index=df.index)
    
    # --- FACTOR CLAVE: H2H COL 3 (Histórico Directo) ---
    bool_df['H2H_Covered'] = safe_col(df, 'H2H_Driver_Covered', False)
    bool_df['H2H_Failed'] = safe_col(df, 'H2H_Driver_Failed', False)
    bool_df['H2H_Line_Higher'] = safe_col(df, 'H2H_Line_Higher', False)
    # Patrón oro: Se cubrió antes y ahora la línea es similar o mejor
    bool_df['H2H_Gold_Pattern'] = bool_df['H2H_Covered'] & (~bool_df['H2H_Line_Higher'])
    
    # Triangulación
    bridge_da = safe_col(df, 'bridge_D_gap_DA_mean', 0)
    bridge_m = safe_col(df, 'bridge_M_gap_mean', 0)
    bool_df['TRIANG_Home_Dom'] = bridge_da > 5
    bool_df['TRIANG_Away_Weak'] = bridge_da > 10
    bool_df['IND_Strong_DA'] = bridge_da > 3
    bool_df['IND_Market_Home'] = bridge_m < -0.1
    
    # Stats previas Home
    prev_dda = safe_col(df, 'prev_home_dDA', 0)
    prev_dsot = safe_col(df, 'prev_home_dSOT', 0)
    bool_df['PREV_Dom_DA'] = prev_dda > 15
    bool_df['PREV_Dom_SOT'] = prev_dsot > 3
    bool_df['PREV_Solid'] = prev_dda > 0
    
    # Falso Perdedor Home
    lost_prev = safe_col(df, 'prev_home_exp_failed', False)
    bool_df['FALSE_LOSER'] = lost_prev & (prev_dda > 10)
    
    # Smart Money
    mov_dir = safe_col(df, 'movement_dir', 'NONE')
    bool_df['MONEY_Home'] = mov_dir == 'DOWN'
    bool_df['MONEY_Strong'] = (mov_dir == 'DOWN') & (safe_col(df, 'ah_delta', 0).abs() >= 0.25)
    
    # Benchmark
    current_ah = safe_col(df, 'current_ah', 0)
    prev_line = safe_col(df, 'prev_home_exp_line', current_ah)
    ah_gap = current_ah - prev_line
    bool_df['TAG_Mejora'] = ah_gap > 0.1
    bool_df['TAG_Empeora'] = ah_gap < -0.1
    
    # Expectativas Historicas
    exp_rate = safe_col(df, 'exp_cover_own_rate', 0.5)
    bool_df['EXP_Reliable'] = exp_rate > 0.6
    bool_df['EXP_High_Reliability'] = exp_rate > 0.75 # Muy fiable históricamente
    
    # Contexto
    bool_df['CTX_Home_Top'] = safe_col(df, 'home_top5', False)
    bool_df['CTX_Away_Bottom'] = safe_col(df, 'away_bottom5', False)
    bool_df['CTX_Derby'] = safe_col(df, 'rank_diff', 0).abs() < 3
    
    # Triggers
    bool_df['TRIGGER_Sniper'] = bool_df['FALSE_LOSER'] & bool_df['MONEY_Home']
    
    for col in bool_df.columns:
        bool_df[col] = bool_df[col].astype(bool)
    return bool_df

def build_away_features(df):
    """Features para apostar por VISITANTE."""
    bool_df = pd.DataFrame(index=df.index)
    
    # --- FACTOR CLAVE: H2H COL 3 (Histórico Directo) ---
    # Para Away, 'Covered' significa que el LOCAL cubrió (normalmente), así que Away necesita 'Failed'
    # O dependerá de cómo definimos H2H_Driver. Asumamos H2H_Last_Covered se refiere a si el FAVORITO cubrió.
    # Mejor usar la feature raw H2H_Last_Covered (True/False) y combinar.
    
    bool_df['H2H_Covered'] = safe_col(df, 'H2H_Driver_Covered', False) # Favorito cubrió
    bool_df['H2H_Failed'] = safe_col(df, 'H2H_Driver_Failed', False)   # Favorito falló
    
    # Feature compleja: Si Away es favorito (AH negativo), queremos que haya cubierto antes.
    # Si Away es underdog (AH positivo), queremos que haya cubierto (o Home fallado).
    
    bool_df['H2H_Line_Higher'] = safe_col(df, 'H2H_Line_Higher', False)
    
    # Triangulación (invertida)
    bridge_da = safe_col(df, 'bridge_D_gap_DA_mean', 0)
    bridge_m = safe_col(df, 'bridge_M_gap_mean', 0)
    bool_df['TRIANG_Away_Dom'] = bridge_da < -5
    bool_df['TRIANG_Home_Weak'] = bridge_da < -10
    bool_df['IND_Strong_DA'] = bridge_da < -3
    bool_df['IND_Market_Away'] = bridge_m > 0.1
    
    # Stats previas Away
    prev_dda = safe_col(df, 'prev_away_dDA', 0)
    prev_dsot = safe_col(df, 'prev_away_dSOT', 0)
    bool_df['PREV_Dom_DA'] = prev_dda > 15
    bool_df['PREV_Dom_SOT'] = prev_dsot > 3
    bool_df['PREV_Solid'] = prev_dda > 0
    
    # Falso Perdedor Away
    lost_prev = safe_col(df, 'prev_away_exp_failed', False)
    bool_df['FALSE_LOSER'] = lost_prev & (prev_dda > 10)
    
    # Smart Money
    mov_dir = safe_col(df, 'movement_dir', 'NONE')
    bool_df['MONEY_Away'] = mov_dir == 'UP'
    bool_df['MONEY_Strong'] = (mov_dir == 'UP') & (safe_col(df, 'ah_delta', 0).abs() >= 0.25)
    
    # Benchmark
    current_ah = safe_col(df, 'current_ah', 0)
    prev_line = safe_col(df, 'prev_away_exp_line', current_ah)
    ah_gap = current_ah - prev_line
    bool_df['TAG_Mejora'] = ah_gap < -0.1
    bool_df['TAG_Empeora'] = ah_gap > 0.1
    
    # Expectativas Historicas
    exp_rate = safe_col(df, 'exp_cover_away_rate', 0.5)
    bool_df['EXP_Reliable'] = exp_rate > 0.6
    bool_df['EXP_High_Reliability'] = exp_rate > 0.75
    
    # Contexto
    bool_df['CTX_Away_Top'] = safe_col(df, 'away_top5', False)
    bool_df['CTX_Home_Bottom'] = safe_col(df, 'home_bottom5', False)
    bool_df['CTX_Derby'] = safe_col(df, 'rank_diff', 0).abs() < 3
    
    # Triggers
    bool_df['TRIGGER_Sniper'] = bool_df['FALSE_LOSER'] & bool_df['MONEY_Away']
    
    for col in bool_df.columns:
        bool_df[col] = bool_df[col].astype(bool)
    return bool_df

def train_patterns(df, feat_df, family_name, target, invert_profit=False):
    """Entrena patrones para una familia y target específico."""
    feature_names = list(feat_df.columns)
    
    n_split = int(len(df) * 0.8)
    train_idx = df.index[:n_split]
    test_idx = df.index[n_split:]
    
    feat_train = feat_df.loc[train_idx]
    feat_test = feat_df.loc[test_idx]
    df_train = df.loc[train_idx]
    df_test = df.loc[test_idx]
    
    found_patterns = []
    multiplier = -1 if invert_profit else 1
    
    for gen in range(GENERATIONS):
        n_conds = random.randint(MIN_CONDITIONS, MAX_CONDITIONS)
        conds = random.sample(feature_names, min(n_conds, len(feature_names)))
        
        mask_train = np.ones(len(feat_train), dtype=bool)
        for c in conds:
            mask_train &= feat_train[c].values
        
        n_train = mask_train.sum()
        if n_train < MIN_SAMPLES:
            continue
        
        profit_train = multiplier * df_train.loc[feat_train[mask_train].index, 'ah_profit'].sum()
        roi_train = profit_train / n_train
        
        if roi_train < 0.10:
            continue
        
        mask_test = np.ones(len(feat_test), dtype=bool)
        for c in conds:
            mask_test &= feat_test[c].values
        
        n_test = mask_test.sum()
        if n_test < MIN_SAMPLES:
            continue
        
        profit_test = multiplier * df_test.loc[feat_test[mask_test].index, 'ah_profit'].sum()
        roi_test = profit_test / n_test
        
        if roi_test >= MIN_ROI:
            conds_set = frozenset(conds)
            if any(frozenset(p['conditions']) == conds_set for p in found_patterns):
                continue
            
            pattern = {
                'name': f"{family_name}-{target}-{len(found_patterns)+1:03d}",
                'target': target,
                'family': family_name,
                'conditions': sorted(conds),
                'train': {'n': int(n_train), 'roi': round(roi_train, 3)},
                'test': {'n': int(n_test), 'roi': round(roi_test, 3)}
            }
            found_patterns.append(pattern)
            
        if gen % 20000 == 0 and gen > 0:
            print(f"      {gen//1000}k... [{len(found_patterns)}]")
    
    return found_patterns

def main():
    print("="*70)
    print("🎯 ENTRENADOR COMPLETO AH")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Cargar datos
    print("\n📂 Cargando datos...")
    data_dir = str(PROJECT_ROOT / 'data')
    matches = load_all_training_data(data_dir)
    df = build_training_dataframe(matches)
    print(f"   Total: {len(df)} partidos")
    
    output_dir = PROJECT_ROOT / 'data' / 'patterns_v2'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    all_patterns = {}
    
    # ============ LOCAL FAVORITO (AH Positivo) ============
    print("\n" + "="*70)
    print("📊 LOCAL FAVORITO (AH Positivo)")
    print("="*70)
    
    for family_name, config in HOME_FAV_FAMILIES.items():
        print(f"\n[{family_name}] AH {config['min']}-{config['max']}")
        
        ah_col = safe_col(df, 'current_ah', 0)
        mask = (ah_col >= config['min']) & (ah_col <= config['max'])
        df_target = df[mask].copy()
        
        if len(df_target) < 100:
            print(f"   ⚠️ Solo {len(df_target)} partidos")
            continue
        
        print(f"   {len(df_target)} partidos")
        
        # Entrenar FAVORITO (HOME)
        print(f"   [HOME - Favorito]")
        feat_home = build_home_features(df_target)
        patterns_home = train_patterns(df_target, feat_home, family_name, 'HOME', invert_profit=False)
        
        # Entrenar UNDERDOG (AWAY)
        print(f"   [AWAY - Underdog]")
        feat_away = build_away_features(df_target)
        patterns_away = train_patterns(df_target, feat_away, family_name, 'AWAY', invert_profit=True)
        
        all_patterns[family_name] = patterns_home + patterns_away
        
        if all_patterns[family_name]:
            all_patterns[family_name].sort(key=lambda x: x['test']['roi'], reverse=True)
            out_file = output_dir / f'specialist_ah_{family_name}.json'
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'meta': {'family': family_name, 'generated_at': datetime.now().isoformat()},
                    'patterns': all_patterns[family_name]
                }, f, indent=2)
            h_count = len(patterns_home)
            a_count = len(patterns_away)
            print(f"   ✅ {h_count} HOME + {a_count} AWAY = {h_count+a_count} patrones")
    
    # ============ VISITANTE FAVORITO (AH Negativo) ============
    print("\n" + "="*70)
    print("📊 VISITANTE FAVORITO (AH Negativo)")
    print("="*70)
    
    for family_name, config in AWAY_FAV_FAMILIES.items():
        print(f"\n[{family_name}] AH {config['min']} a {config['max']}")
        
        ah_col = safe_col(df, 'current_ah', 0)
        mask = (ah_col >= config['min']) & (ah_col <= config['max'])
        df_target = df[mask].copy()
        
        if len(df_target) < 100:
            print(f"   ⚠️ Solo {len(df_target)} partidos")
            continue
        
        print(f"   {len(df_target)} partidos")
        
        # Para AH negativo (Visitante Favorito):
        # ah_profit > 0 = HOME ganó (HOME es underdog aquí)
        # ah_profit < 0 = AWAY ganó (AWAY es favorito aquí)
        
        # Entrenar FAVORITO (AWAY) - ah_profit<0 cuando AWAY gana, INVERTIR para ROI positivo
        print(f"   [AWAY - Favorito]")
        feat_away = build_away_features(df_target)
        patterns_away = train_patterns(df_target, feat_away, family_name, 'AWAY', invert_profit=True)
        
        # Entrenar UNDERDOG (HOME) - ah_profit>0 cuando HOME gana, NO invertir
        print(f"   [HOME - Underdog]")
        feat_home = build_home_features(df_target)
        patterns_home = train_patterns(df_target, feat_home, family_name, 'HOME', invert_profit=False)
        
        all_patterns[family_name] = patterns_away + patterns_home
        
        if all_patterns[family_name]:
            all_patterns[family_name].sort(key=lambda x: x['test']['roi'], reverse=True)
            out_file = output_dir / f'specialist_ah_{family_name}.json'
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'meta': {'family': family_name, 'generated_at': datetime.now().isoformat()},
                    'patterns': all_patterns[family_name]
                }, f, indent=2)
            a_count = len(patterns_away)
            h_count = len(patterns_home)
            print(f"   ✅ {a_count} AWAY + {h_count} HOME = {a_count+h_count} patrones")
    
    print("\n" + "="*70)
    print("✅ ENTRENAMIENTO COMPLETO")
    total = sum(len(p) for p in all_patterns.values())
    print(f"   Total patrones: {total}")
    print("="*70)

if __name__ == "__main__":
    main()
