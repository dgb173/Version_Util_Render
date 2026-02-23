#!/usr/bin/env python
"""
============================================================
ENTRENADOR UNIVERSAL DE PATRONES
============================================================
Entrena todos los patrones AH y O/U con la lógica corregida.
Families:
- AH: H0.5, H1.0, H1.5, H2.0+
- OU: 2.0, 2.25, 2.5, 2.75, 3.0+
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
MIN_SAMPLES = 10      # Reducido de 15
MIN_ROI = 0.15        # Reducido de 0.20
GENERATIONS = 100000  # Aumentado de 50000
MIN_CONDITIONS = 3    # Reducido de 4
MAX_CONDITIONS = 6

# Familias a entrenar
AH_FAMILIES = {
    'H0_5': {'min': 0.25, 'max': 0.75, 'target': 'HOME'},
    'H1_0': {'min': 0.75, 'max': 1.25, 'target': 'HOME'},
    'H1_5': {'min': 1.25, 'max': 1.75, 'target': 'HOME'},
    'H2_0_plus': {'min': 1.75, 'max': 10, 'target': 'HOME'},
}

OU_FAMILIES = {
    '2_0': {'min': 1.75, 'max': 2.25},
    '2_25': {'min': 2.0, 'max': 2.5},
    '2_5': {'min': 2.25, 'max': 2.75},
    '2_75': {'min': 2.5, 'max': 3.0},
    '3_0_plus': {'min': 2.75, 'max': 10},
}

def safe_col(df, col, default=False):
    if col in df.columns:
        return df[col].fillna(default)
    return pd.Series(default, index=df.index)

def build_ah_features(df):
    """Features booleanas para AH."""
    bool_df = pd.DataFrame(index=df.index)
    
    # Triangulación
    bridge_da = safe_col(df, 'bridge_D_gap_DA_mean', 0)
    bridge_m = safe_col(df, 'bridge_M_gap_mean', 0)
    bool_df['TRIANG_Home_Dom'] = bridge_da > 5
    bool_df['TRIANG_Away_Weak'] = bridge_da > 10
    bool_df['IND_Strong_DA'] = bridge_da > 3
    bool_df['IND_Market_Home'] = bridge_m < -0.1
    
    # Stats previas
    prev_dda = safe_col(df, 'prev_home_dDA', 0)
    prev_dsot = safe_col(df, 'prev_home_dSOT', 0)
    bool_df['PREV_Dom_DA'] = prev_dda > 15
    bool_df['PREV_Dom_SOT'] = prev_dsot > 3
    bool_df['PREV_Solid'] = prev_dda > 0
    
    # Falso Perdedor
    lost_prev = safe_col(df, 'ah_covered', 'NONE') == 'AWAY'
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
    bool_df['TAG_Iguala'] = ah_gap.abs() <= 0.1
    
    # Expectativas
    exp_rate = safe_col(df, 'exp_cover_own_rate', 0.5)
    bool_df['EXP_Reliable'] = exp_rate > 0.6
    bool_df['EXP_Unreliable'] = exp_rate < 0.4
    
    # Contexto
    bool_df['CTX_Urgency'] = safe_col(df, 'home_bottom5', False)
    bool_df['CTX_Home_Top'] = safe_col(df, 'home_top5', False)
    bool_df['CTX_Away_Bottom'] = safe_col(df, 'away_bottom5', False)
    bool_df['CTX_Derby'] = safe_col(df, 'rank_diff', 0).abs() < 3
    
    # Triggers
    bool_df['TRIGGER_Sniper'] = bool_df['FALSE_LOSER'] & bool_df['MONEY_Home']
    bool_df['TRIGGER_Trap'] = bool_df['TAG_Empeora'] & bool_df['PREV_Dom_DA']
    
    for col in bool_df.columns:
        bool_df[col] = bool_df[col].astype(bool)
    
    return bool_df

def build_ou_features(df):
    """Features booleanas para O/U."""
    bool_df = pd.DataFrame(index=df.index)
    
    # Tendencia goles equipos
    bool_df['HOME_High_Scoring'] = safe_col(df, 'home_avg_goals', 1.5) > 1.5
    bool_df['AWAY_High_Scoring'] = safe_col(df, 'away_avg_goals', 1.5) > 1.5
    bool_df['HOME_Defensive'] = safe_col(df, 'home_avg_conceded', 1.5) < 1.0
    bool_df['AWAY_Defensive'] = safe_col(df, 'away_avg_conceded', 1.5) < 1.0
    
    # Stats previas (ritmo de juego)
    prev_da_total = safe_col(df, 'prev_home_dDA', 0).abs() + safe_col(df, 'prev_away_dDA', 0).abs()
    bool_df['HIGH_TEMPO'] = prev_da_total > 30
    bool_df['LOW_TEMPO'] = prev_da_total < 15
    
    # Dominancia
    bridge_da = safe_col(df, 'bridge_D_gap_DA_mean', 0)
    bool_df['CLASH_DOMINANT'] = bridge_da.abs() > 10  # Un equipo muy dominante
    bool_df['EVEN_MATCH'] = bridge_da.abs() < 3       # Partido parejo
    
    # Expectativas
    exp_rate = safe_col(df, 'exp_cover_own_rate', 0.5)
    bool_df['HIGH_EXP'] = exp_rate > 0.6  # Equipos que cumplen
    
    # Contexto
    bool_df['TOP_MATCH'] = safe_col(df, 'home_top5', False) & safe_col(df, 'away_top5', False)
    bool_df['BOTTOM_MATCH'] = safe_col(df, 'home_bottom5', False) & safe_col(df, 'away_bottom5', False)
    
    # Mercado
    bool_df['LINE_UP'] = safe_col(df, 'movement_dir', 'NONE') == 'UP'
    bool_df['LINE_DOWN'] = safe_col(df, 'movement_dir', 'NONE') == 'DOWN'
    
    for col in bool_df.columns:
        bool_df[col] = bool_df[col].astype(bool)
    
    return bool_df

def train_family(df, feat_df, family_name, target_col, market='AH'):
    """Entrena una familia específica."""
    feature_names = list(feat_df.columns)
    
    n_split = int(len(df) * 0.8)
    train_idx = df.index[:n_split]
    test_idx = df.index[n_split:]
    
    feat_train = feat_df.loc[train_idx]
    feat_test = feat_df.loc[test_idx]
    df_train = df.loc[train_idx]
    df_test = df.loc[test_idx]
    
    found_patterns = []
    
    for gen in range(GENERATIONS):
        n_conds = random.randint(MIN_CONDITIONS, MAX_CONDITIONS)
        conds = random.sample(feature_names, min(n_conds, len(feature_names)))
        
        mask_train = np.ones(len(feat_train), dtype=bool)
        for c in conds:
            mask_train &= feat_train[c].values
        
        n_train = mask_train.sum()
        if n_train < MIN_SAMPLES:
            continue
        
        profit_train = df_train.loc[feat_train[mask_train].index, target_col].sum()
        roi_train = profit_train / n_train
        
        if roi_train < 0.10:
            continue
        
        mask_test = np.ones(len(feat_test), dtype=bool)
        for c in conds:
            mask_test &= feat_test[c].values
        
        n_test = mask_test.sum()
        if n_test < MIN_SAMPLES:
            continue
        
        profit_test = df_test.loc[feat_test[mask_test].index, target_col].sum()
        roi_test = profit_test / n_test
        
        if roi_test >= MIN_ROI:
            conds_set = frozenset(conds)
            if any(frozenset(p['conditions']) == conds_set for p in found_patterns):
                continue
            
            pattern = {
                'name': f"{market}-{family_name}-{len(found_patterns)+1:03d}",
                'target': 'HOME' if market == 'AH' else 'OVER',
                'family': family_name,
                'conditions': sorted(conds),
                'train': {'n': int(n_train), 'roi': round(roi_train, 3)},
                'test': {'n': int(n_test), 'roi': round(roi_test, 3)}
            }
            found_patterns.append(pattern)
            
        if gen % 10000 == 0 and gen > 0:
            print(f"      {gen//1000}k... [{len(found_patterns)} encontrados]")
    
    return found_patterns

def main():
    print("="*70)
    print("🎯 ENTRENADOR UNIVERSAL DE PATRONES")
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
    
    # ============ ENTRENAR AH ============
    print("\n" + "="*70)
    print("📊 ENTRENANDO PATRONES AH")
    print("="*70)
    
    for family_name, config in AH_FAMILIES.items():
        print(f"\n[{family_name}] Filtrando AH {config['min']}-{config['max']}...")
        
        ah_col = safe_col(df, 'current_ah', 0)
        mask = (ah_col >= config['min']) & (ah_col <= config['max'])
        df_target = df[mask].copy()
        
        if len(df_target) < 100:
            print(f"   ⚠️ Solo {len(df_target)} partidos, omitiendo.")
            continue
        
        print(f"   {len(df_target)} partidos")
        feat_df = build_ah_features(df_target)
        
        print(f"   Entrenando ({GENERATIONS//1000}k gens)...")
        patterns = train_family(df_target, feat_df, family_name, 'ah_profit', 'AH')
        
        if patterns:
            patterns.sort(key=lambda x: x['test']['roi'], reverse=True)
            out_file = output_dir / f'specialist_ah_{family_name}.json'
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'meta': {'generated_at': datetime.now().isoformat(), 'family': family_name},
                    'patterns': patterns
                }, f, indent=2)
            print(f"   ✅ {len(patterns)} patrones -> {out_file.name}")
            print(f"      TOP: ROI {patterns[0]['test']['roi']*100:.1f}% (N={patterns[0]['test']['n']})")
        else:
            print(f"   ❌ No se encontraron patrones")
    
    # ============ ENTRENAR O/U ============
    print("\n" + "="*70)
    print("📊 ENTRENANDO PATRONES O/U")
    print("="*70)
    
    for family_name, config in OU_FAMILIES.items():
        print(f"\n[OU {family_name}] Filtrando OU {config['min']}-{config['max']}...")
        
        ou_col = safe_col(df, 'current_ou', 2.5)
        mask = (ou_col >= config['min']) & (ou_col <= config['max'])
        df_target = df[mask].copy()
        
        if len(df_target) < 100:
            print(f"   ⚠️ Solo {len(df_target)} partidos, omitiendo.")
            continue
        
        print(f"   {len(df_target)} partidos")
        feat_df = build_ou_features(df_target)
        
        print(f"   Entrenando ({GENERATIONS//1000}k gens)...")
        patterns = train_family(df_target, feat_df, family_name, 'ou_profit', 'OU')
        
        if patterns:
            patterns.sort(key=lambda x: x['test']['roi'], reverse=True)
            out_file = output_dir / f'specialist_ou_{family_name}.json'
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'meta': {'generated_at': datetime.now().isoformat(), 'family': family_name},
                    'patterns': patterns
                }, f, indent=2)
            print(f"   ✅ {len(patterns)} patrones -> {out_file.name}")
            print(f"      TOP: ROI {patterns[0]['test']['roi']*100:.1f}% (N={patterns[0]['test']['n']})")
        else:
            print(f"   ❌ No se encontraron patrones")
    
    print("\n" + "="*70)
    print("✅ ENTRENAMIENTO COMPLETO")
    print(f"   Archivos en: {output_dir}")
    print("="*70)

if __name__ == "__main__":
    main()
