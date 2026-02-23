#!/usr/bin/env python
"""
============================================================
ENTRENADOR DE PATRONES - VISITANTE FAVORITO
============================================================
Entrena patrones para handicaps NEGATIVOS (Away Favorite)
Families:
- A0.5: AH -0.25 a -0.75 (Visitante Favorito ligero)
- A1.0: AH -0.75 a -1.25 (Visitante Favorito medio)
- A1.5: AH -1.25 a -1.75 (Visitante Favorito fuerte)
- A2.0+: AH -1.75+ (Visitante Muy Favorito)
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
MIN_SAMPLES = 10
MIN_ROI = 0.15
GENERATIONS = 100000
MIN_CONDITIONS = 3
MAX_CONDITIONS = 6

# Familias NEGATIVAS (Visitante Favorito)
AH_FAMILIES = {
    'A0_5': {'min': -0.75, 'max': -0.25, 'target': 'AWAY'},
    'A1_0': {'min': -1.25, 'max': -0.75, 'target': 'AWAY'},
    'A1_5': {'min': -1.75, 'max': -1.25, 'target': 'AWAY'},
    'A2_0_plus': {'min': -10, 'max': -1.75, 'target': 'AWAY'},
}

def safe_col(df, col, default=False):
    if col in df.columns:
        return df[col].fillna(default)
    return pd.Series(default, index=df.index)

def build_away_features(df):
    """Features booleanas para VISITANTE Favorito."""
    bool_df = pd.DataFrame(index=df.index)
    
    # Triangulación (invertida para Away)
    bridge_da = safe_col(df, 'bridge_D_gap_DA_mean', 0)
    bridge_m = safe_col(df, 'bridge_M_gap_mean', 0)
    # Para Away favorito, queremos que Away domine (bridge_da negativo = Away mejor)
    bool_df['TRIANG_Away_Dom'] = bridge_da < -5
    bool_df['TRIANG_Home_Weak'] = bridge_da < -10
    bool_df['IND_Strong_DA'] = bridge_da < -3
    bool_df['IND_Market_Away'] = bridge_m > 0.1  # Mercado favorece Away
    
    # Stats previas del Visitante
    prev_away_dda = safe_col(df, 'prev_away_dDA', 0)
    prev_away_dsot = safe_col(df, 'prev_away_dSOT', 0)
    bool_df['PREV_Away_Dom_DA'] = prev_away_dda > 15  # Away dominó en su anterior
    bool_df['PREV_Away_Dom_SOT'] = prev_away_dsot > 3
    bool_df['PREV_Away_Solid'] = prev_away_dda > 0
    
    # Falso Perdedor Away (Away perdió su handicap pero dominó)
    away_lost = safe_col(df, 'prev_away_exp_failed', False)
    bool_df['FALSE_LOSER_Away'] = away_lost & (prev_away_dda > 10)
    
    # Smart Money (para Away favorito, UP = dinero con Away)
    mov_dir = safe_col(df, 'movement_dir', 'NONE')
    ah_delta = safe_col(df, 'ah_delta', 0)
    bool_df['MONEY_Away'] = mov_dir == 'UP'  # Línea sube = Away más fuerte
    bool_df['MONEY_Strong'] = (mov_dir == 'UP') & (ah_delta.abs() >= 0.25)
    
    # Benchmark Tags (invertidos para Away)
    current_ah = safe_col(df, 'current_ah', 0)
    prev_line = safe_col(df, 'prev_away_exp_line', current_ah)
    ah_gap = current_ah - prev_line
    # Para Away: más negativo = Away más favorito = MEJORA
    bool_df['TAG_Mejora'] = ah_gap < -0.1  # Más negativo = Away más favorito
    bool_df['TAG_Empeora'] = ah_gap > 0.1  # Menos negativo = Away menos favorito
    bool_df['TAG_Iguala'] = ah_gap.abs() <= 0.1
    
    # Expectativas Away
    exp_rate = safe_col(df, 'exp_cover_away_rate', 0.5)
    bool_df['EXP_Reliable'] = exp_rate > 0.6
    bool_df['EXP_Unreliable'] = exp_rate < 0.4
    
    # Contexto (invertido)
    bool_df['CTX_Urgency'] = safe_col(df, 'away_bottom5', False)  # Away en urgencia
    bool_df['CTX_Away_Top'] = safe_col(df, 'away_top5', False)
    bool_df['CTX_Home_Bottom'] = safe_col(df, 'home_bottom5', False)
    bool_df['CTX_Derby'] = safe_col(df, 'rank_diff', 0).abs() < 3
    
    # Triggers
    bool_df['TRIGGER_Sniper'] = bool_df['FALSE_LOSER_Away'] & bool_df['MONEY_Away']
    bool_df['TRIGGER_Trap'] = bool_df['TAG_Empeora'] & bool_df['PREV_Away_Dom_DA']
    
    for col in bool_df.columns:
        bool_df[col] = bool_df[col].astype(bool)
    
    return bool_df

def train_family(df, feat_df, family_name, target_col):
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
        
        # Para AWAY favorito, invertimos el profit (ah_profit negativo = Away ganó)
        profit_train = -df_train.loc[feat_train[mask_train].index, target_col].sum()
        roi_train = profit_train / n_train
        
        if roi_train < 0.10:
            continue
        
        mask_test = np.ones(len(feat_test), dtype=bool)
        for c in conds:
            mask_test &= feat_test[c].values
        
        n_test = mask_test.sum()
        if n_test < MIN_SAMPLES:
            continue
        
        profit_test = -df_test.loc[feat_test[mask_test].index, target_col].sum()
        roi_test = profit_test / n_test
        
        if roi_test >= MIN_ROI:
            conds_set = frozenset(conds)
            if any(frozenset(p['conditions']) == conds_set for p in found_patterns):
                continue
            
            pattern = {
                'name': f"AH-{family_name}-{len(found_patterns)+1:03d}",
                'target': 'AWAY',
                'family': family_name,
                'conditions': sorted(conds),
                'train': {'n': int(n_train), 'roi': round(roi_train, 3)},
                'test': {'n': int(n_test), 'roi': round(roi_test, 3)}
            }
            found_patterns.append(pattern)
            
        if gen % 20000 == 0 and gen > 0:
            print(f"      {gen//1000}k... [{len(found_patterns)} encontrados]")
    
    return found_patterns

def main():
    print("="*70)
    print("🎯 ENTRENADOR PATRONES - VISITANTE FAVORITO (AH Negativo)")
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
    
    # ============ ENTRENAR AH NEGATIVO ============
    print("\n" + "="*70)
    print("📊 ENTRENANDO PATRONES AH - VISITANTE FAVORITO")
    print("="*70)
    
    for family_name, config in AH_FAMILIES.items():
        print(f"\n[{family_name}] Filtrando AH {config['min']} a {config['max']}...")
        
        ah_col = safe_col(df, 'current_ah', 0)
        mask = (ah_col >= config['min']) & (ah_col <= config['max'])
        df_target = df[mask].copy()
        
        if len(df_target) < 100:
            print(f"   ⚠️ Solo {len(df_target)} partidos, omitiendo.")
            continue
        
        print(f"   {len(df_target)} partidos")
        feat_df = build_away_features(df_target)
        
        print(f"   Entrenando ({GENERATIONS//1000}k gens)...")
        patterns = train_family(df_target, feat_df, family_name, 'ah_profit')
        
        if patterns:
            patterns.sort(key=lambda x: x['test']['roi'], reverse=True)
            out_file = output_dir / f'specialist_ah_{family_name}.json'
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'meta': {'generated_at': datetime.now().isoformat(), 'family': family_name, 'target': 'AWAY'},
                    'patterns': patterns
                }, f, indent=2)
            print(f"   ✅ {len(patterns)} patrones -> {out_file.name}")
            print(f"      TOP: ROI {patterns[0]['test']['roi']*100:.1f}% (N={patterns[0]['test']['n']})")
        else:
            print(f"   ❌ No se encontraron patrones")
    
    print("\n" + "="*70)
    print("✅ ENTRENAMIENTO VISITANTE FAVORITO COMPLETO")
    print(f"   Archivos en: {output_dir}")
    print("="*70)

if __name__ == "__main__":
    main()
