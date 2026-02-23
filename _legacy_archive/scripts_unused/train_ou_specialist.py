#!/usr/bin/env python
"""
============================================================
ENTRENADOR ESPECIALIZADO O/U (Over/Under)
============================================================
Entrena patrones para OVER y UNDER por separado.
Criterios relajados para encontrar más patrones.
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

# ============ CONFIGURACIÓN ESTRICTA (USER REQUEST) ============
MIN_SAMPLES = 15
MIN_ROI = 0.20
GENERATIONS = 120000
MIN_CONDITIONS = 3
MAX_CONDITIONS = 8

# Familias O/U - INCLUYENDO 1.5 y 2.5
OU_FAMILIES = {
    '1_5': {'min': 1.25, 'max': 1.75},
    '2_0': {'min': 1.75, 'max': 2.25},
    '2_25': {'min': 2.0, 'max': 2.5},
    '2_5': {'min': 2.25, 'max': 2.75},
    '2_75': {'min': 2.5, 'max': 3.0},
    '3_0': {'min': 2.75, 'max': 3.25},
    '3_5': {'min': 3.25, 'max': 4.0},
}

def safe_col(df, col, default=False):
    if col in df.columns:
        return df[col].fillna(default)
    return pd.Series(default, index=df.index)

def build_over_features(df):
    """Features para apostar por OVER."""
    bool_df = pd.DataFrame(index=df.index)
    
    # --- FACTOR CLAVE: H2H (Histórico Goles) ---
    bool_df['H2H_Was_Over'] = safe_col(df, 'H2H_Over_Line', False)
    
    # Equipos goleadores
    home_goals = safe_col(df, 'home_avg_goals', 1.5)
    away_goals = safe_col(df, 'away_avg_goals', 1.5)
    bool_df['HOME_Scoring'] = home_goals > 1.3
    bool_df['HOME_HighScoring'] = home_goals > 1.8
    bool_df['AWAY_Scoring'] = away_goals > 1.3
    bool_df['AWAY_HighScoring'] = away_goals > 1.8
    
    # Defensas débiles
    home_conceded = safe_col(df, 'home_avg_conceded', 1.5)
    away_conceded = safe_col(df, 'away_avg_conceded', 1.5)
    bool_df['HOME_LeakyDef'] = home_conceded > 1.5
    bool_df['AWAY_LeakyDef'] = away_conceded > 1.5
    
    # Ritmo alto (DA acumulado)
    prev_home_da = safe_col(df, 'prev_home_dDA', 0).abs()
    prev_away_da = safe_col(df, 'prev_away_dDA', 0).abs()
    bool_df['HIGH_TEMPO'] = (prev_home_da + prev_away_da) > 20
    bool_df['OFFENSIVE_MATCH'] = (prev_home_da > 10) | (prev_away_da > 10)
    
    # Partido parejo (sin dominio claro = más goles)
    bridge_da = safe_col(df, 'bridge_D_gap_DA_mean', 0)
    bool_df['EVEN_MATCH'] = bridge_da.abs() < 5
    
    # Equipos en buena racha
    bool_df['HOME_TOP'] = safe_col(df, 'home_top5', False)
    bool_df['AWAY_TOP'] = safe_col(df, 'away_top5', False)
    bool_df['TOP_CLASH'] = bool_df['HOME_TOP'] | bool_df['AWAY_TOP']
    
    # Tendencia O/U
    exp_over_rate = safe_col(df, 'exp_over_rate', 0.5)
    bool_df['OVER_TENDENCY'] = exp_over_rate > 0.55
    bool_df['OVER_STRONG_TENDENCY'] = exp_over_rate > 0.70
    
    # Línea movimiento
    mov_dir = safe_col(df, 'ou_movement_dir', 'NONE')
    bool_df['LINE_DOWN'] = mov_dir == 'DOWN'  # Línea baja = más goles esperados
    
    for col in bool_df.columns:
        bool_df[col] = bool_df[col].astype(bool)
    return bool_df

def build_under_features(df):
    """Features para apostar por UNDER."""
    bool_df = pd.DataFrame(index=df.index)
    
    # --- FACTOR CLAVE: H2H (Histórico Goles) ---
    bool_df['H2H_Was_Under'] = safe_col(df, 'H2H_Under_Line', False)
    
    # Equipos defensivos
    home_goals = safe_col(df, 'home_avg_goals', 1.5)
    away_goals = safe_col(df, 'away_avg_goals', 1.5)
    bool_df['HOME_LowScoring'] = home_goals < 1.2
    bool_df['AWAY_LowScoring'] = away_goals < 1.2
    
    # Defensas sólidas
    home_conceded = safe_col(df, 'home_avg_conceded', 1.5)
    away_conceded = safe_col(df, 'away_avg_conceded', 1.5)
    bool_df['HOME_SolidDef'] = home_conceded < 1.0
    bool_df['AWAY_SolidDef'] = away_conceded < 1.0
    
    # Ritmo bajo
    prev_home_da = safe_col(df, 'prev_home_dDA', 0).abs()
    prev_away_da = safe_col(df, 'prev_away_dDA', 0).abs()
    bool_df['LOW_TEMPO'] = (prev_home_da + prev_away_da) < 15
    bool_df['DEFENSIVE_MATCH'] = (prev_home_da < 8) & (prev_away_da < 8)
    
    # Un equipo muy dominante (controlará el partido)
    bridge_da = safe_col(df, 'bridge_D_gap_DA_mean', 0)
    bool_df['DOMINANT_ONE'] = bridge_da.abs() > 10
    
    # Equipos en zona baja (juegan con miedo)
    bool_df['HOME_BOTTOM'] = safe_col(df, 'home_bottom5', False)
    bool_df['AWAY_BOTTOM'] = safe_col(df, 'away_bottom5', False)
    bool_df['TENSE_MATCH'] = bool_df['HOME_BOTTOM'] | bool_df['AWAY_BOTTOM']
    
    # Tendencia Under
    exp_over_rate = safe_col(df, 'exp_over_rate', 0.5)
    bool_df['UNDER_TENDENCY'] = exp_over_rate < 0.45
    bool_df['UNDER_STRONG_TENDENCY'] = exp_over_rate < 0.30
    
    # Línea movimiento
    mov_dir = safe_col(df, 'ou_movement_dir', 'NONE')
    bool_df['LINE_UP'] = mov_dir == 'UP'  # Línea sube = menos goles esperados
    
    for col in bool_df.columns:
        bool_df[col] = bool_df[col].astype(bool)
    return bool_df

def train_patterns(df, feat_df, family_name, target, use_ou_profit=True, invert=False):
    """Entrena patrones para una familia."""
    feature_names = list(feat_df.columns)
    
    n_split = int(len(df) * 0.8)
    train_idx = df.index[:n_split]
    test_idx = df.index[n_split:]
    
    feat_train = feat_df.loc[train_idx]
    feat_test = feat_df.loc[test_idx]
    df_train = df.loc[train_idx]
    df_test = df.loc[test_idx]
    
    found_patterns = []
    multiplier = -1 if invert else 1
    
    for gen in range(GENERATIONS):
        n_conds = random.randint(MIN_CONDITIONS, MAX_CONDITIONS)
        conds = random.sample(feature_names, min(n_conds, len(feature_names)))
        
        mask_train = np.ones(len(feat_train), dtype=bool)
        for c in conds:
            mask_train &= feat_train[c].values
        
        n_train = mask_train.sum()
        if n_train < MIN_SAMPLES:
            continue
        
        # Usar ou_profit
        profit_col = 'ou_profit' if use_ou_profit and 'ou_profit' in df_train.columns else 'ah_profit'
        profit_train = multiplier * df_train.loc[feat_train[mask_train].index, profit_col].sum()
        roi_train = profit_train / n_train
        
        if roi_train < 0.08:
            continue
        
        mask_test = np.ones(len(feat_test), dtype=bool)
        for c in conds:
            mask_test &= feat_test[c].values
        
        n_test = mask_test.sum()
        if n_test < MIN_SAMPLES:
            continue
        
        profit_test = multiplier * df_test.loc[feat_test[mask_test].index, profit_col].sum()
        roi_test = profit_test / n_test
        
        if roi_test >= MIN_ROI:
            conds_set = frozenset(conds)
            if any(frozenset(p['conditions']) == conds_set for p in found_patterns):
                continue
            
            pattern = {
                'name': f"OU-{family_name}-{target}-{len(found_patterns)+1:03d}",
                'target': target,
                'family': family_name,
                'conditions': sorted(conds),
                'train': {'n': int(n_train), 'roi': round(roi_train, 3)},
                'test': {'n': int(n_test), 'roi': round(roi_test, 3)}
            }
            found_patterns.append(pattern)
            
        if gen % 25000 == 0 and gen > 0:
            print(f"      {gen//1000}k... [{len(found_patterns)}]")
    
    return found_patterns

def main():
    print("="*70)
    print("🎯 ENTRENADOR O/U ESPECIALIZADO")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Config: ROI≥{MIN_ROI*100:.0f}% | N≥{MIN_SAMPLES}")
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
    
    print("\n" + "="*70)
    print("📊 ENTRENANDO PATRONES O/U")
    print("="*70)
    
    for family_name, config in OU_FAMILIES.items():
        print(f"\n[OU {family_name}] Línea {config['min']}-{config['max']}")
        
        ou_col = safe_col(df, 'current_ou', 2.5)
        mask = (ou_col >= config['min']) & (ou_col <= config['max'])
        df_target = df[mask].copy()
        
        if len(df_target) < 100:
            print(f"   ⚠️ Solo {len(df_target)} partidos")
            continue
        
        print(f"   {len(df_target)} partidos")
        
        # Entrenar OVER
        print(f"   [OVER]")
        feat_over = build_over_features(df_target)
        patterns_over = train_patterns(df_target, feat_over, family_name, 'OVER', invert=False)
        
        # Entrenar UNDER
        print(f"   [UNDER]")
        feat_under = build_under_features(df_target)
        patterns_under = train_patterns(df_target, feat_under, family_name, 'UNDER', invert=True)
        
        all_patterns[family_name] = patterns_over + patterns_under
        
        if all_patterns[family_name]:
            all_patterns[family_name].sort(key=lambda x: x['test']['roi'], reverse=True)
            out_file = output_dir / f'specialist_ou_{family_name}.json'
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'meta': {'family': family_name, 'generated_at': datetime.now().isoformat()},
                    'patterns': all_patterns[family_name]
                }, f, indent=2)
            over_count = len(patterns_over)
            under_count = len(patterns_under)
            print(f"   ✅ {over_count} OVER + {under_count} UNDER = {over_count+under_count} patrones")
        else:
            print(f"   ❌ No se encontraron patrones")
    
    print("\n" + "="*70)
    print("✅ ENTRENAMIENTO O/U COMPLETO")
    total = sum(len(p) for p in all_patterns.values())
    print(f"   Total patrones O/U: {total}")
    print("="*70)

if __name__ == "__main__":
    main()
