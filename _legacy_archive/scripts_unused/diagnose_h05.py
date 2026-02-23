#!/usr/bin/env python
"""
Diagnóstico del Dataset H0.5
Analiza distribución de features y ROI baseline
"""
import sys
import numpy as np
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pattern_miner_v2.features_builder_v2 import load_all_training_data, build_training_dataframe

def diagnose():
    print("="*60)
    print("DIAGNÓSTICO H0.5")
    print("="*60)
    
    # Cargar datos
    data_dir = str(PROJECT_ROOT / 'data')
    matches = load_all_training_data(data_dir)
    df = build_training_dataframe(matches)
    
    # Filtrar H0.5 Local Favorito
    mask = (df['current_ah'] >= -0.75) & (df['current_ah'] <= -0.25)
    df_target = df[mask].copy()
    
    print(f"\nPartidos H0.5: {len(df_target)}")
    
    # ROI Baseline (sin filtros)
    if 'ah_profit' in df_target.columns:
        total_profit = df_target['ah_profit'].sum()
        avg_roi = total_profit / len(df_target)
        print(f"\nROI BASELINE (apostar HOME siempre):")
        print(f"  Profit Total: {total_profit:.2f}")
        print(f"  ROI Promedio: {avg_roi*100:.2f}%")
        
        # Distribución de resultados
        print(f"\nDISTRIBUCIÓN DE RESULTADOS:")
        wins = (df_target['ah_profit'] > 0).sum()
        losses = (df_target['ah_profit'] < 0).sum()
        pushes = (df_target['ah_profit'] == 0).sum()
        print(f"  Wins: {wins} ({wins/len(df_target)*100:.1f}%)")
        print(f"  Losses: {losses} ({losses/len(df_target)*100:.1f}%)")
        print(f"  Push: {pushes} ({pushes/len(df_target)*100:.1f}%)")
    
    # Verificar features clave
    print(f"\nFEATURES DISPONIBLES:")
    key_cols = [
        'bridge_D_gap_DA_mean', 'bridge_M_gap_mean',
        'prev_home_dDA', 'prev_home_dSOT',
        'prev_home_exp_failed', 'movement_dir', 'ah_delta',
        'exp_cover_own_rate', 'home_bottom5', 'away_mid_table',
        'home_top5', 'away_bottom5', 'rank_diff'
    ]
    
    for col in key_cols:
        if col in df_target.columns:
            if df_target[col].dtype == 'bool':
                pct_true = df_target[col].sum() / len(df_target) * 100
                print(f"  {col}: {pct_true:.1f}% True")
            elif df_target[col].dtype in ['float64', 'int64']:
                print(f"  {col}: mean={df_target[col].mean():.2f}, std={df_target[col].std():.2f}")
            else:
                vals = df_target[col].value_counts()
                print(f"  {col}: {dict(vals.head(3))}")
        else:
            print(f"  {col}: ❌ NO EXISTE")
    
    # Probar una condición simple
    print(f"\nTEST DE CONDICIONES SIMPLES:")
    
    # Condition: bridge_D_gap_DA_mean > 5
    if 'bridge_D_gap_DA_mean' in df_target.columns:
        cond = df_target['bridge_D_gap_DA_mean'] > 5
        n = cond.sum()
        if n > 0:
            roi = df_target.loc[cond, 'ah_profit'].sum() / n
            print(f"  bridge_D_gap_DA_mean > 5: N={n}, ROI={roi*100:.1f}%")
    
    # Condition: prev_home_dDA > 15
    if 'prev_home_dDA' in df_target.columns:
        cond = df_target['prev_home_dDA'] > 15
        n = cond.sum()
        if n > 0:
            roi = df_target.loc[cond, 'ah_profit'].sum() / n
            print(f"  prev_home_dDA > 15: N={n}, ROI={roi*100:.1f}%")
    
    # Condition: movement_dir == 'DOWN'
    if 'movement_dir' in df_target.columns:
        cond = df_target['movement_dir'] == 'DOWN'
        n = cond.sum()
        if n > 0:
            roi = df_target.loc[cond, 'ah_profit'].sum() / n
            print(f"  movement_dir == 'DOWN': N={n}, ROI={roi*100:.1f}%")

if __name__ == "__main__":
    diagnose()
