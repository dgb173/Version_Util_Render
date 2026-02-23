#!/usr/bin/env python
"""
============================================================
ADVANCED H0.5 SPECIALIST TRAINER (FIXED)
============================================================
Entrena patrones para Handicap 0.25/0.5/0.75 (Local Favorito)
Con corrección de convención de signo para ah_line.

IMPORTANTE:
- NowGoal: ah_line POSITIVO = Local Favorito (ej: 0.5 = Local -0.5)
- settle_ah: Usa convención opuesta (negativo = favorito)
- Este script adapta la lógica para usar la convención correcta
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
MIN_SAMPLES = 15
MIN_ROI = 0.20
GENERATIONS = 100000
MIN_CONDITIONS = 4
MAX_CONDITIONS = 7

# Descripciones para justificación
FEATURE_DESCRIPTIONS = {
    'TRIANG_Home_Dominant': 'Triangulación muestra superioridad del local contra rivales comunes',
    'TRIANG_Away_Weak': 'Triangulación expone debilidad del visitante',
    'IND_Home_Strong_DA': 'Local superior en Ataques Peligrosos en comparativas indirectas',
    'IND_Market_Favors_Home': 'Mercado indirecto favorece claramente al local',
    'PREV_Home_Dominant_DA': 'Local dominó en Ataques Peligrosos su partido anterior',
    'PREV_Home_Dominant_SOT': 'Local dominó en Tiros a Puerta su partido anterior',
    'PREV_Home_Solid': 'Local tuvo más Ataques Peligrosos que su rival anterior',
    'FALSE_LOSER_Home': '🎯 FALSO PERDEDOR: Local perdió handicap pero dominó estadísticas',
    'MONEY_With_Home': '💰 SMART MONEY: Línea se movió a favor del local',
    'MONEY_Strong': '💰💰 DINERO FUERTE: Movimiento significativo (>0.25) a favor',
    'TAG_Mejora': '📈 MEJORA: El local es MÁS favorito que en su partido anterior',
    'TAG_Empeora': '📉 EMPEORA: El local es MENOS favorito que antes',
    'TAG_Iguala': '➖ IGUALA: Línea similar al partido anterior',
    'EXP_Home_Reliable': '✅ Local cubre su línea >60% de las veces (fiable)',
    'EXP_Home_Unreliable': '⚠️ Local cubre su línea <40% (poco fiable)',
    'CTX_Urgency': '🔥 URGENCIA: Local en zona baja vs visitante sin presión',
    'CTX_Home_Top': '🏆 Local en TOP 5 de la tabla',
    'CTX_Away_Bottom': '📉 Visitante en zona baja',
    'CTX_Derby': '⚔️ Derby/Clásico (equipos cercanos en tabla)',
    'TRIGGER_Sniper': '🎯 TRIGGER: Falso Perdedor + Smart Money = Alta probabilidad',
    'TRIGGER_Trap': '🪤 TRIGGER: Etiqueta Empeora pero dominó = Trampa del mercado',
    'H2H_Home_Covers': 'Local cubrió en H2H previo en este estadio',
    'H2H_General_Covers': 'Local cubrió en H2H general reciente',
    'BRIDGE_Contradiction': '⚠️ Mercado y dominancia apuntan a lados opuestos'
}

def safe_col(df, col, default=False):
    """Obtiene columna de forma segura."""
    if col in df.columns:
        return df[col].fillna(default)
    return pd.Series(default, index=df.index)

def build_feature_matrix(df):
    """Construye matriz de features booleanas."""
    print("\n📊 Construyendo matriz de features...")
    
    bool_df = pd.DataFrame(index=df.index)
    
    # === 1. TRIANGULACIÓN (PUENTES) ===
    bridge_da = safe_col(df, 'bridge_D_gap_DA_mean', 0)
    bridge_m = safe_col(df, 'bridge_M_gap_mean', 0)
    
    bool_df['TRIANG_Home_Dominant'] = bridge_da > 5
    bool_df['TRIANG_Away_Weak'] = bridge_da > 10
    bool_df['IND_Home_Strong_DA'] = bridge_da > 3
    bool_df['IND_Market_Favors_Home'] = bridge_m < -0.1
    
    # === 2. ESTADÍSTICAS PARTIDO ANTERIOR ===
    prev_dda = safe_col(df, 'prev_home_dDA', 0)
    prev_dsot = safe_col(df, 'prev_home_dSOT', 0)
    
    bool_df['PREV_Home_Dominant_DA'] = prev_dda > 15
    bool_df['PREV_Home_Dominant_SOT'] = prev_dsot > 3
    bool_df['PREV_Home_Solid'] = prev_dda > 0
    
    # === 3. FALSO PERDEDOR ===
    # Usando ah_covered = 'AWAY' como indicador de pérdida anterior (invertido)
    lost_prev = safe_col(df, 'ah_covered', 'NONE') == 'AWAY'
    bool_df['FALSE_LOSER_Home'] = lost_prev & (prev_dda > 10)
    
    # === 4. SMART MONEY ===
    mov_dir = safe_col(df, 'movement_dir', 'NONE')
    ah_delta = safe_col(df, 'ah_delta', 0)
    
    bool_df['MONEY_With_Home'] = mov_dir == 'DOWN'
    bool_df['MONEY_Strong'] = (mov_dir == 'DOWN') & (ah_delta.abs() >= 0.25)
    
    # === 5. BENCHMARK (MEJORA/EMPEORA) ===
    current_ah = safe_col(df, 'current_ah', 0)
    prev_line = safe_col(df, 'prev_home_exp_line', current_ah)
    ah_gap = current_ah - prev_line
    
    bool_df['TAG_Mejora'] = ah_gap > 0.1   # Más positivo = más favorito en convención NowGoal
    bool_df['TAG_Empeora'] = ah_gap < -0.1
    bool_df['TAG_Iguala'] = ah_gap.abs() <= 0.1
    
    # === 6. EXPECTATIVAS ===
    exp_rate = safe_col(df, 'exp_cover_own_rate', 0.5)
    bool_df['EXP_Home_Reliable'] = exp_rate > 0.6
    bool_df['EXP_Home_Unreliable'] = exp_rate < 0.4
    
    # === 7. CONTEXTO ===
    bool_df['CTX_Urgency'] = safe_col(df, 'home_bottom5', False)
    bool_df['CTX_Home_Top'] = safe_col(df, 'home_top5', False)
    bool_df['CTX_Away_Bottom'] = safe_col(df, 'away_bottom5', False)
    
    rank_diff = safe_col(df, 'rank_diff', 0)
    bool_df['CTX_Derby'] = rank_diff.abs() < 3
    
    # === 8. H2H ===
    bool_df['H2H_Home_Covers'] = safe_col(df, 'h2h_stadium_covered', False) == True
    bool_df['H2H_General_Covers'] = safe_col(df, 'h2h_general_covered', False) == True
    
    # === 9. CONTRADICCIÓN ===
    bool_df['BRIDGE_Contradiction'] = safe_col(df, 'bridge_contradiction_rate', 0) > 0.5
    
    # === 10. TRIGGERS COMBINADOS ===
    bool_df['TRIGGER_Sniper'] = bool_df['FALSE_LOSER_Home'] & bool_df['MONEY_With_Home']
    bool_df['TRIGGER_Trap'] = bool_df['TAG_Empeora'] & bool_df['PREV_Home_Dominant_DA']
    
    for col in bool_df.columns:
        bool_df[col] = bool_df[col].astype(bool)
    
    print(f"   ✅ {len(bool_df.columns)} features generadas")
    return bool_df

def generate_justification(conditions):
    """Genera una justificación legible para un patrón."""
    lines = []
    for cond in conditions:
        if cond in FEATURE_DESCRIPTIONS:
            lines.append(f"  • {FEATURE_DESCRIPTIONS[cond]}")
        else:
            lines.append(f"  • {cond}")
    return "\n".join(lines)

def train():
    print("="*70)
    print("🎯 ENTRENADOR AVANZADO H0.5 (LOCAL FAVORITO) - FIXED")
    print(f"   Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Config: ROI≥{MIN_ROI*100:.0f}% | N≥{MIN_SAMPLES} | Condiciones: {MIN_CONDITIONS}-{MAX_CONDITIONS}")
    print("="*70)
    
    # 1. Cargar datos
    print("\n📂 Cargando datos de entrenamiento...")
    data_dir = str(PROJECT_ROOT / 'data')
    matches = load_all_training_data(data_dir)
    df = build_training_dataframe(matches)
    print(f"   Total partidos: {len(df)}")
    
    # 2. Filtrar familia H0.5 (Local Favorito)
    # En convención NowGoal: current_ah POSITIVO = Local Favorito
    ah_col = safe_col(df, 'current_ah', 0)
    mask = (ah_col >= 0.25) & (ah_col <= 0.75)  # POSITIVO para Local Favorito
    df_target = df[mask].copy()
    
    print(f"   Partidos H0.5 (Local Favorito, AH 0.25-0.75): {len(df_target)}")
    
    # Verificar baseline - ah_profit AHORA es correcto (no invertir)
    if 'ah_profit' in df_target.columns:
        baseline_profit = df_target['ah_profit'].sum()
        baseline_roi = baseline_profit / len(df_target)
        print(f"   ROI Baseline (apostar HOME): {baseline_roi*100:.1f}%")
    else:
        print("   ⚠️ No hay columna ah_profit")
        return
    
    if len(df_target) < 100:
        print("❌ ERROR: Muy pocos datos.")
        return
    
    # 3. Construir features
    feat_df = build_feature_matrix(df_target)
    feature_names = list(feat_df.columns)
    
    # 4. Split Train/Test
    n_split = int(len(df_target) * 0.8)
    train_idx = df_target.index[:n_split]
    test_idx = df_target.index[n_split:]
    
    feat_train = feat_df.loc[train_idx]
    feat_test = feat_df.loc[test_idx]
    df_train = df_target.loc[train_idx]
    df_test = df_target.loc[test_idx]
    
    print(f"   Train: {len(train_idx)} | Test: {len(test_idx)}")
    
    # 5. Búsqueda de patrones
    print(f"\n🔍 Buscando patrones ({GENERATIONS:,} iteraciones)...")
    print("-"*70)
    
    found_patterns = []
    last_progress = 0
    
    for gen in range(GENERATIONS):
        progress = int(gen / GENERATIONS * 100)
        if progress >= last_progress + 10:
            print(f"   ⏳ {progress}% completado... [{len(found_patterns)} patrones encontrados]")
            last_progress = progress
        
        n_conds = random.randint(MIN_CONDITIONS, MAX_CONDITIONS)
        conds = random.sample(feature_names, n_conds)
        
        mask_train = np.ones(len(feat_train), dtype=bool)
        for c in conds:
            mask_train &= feat_train[c].values
        
        n_train = mask_train.sum()
        if n_train < MIN_SAMPLES:
            continue
        
        # Usar ah_profit directamente (ya corregido en features_builder)
        profit_train = df_train.loc[feat_train[mask_train].index, 'ah_profit'].sum()
        roi_train = profit_train / n_train
        
        if roi_train < 0.10:
            continue
        
        mask_test = np.ones(len(feat_test), dtype=bool)
        for c in conds:
            mask_test &= feat_test[c].values
        
        n_test = mask_test.sum()
        if n_test < MIN_SAMPLES:
            continue
        
        profit_test = df_test.loc[feat_test[mask_test].index, 'ah_profit'].sum()
        roi_test = profit_test / n_test
        
        if roi_test >= MIN_ROI:
            conds_set = frozenset(conds)
            if any(frozenset(p['conditions']) == conds_set for p in found_patterns):
                continue
            
            pattern = {
                'name': f"ADV-H0.5-{len(found_patterns)+1:03d}",
                'target': 'HOME',
                'family': 'H0.5',
                'conditions': sorted(conds),
                'justification': generate_justification(conds),
                'train': {
                    'n': int(n_train),
                    'roi': round(roi_train, 3),
                    'profit': round(profit_train, 2)
                },
                'test': {
                    'n': int(n_test),
                    'roi': round(roi_test, 3),
                    'profit': round(profit_test, 2)
                }
            }
            found_patterns.append(pattern)
            
            print(f"\n   🎯 ¡PATRÓN ENCONTRADO! #{len(found_patterns)}")
            print(f"      Test ROI: {roi_test*100:.1f}% | N={n_test}")
            print(f"      Condiciones ({n_conds}):")
            for c in sorted(conds)[:5]:
                desc = FEATURE_DESCRIPTIONS.get(c, c)
                print(f"        → {desc[:60]}...")
    
    # 6. Guardar resultados
    print("\n" + "="*70)
    print(f"✅ ENTRENAMIENTO COMPLETADO")
    print(f"   Patrones encontrados: {len(found_patterns)}")
    
    if found_patterns:
        found_patterns.sort(key=lambda x: x['test']['roi'], reverse=True)
        
        output_path = PROJECT_ROOT / 'data' / 'patterns_v2' / 'specialist_ah_H0_5.json'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        output = {
            'meta': {
                'generated_at': datetime.now().isoformat(),
                'family': 'H0.5 (0.25, 0.5, 0.75)',
                'target': 'HOME (Local Favorito)',
                'min_roi': MIN_ROI,
                'min_samples': MIN_SAMPLES,
                'min_conditions': MIN_CONDITIONS
            },
            'patterns': found_patterns
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"   Guardado en: {output_path.name}")
        
        print("\n🏆 TOP 5 PATRONES:")
        print("-"*70)
        for i, p in enumerate(found_patterns[:5], 1):
            print(f"\n#{i} {p['name']} | ROI: {p['test']['roi']*100:.1f}% | N={p['test']['n']}")
            print(f"   Justificación:")
            for line in p['justification'].split('\n')[:3]:
                print(f"   {line}")
    else:
        print("   ⚠️ No se encontraron patrones que cumplan los criterios.")
    
    print("="*70)

if __name__ == "__main__":
    train()
