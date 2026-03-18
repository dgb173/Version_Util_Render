"""
Rule Miner v2 - Minador de patrones complejos

Implementa la minería de reglas usando:
- Combinaciones aleatorias de features (5-10 variables)
- Validación temporal (train/test split por fecha)
- ROI real con liquidación asiática

Genera patrones por familia (AH y O/U) con condiciones interpretables.
"""

import random
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Callable
from collections import defaultdict

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("[WARN] pandas/numpy no disponible, algunas funciones no funcionaran")

from .settle_asian import calculate_profit, get_ah_family, get_ou_family, calculate_roi
from .gates import safe_float


# Configuración por defecto
DEFAULT_CONFIG = {
    'min_samples': 25,
    'min_samples_test': 20,  # Mínimo 20 muestras en test (usuario lo pidió)
    'min_accuracy': 0.70,  # Para ROI ~20% con cuotas 1.8
    'min_roi_train': 0.15,
    'min_roi_oos': 0.05,
    'max_degradation': 0.50,  # Max 50% drop de train a OOS
    'min_features': 3,  # Reducido para encontrar más patrones
    'max_features': 6,  # Reducido para tener más muestras
    'generations': 50000,
    'default_odds': 1.80
}

# Familias
AH_FAMILIES = ['H0', 'H0.5', 'H1.0', 'H1.25_1.75', 'H2.0_plus']
OU_FAMILIES = ['OU2.0_2.25', 'OU2.5', 'OU2.75_3.0', 'OU3.0_plus']


def get_boolean_features(df: 'pd.DataFrame') -> List[str]:
    """
    Extrae columnas que pueden usarse como condiciones booleanas.
    Solo incluye features con frecuencia razonable (>5% del dataset).
    """
    features = []
    min_freq = len(df) * 0.05  # Mínimo 5% de frecuencia
    
    for col in df.columns:
        # Skip ID y target columns
        if col in ['match_id', 'ah_outcome', 'ah_covered', 'ah_profit', 'ou_outcome', 'ou_profit', 
                   'home_name', 'away_name', 'league', 'match_date', 'home_goals', 'away_goals',
                   'total_goals', 'goal_diff', '_date']:
            continue
        
        dtype = df[col].dtype
        
        # Booleanos
        if dtype == 'bool':
            true_count = df[col].sum()
            if true_count >= min_freq and true_count <= len(df) - min_freq:
                features.append(col)
        
        # Strings/categorías (bins) - solo valores frecuentes
        elif dtype == 'object':
            unique = df[col].dropna().unique()
            for val in unique:
                count = (df[col] == val).sum()
                if count >= min_freq:
                    features.append(f"{col}=={val}")
        
        # Numéricos - crear comparaciones basadas en percentiles
        elif dtype in ['int64', 'float64']:
            col_data = df[col].dropna()
            if len(col_data) < 100:
                continue
            
            # Comparaciones típicas por tipo de columna
            if 'count' in col:
                for threshold in [1, 2, 3]:
                    above = (df[col].fillna(0) >= threshold).sum()
                    if above >= min_freq and above <= len(df) - min_freq:
                        features.append(f"{col}>={threshold}")
            elif 'rate' in col or 'quality' in col:
                for threshold in [0.3, 0.5, 0.7]:
                    above = (df[col].fillna(0) >= threshold).sum()
                    if above >= min_freq and above <= len(df) - min_freq:
                        features.append(f"{col}>={threshold}")
            elif 'dSOT' in col or 'dDA' in col or 'diff' in col:
                for threshold in [-2, 0, 2, 5]:
                    above = (df[col].fillna(0) >= threshold).sum()
                    if above >= min_freq and above <= len(df) - min_freq:
                        features.append(f"{col}>={threshold}")
                for threshold in [-5, -2, 0]:
                    below = (df[col].fillna(0) <= threshold).sum()    
                    if below >= min_freq and below <= len(df) - min_freq:
                        features.append(f"{col}<={threshold}")
            elif 'rank' in col:
                for threshold in [5, 10, 15]:
                    below = (df[col].fillna(99) <= threshold).sum()
                    if below >= min_freq and below <= len(df) - min_freq:
                        features.append(f"{col}<={threshold}")
    
    return features


def evaluate_condition(df: 'pd.DataFrame', condition: str) -> 'pd.Series':
    """
    Evalúa una condición como string y retorna máscara booleana.
    
    Soporta:
    - col==value (categoría)
    - col>=value, col<=value, col>value, col<value (numérico)
    - col (booleano directo)
    """
    if '==' in condition:
        col, val = condition.split('==')
        return df[col] == val
    elif '>=' in condition:
        col, val = condition.split('>=')
        return df[col].fillna(-999) >= float(val)
    elif '<=' in condition:
        col, val = condition.split('<=')
        return df[col].fillna(999) <= float(val)
    elif '>' in condition:
        col, val = condition.split('>')
        return df[col].fillna(-999) > float(val)
    elif '<' in condition:
        col, val = condition.split('<')
        return df[col].fillna(999) < float(val)
    else:
        # Booleano directo
        return df[condition].fillna(False).astype(bool)


def evaluate_pattern(df: 'pd.DataFrame', conditions: List[str]) -> 'pd.Series':
    """
    Evalúa un patrón (conjunto de condiciones) y retorna máscara.
    """
    mask = pd.Series([True] * len(df), index=df.index)
    
    for cond in conditions:
        try:
            mask = mask & evaluate_condition(df, cond)
        except Exception:
            mask = pd.Series([False] * len(df), index=df.index)
            break
    
    return mask


def calculate_pattern_stats(
    df: 'pd.DataFrame',
    conditions: List[str],
    target_col: str,
    target_value: str,
    profit_col: str = 'ah_profit',
    odds: float = 1.80
) -> Dict[str, Any]:
    """
    Calcula estadísticas de un patrón.
    
    Returns:
        Dict con n, wins, accuracy, roi, breakdown
    """
    mask = evaluate_pattern(df, conditions)
    matching = df[mask]
    
    n = len(matching)
    if n == 0:
        return {'n': 0, 'wins': 0, 'accuracy': 0, 'roi': 0, 'breakdown': {}}
    
    # Contar wins (target matches)
    wins = (matching[target_col] == target_value).sum()
    accuracy = wins / n
    
    # Calcular ROI real
    if profit_col in matching.columns:
        profits = matching[profit_col].dropna().tolist()
        roi = calculate_roi(profits) if profits else 0
    else:
        # Estimar ROI desde accuracy
        roi = (accuracy * odds) - 1
    
    # Breakdown de outcomes
    breakdown = {}
    if 'ah_outcome' in matching.columns:
        breakdown = matching['ah_outcome'].value_counts().to_dict()
    elif 'ou_outcome' in matching.columns:
        breakdown = matching['ou_outcome'].value_counts().to_dict()
    
    return {
        'n': n,
        'wins': wins,
        'accuracy': accuracy,
        'roi': roi,
        'breakdown': breakdown
    }


class PatternMinerV2:
    """
    Minador de patrones v2 con validación temporal y ROI real.
    """
    
    def __init__(self, config: Dict = None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.patterns = []
        self.ah_patterns = []
        self.ou_patterns = []
        
    def split_temporal(self, df: 'pd.DataFrame', train_ratio: float = 0.8) -> Tuple['pd.DataFrame', 'pd.DataFrame']:
        """
        Split temporal por fecha.
        """
        # Intentar parsear fecha
        date_col = None
        for col in ['match_date', 'date', 'fecha']:
            if col in df.columns:
                date_col = col
                break
        
        if date_col:
            # Ordenar por fecha
            df_sorted = df.copy()
            try:
                df_sorted['_date'] = pd.to_datetime(df_sorted[date_col], errors='coerce')
                df_sorted = df_sorted.sort_values('_date')
            except:
                pass
        else:
            df_sorted = df
        
        split_idx = int(len(df_sorted) * train_ratio)
        train = df_sorted.iloc[:split_idx].copy()
        test = df_sorted.iloc[split_idx:].copy()
        
        return train, test
    
    def mine_ah_patterns(
        self,
        df: 'pd.DataFrame',
        generations: int = None,
        progress_callback: Callable = None
    ) -> List[Dict]:
        """
        Mina patrones para Asian Handicap.
        """
        if generations is None:
            generations = self.config['generations']
        
        # Split temporal
        train_df, test_df = self.split_temporal(df)
        print(f"[INFO] Split: Train={len(train_df)}, Test={len(test_df)}")
        
        # Obtener features booleanas
        all_features = get_boolean_features(train_df)
        print(f"[INFO] Features disponibles: {len(all_features)}")
        
        patterns = []
        
        # Targets: HOME y AWAY cubrieron
        for target in ['HOME', 'AWAY']:
            for gen in range(generations):
                # Seleccionar features aleatorias
                n_feat = random.randint(
                    self.config['min_features'],
                    min(self.config['max_features'], len(all_features))
                )
                selected = random.sample(all_features, n_feat)
                
                # Evaluar en cada familia
                for family in AH_FAMILIES:
                    # Filtrar por familia
                    family_mask = train_df['ah_family'] == family
                    family_train = train_df[family_mask]
                    
                    if len(family_train) < 30:
                        continue
                    
                    # Calcular stats en train
                    train_stats = calculate_pattern_stats(
                        family_train, selected, 'ah_covered', target
                    )
                    
                    # Filtros mínimos
                    if train_stats['n'] < self.config['min_samples']:
                        continue
                    if train_stats['accuracy'] < self.config['min_accuracy']:
                        continue
                    
                    # Validar en test
                    family_mask_test = test_df['ah_family'] == family
                    family_test = test_df[family_mask_test]
                    
                    test_stats = calculate_pattern_stats(
                        family_test, selected, 'ah_covered', target
                    )
                    
                    # Verificar degradación (mínimo 20 muestras en test)
                    min_test = self.config.get('min_samples_test', 20)
                    if train_stats['roi'] > 0 and test_stats['n'] >= min_test:
                        degradation = 1 - (test_stats['roi'] / train_stats['roi']) if train_stats['roi'] > 0 else 1
                        
                        if degradation <= self.config['max_degradation']:
                            pattern = {
                                'type': 'AH',
                                'family': family,
                                'target': target,
                                'conditions': sorted(selected),
                                'train': {
                                    'n': train_stats['n'],
                                    'wins': train_stats['wins'],
                                    'accuracy': round(train_stats['accuracy'], 3),
                                    'roi': round(train_stats['roi'], 3)
                                },
                                'test': {
                                    'n': test_stats['n'],
                                    'accuracy': round(test_stats['accuracy'], 3) if test_stats['n'] > 0 else 0,
                                    'roi': round(test_stats['roi'], 3) if test_stats['n'] > 0 else 0
                                },
                                'breakdown': train_stats['breakdown']
                            }
                            
                            # Evitar duplicados
                            is_dup = any(
                                set(p['conditions']) == set(selected) and 
                                p['target'] == target and 
                                p['family'] == family
                                for p in patterns
                            )
                            
                            if not is_dup:
                                patterns.append(pattern)
                                if progress_callback:
                                    progress_callback(pattern)
                
                # Progreso
                if (gen + 1) % 1000 == 0:
                    print(f"  Gen {gen+1}/{generations}... ({len(patterns)} patrones AH)")
        
        self.ah_patterns = patterns
        return patterns
    
    def mine_ou_patterns(
        self,
        df: 'pd.DataFrame',
        generations: int = None,
        progress_callback: Callable = None
    ) -> List[Dict]:
        """
        Mina patrones para Over/Under.
        """
        if generations is None:
            generations = self.config['generations']
        
        # Split temporal
        train_df, test_df = self.split_temporal(df)
        
        # Obtener features booleanas
        all_features = get_boolean_features(train_df)
        
        patterns = []
        
        # Targets: OVER y UNDER
        for target in ['OVER', 'UNDER']:
            for gen in range(generations):
                n_feat = random.randint(
                    self.config['min_features'],
                    min(self.config['max_features'], len(all_features))
                )
                selected = random.sample(all_features, n_feat)
                
                for family in OU_FAMILIES:
                    family_mask = train_df['ou_family'] == family
                    family_train = train_df[family_mask]
                    
                    if len(family_train) < 30:
                        continue
                    
                    train_stats = calculate_pattern_stats(
                        family_train, selected, 'ou_outcome', target, 'ou_profit'
                    )
                    
                    if train_stats['n'] < self.config['min_samples']:
                        continue
                    if train_stats['accuracy'] < self.config['min_accuracy']:
                        continue
                    
                    # Validar en test
                    family_mask_test = test_df['ou_family'] == family
                    family_test = test_df[family_mask_test]
                    
                    test_stats = calculate_pattern_stats(
                        family_test, selected, 'ou_outcome', target, 'ou_profit'
                    )
                    
                    # Mínimo 20 muestras en test
                    min_test = self.config.get('min_samples_test', 20)
                    if train_stats['roi'] > 0 and test_stats['n'] >= min_test:
                        degradation = 1 - (test_stats['roi'] / train_stats['roi']) if train_stats['roi'] > 0 else 1
                        
                        if degradation <= self.config['max_degradation']:
                            pattern = {
                                'type': 'OU',
                                'family': family,
                                'target': target,
                                'conditions': sorted(selected),
                                'train': {
                                    'n': train_stats['n'],
                                    'wins': train_stats['wins'],
                                    'accuracy': round(train_stats['accuracy'], 3),
                                    'roi': round(train_stats['roi'], 3)
                                },
                                'test': {
                                    'n': test_stats['n'],
                                    'accuracy': round(test_stats['accuracy'], 3) if test_stats['n'] > 0 else 0,
                                    'roi': round(test_stats['roi'], 3) if test_stats['n'] > 0 else 0
                                }
                            }
                            
                            is_dup = any(
                                set(p['conditions']) == set(selected) and 
                                p['target'] == target and 
                                p['family'] == family
                                for p in patterns
                            )
                            
                            if not is_dup:
                                patterns.append(pattern)
                                if progress_callback:
                                    progress_callback(pattern)
                
                if (gen + 1) % 1000 == 0:
                    print(f"  Gen {gen+1}/{generations}... ({len(patterns)} patrones OU)")
        
        self.ou_patterns = patterns
        return patterns
    
    def mine_all(self, df: 'pd.DataFrame', generations: int = None) -> Dict[str, List]:
        """
        Mina todos los patrones (AH y OU).
        """
        print("\n" + "="*50)
        print("--- Minando patrones AH ---")
        self.mine_ah_patterns(df, generations)
        
        # GUARDADO INTERMEDIO DE SEGURIDAD
        print(f"\n[SAVE] Guardando {len(self.ah_patterns)} patrones AH (backup)...")
        try:
            from pathlib import Path
            from datetime import datetime
            import json
            # Crear directorio si no existe (el path viene del script principal, pero aquí improvisamos un dump temporal)
            backup_path = Path("data/patterns_v2_backup_ah.json")
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            data = {
                'generated': datetime.now().isoformat(),
                'config': self.config,
                'total_ah': len(self.ah_patterns),
                'ah_patterns': self.ah_patterns
            }
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            print("[OK] Backup AH guardado correctamente.")
        except Exception as e:
            print(f"[WARN] Error guardando backup AH: {e}")

        print("\n" + "="*50)
        print("--- Minando patrones OU ---")
        self.mine_ou_patterns(df, generations)
        
        # Ordenar por ROI de test
        self.ah_patterns = sorted(self.ah_patterns, key=lambda x: -x['test']['roi'])
        self.ou_patterns = sorted(self.ou_patterns, key=lambda x: -x['test']['roi'])
        
        print(f"\n{'='*60}")
        print(f"TOTAL: {len(self.ah_patterns)} AH + {len(self.ou_patterns)} OU")
        print(f"{'='*60}")
        
        return {
            'ah_patterns': self.ah_patterns,
            'ou_patterns': self.ou_patterns
        }
    
    def get_top_patterns(self, n: int = 20, min_test_roi: float = 0.05) -> Dict[str, List]:
        """
        Obtiene los top N patrones filtrados por ROI de test.
        """
        ah = [p for p in self.ah_patterns if p['test']['roi'] >= min_test_roi][:n]
        ou = [p for p in self.ou_patterns if p['test']['roi'] >= min_test_roi][:n]
        
        return {'ah_patterns': ah, 'ou_patterns': ou}
    
    def save(self, filepath: str):
        """
        Guarda patrones en JSON.
        """
        output = {
            'generated': datetime.now().isoformat(),
            'config': self.config,
            'total_ah': len(self.ah_patterns),
            'total_ou': len(self.ou_patterns),
            'ah_patterns': self.ah_patterns,
            'ou_patterns': self.ou_patterns
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"[SAVE] Guardado: {filepath}")
    
    def print_top(self, n: int = 20):
        """
        Imprime los top patrones.
        """
        print("\n========== TOP PATRONES AH ==========")
        for i, p in enumerate(self.ah_patterns[:n], 1):
            print(f"{i}. {p['target']} [{p['family']}]")
            print(f"   Train: {p['train']['accuracy']*100:.0f}% ({p['train']['n']}) ROI={p['train']['roi']*100:.1f}%")
            print(f"   Test:  {p['test']['accuracy']*100:.0f}% ({p['test']['n']}) ROI={p['test']['roi']*100:.1f}%")
            print(f"   Conds: {p['conditions'][:3]}...")
        
        print("\n========== TOP PATRONES O/U ==========")
        for i, p in enumerate(self.ou_patterns[:n], 1):
            print(f"{i}. {p['target']} [{p['family']}]")
            print(f"   Train: {p['train']['accuracy']*100:.0f}% ({p['train']['n']}) ROI={p['train']['roi']*100:.1f}%")
            print(f"   Test:  {p['test']['accuracy']*100:.0f}% ({p['test']['n']}) ROI={p['test']['roi']*100:.1f}%")
            print(f"   Conds: {p['conditions'][:3]}...")
