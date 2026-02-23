"""
Rule Miner - Genera y evalúa miles de reglas de apuestas
mediante combinatoria y árboles de decisión.

Las reglas que pasen los filtros se guardan con nombres descriptivos.
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from itertools import combinations, product
from collections import defaultdict
import warnings
import sys
warnings.filterwarnings('ignore')

# Force UTF-8 output for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Nombres creativos para las reglas
RULE_NAMES = {
    'AH': {
        'prefix': ['🔥 Fuego', '⚡ Rayo', '🎯 Sniper', '💎 Diamante', '🚀 Cohete', 
                   '👑 Rey', '🦁 León', '🐉 Dragón', '⭐ Estrella', '🏆 Campeón',
                   '🎪 Circo', '🌊 Tsunami', '🌪️ Tornado', '☄️ Meteoro', '🔮 Oráculo'],
        'suffix': ['Seguro', 'Premium', 'Elite', 'Gold', 'Master', 'Pro', 'Ultra', 'Max']
    },
    'OU': {
        'prefix': ['📈 Over', '📉 Under', '🎰 Casino', '💰 Banco', '🎲 Dados'],
        'suffix': ['Lock', 'Score', 'Goals', 'Net', 'Total']
    }
}


class Rule:
    """Representa una regla de apuestas."""
    
    def __init__(self, conditions: List[Dict], market: str, bet_side: str, name: str = None):
        self.conditions = conditions  # [{'feature': 'x', 'op': '==', 'value': 'y'}, ...]
        self.market = market  # 'AH' o 'OU'
        self.bet_side = bet_side  # 'HOME'/'AWAY' o 'OVER'/'UNDER'
        self.name = name
        self.metrics = {}
    
    def evaluate(self, df: pd.DataFrame) -> Dict:
        """Evalúa la regla en un DataFrame (Vectorizado)."""
        
        # Máscara inicial True
        mask = np.ones(len(df), dtype=bool)
        
        for cond in self.conditions:
            feat = cond['feature']
            op = cond['op']
            val = cond['value']
            
            if feat not in df.columns:
                return {'n': 0, 'accuracy': 0, 'payout': 0}
            
            # Vectorized comparisons
            if op == '==':
                mask &= (df[feat] == val)
            elif op == '!=':
                mask &= (df[feat] != val)
            elif op == '>':
                mask &= (df[feat] > val)
            elif op == '>=':
                mask &= (df[feat] >= val)
            elif op == '<':
                mask &= (df[feat] < val)
            elif op == '<=':
                mask &= (df[feat] <= val)
            elif op == 'in':
                mask &= (df[feat].isin(val))
        
        # Filtrar
        matched_df = df[mask]
        n = len(matched_df)
        
        if n == 0:
            return {'n': 0, 'accuracy': 0, 'payout': 0}
        
        if self.market == 'AH':
            if self.bet_side == 'HOME':
                # Payouts are already correct for HOME perspective
                payouts = matched_df['ah_payout']
                wins = (payouts > 0).sum()
                pushes = (payouts == 0).sum()
                mean_payout = payouts.mean()
            else:  # AWAY
                # Invert logic: Win if payout < 0
                payouts = matched_df['ah_payout']
                wins = (payouts < 0).sum()
                pushes = (payouts == 0).sum()
                # Payout for away is inverted home payout
                mean_payout = -(payouts.mean())
                
        else:  # OU
            if self.bet_side == 'OVER':
                wins = matched_df['ou_outcome'].isin(['OVER_W', 'OVER_HW']).sum()
            else:
                wins = matched_df['ou_outcome'].isin(['UNDER_W', 'UNDER_HW']).sum()
            pushes = (matched_df['ou_outcome'] == 'PUSH').sum()
            mean_payout = matched_df['ou_payout'].mean() if pd.notna(matched_df['ou_payout']).any() else 0
        
        accuracy = wins / n
        
        return {
            'n': n,
            'wins': int(wins),
            'pushes': int(pushes),
            'accuracy': accuracy,
            'payout': mean_payout
        }
    
    def to_dict(self) -> Dict:
        """Convierte a diccionario para JSON."""
        return {
            'name': self.name,
            'market': self.market,
            'bet_side': self.bet_side,
            'conditions': self.conditions,
            'metrics': self.metrics
        }
    
    def to_readable(self) -> str:
        """Genera descripción legible de la regla."""
        parts = []
        for c in self.conditions:
            parts.append(f"{c['feature']} {c['op']} {c['value']}")
        return " AND ".join(parts)


def generate_combinatorial_rules(df: pd.DataFrame, max_conditions: int = 3) -> List[Rule]:
    """Genera reglas mediante combinatoria de features discretizadas."""
    
    rules = []
    
    # Features categóricas a usar
    categorical_features = {
        'zone': ['PICK_EM', 'MUST_WIN', 'WIN_BY_1', 'WIN_BY_2'],
        'fav_side': ['HOME', 'AWAY'],
        'h2h_stadium_cover': ['COVER', 'PUSH', 'NO_COVER'],
        'h2h_general_cover': ['COVER', 'PUSH', 'NO_COVER'],
        'prev_home_wdl': ['W', 'D', 'L'],
        'prev_away_wdl': ['W', 'D', 'L'],
        'ind_local_localia': ['H', 'A'],
        'ind_visitante_localia': ['H', 'A'],
    }
    
    # Features binned
    binned_features = {
        'ticks_bin': ['low', 'mid', 'high', 'very_high'],
        'delta_bin': ['strong_drop', 'drop', 'rise', 'strong_rise'],
        'prev_home_danger_diff_bin': ['muy_neg', 'neg', 'neutro', 'pos', 'muy_pos'],
        'prev_away_danger_diff_bin': ['muy_neg', 'neg', 'neutro', 'pos', 'muy_pos'],
    }
    
    all_features = {**categorical_features, **binned_features}
    
    # Filtrar a las que existen en el DataFrame
    available = {k: v for k, v in all_features.items() if k in df.columns}
    
    print(f"🔧 Generando reglas con {len(available)} features...")
    
    # Generar combinaciones de 2-3 features
    for num_conds in range(2, min(max_conditions + 1, len(available) + 1)):
        for feature_combo in combinations(available.keys(), num_conds):
            # Generar todas las combinaciones de valores
            value_lists = [available[f] for f in feature_combo]
            
            for value_combo in product(*value_lists):
                conditions = [
                    {'feature': f, 'op': '==', 'value': v}
                    for f, v in zip(feature_combo, value_combo)
                ]
                
                # Crear regla para HOME
                rule_home = Rule(conditions, 'AH', 'HOME')
                rules.append(rule_home)
                
                # Crear regla para AWAY
                rule_away = Rule(conditions, 'AH', 'AWAY')
                rules.append(rule_away)
    
    print(f"   Generadas {len(rules)} reglas candidatas")
    return rules


def generate_tree_rules(df: pd.DataFrame) -> List[Rule]:
    """Genera reglas desde árboles de decisión."""
    
    try:
        from sklearn.tree import DecisionTreeClassifier
        from sklearn.preprocessing import LabelEncoder
    except ImportError:
        print("⚠️ sklearn no disponible, saltando árbol de decisión")
        return []
    
    rules = []
    
    # Preparar features numéricas
    feature_cols = ['ticks', 'delta_ticks', 'prev_home_danger_diff', 'prev_away_danger_diff',
                    'prev_home_shots_diff', 'prev_away_shots_diff', 'rank_diff']
    
    available_cols = [c for c in feature_cols if c in df.columns]
    
    # Target: si el favorito cubrió
    df_clean = df.dropna(subset=['ah_payout'] + available_cols)
    
    if len(df_clean) < 100:
        return []
    
    X = df_clean[available_cols].fillna(0)
    y = (df_clean['ah_payout'] > 0).astype(int)
    
    # Entrenar árbol poco profundo
    tree = DecisionTreeClassifier(max_depth=4, min_samples_leaf=20, random_state=42)
    tree.fit(X, y)
    
    # Extraer paths del árbol
    tree_rules = extract_tree_paths(tree, available_cols)
    
    for conditions in tree_rules:
        rule = Rule(conditions, 'AH', 'HOME')
        rules.append(rule)
    
    print(f"   Extraídas {len(rules)} reglas del árbol")
    return rules


def extract_tree_paths(tree, feature_names: List[str]) -> List[List[Dict]]:
    """Extrae paths del árbol como condiciones."""
    from sklearn.tree import _tree
    
    rules = []
    tree_ = tree.tree_
    feature_name = [feature_names[i] if i != _tree.TREE_UNDEFINED else "undefined!" 
                    for i in tree_.feature]
    
    def recurse(node, path):
        if tree_.feature[node] != _tree.TREE_UNDEFINED:
            name = feature_name[node]
            threshold = tree_.threshold[node]
            
            # Left branch: <= threshold
            left_path = path + [{'feature': name, 'op': '<=', 'value': round(threshold, 2)}]
            recurse(tree_.children_left[node], left_path)
            
            # Right branch: > threshold
            right_path = path + [{'feature': name, 'op': '>', 'value': round(threshold, 2)}]
            recurse(tree_.children_right[node], right_path)
        else:
            # Leaf node
            if len(path) >= 2:
                # Solo guardar si hay buena proporción de clase positiva
                value = tree_.value[node]
                if value[0][1] / (value[0][0] + value[0][1]) > 0.6:
                    rules.append(path)
    
    recurse(0, [])
    return rules


def filter_and_rank_rules(rules: List[Rule], df: pd.DataFrame, 
                          min_samples: int = 20, min_accuracy: float = 0.80) -> List[Rule]:
    """Filtra y rankea reglas según criterios de calidad."""
    
    print(f"\n📊 Evaluando {len(rules)} reglas...")
    
    valid_rules = []
    
    for i, rule in enumerate(rules):
        if i % 500 == 0:
            print(f"   Evaluando regla {i}/{len(rules)}...")
        
        metrics = rule.evaluate(df)
        rule.metrics = metrics
        
        # Filtros de calidad
        if metrics['n'] >= min_samples and metrics['accuracy'] >= min_accuracy:
            valid_rules.append(rule)
    
    # Ordenar por accuracy y luego por muestras
    valid_rules.sort(key=lambda r: (r.metrics['accuracy'], r.metrics['n']), reverse=True)
    
    print(f"✅ {len(valid_rules)} reglas pasan filtros (≥{min_samples} muestras, ≥{min_accuracy*100:.0f}% accuracy)")
    
    return valid_rules


def assign_creative_names(rules: List[Rule]) -> List[Rule]:
    """Asigna nombres creativos y descriptivos a las reglas."""
    
    import random
    
    for i, rule in enumerate(rules):
        market = rule.market
        prefixes = RULE_NAMES[market]['prefix']
        suffixes = RULE_NAMES[market]['suffix']
        
        # Elegir basado en características
        prefix = prefixes[i % len(prefixes)]
        suffix = suffixes[i % len(suffixes)]
        
        # Añadir zona si está en condiciones
        zone_name = ''
        for cond in rule.conditions:
            if cond['feature'] == 'zone':
                zone_map = {
                    'PICK_EM': 'Duelo',
                    'MUST_WIN': 'Victoria',
                    'WIN_BY_1': 'Gol+',
                    'WIN_BY_2': '2Goles+'
                }
                zone_name = zone_map.get(cond['value'], '')
                break
        
        side_emoji = '🏠' if rule.bet_side == 'HOME' else '✈️'
        
        rule.name = f"{prefix} {zone_name} {suffix} {side_emoji}".strip()
        rule.name = rule.name.replace('  ', ' ')
    
    return rules


def save_rules_to_json(rules: List[Rule], output_path: str):
    """Guarda las reglas en formato JSON/DSL."""
    
    rules_data = {
        'version': '1.0',
        'generated': pd.Timestamp.now().isoformat(),
        'total_rules': len(rules),
        'rules': [r.to_dict() for r in rules]
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(rules_data, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Guardadas {len(rules)} reglas en: {output_path}")


def generate_report(rules: List[Rule], output_path: str):
    """Genera informe de las mejores reglas."""
    
    lines = [
        "# 📊 Informe de Reglas Generadas",
        "",
        f"**Total reglas válidas**: {len(rules)}",
        "",
        "## 🏆 Top 50 Reglas",
        ""
    ]
    
    for i, rule in enumerate(rules[:50], 1):
        m = rule.metrics
        lines.append(f"### {i}. {rule.name}")
        lines.append(f"- **Mercado**: {rule.market} → {rule.bet_side}")
        lines.append(f"- **Muestras**: {m['n']}")
        lines.append(f"- **Accuracy**: {m['accuracy']*100:.1f}%")
        lines.append(f"- **Condiciones**: `{rule.to_readable()}`")
        lines.append("")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"📄 Informe guardado en: {output_path}")


def main():
    """Pipeline principal de generación de reglas."""
    
    base_path = Path(__file__).parent.parent
    features_path = base_path / 'data' / 'features_matrix.csv'
    
    if not features_path.exists():
        print("❌ No se encontró features_matrix.csv")
        print("   Ejecuta primero: python scripts/features_builder.py")
        return
    
    print("📥 Cargando features...")
    df = pd.read_csv(features_path)
    print(f"   {len(df)} partidos cargados")
    
    # Generar reglas
    rules = []
    rules.extend(generate_combinatorial_rules(df, max_conditions=3))
    rules.extend(generate_tree_rules(df))
    
    # Filtrar y rankear
    # Primero intentar 80%, si no hay suficientes bajar a 75%
    valid_rules = filter_and_rank_rules(rules, df, min_samples=20, min_accuracy=0.80)
    
    if len(valid_rules) < 10:
        print("⚠️ Pocas reglas con 80%, bajando a 75%...")
        valid_rules = filter_and_rank_rules(rules, df, min_samples=30, min_accuracy=0.75)
    
    if len(valid_rules) < 5:
        print("⚠️ Aún pocas reglas, bajando a 70%...")
        valid_rules = filter_and_rank_rules(rules, df, min_samples=30, min_accuracy=0.70)
    
    # Asignar nombres creativos
    valid_rules = assign_creative_names(valid_rules)
    
    # Guardar
    output_dir = base_path / 'models'
    output_dir.mkdir(exist_ok=True)
    
    save_rules_to_json(valid_rules, str(output_dir / 'top_rules.json'))
    generate_report(valid_rules, str(output_dir / 'rules_report.md'))
    
    print("\n✨ Proceso completado!")
    print(f"   - Reglas generadas: {len(rules)}")
    print(f"   - Reglas válidas: {len(valid_rules)}")


if __name__ == '__main__':
    main()
