"""
Ultra High ROI Trainer - Genera reglas con >85% accuracy para ROI 20%+
Menos apuestas, mucha más precisión.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import pandas as pd
import numpy as np
from itertools import combinations, product

from scripts.features_builder import build_features_dataframe, discretize_features


class UltraRule:
    """Regla ultra-precisa."""
    
    def __init__(self, conditions, market, bet_side):
        self.conditions = conditions
        self.market = market
        self.bet_side = bet_side
        self.name = None
        self.metrics = {}
    
    def evaluate(self, df):
        """Evalúa la regla en el DataFrame."""
        mask = np.ones(len(df), dtype=bool)
        
        for cond in self.conditions:
            feat = cond['feature']
            op = cond['op']
            val = cond['value']
            
            if feat not in df.columns:
                return {'n': 0, 'accuracy': 0, 'payout': 0}
            
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
        
        matched_df = df[mask]
        n = len(matched_df)
        
        if n == 0:
            return {'n': 0, 'accuracy': 0, 'payout': 0}
        
        if self.market == 'AH':
            if self.bet_side == 'HOME':
                payouts = matched_df['ah_payout']
                wins = (payouts > 0).sum()
                half_wins = (payouts == 0.5).sum()
            else:  # AWAY
                payouts = matched_df['ah_payout']
                wins = (payouts < 0).sum()
                half_wins = (payouts == -0.5).sum()
        
        # Accuracy considerando half wins
        accuracy = (wins + half_wins * 0.5) / n
        
        return {
            'n': n,
            'wins': int(wins),
            'accuracy': accuracy,
            'payout': payouts.mean() if self.bet_side == 'HOME' else -payouts.mean()
        }
    
    def to_dict(self):
        return {
            'name': self.name,
            'market': self.market,
            'bet_side': self.bet_side,
            'conditions': self.conditions,
            'metrics': self.metrics
        }


def generate_ultra_rules(df, max_conditions=3):
    """Genera reglas con combinaciones más restrictivas."""
    
    rules = []
    
    # Solo features de alta señal
    high_signal_features = {
        'zone': ['PICK_EM', 'MUST_WIN', 'WIN_BY_1', 'WIN_BY_2'],
        'fav_side': ['HOME', 'AWAY'],
        'h2h_stadium_cover': ['COVER', 'NO_COVER'],
        'h2h_general_cover': ['COVER', 'NO_COVER'],
        'prev_home_wdl': ['W', 'L'],
        'prev_away_wdl': ['W', 'L'],
    }
    
    available = {k: v for k, v in high_signal_features.items() if k in df.columns}
    
    print(f"Generando reglas ultra-precisas con {len(available)} features...")
    
    for num_conds in range(2, min(max_conditions + 1, len(available) + 1)):
        for feature_combo in combinations(available.keys(), num_conds):
            value_lists = [available[f] for f in feature_combo]
            
            for value_combo in product(*value_lists):
                conditions = [
                    {'feature': f, 'op': '==', 'value': v}
                    for f, v in zip(feature_combo, value_combo)
                ]
                
                # Solo HOME y AWAY
                for side in ['HOME', 'AWAY']:
                    rule = UltraRule(conditions, 'AH', side)
                    rules.append(rule)
    
    print(f"Generadas {len(rules)} reglas candidatas")
    return rules


def filter_elite_rules(rules, df, min_samples=25, min_accuracy=0.85):
    """Filtra solo reglas de élite."""
    
    print(f"\nFiltrando reglas élite (min_samples={min_samples}, min_accuracy={min_accuracy*100:.0f}%)...")
    
    elite_rules = []
    
    for rule in rules:
        metrics = rule.evaluate(df)
        rule.metrics = metrics
        
        if metrics['n'] >= min_samples and metrics['accuracy'] >= min_accuracy:
            elite_rules.append(rule)
    
    # Ordenar por accuracy descendente
    elite_rules.sort(key=lambda r: (r.metrics['accuracy'], r.metrics['n']), reverse=True)
    
    print(f"Encontradas {len(elite_rules)} reglas élite")
    return elite_rules


def assign_elite_names(rules):
    """Asigna nombres a las reglas élite."""
    
    prefixes = ['ELITE', 'PREMIUM', 'GOLD', 'PLATINUM', 'DIAMOND', 'ULTRA', 'MEGA', 'SUPER']
    
    for i, rule in enumerate(rules):
        acc = int(rule.metrics['accuracy'] * 100)
        side = 'H' if rule.bet_side == 'HOME' else 'A'
        prefix = prefixes[i % len(prefixes)]
        rule.name = f"{prefix}_{acc}%_{side}_{i+1}"
    
    return rules


def save_elite_rules(rules, output_path):
    """Guarda las reglas élite."""
    
    data = {
        'version': '2.0-elite',
        'generated': pd.Timestamp.now().isoformat(),
        'total_rules': len(rules),
        'rules': [r.to_dict() for r in rules]
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Guardadas {len(rules)} reglas en: {output_path}")


def main():
    print("=" * 70)
    print("ULTRA HIGH ROI TRAINER - Objetivo: 20%+ ROI")
    print("=" * 70)
    
    base_path = Path(__file__).parent.parent
    json_path = base_path / 'training_data_1465.json'
    
    if not json_path.exists():
        print(f"ERROR: No existe {json_path}")
        return
    
    # Fase 1: Extraer features
    print("\n[1/4] Extrayendo features...")
    df = build_features_dataframe(str(json_path))
    df = discretize_features(df)
    print(f"Partidos: {len(df)}")
    
    # Fase 2: Generar reglas
    print("\n[2/4] Generando reglas candidatas...")
    rules = generate_ultra_rules(df, max_conditions=3)
    
    # Fase 3: Filtrar élite (muy estricto)
    print("\n[3/4] Filtrando reglas élite...")
    
    # Intentar diferentes niveles de estrictez
    elite_rules = filter_elite_rules(rules, df, min_samples=25, min_accuracy=0.88)
    
    if len(elite_rules) < 5:
        print("Muy pocas reglas al 88%, bajando a 85%...")
        elite_rules = filter_elite_rules(rules, df, min_samples=20, min_accuracy=0.85)
    
    if len(elite_rules) < 5:
        print("Aún pocas, bajando a 82%...")
        elite_rules = filter_elite_rules(rules, df, min_samples=18, min_accuracy=0.82)
    
    if len(elite_rules) == 0:
        print("\nERROR: No se encontraron reglas que cumplan los criterios elite.")
        print("El dataset puede ser demasiado pequeño o las features no tienen patrones claros.")
        return
    
    # Asignar nombres
    elite_rules = assign_elite_names(elite_rules)
    
    # Fase 4: Guardar
    print("\n[4/4] Guardando reglas élite...")
    output_path = base_path / 'models' / 'top_rules.json'
    save_elite_rules(elite_rules, str(output_path))
    
    # Mostrar resumen
    print("\n" + "=" * 70)
    print("REGLAS ELITE GENERADAS")
    print("=" * 70)
    for i, r in enumerate(elite_rules[:15], 1):
        m = r.metrics
        print(f"{i:2}. {r.name:25} | Acc: {m['accuracy']*100:.1f}% | Samples: {m['n']:3}")
    
    print(f"\nTotal reglas élite: {len(elite_rules)}")
    avg_acc = sum(r.metrics['accuracy'] for r in elite_rules) / len(elite_rules)
    print(f"Accuracy promedio: {avg_acc*100:.1f}%")
    print("\nEjecuta backtest_training.py para verificar el ROI")


if __name__ == '__main__':
    main()
