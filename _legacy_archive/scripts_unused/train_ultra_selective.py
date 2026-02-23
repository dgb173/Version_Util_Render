"""
Ultra Selective Trainer - Solo reglas con >90% accuracy
Máxima selectividad para ROI 20%+
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import pandas as pd
import numpy as np
from itertools import combinations, product

from scripts.features_builder import build_features_dataframe, discretize_features


class SelectiveRule:
    def __init__(self, conditions, market, bet_side):
        self.conditions = conditions
        self.market = market
        self.bet_side = bet_side
        self.name = None
        self.metrics = {}
    
    def evaluate(self, df):
        mask = np.ones(len(df), dtype=bool)
        
        for cond in self.conditions:
            feat = cond['feature']
            op = cond['op']
            val = cond['value']
            
            if feat not in df.columns:
                return {'n': 0, 'accuracy': 0, 'payout': 0}
            
            if op == '==':
                mask &= (df[feat] == val)
            elif op == '>':
                mask &= (df[feat] > val)
            elif op == '<':
                mask &= (df[feat] < val)
        
        matched_df = df[mask]
        n = len(matched_df)
        
        if n == 0:
            return {'n': 0, 'accuracy': 0, 'payout': 0}
        
        if self.bet_side == 'HOME':
            payouts = matched_df['ah_payout']
            wins = (payouts > 0).sum()
            half_wins = (payouts == 0.5).sum()
        else:
            payouts = matched_df['ah_payout']
            wins = (payouts < 0).sum()
            half_wins = (payouts == -0.5).sum()
        
        # Accuracy incluyendo half wins como 0.5
        accuracy = (wins + half_wins * 0.5) / n
        
        return {
            'n': n,
            'wins': int(wins),
            'half_wins': int(half_wins),
            'accuracy': accuracy,
        }
    
    def to_dict(self):
        return {
            'name': self.name,
            'market': self.market,
            'bet_side': self.bet_side,
            'conditions': self.conditions,
            'metrics': self.metrics
        }


def generate_all_rules(df, max_conditions=4):
    """Genera reglas con más combinaciones."""
    
    rules = []
    
    features = {
        'zone': ['PICK_EM', 'MUST_WIN', 'WIN_BY_1', 'WIN_BY_2'],
        'fav_side': ['HOME', 'AWAY'],
        'h2h_stadium_cover': ['COVER', 'NO_COVER'],
        'h2h_general_cover': ['COVER', 'NO_COVER'],
        'prev_home_wdl': ['W', 'D', 'L'],
        'prev_away_wdl': ['W', 'D', 'L'],
    }
    
    available = {k: v for k, v in features.items() if k in df.columns}
    
    print(f"Generando reglas selectivas con {len(available)} features...")
    
    # 2-4 condiciones
    for num_conds in range(2, min(max_conditions + 1, len(available) + 1)):
        for feature_combo in combinations(available.keys(), num_conds):
            value_lists = [available[f] for f in feature_combo]
            
            for value_combo in product(*value_lists):
                conditions = [
                    {'feature': f, 'op': '==', 'value': v}
                    for f, v in zip(feature_combo, value_combo)
                ]
                
                for side in ['HOME', 'AWAY']:
                    rule = SelectiveRule(conditions, 'AH', side)
                    rules.append(rule)
    
    print(f"Generadas {len(rules)} reglas candidatas")
    return rules


def filter_ultra_selective(rules, df, min_samples=15, min_accuracy=0.90):
    """Filtra solo reglas ultra-selectivas."""
    
    print(f"\nFiltrando reglas (min_samples={min_samples}, min_accuracy={min_accuracy*100:.0f}%)...")
    
    selected = []
    
    for rule in rules:
        metrics = rule.evaluate(df)
        rule.metrics = metrics
        
        if metrics['n'] >= min_samples and metrics['accuracy'] >= min_accuracy:
            selected.append(rule)
    
    selected.sort(key=lambda r: r.metrics['accuracy'], reverse=True)
    
    print(f"Encontradas {len(selected)} reglas ultra-selectivas")
    return selected


def main():
    print("=" * 70)
    print("ULTRA SELECTIVE TRAINER - Objetivo: 20%+ ROI")
    print("=" * 70)
    
    base_path = Path(__file__).parent.parent
    json_path = base_path / 'training_data_1465.json'
    
    if not json_path.exists():
        print(f"ERROR: No existe {json_path}")
        return
    
    # Features
    print("\n[1/4] Extrayendo features...")
    df = build_features_dataframe(str(json_path))
    df = discretize_features(df)
    print(f"Partidos: {len(df)}")
    
    # Generar reglas
    print("\n[2/4] Generando reglas...")
    rules = generate_all_rules(df, max_conditions=4)
    
    # Filtrar ultra-selectivas
    print("\n[3/4] Filtrando reglas ultra-selectivas...")
    
    # Intentar 90%+ primero
    selected = filter_ultra_selective(rules, df, min_samples=15, min_accuracy=0.90)
    
    if len(selected) < 3:
        print("Muy pocas al 90%, bajando a 88%...")
        selected = filter_ultra_selective(rules, df, min_samples=12, min_accuracy=0.88)
    
    if len(selected) == 0:
        print("ERROR: No se encontraron reglas ultra-selectivas")
        return
    
    # Asignar nombres
    for i, rule in enumerate(selected):
        acc = int(rule.metrics['accuracy'] * 100)
        side = 'H' if rule.bet_side == 'HOME' else 'A'
        rule.name = f"ULTRA_{acc}%_{side}_{i+1}"
    
    # Guardar
    print("\n[4/4] Guardando...")
    output_path = base_path / 'models' / 'top_rules.json'
    data = {
        'version': '3.0-ultra-selective',
        'generated': pd.Timestamp.now().isoformat(),
        'total_rules': len(selected),
        'rules': [r.to_dict() for r in selected]
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Guardadas {len(selected)} reglas en: {output_path}")
    
    # Resumen
    print("\n" + "=" * 70)
    print("REGLAS ULTRA-SELECTIVAS GENERADAS")
    print("=" * 70)
    for i, r in enumerate(selected[:20], 1):
        m = r.metrics
        print(f"{i:2}. {r.name:25} | Acc: {m['accuracy']*100:.1f}% | Samples: {m['n']:3}")
    
    avg_acc = sum(r.metrics['accuracy'] for r in selected) / len(selected)
    print(f"\nTotal: {len(selected)} reglas")
    print(f"Accuracy promedio: {avg_acc*100:.1f}%")
    print("\nEjecuta backtest_training.py para verificar el ROI")


if __name__ == '__main__':
    main()
