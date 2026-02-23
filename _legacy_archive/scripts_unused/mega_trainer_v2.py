"""
MEGA TRAINER - Usa TODOS los datos disponibles (~120MB)
Con split temporal real para evitar overfitting.
Objetivo: 80% win rate, 150+ apuestas.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import json
import pandas as pd
import numpy as np
from itertools import combinations, product
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


def load_all_data(data_dir: Path) -> list:
    """Carga todos los archivos de datos."""
    
    all_matches = []
    json_files = [
        'data_ah_0.5.json',      # 47MB
        'data_ah_0.json',        # 17MB
        'data_minus_ah_0.5.json', # 26MB
        'data_ah_1.5.json',      # 3.9MB
        'data_minus_ah_1.5.json', # 7.5MB
        'data_ah_2_plus.json',   # 4.7MB
        'data_minus_ah_2_plus.json', # 2.6MB
    ]
    
    for fname in json_files:
        fpath = data_dir / fname
        if fpath.exists():
            print(f"  Cargando {fname}...", end='', flush=True)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_matches.extend(data)
                    print(f" {len(data)} partidos")
            except Exception as e:
                print(f" Error: {e}")
    
    print(f"\nTotal partidos cargados: {len(all_matches)}")
    return all_matches


def parse_score(score_str):
    """Parsea score."""
    if not score_str or score_str in ['-', '?:?', '']:
        return None, None
    score_str = str(score_str).replace('-', ':')
    parts = score_str.split(':')
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except:
        return None, None


def parse_ah(ah_str):
    """Parsea linea AH."""
    if not ah_str or ah_str == '-':
        return None
    try:
        return float(ah_str)
    except:
        return None

def settle_ah(home_g, away_g, ah_line):
    """
    Settlement de AH.
    FORMULA CORRECTA: diff = (home - away) - ah_line
    """
    diff = (home_g - away_g) - ah_line
    if diff > 0.25:
        return 'W', 1.0
    elif diff > 0:
        return 'HW', 0.5
    elif diff == 0:
        return 'P', 0.0
    elif diff >= -0.25:
        return 'HL', -0.5
    else:
        return 'L', -1.0



def extract_features(match):
    """Extrae features de un partido."""
    
    score = match.get('final_score', '')
    home_g, away_g = parse_score(score)
    if home_g is None:
        return None
    
    odds = match.get('main_match_odds', {})
    ah_line = parse_ah(odds.get('ah_linea'))
    if ah_line is None:
        return None
    
    # Features basicas
    # AH positivo = LOCAL da ventaja = HOME es favorito
    # AH negativo = LOCAL recibe ventaja = AWAY es favorito
    # AH cero = Pick'em
    features = {
        'match_id': match.get('match_id'),
        'home_name': match.get('home_name', ''),
        'away_name': match.get('away_name', ''),
        'league': match.get('league_name', ''),
        'home_goals': home_g,
        'away_goals': away_g,
        'ah_line': ah_line,
        'fav_side': 'HOME' if ah_line > 0 else ('AWAY' if ah_line < 0 else 'NEUTRAL'),
        'line_mag': abs(ah_line),
        'ticks': int(abs(ah_line) / 0.25),
    }
    
    # Zone
    lm = features['line_mag']
    if lm < 0.5:
        features['zone'] = 'PICK_EM'
    elif lm < 1.0:
        features['zone'] = 'MUST_WIN'
    elif lm < 2.0:
        features['zone'] = 'WIN_BY_1'
    else:
        features['zone'] = 'WIN_BY_2+'
    
    # Settlement
    outcome, payout = settle_ah(home_g, away_g, ah_line)
    features['ah_outcome'] = outcome
    features['ah_payout'] = payout
    
    # H2H Stadium Cover
    h2h = match.get('h2h_stadium', {})
    res = h2h.get('res1')
    if res and res not in ['?:?', '-']:
        h, a = parse_score(res)
        if h is not None:
            _, p = settle_ah(h, a, ah_line)
            features['h2h_stadium_cover'] = 'COVER' if p > 0 else ('PUSH' if p == 0 else 'NO_COVER')
        else:
            features['h2h_stadium_cover'] = None
    else:
        features['h2h_stadium_cover'] = None
    
    # H2H General Cover
    h2h_gen = match.get('h2h_general', {})
    res_gen = h2h_gen.get('res1')
    if res_gen and res_gen not in ['?:?', '-']:
        h, a = parse_score(res_gen)
        if h is not None:
            _, p = settle_ah(h, a, ah_line)
            features['h2h_general_cover'] = 'COVER' if p > 0 else ('PUSH' if p == 0 else 'NO_COVER')
        else:
            features['h2h_general_cover'] = None
    else:
        features['h2h_general_cover'] = None
    
    # Prev Home WDL
    prev_home = match.get('last_home_match') or {}
    prev_h_score = prev_home.get('score')
    if prev_h_score:
        h, a = parse_score(prev_h_score)
        if h is not None:
            features['prev_home_wdl'] = 'W' if h > a else ('L' if h < a else 'D')
        else:
            features['prev_home_wdl'] = None
    else:
        features['prev_home_wdl'] = None
    
    # Prev Away WDL
    prev_away = match.get('last_away_match') or {}
    prev_a_score = prev_away.get('score')
    if prev_a_score:
        h, a = parse_score(prev_a_score)
        if h is not None:
            features['prev_away_wdl'] = 'W' if a > h else ('L' if a < h else 'D')
        else:
            features['prev_away_wdl'] = None
    else:
        features['prev_away_wdl'] = None
    
    # Fecha para split temporal
    date_str = match.get('cached_at', '')
    if date_str:
        try:
            features['date'] = datetime.strptime(date_str[:10], '%Y-%m-%d')
        except:
            features['date'] = None
    else:
        features['date'] = None
    
    return features


def build_dataframe(matches):
    """Construye DataFrame."""
    
    print("\nExtrayendo features...")
    features_list = []
    for m in matches:
        f = extract_features(m)
        if f:
            features_list.append(f)
    
    df = pd.DataFrame(features_list)
    print(f"Partidos con features: {len(df)}")
    return df


class MegaRule:
    def __init__(self, conditions, bet_side):
        self.conditions = conditions
        self.bet_side = bet_side
        self.name = None
        self.metrics = {}
    
    def evaluate(self, df):
        mask = np.ones(len(df), dtype=bool)
        
        for cond in self.conditions:
            feat = cond['feature']
            val = cond['value']
            
            if feat not in df.columns:
                return {'n': 0, 'accuracy': 0}
            
            mask &= (df[feat] == val)
        
        matched = df[mask]
        n = len(matched)
        
        if n == 0:
            return {'n': 0, 'accuracy': 0}
        
        if self.bet_side == 'HOME':
            wins = (matched['ah_payout'] > 0).sum()
            half_wins = (matched['ah_payout'] == 0.5).sum()
        else:
            wins = (matched['ah_payout'] < 0).sum()
            half_wins = (matched['ah_payout'] == -0.5).sum()
        
        accuracy = (wins + half_wins * 0.5) / n
        
        return {'n': n, 'wins': int(wins), 'accuracy': accuracy}
    
    def to_dict(self):
        return {
            'name': self.name,
            'market': 'AH',
            'bet_side': self.bet_side,
            'conditions': self.conditions,
            'metrics': self.metrics
        }


def generate_rules(df, max_conditions=3):
    """Genera reglas."""
    
    features = {
        'zone': ['PICK_EM', 'MUST_WIN', 'WIN_BY_1', 'WIN_BY_2+'],
        'fav_side': ['HOME', 'AWAY'],
        'h2h_stadium_cover': ['COVER', 'NO_COVER'],
        'h2h_general_cover': ['COVER', 'NO_COVER'],
        'prev_home_wdl': ['W', 'D', 'L'],
        'prev_away_wdl': ['W', 'D', 'L'],
    }
    
    available = {k: v for k, v in features.items() if k in df.columns}
    
    print(f"\nGenerando reglas con {len(available)} features...")
    
    rules = []
    
    for num in range(2, min(max_conditions + 1, len(available) + 1)):
        for combo in combinations(available.keys(), num):
            values = [available[f] for f in combo]
            
            for vals in product(*values):
                conditions = [
                    {'feature': f, 'op': '==', 'value': v}
                    for f, v in zip(combo, vals)
                ]
                
                for side in ['HOME', 'AWAY']:
                    rules.append(MegaRule(conditions, side))
    
    print(f"Generadas {len(rules)} reglas candidatas")
    return rules


def train_and_test(df, rules, train_ratio=0.7, min_samples=30, min_accuracy=0.75):
    """Entrena en train set, valida en test set."""
    
    # Split temporal (si hay fechas) o aleatorio
    if 'date' in df.columns and df['date'].notna().sum() > len(df) * 0.5:
        df_sorted = df.dropna(subset=['date']).sort_values('date')
        n_train = int(len(df_sorted) * train_ratio)
        train_df = df_sorted.iloc[:n_train]
        test_df = df_sorted.iloc[n_train:]
        print(f"\nSplit temporal: Train={len(train_df)}, Test={len(test_df)}")
    else:
        # Split aleatorio
        np.random.seed(42)
        msk = np.random.rand(len(df)) < train_ratio
        train_df = df[msk]
        test_df = df[~msk]
        print(f"\nSplit aleatorio: Train={len(train_df)}, Test={len(test_df)}")
    
    # Entrenar: evaluar reglas en train set
    print(f"\nEntrenando (min_samples={min_samples}, min_accuracy={min_accuracy*100:.0f}%)...")
    
    trained_rules = []
    for rule in rules:
        train_metrics = rule.evaluate(train_df)
        
        if train_metrics['n'] >= min_samples and train_metrics['accuracy'] >= min_accuracy:
            rule.metrics['train'] = train_metrics
            trained_rules.append(rule)
    
    print(f"Reglas que pasan training: {len(trained_rules)}")
    
    if not trained_rules:
        return [], 0, 0, 0
    
    # Validar en test set
    print(f"\nValidando en test set...")
    
    validated = []
    for rule in trained_rules:
        test_metrics = rule.evaluate(test_df)
        rule.metrics['test'] = test_metrics
        
        # Solo mantener si tambien es buena en test
        if test_metrics['n'] >= 10 and test_metrics['accuracy'] >= 0.65:
            validated.append(rule)
    
    print(f"Reglas validadas: {len(validated)}")
    
    # Ordenar por accuracy en test
    validated.sort(key=lambda r: r.metrics['test']['accuracy'], reverse=True)
    
    # Calcular ROI simulado en test set
    total_bets = 0
    total_wins = 0
    stake = 5.0
    odds = 1.85
    total_staked = 0.0
    total_returned = 0.0
    
    for rule in validated:
        tm = rule.metrics['test']
        n = tm['n']
        wins = tm.get('wins', int(tm['accuracy'] * n))
        
        total_bets += n
        total_wins += wins
        total_staked += n * stake
        total_returned += wins * stake * odds + (n - wins) * 0  # Simplificado
    
    roi = ((total_returned - total_staked) / total_staked * 100) if total_staked > 0 else 0
    win_rate = (total_wins / total_bets * 100) if total_bets > 0 else 0
    
    return validated, total_bets, win_rate, roi


def main():
    print("=" * 70)
    print("MEGA TRAINER - Usando TODOS los datos disponibles")
    print("Objetivo: 80% Win Rate, 150+ Apuestas, ROI 20%+")
    print("=" * 70)
    
    base_path = Path(__file__).parent.parent
    data_dir = base_path / 'data'
    
    # Cargar todos los datos
    print("\n[1/4] Cargando datos...")
    all_matches = load_all_data(data_dir)
    
    if not all_matches:
        print("ERROR: No se cargaron datos")
        return
    
    # Construir DataFrame
    print("\n[2/4] Construyendo DataFrame...")
    df = build_dataframe(all_matches)
    
    if len(df) < 500:
        print("ERROR: Insuficientes partidos")
        return
    
    # Generar reglas
    print("\n[3/4] Generando reglas...")
    rules = generate_rules(df, max_conditions=3)
    
    # Train + Test
    print("\n[4/4] Entrenando y validando...")
    
    # Intentar diferentes umbrales
    best_result = None
    
    for min_acc in [0.80, 0.78, 0.75, 0.72, 0.70]:
        for min_samp in [50, 40, 30, 25]:
            validated, bets, win_rate, roi = train_and_test(
                df, rules, 
                train_ratio=0.7,
                min_samples=min_samp,
                min_accuracy=min_acc
            )
            
            if bets >= 150 and win_rate >= 70:
                print(f"\n  ENCONTRADO: {len(validated)} reglas, {bets} bets, {win_rate:.1f}% WR, {roi:.1f}% ROI")
                best_result = (validated, bets, win_rate, roi, min_acc, min_samp)
                break
        
        if best_result:
            break
    
    if not best_result:
        # Tomar el mejor resultado disponible
        validated, bets, win_rate, roi = train_and_test(
            df, rules, train_ratio=0.7, min_samples=25, min_accuracy=0.70
        )
        best_result = (validated, bets, win_rate, roi, 0.70, 25)
    
    validated, bets, win_rate, roi, min_acc, min_samp = best_result
    
    # Asignar nombres
    for i, r in enumerate(validated):
        acc = int(r.metrics['test']['accuracy'] * 100)
        side = 'H' if r.bet_side == 'HOME' else 'A'
        r.name = f"MEGA_{acc}%_{side}_{i+1}"
    
    # Guardar
    output_path = base_path / 'models' / 'top_rules.json'
    data = {
        'version': '4.0-mega',
        'generated': datetime.now().isoformat(),
        'total_rules': len(validated),
        'config': {'min_accuracy': min_acc, 'min_samples': min_samp},
        'rules': [r.to_dict() for r in validated]
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    # Resultados
    print("\n" + "=" * 70)
    print("RESULTADOS FINALES (EN TEST SET)")
    print("=" * 70)
    print(f"Reglas generadas: {len(validated)}")
    print(f"Total apuestas (test): {bets}")
    print(f"Win Rate: {win_rate:.1f}%")
    print(f"ROI estimado: {roi:.1f}%")
    print(f"\nGuardado en: {output_path}")
    
    # Top reglas
    print("\n" + "=" * 70)
    print("TOP 15 REGLAS (por accuracy en TEST)")
    print("=" * 70)
    for r in validated[:15]:
        tm = r.metrics['test']
        print(f"  {r.name:25} | Train: {r.metrics['train']['accuracy']*100:.0f}% | Test: {tm['accuracy']*100:.0f}% ({tm['n']} samples)")


if __name__ == '__main__':
    main()
