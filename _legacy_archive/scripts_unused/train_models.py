# scripts/train_models.py
"""
Script de entrenamiento de modelos ML para predicción de apuestas AH y O/U.
Entrena modelos específicos por rango de handicap usando los datos históricos.

Uso: py scripts/train_models.py
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime

# Añadir path del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Imports de ML
try:
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.metrics import log_loss, brier_score_loss, accuracy_score
    import joblib
    import numpy as np
except ImportError:
    print("ERROR: Faltan dependencias. Ejecuta: pip install scikit-learn joblib numpy")
    sys.exit(1)


# Directorio de datos y modelos
DATA_DIR = PROJECT_ROOT / 'data'
MODELS_DIR = PROJECT_ROOT / 'models'
MODELS_DIR.mkdir(exist_ok=True)

# Mapeo de archivos de datos por bucket de handicap
DATA_FILES = {
    'ah_0': DATA_DIR / 'data_ah_0.json',
    'ah_0.5': DATA_DIR / 'data_ah_0.5.json',
    'ah_1.5': DATA_DIR / 'data_ah_1.5.json',
    'ah_2_plus': DATA_DIR / 'data_ah_2_plus.json',
    'ah_minus_0.5': DATA_DIR / 'data_minus_ah_0.5.json',
    'ah_minus_1.5': DATA_DIR / 'data_minus_ah_1.5.json',
    'ah_minus_2_plus': DATA_DIR / 'data_minus_ah_2_plus.json',
}

# Las 4 estadísticas clave
STATS_KEYS = ['Tiros', 'Tiros a Puerta', 'Ataques', 'Ataques Peligrosos']


def parse_stats_rows(stats_rows):
    """Convierte stats_rows a diccionario {label: {home: val, away: val}}"""
    result = {}
    if not stats_rows or not isinstance(stats_rows, list):
        return result
    for r in stats_rows:
        label = (r.get('label') or '').strip()
        try:
            home_val = float(r.get('home', 0) or 0)
            away_val = float(r.get('away', 0) or 0)
            result[label] = {'home': home_val, 'away': away_val}
        except (ValueError, TypeError):
            continue
    return result


def extract_features(match):
    """
    Extrae features de un partido para entrenamiento.
    Retorna dict con features o None si no hay suficientes datos.
    """
    features = {}
    
    # AH y O/U del partido
    main_odds = match.get('main_match_odds', {})
    try:
        ah_linea = float(main_odds.get('ah_linea', 0) or 0)
    except (ValueError, TypeError):
        ah_linea = 0
    try:
        ou_linea = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except (ValueError, TypeError):
        ou_linea = 2.5
    
    features['ah_linea'] = ah_linea
    features['ou_linea'] = ou_linea
    
    # Rankings
    home_standings = match.get('home_standings', {})
    away_standings = match.get('away_standings', {})
    
    try:
        home_rank = int(home_standings.get('ranking', 0) or 0)
    except (ValueError, TypeError):
        home_rank = 0
    try:
        away_rank = int(away_standings.get('ranking', 0) or 0)
    except (ValueError, TypeError):
        away_rank = 0
    
    features['ranking_diff'] = home_rank - away_rank if home_rank and away_rank else 0
    
    # Extraer stats de cada fuente
    sources = [
        ('prev_home', match.get('last_home_match', {})),
        ('prev_away', match.get('last_away_match', {})),
        ('h2h_stadium', match.get('h2h_stadium', {})),
        ('h2h_general', match.get('h2h_general', {})),
        ('h2h_col3', match.get('h2h_col3', {})),
    ]
    
    # Comparativas indirectas
    comparativas = match.get('comparativas_indirectas', {})
    sources.append(('ind_left', comparativas.get('left', {})))
    sources.append(('ind_right', comparativas.get('right', {})))
    
    total_local_pts = 0
    total_away_pts = 0
    valid_sources = 0
    
    for source_name, source_data in sources:
        if not source_data:
            # Features vacías para esta fuente
            for stat in STATS_KEYS:
                features[f'{source_name}_{stat.replace(" ", "_")}_diff'] = 0
            continue
            
        stats_rows = source_data.get('stats_rows', [])
        if not stats_rows:
            # Features vacías para esta fuente
            for stat in STATS_KEYS:
                features[f'{source_name}_{stat.replace(" ", "_")}_diff'] = 0
            continue
        
        stats_dict = parse_stats_rows(stats_rows)
        if not stats_dict:
            for stat in STATS_KEYS:
                features[f'{source_name}_{stat.replace(" ", "_")}_diff'] = 0
            continue
        
        valid_sources += 1
        
        # Calcular diferenciales para cada stat
        for stat in STATS_KEYS:
            if stat in stats_dict:
                home_val = stats_dict[stat]['home']
                away_val = stats_dict[stat]['away']
                diff = home_val - away_val
                features[f'{source_name}_{stat.replace(" ", "_")}_diff'] = diff
                
                # Sumar puntos
                if home_val > away_val:
                    total_local_pts += 1
                elif away_val > home_val:
                    total_away_pts += 1
            else:
                features[f'{source_name}_{stat.replace(" ", "_")}_diff'] = 0
    
    # Score compuesto
    features['total_local_pts'] = total_local_pts
    features['total_away_pts'] = total_away_pts
    features['score_diff'] = total_local_pts - total_away_pts
    features['valid_sources'] = valid_sources
    
    # Backtest existente (si lo hay)
    backtest = match.get('backtest_global', {})
    if backtest.get('validez'):
        features['backtest_prob_ah'] = backtest.get('prob_ah', 50) / 100
        features['backtest_prob_over'] = backtest.get('prob_over', 50) / 100
    else:
        features['backtest_prob_ah'] = 0.5
        features['backtest_prob_over'] = 0.5
    
    return features


def settle_ah_result(home_goals, away_goals, home_hcap):
    """
    Calcula resultado de apuesta AH.
    Retorna: +1 (win), +0.5 (half-win), 0 (push), -0.5 (half-loss), -1 (loss)
    """
    diff = home_goals - away_goals
    
    def settle_simple(h):
        v = diff + h
        if v > 0:
            return 1.0
        if v == 0:
            return 0.0
        return -1.0
    
    frac = abs(home_hcap) % 1
    
    # Quarter lines (0.25, 0.75)
    if abs(frac - 0.25) < 0.01:
        if home_hcap > 0:
            return 0.5 * settle_simple(0.0) + 0.5 * settle_simple(0.5)
        else:
            return 0.5 * settle_simple(0.0) + 0.5 * settle_simple(-0.5)
    
    if abs(frac - 0.75) < 0.01:
        if home_hcap > 0:
            return 0.5 * settle_simple(0.5) + 0.5 * settle_simple(1.0)
        else:
            return 0.5 * settle_simple(-0.5) + 0.5 * settle_simple(-1.0)
    
    return settle_simple(home_hcap)


def calculate_target_ah(match):
    """
    Calcula el target para AH: ¿el LOCAL cubrió el handicap?
    Retorna: 1 (sí), 0 (no), None (push o sin datos)
    """
    # Obtener resultado final
    score = match.get('final_score', match.get('score', ''))
    if not score or ':' not in score:
        return None
    
    try:
        parts = score.replace('-', ':').split(':')
        home_goals = int(parts[0])
        away_goals = int(parts[1])
    except (ValueError, IndexError):
        return None
    
    # Obtener handicap
    main_odds = match.get('main_match_odds', {})
    try:
        ah_linea = float(main_odds.get('ah_linea', 0) or 0)
    except (ValueError, TypeError):
        return None
    
    # Normalizar handicap a perspectiva LOCAL
    # En tus datos: AH positivo = Local favorito (da ventaja)
    #               AH negativo = Visitante favorito (local recibe ventaja)
    # Invertimos para cálculo: home_hcap = -ah_linea
    home_hcap = -ah_linea
    
    settle = settle_ah_result(home_goals, away_goals, home_hcap)
    
    if settle > 0:  # Win o half-win
        return 1
    elif settle < 0:  # Loss o half-loss
        return 0
    else:  # Push
        return None


def calculate_target_ou(match):
    """
    Calcula el target para O/U: ¿fue OVER?
    Retorna: 1 (over), 0 (under), None (push)
    """
    score = match.get('final_score', match.get('score', ''))
    if not score or ':' not in score:
        return None
    
    try:
        parts = score.replace('-', ':').split(':')
        home_goals = int(parts[0])
        away_goals = int(parts[1])
        total_goals = home_goals + away_goals
    except (ValueError, IndexError):
        return None
    
    main_odds = match.get('main_match_odds', {})
    try:
        ou_linea = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except (ValueError, TypeError):
        ou_linea = 2.5
    
    if total_goals > ou_linea:
        return 1  # Over
    elif total_goals < ou_linea:
        return 0  # Under
    else:
        return None  # Push


def load_training_data(data_file):
    """Carga y procesa datos de entrenamiento desde un archivo JSON."""
    if not data_file.exists():
        print(f"  ⚠️ Archivo no encontrado: {data_file}")
        return [], [], []
    
    with open(data_file, 'r', encoding='utf-8') as f:
        matches = json.load(f)
    
    X = []  # Features
    y_ah = []  # Target AH
    y_ou = []  # Target O/U
    
    for match in matches:
        # Extraer features
        features = extract_features(match)
        if not features:
            continue
        
        # Calcular targets
        target_ah = calculate_target_ah(match)
        target_ou = calculate_target_ou(match)
        
        # Solo usar si tenemos al menos un target válido
        if target_ah is None and target_ou is None:
            continue
        
        # Convertir features a lista ordenada
        feature_names = sorted(features.keys())
        feature_values = [features[k] for k in feature_names]
        
        X.append(feature_values)
        y_ah.append(target_ah if target_ah is not None else -1)  # -1 = push/ignore
        y_ou.append(target_ou if target_ou is not None else -1)
    
    return X, y_ah, y_ou


def train_model(X, y, model_name):
    """Entrena y calibra un modelo."""
    # Filtrar pushes (-1)
    valid_idx = [i for i, target in enumerate(y) if target != -1]
    if len(valid_idx) < 50:
        print(f"  ⚠️ Pocos datos para {model_name}: {len(valid_idx)} muestras")
        return None, {}
    
    X_valid = np.array([X[i] for i in valid_idx])
    y_valid = np.array([y[i] for i in valid_idx])
    
    print(f"  📊 {model_name}: {len(X_valid)} muestras (Y=1: {sum(y_valid)}, Y=0: {len(y_valid) - sum(y_valid)})")
    
    # Train/test split temporal (80/20)
    split_idx = int(len(X_valid) * 0.8)
    X_train, X_test = X_valid[:split_idx], X_valid[split_idx:]
    y_train, y_test = y_valid[:split_idx], y_valid[split_idx:]
    
    if len(X_test) < 10:
        print(f"  ⚠️ Test set muy pequeño para {model_name}")
        X_test, y_test = X_train[-20:], y_train[-20:]
    
    # Entrenar modelo base
    base_model = HistGradientBoostingClassifier(
        max_iter=100,
        max_depth=5,
        min_samples_leaf=20,
        random_state=42
    )
    base_model.fit(X_train, y_train)
    
    # Calibrar
    try:
        calibrated_model = CalibratedClassifierCV(base_model, method='isotonic', cv='prefit')
        calibrated_model.fit(X_test, y_test)
        model = calibrated_model
    except Exception as e:
        print(f"  ⚠️ Calibración falló, usando modelo base: {e}")
        model = base_model
    
    # Métricas
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)
    
    metrics = {
        'accuracy': accuracy_score(y_test, y_pred),
        'log_loss': log_loss(y_test, y_pred_proba),
        'brier': brier_score_loss(y_test, y_pred_proba),
        'n_train': len(X_train),
        'n_test': len(X_test),
    }
    
    print(f"  ✅ Acc: {metrics['accuracy']:.2%}, LogLoss: {metrics['log_loss']:.3f}, Brier: {metrics['brier']:.3f}")
    
    return model, metrics


def main():
    print("=" * 60)
    print("🎯 ENTRENAMIENTO DE MODELOS ML PARA PREDICCIÓN DE APUESTAS")
    print("=" * 60)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    all_models = {}
    all_metrics = {}
    feature_names = None
    
    for bucket_name, data_file in DATA_FILES.items():
        print(f"\n📁 Procesando: {bucket_name}")
        print("-" * 40)
        
        X, y_ah, y_ou = load_training_data(data_file)
        
        if not X:
            print(f"  ❌ Sin datos válidos para {bucket_name}")
            continue
        
        print(f"  Total partidos cargados: {len(X)}")
        
        # Guardar nombres de features (del primer modelo)
        if feature_names is None:
            # Reconstruir desde un partido de ejemplo
            with open(data_file, 'r', encoding='utf-8') as f:
                sample = json.load(f)[0]
            features = extract_features(sample)
            feature_names = sorted(features.keys())
        
        # Entrenar modelo AH
        model_ah, metrics_ah = train_model(X, y_ah, f"{bucket_name}_AH")
        if model_ah:
            all_models[f'{bucket_name}_ah'] = model_ah
            all_metrics[f'{bucket_name}_ah'] = metrics_ah
        
        # Entrenar modelo O/U
        model_ou, metrics_ou = train_model(X, y_ou, f"{bucket_name}_OU")
        if model_ou:
            all_models[f'{bucket_name}_ou'] = model_ou
            all_metrics[f'{bucket_name}_ou'] = metrics_ou
    
    # Guardar modelos
    print("\n" + "=" * 60)
    print("💾 GUARDANDO MODELOS")
    print("=" * 60)
    
    for model_name, model in all_models.items():
        model_path = MODELS_DIR / f'{model_name}.joblib'
        joblib.dump(model, model_path)
        print(f"  ✅ Guardado: {model_path.name}")
    
    # Guardar feature names
    feature_names_path = MODELS_DIR / 'feature_names.json'
    with open(feature_names_path, 'w', encoding='utf-8') as f:
        json.dump(feature_names, f, indent=2)
    print(f"  ✅ Guardado: feature_names.json")
    
    # Guardar métricas
    metrics_path = MODELS_DIR / 'training_metrics.json'
    with open(metrics_path, 'w', encoding='utf-8') as f:
        json.dump(all_metrics, f, indent=2)
    print(f"  ✅ Guardado: training_metrics.json")
    
    # Resumen final
    print("\n" + "=" * 60)
    print("📊 RESUMEN DE ENTRENAMIENTO")
    print("=" * 60)
    print(f"Total modelos entrenados: {len(all_models)}")
    for name, metrics in all_metrics.items():
        print(f"  {name}: Acc={metrics['accuracy']:.1%}, N={metrics['n_train']+metrics['n_test']}")
    
    print("\n✅ Entrenamiento completado!")
    print(f"   Modelos guardados en: {MODELS_DIR}")


if __name__ == '__main__':
    main()
