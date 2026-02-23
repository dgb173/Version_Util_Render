# src/modules/ml_predictor.py
"""
Módulo predictor ML que usa los modelos entrenados para generar predicciones
con porcentajes y justificaciones.
"""

import json
from pathlib import Path
from typing import Dict, Optional, Any

# Intentar importar dependencias ML
try:
    import joblib
    import numpy as np
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False

# Directorio de modelos
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = PROJECT_ROOT / 'models'

# Las 4 estadísticas clave
STATS_KEYS = ['Tiros', 'Tiros a Puerta', 'Ataques', 'Ataques Peligrosos']

# Cache de modelos cargados
_models_cache = {}
_feature_names = None


def _get_ah_bucket(ah_value: float) -> str:
    """Determina el bucket de handicap para elegir el modelo correcto."""
    abs_ah = abs(ah_value)
    
    if ah_value >= 0:
        if abs_ah < 0.375:
            return 'ah_0'
        elif abs_ah < 1.0:
            return 'ah_0.5'
        elif abs_ah < 1.75:
            return 'ah_1.5'
        else:
            return 'ah_2_plus'
    else:
        if abs_ah < 1.0:
            return 'ah_minus_0.5'
        elif abs_ah < 1.75:
            return 'ah_minus_1.5'
        else:
            return 'ah_minus_2_plus'


def _load_model(model_name: str):
    """Carga un modelo del cache o del disco."""
    global _models_cache
    
    if model_name in _models_cache:
        return _models_cache[model_name]
    
    model_path = MODELS_DIR / f'{model_name}.joblib'
    if not model_path.exists():
        return None
    
    try:
        model = joblib.load(model_path)
        _models_cache[model_name] = model
        return model
    except Exception:
        return None


def _get_feature_names() -> list:
    """Obtiene los nombres de features usados en el entrenamiento."""
    global _feature_names
    
    if _feature_names is not None:
        return _feature_names
    
    feature_path = MODELS_DIR / 'feature_names.json'
    if not feature_path.exists():
        return []
    
    try:
        with open(feature_path, 'r', encoding='utf-8') as f:
            _feature_names = json.load(f)
        return _feature_names
    except Exception:
        return []


def parse_stats_rows(stats_rows) -> Dict:
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


def extract_features(match: Dict) -> Dict[str, float]:
    """Extrae features de un partido para predicción."""
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
    home_standings = match.get('home_standings', {}) or {}
    away_standings = match.get('away_standings', {}) or {}
    
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
    comparativas = match.get('comparativas_indirectas', {}) or {}
    sources.append(('ind_left', comparativas.get('left', {})))
    sources.append(('ind_right', comparativas.get('right', {})))
    
    total_local_pts = 0
    total_away_pts = 0
    valid_sources = 0
    
    justifications = []
    
    for source_name, source_data in sources:
        if not source_data:
            for stat in STATS_KEYS:
                features[f'{source_name}_{stat.replace(" ", "_")}_diff'] = 0
            continue
        
        stats_rows = source_data.get('stats_rows', [])
        if not stats_rows:
            for stat in STATS_KEYS:
                features[f'{source_name}_{stat.replace(" ", "_")}_diff'] = 0
            continue
        
        stats_dict = parse_stats_rows(stats_rows)
        if not stats_dict:
            for stat in STATS_KEYS:
                features[f'{source_name}_{stat.replace(" ", "_")}_diff'] = 0
            continue
        
        valid_sources += 1
        source_local = 0
        source_away = 0
        
        for stat in STATS_KEYS:
            if stat in stats_dict:
                home_val = stats_dict[stat]['home']
                away_val = stats_dict[stat]['away']
                diff = home_val - away_val
                features[f'{source_name}_{stat.replace(" ", "_")}_diff'] = diff
                
                if home_val > away_val:
                    total_local_pts += 1
                    source_local += 1
                elif away_val > home_val:
                    total_away_pts += 1
                    source_away += 1
            else:
                features[f'{source_name}_{stat.replace(" ", "_")}_diff'] = 0
        
        # Añadir justificación por fuente
        if source_local > source_away:
            justifications.append(f'{source_name}: LOCAL+{source_local}')
        elif source_away > source_local:
            justifications.append(f'{source_name}: VISITA+{source_away}')
    
    features['total_local_pts'] = total_local_pts
    features['total_away_pts'] = total_away_pts
    features['score_diff'] = total_local_pts - total_away_pts
    features['valid_sources'] = valid_sources
    
    # Backtest removido
    features['backtest_prob_ah'] = 0.5
    features['backtest_prob_over'] = 0.5
    
    return features, justifications


def predict(match: Dict) -> Dict[str, Any]:
    """
    Genera predicción ML para un partido.
    
    Returns:
        {
            'ah_prediction': {
                'pick': 'LOCAL' | 'VISITA',
                'probability': float (0-100),
                'confidence': 'high' | 'medium' | 'low',
                'justification': str
            },
            'ou_prediction': {
                'pick': 'OVER' | 'UNDER',
                'probability': float (0-100),
                'confidence': 'high' | 'medium' | 'low',
                'justification': str
            },
            'stats_summary': str,
            'model_used': str
        }
    """
    if not ML_AVAILABLE:
        return _fallback_prediction(match)
    
    # Extraer features
    features, justifications = extract_features(match)
    
    # Determinar bucket de handicap
    ah_value = features.get('ah_linea', 0)
    bucket = _get_ah_bucket(ah_value)
    
    # Cargar modelos
    model_ah = _load_model(f'{bucket}_ah')
    model_ou = _load_model(f'{bucket}_ou')
    
    if not model_ah and not model_ou:
        return _fallback_prediction(match, features, justifications)
    
    # Preparar features
    feature_names = _get_feature_names()
    if not feature_names:
        return _fallback_prediction(match, features, justifications)
    
    try:
        feature_values = [features.get(k, 0) for k in feature_names]
        X = np.array([feature_values])
    except Exception:
        return _fallback_prediction(match, features, justifications)
    
    result = {
        'model_used': bucket,
        'stats_summary': f"L:{features['total_local_pts']} V:{features['total_away_pts']} ({features['valid_sources']} fuentes)"
    }
    
    # Predicción AH
    if model_ah:
        try:
            proba = model_ah.predict_proba(X)[0]
            prob_local = proba[1] * 100  # Probabilidad de que LOCAL cubra
            prob_visita = proba[0] * 100
            
            if prob_local > prob_visita:
                pick = 'LOCAL'
                probability = prob_local
            else:
                pick = 'VISITA'
                probability = prob_visita
            
            # Determinar confianza
            if probability >= 65:
                confidence = 'high'
            elif probability >= 55:
                confidence = 'medium'
            else:
                confidence = 'low'
            
            # Justificación
            just_parts = []
            if features['score_diff'] > 2:
                just_parts.append(f"Stats Local +{features['score_diff']}")
            elif features['score_diff'] < -2:
                just_parts.append(f"Stats Visita +{abs(features['score_diff'])}")
            
            if features['ranking_diff'] < -3:
                just_parts.append("Ranking Local mejor")
            elif features['ranking_diff'] > 3:
                just_parts.append("Ranking Visita mejor")
            
            if justifications:
                just_parts.extend(justifications[:2])  # Máximo 2
            
            result['ah_prediction'] = {
                'pick': pick,
                'probability': round(probability, 1),
                'confidence': confidence,
                'justification': ' | '.join(just_parts) if just_parts else 'Modelo ML'
            }
        except Exception:
            result['ah_prediction'] = _default_ah_prediction(features)
    else:
        result['ah_prediction'] = _default_ah_prediction(features)
    
    # Predicción O/U
    if model_ou:
        try:
            proba = model_ou.predict_proba(X)[0]
            prob_over = proba[1] * 100
            prob_under = proba[0] * 100
            
            if prob_over > prob_under:
                pick = 'OVER'
                probability = prob_over
            else:
                pick = 'UNDER'
                probability = prob_under
            
            if probability >= 60:
                confidence = 'high'
            elif probability >= 52:
                confidence = 'medium'
            else:
                confidence = 'low'
            
            result['ou_prediction'] = {
                'pick': pick,
                'probability': round(probability, 1),
                'confidence': confidence,
                'justification': f"O/U {features['ou_linea']}"
            }
        except Exception:
            result['ou_prediction'] = _default_ou_prediction(features)
    else:
        result['ou_prediction'] = _default_ou_prediction(features)
    
    return result


def _default_ah_prediction(features: Dict) -> Dict:
    """Predicción AH por defecto basada en reglas."""
    score_diff = features.get('score_diff', 0)
    total = features.get('total_local_pts', 0) + features.get('total_away_pts', 0)
    
    if total == 0:
        return {'pick': 'LOCAL', 'probability': 50.0, 'confidence': 'low', 'justification': 'Sin datos'}
    
    pct_local = (features.get('total_local_pts', 0) / total) * 100
    
    if pct_local > 55:
        return {'pick': 'LOCAL', 'probability': pct_local, 'confidence': 'medium', 'justification': f'Stats {score_diff:+d}'}
    elif pct_local < 45:
        return {'pick': 'VISITA', 'probability': 100 - pct_local, 'confidence': 'medium', 'justification': f'Stats {score_diff:+d}'}
    else:
        return {'pick': 'LOCAL', 'probability': 50.0, 'confidence': 'low', 'justification': 'Equilibrado'}


def _default_ou_prediction(features: Dict) -> Dict:
    """Predicción O/U por defecto."""
    return {'pick': 'OVER', 'probability': 50.0, 'confidence': 'low', 'justification': 'Sin modelo'}


def _fallback_prediction(match: Dict, features: Dict = None, justifications: list = None) -> Dict:
    """Predicción de respaldo cuando ML no está disponible."""
    if features is None:
        features, justifications = extract_features(match)
    
    return {
        'ah_prediction': _default_ah_prediction(features),
        'ou_prediction': _default_ou_prediction(features),
        'stats_summary': f"L:{features.get('total_local_pts', 0)} V:{features.get('total_away_pts', 0)}",
        'model_used': 'fallback'
    }


# API de alto nivel
def get_prediction(match_data: Dict) -> Dict:
    """Función principal para obtener predicción de un partido."""
    return predict(match_data)
