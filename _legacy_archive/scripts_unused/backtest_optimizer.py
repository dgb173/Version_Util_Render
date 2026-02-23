# scripts/backtest_optimizer.py
"""
Script de backtesting y optimización de parámetros.
Evalúa el modelo actual con partidos históricos y ajusta parámetros
para maximizar la precisión.

Uso: py scripts/backtest_optimizer.py
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

# Estadísticas a analizar
STATS_KEYS = ['Tiros', 'Tiros a Puerta', 'Ataques', 'Ataques Peligrosos']

# Archivos de datos
DATA_FILES = [
    DATA_DIR / 'data_ah_0.json',
    DATA_DIR / 'data_ah_0.5.json',
    DATA_DIR / 'data_ah_1.5.json',
    DATA_DIR / 'data_ah_2_plus.json',
    DATA_DIR / 'data_minus_ah_0.5.json',
    DATA_DIR / 'data_minus_ah_1.5.json',
    DATA_DIR / 'data_minus_ah_2_plus.json',
]


def parse_score(score_str):
    """Parsea un score como '2:1' y devuelve (home, away) o None."""
    if not score_str or ':' not in str(score_str):
        return None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        return None


def get_ah_result(home_goals, away_goals, ah_line):
    """
    Determina si el LOCAL cubrió el handicap.
    ah_line positivo = Local favorito (da ventaja al visitante)
    ah_line negativo = Visitante favorito (da ventaja al local)
    
    Retorna: 'LOCAL_WIN', 'VISITA_WIN', o 'PUSH'
    """
    # Ajustamos el handicap a perspectiva del local
    # Si ah_line = 0.5, el local da 0.5 goles, necesita ganar por 1+
    # Si ah_line = -0.5, el local recibe 0.5 goles, le vale con empate
    adjusted_diff = (home_goals - away_goals) - ah_line
    
    if adjusted_diff > 0.25:
        return 'LOCAL_WIN'
    elif adjusted_diff < -0.25:
        return 'VISITA_WIN'
    else:
        return 'PUSH'


def parse_stats_rows(stats_rows):
    """Parsea stats_rows a diccionario."""
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


def calculate_score_with_weights(match, weights):
    """
    Calcula el score compuesto con pesos configurables.
    
    weights = {
        'prev_home': 1.0,
        'prev_away': 1.0,
        'h2h_stadium': 1.5,
        'h2h_general': 1.0,
        'h2h_col3': 2.0,
        'ind_left': 0.5,
        'ind_right': 0.5,
        'stat_Tiros': 1.0,
        'stat_Tiros a Puerta': 1.5,
        'stat_Ataques': 0.5,
        'stat_Ataques Peligrosos': 2.0,
        'min_pct_threshold': 55,  # % mínimo para hacer pick
        'ha_bonus': 5,  # Bonus si coincide con HA
    }
    """
    sources = [
        ('prev_home', match.get('last_home_match'), True),
        ('prev_away', match.get('last_away_match'), False),
        ('h2h_stadium', match.get('h2h_stadium'), True),
        ('h2h_general', match.get('h2h_general'), True),
        ('h2h_col3', match.get('h2h_col3'), True),
    ]
    
    comparativas = match.get('comparativas_indirectas') or {}
    left = comparativas.get('left')
    right = comparativas.get('right')
    
    if left:
        sources.append(('ind_left', left, left.get('localia') == 'H'))
    if right:
        sources.append(('ind_right', right, right.get('localia') != 'A'))
    
    total_local = 0.0
    total_visit = 0.0
    valid_sources = 0
    
    for source_name, source_data, home_perspective in sources:
        if not source_data:
            continue
        
        stats_rows = source_data.get('stats_rows', [])
        if not stats_rows:
            continue
            
        stats_dict = parse_stats_rows(stats_rows)
        if not stats_dict:
            continue
        
        source_weight = weights.get(source_name, 1.0)
        source_local = 0.0
        source_visit = 0.0
        
        for stat_name in STATS_KEYS:
            if stat_name not in stats_dict:
                continue
                
            stat_weight = weights.get(f'stat_{stat_name}', 1.0)
            
            if home_perspective:
                local_val = stats_dict[stat_name]['home']
                visit_val = stats_dict[stat_name]['away']
            else:
                local_val = stats_dict[stat_name]['away']
                visit_val = stats_dict[stat_name]['home']
            
            if local_val > visit_val:
                source_local += stat_weight
            elif visit_val > local_val:
                source_visit += stat_weight
        
        total_local += source_local * source_weight
        total_visit += source_visit * source_weight
        valid_sources += 1
    
    return total_local, total_visit, valid_sources


def make_prediction(match, weights):
    """
    Hace una predicción para un partido usando los pesos dados.
    Retorna: ('LOCAL', probability) o ('VISITA', probability) o None si no hay datos
    """
    total_local, total_visit, valid_sources = calculate_score_with_weights(match, weights)
    
    if valid_sources < weights.get('min_sources', 1):
        return None
    
    total = total_local + total_visit
    if total == 0:
        return None
    
    pct_local = (total_local / total) * 100
    
    # Obtener AH
    main_odds = match.get('main_match_odds') or {}
    try:
        ah_line = float(main_odds.get('ah_linea', 0) or 0)
    except (ValueError, TypeError):
        ah_line = 0
    
    # Determinar favorito por HA
    ha_fav = 'NEUTRO'
    if ah_line > 0:
        ha_fav = 'LOCAL'
    elif ah_line < 0:
        ha_fav = 'VISITA'
    
    # Calcular probabilidad
    min_pct = weights.get('min_pct_threshold', 50)
    ha_bonus = weights.get('ha_bonus', 5)
    
    if pct_local > 50:
        pick = 'LOCAL'
        base_prob = 50 + ((pct_local - 50) * 0.7)  # Escalar
        if ha_fav == 'LOCAL':
            base_prob += ha_bonus
        elif ha_fav == 'VISITA':
            base_prob -= ha_bonus
    elif pct_local < 50:
        pick = 'VISITA'
        base_prob = 50 + ((50 - pct_local) * 0.7)
        if ha_fav == 'VISITA':
            base_prob += ha_bonus
        elif ha_fav == 'LOCAL':
            base_prob -= ha_bonus
    else:
        # Empate en stats - usar HA
        if ha_fav == 'VISITA':
            pick = 'VISITA'
        else:
            pick = 'LOCAL'
        base_prob = 52
    
    # Solo hacer pick si supera umbral
    if base_prob < min_pct:
        return None
    
    return pick, min(85, base_prob), total_local, total_visit


def run_backtest(matches, weights):
    """
    Ejecuta backtest con un conjunto de partidos y pesos.
    Retorna estadísticas de precisión.
    """
    results = {
        'total': 0,
        'correct': 0,
        'incorrect': 0,
        'push': 0,
        'no_pick': 0,
        'by_confidence': defaultdict(lambda: {'total': 0, 'correct': 0}),
        'by_ah_bucket': defaultdict(lambda: {'total': 0, 'correct': 0}),
    }
    
    for match in matches:
        # Obtener resultado real
        score = match.get('final_score') or match.get('score')
        parsed = parse_score(score)
        if not parsed:
            continue
        
        home_goals, away_goals = parsed
        
        # Obtener AH
        main_odds = match.get('main_match_odds') or {}
        try:
            ah_line = float(main_odds.get('ah_linea', 0) or 0)
        except (ValueError, TypeError):
            continue
        
        # Calcular resultado real
        actual_result = get_ah_result(home_goals, away_goals, ah_line)
        if actual_result == 'PUSH':
            results['push'] += 1
            continue
        
        # Hacer predicción
        prediction = make_prediction(match, weights)
        if not prediction:
            results['no_pick'] += 1
            continue
        
        pick, probability, score_l, score_v = prediction
        results['total'] += 1
        
        # Evaluar
        predicted_win = f'{pick}_WIN'
        if predicted_win == actual_result:
            results['correct'] += 1
            results['by_confidence'][int(probability // 5) * 5]['correct'] += 1
        else:
            results['incorrect'] += 1
        
        results['by_confidence'][int(probability // 5) * 5]['total'] += 1
        
        # Por bucket de AH
        ah_bucket = f"ah_{abs(ah_line):.1f}"
        results['by_ah_bucket'][ah_bucket]['total'] += 1
        if predicted_win == actual_result:
            results['by_ah_bucket'][ah_bucket]['correct'] += 1
    
    # Calcular precisión
    if results['total'] > 0:
        results['accuracy'] = results['correct'] / results['total'] * 100
    else:
        results['accuracy'] = 0
    
    return results


def load_all_matches():
    """Carga todos los partidos de todos los archivos de datos."""
    all_matches = []
    for data_file in DATA_FILES:
        if not data_file.exists():
            continue
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                matches = json.load(f)
                all_matches.extend(matches)
        except Exception as e:
            print(f"Error cargando {data_file}: {e}")
    return all_matches


def optimize_weights(matches, iterations=100):
    """
    Optimiza pesos usando búsqueda iterativa.
    """
    import random
    
    # Pesos base
    base_weights = {
        'prev_home': 1.0,
        'prev_away': 1.0,
        'h2h_stadium': 1.5,
        'h2h_general': 1.0,
        'h2h_col3': 2.0,
        'ind_left': 0.5,
        'ind_right': 0.5,
        'stat_Tiros': 1.0,
        'stat_Tiros a Puerta': 1.5,
        'stat_Ataques': 0.5,
        'stat_Ataques Peligrosos': 2.0,
        'min_pct_threshold': 55,
        'ha_bonus': 5,
        'min_sources': 2,
    }
    
    best_weights = base_weights.copy()
    best_result = run_backtest(matches, base_weights)
    best_accuracy = best_result['accuracy']
    
    print(f"Precisión inicial: {best_accuracy:.2f}% ({best_result['correct']}/{best_result['total']})")
    
    # Parámetros a optimizar
    tunable_params = [
        'prev_home', 'prev_away', 'h2h_stadium', 'h2h_general', 'h2h_col3',
        'ind_left', 'ind_right',
        'stat_Tiros', 'stat_Tiros a Puerta', 'stat_Ataques', 'stat_Ataques Peligrosos',
        'min_pct_threshold', 'ha_bonus', 'min_sources'
    ]
    
    for i in range(iterations):
        # Mutar pesos aleatoriamente
        test_weights = best_weights.copy()
        param = random.choice(tunable_params)
        
        if param == 'min_pct_threshold':
            test_weights[param] = random.choice([50, 52, 55, 58, 60, 62, 65, 68, 70])
        elif param == 'min_sources':
            test_weights[param] = random.choice([1, 2, 3])
        elif param == 'ha_bonus':
            test_weights[param] = random.uniform(0, 15)
        else:
            # Multiplicar por factor aleatorio
            factor = random.uniform(0.5, 2.0)
            test_weights[param] = base_weights[param] * factor
        
        # Evaluar
        result = run_backtest(matches, test_weights)
        
        # Solo aceptar si mejora Y tiene suficientes picks
        if result['accuracy'] > best_accuracy and result['total'] >= len(matches) * 0.3:
            best_accuracy = result['accuracy']
            best_weights = test_weights.copy()
            print(f"  Iteración {i+1}: Nueva mejor precisión: {best_accuracy:.2f}% ({result['correct']}/{result['total']}) - Cambio: {param}")
    
    return best_weights, best_accuracy


def main():
    print("=" * 70)
    print("🎯 BACKTESTING Y OPTIMIZACIÓN DE MODELO DE PICKS")
    print("=" * 70)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Cargar partidos
    print("📂 Cargando partidos históricos...")
    all_matches = load_all_matches()
    print(f"   Total partidos: {len(all_matches)}")
    
    # Filtrar solo partidos con resultado
    valid_matches = []
    for m in all_matches:
        score = m.get('final_score') or m.get('score')
        if parse_score(score):
            valid_matches.append(m)
    print(f"   Partidos con resultado válido: {len(valid_matches)}")
    
    # Test con pesos por defecto
    print("\n" + "=" * 70)
    print("📊 BACKTEST CON CONFIGURACIÓN ACTUAL")
    print("=" * 70)
    
    default_weights = {
        'prev_home': 1.0,
        'prev_away': 1.0,
        'h2h_stadium': 1.0,
        'h2h_general': 1.0,
        'h2h_col3': 1.0,
        'ind_left': 1.0,
        'ind_right': 1.0,
        'stat_Tiros': 1.0,
        'stat_Tiros a Puerta': 1.0,
        'stat_Ataques': 1.0,
        'stat_Ataques Peligrosos': 1.0,
        'min_pct_threshold': 55,
        'ha_bonus': 5,
        'min_sources': 2,
    }
    
    result = run_backtest(valid_matches, default_weights)
    print(f"\n📈 RESULTADOS:")
    print(f"   Picks realizados: {result['total']}")
    print(f"   Aciertos: {result['correct']}")
    print(f"   Fallos: {result['incorrect']}")
    print(f"   Pushes (ignorados): {result['push']}")
    print(f"   Sin pick (datos insuficientes): {result['no_pick']}")
    print(f"\n   ✨ PRECISIÓN: {result['accuracy']:.2f}%")
    
    print("\n   Por rango de confianza:")
    for conf_range in sorted(result['by_confidence'].keys()):
        data = result['by_confidence'][conf_range]
        if data['total'] > 0:
            acc = data['correct'] / data['total'] * 100
            print(f"     {conf_range}%-{conf_range+5}%: {data['correct']}/{data['total']} = {acc:.1f}%")
    
    # Optimizar
    print("\n" + "=" * 70)
    print("🔧 OPTIMIZANDO PARÁMETROS (200 iteraciones)...")
    print("=" * 70)
    
    best_weights, best_accuracy = optimize_weights(valid_matches, iterations=200)
    
    print(f"\n✅ MEJOR CONFIGURACIÓN ENCONTRADA:")
    print(f"   Precisión: {best_accuracy:.2f}%")
    print("\n   Pesos óptimos:")
    for key, value in sorted(best_weights.items()):
        print(f"     {key}: {value:.2f}" if isinstance(value, float) else f"     {key}: {value}")
    
    # Guardar configuración óptima
    config_path = RESULTS_DIR / 'optimal_weights.json'
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump({
            'weights': best_weights,
            'accuracy': best_accuracy,
            'timestamp': datetime.now().isoformat(),
            'total_matches': len(valid_matches),
        }, f, indent=2)
    print(f"\n💾 Configuración guardada en: {config_path}")
    
    # Test final con pesos óptimos
    print("\n" + "=" * 70)
    print("📊 BACKTEST FINAL CON PESOS ÓPTIMOS")
    print("=" * 70)
    
    final_result = run_backtest(valid_matches, best_weights)
    print(f"\n   Picks realizados: {final_result['total']}")
    print(f"   Aciertos: {final_result['correct']}")
    print(f"   Fallos: {final_result['incorrect']}")
    print(f"\n   ✨ PRECISIÓN FINAL: {final_result['accuracy']:.2f}%")
    
    print("\n   Por rango de confianza:")
    for conf_range in sorted(final_result['by_confidence'].keys()):
        data = final_result['by_confidence'][conf_range]
        if data['total'] > 0:
            acc = data['correct'] / data['total'] * 100
            print(f"     {conf_range}%-{conf_range+5}%: {data['correct']}/{data['total']} = {acc:.1f}%")
    
    print("\n✅ Optimización completada!")
    print(f"   Usa los pesos en: {config_path}")


if __name__ == '__main__':
    main()
