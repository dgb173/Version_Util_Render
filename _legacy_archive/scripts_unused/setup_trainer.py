# scripts/setup_trainer.py
"""
SISTEMA AVANZADO DE SETUPS
==========================
Implementa la metodología de:
1. Setups anti-intuitivos (mercado vs forma)
2. Edge de estadísticas (ataques peligrosos, tiros a puerta)
3. Backtest con % cobertura
4. Detección automática de dinámicas ganadoras

Setups a detectar:
- ANTI_INTUITION_AWAY_FAV: Visitante favorito pero local domina stats
- MARKET_HOLDS_FAV_DESPITE_STADIUM: Mercado mantiene favorito aunque H2H no cubrió
- DANGER_EDGE_LOCAL: Edge fuerte en ataques peligrosos para local
- DANGER_EDGE_AWAY: Edge fuerte en ataques peligrosos para visitante
- CONSISTENT_COVER_PATTERN: Patrón consistente de cobertura en múltiples fuentes
- OVER_EDGE: Edge fuerte hacia OVER por goles promedio
- UNDER_EDGE: Edge fuerte hacia UNDER por goles bajos
"""

import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

# Archivos de datos
DATA_FILES = list(DATA_DIR.glob('data*.json'))

# Estadísticas clave
STATS_KEYS = ['Tiros', 'Tiros a Puerta', 'Ataques', 'Ataques Peligrosos']


def parse_score(score_str) -> Optional[Tuple[int, int]]:
    if not score_str or ':' not in str(score_str):
        return None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except:
        return None


def get_ah_winner(home_goals, away_goals, ah_line) -> str:
    adjusted = (home_goals - away_goals) - ah_line
    if adjusted > 0.25:
        return 'LOCAL'
    elif adjusted < -0.25:
        return 'VISITA'
    return 'PUSH'


def get_ou_result(home_goals, away_goals, ou_line) -> str:
    total = home_goals + away_goals
    if total > ou_line + 0.25:
        return 'OVER'
    elif total < ou_line - 0.25:
        return 'UNDER'
    return 'PUSH'


def get_cover_result(home_goals, away_goals, ah_line, perspective='home') -> str:
    if perspective == 'home':
        adjusted = (home_goals - away_goals) - ah_line
    else:
        adjusted = (away_goals - home_goals) + ah_line
    if adjusted > 0.25:
        return 'COVER'
    elif adjusted < -0.25:
        return 'NO_COVER'
    return 'PUSH'


def parse_stats_rows(stats_rows: list) -> Dict[str, Dict]:
    """Parsea stats_rows a diccionario."""
    result = {}
    if not stats_rows or not isinstance(stats_rows, list):
        return result
    for r in stats_rows:
        label = (r.get('label') or '').strip()
        try:
            result[label] = {
                'home': float(r.get('home', 0) or 0),
                'away': float(r.get('away', 0) or 0)
            }
        except:
            continue
    return result


def calculate_edge(stats_dict: Dict, home_perspective: bool = True) -> Dict:
    """
    Calcula el edge (ventaja) para cada estadística.
    Retorna diccionario con edge por stat y edge total.
    """
    edges = {}
    total_edge = 0
    count = 0
    
    for stat in STATS_KEYS:
        if stat not in stats_dict:
            continue
        
        if home_perspective:
            local_val = stats_dict[stat]['home']
            visit_val = stats_dict[stat]['away']
        else:
            local_val = stats_dict[stat]['away']
            visit_val = stats_dict[stat]['home']
        
        diff = local_val - visit_val
        edges[stat] = diff
        total_edge += diff
        count += 1
    
    return {
        'edges': edges,
        'total_edge': total_edge,
        'edge_tiros': edges.get('Tiros', 0),
        'edge_sot': edges.get('Tiros a Puerta', 0),
        'edge_attacks': edges.get('Ataques', 0),
        'edge_danger': edges.get('Ataques Peligrosos', 0),
        'num_stats': count
    }


def extract_full_features(match: Dict) -> Dict:
    """
    Extrae TODAS las features incluyendo edges de estadísticas.
    """
    features = {}
    
    # Datos básicos
    main_odds = match.get('main_match_odds') or {}
    try:
        ah_line = float(main_odds.get('ah_linea', 0) or 0)
        ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah_line, ou_line = 0, 2.5
    
    features['ah_line'] = ah_line
    features['ou_line'] = ou_line
    features['ah_bucket'] = round(abs(ah_line) * 2) / 2
    
    # Favorito por mercado
    features['fav_market'] = 'LOCAL' if ah_line > 0 else ('AWAY' if ah_line < 0 else 'NEUTRAL')
    features['away_fav_market'] = ah_line < 0
    features['local_fav_market'] = ah_line > 0
    
    # Procesar todas las fuentes y calcular edges
    all_edges = {
        'edge_danger': [],
        'edge_sot': [],
        'edge_attacks': [],
        'edge_tiros': []
    }
    
    cover_data = {}
    ou_data = {}
    goals_data = {}
    
    sources = [
        ('prev_home', match.get('last_home_match'), True),
        ('prev_away', match.get('last_away_match'), False),
        ('h2h_stadium', match.get('h2h_stadium'), True),
        ('h2h_general', match.get('h2h_general'), True),
        ('h2h_col3', match.get('h2h_col3'), True),
    ]
    
    comp = match.get('comparativas_indirectas') or {}
    if comp.get('left'):
        sources.append(('ind_left', comp['left'], comp['left'].get('localia') == 'H'))
    if comp.get('right'):
        sources.append(('ind_right', comp['right'], comp['right'].get('localia') != 'A'))
    
    for name, data, home_perspective in sources:
        if not data:
            continue
        
        # Stats del partido
        stats_rows = data.get('stats_rows', [])
        if stats_rows:
            stats_dict = parse_stats_rows(stats_rows)
            edge_info = calculate_edge(stats_dict, home_perspective)
            
            # Guardar edges
            for key in all_edges:
                if edge_info.get(key):
                    all_edges[key].append(edge_info[key])
            
            features[f'{name}_edge_danger'] = edge_info.get('edge_danger', 0)
            features[f'{name}_edge_sot'] = edge_info.get('edge_sot', 0)
        
        # Cover
        score = None
        if data.get('goles_home') is not None:
            try:
                score = (int(data['goles_home']), int(data['goles_away']))
            except:
                pass
        elif data.get('score'):
            score = parse_score(data['score'])
        
        if score:
            try:
                src_ah = float(data.get('handicap_line_raw') or data.get('handicap') or 
                              data.get('ah_line') or data.get('ah') or 0)
            except:
                src_ah = 0
            
            perspective = 'home' if home_perspective else 'away'
            cover_data[name] = get_cover_result(score[0], score[1], src_ah, perspective)
            
            total_goals = score[0] + score[1]
            ou_data[name] = 'OVER' if total_goals > 2.5 else 'UNDER'
            goals_data[name] = total_goals
            
            features[f'{name}_cover'] = cover_data[name]
            features[f'{name}_ou'] = ou_data[name]
            features[f'{name}_goals'] = total_goals
    
    # === EDGES AGREGADOS ===
    # Promedio de edge en ataques peligrosos
    if all_edges['edge_danger']:
        features['avg_edge_danger'] = sum(all_edges['edge_danger']) / len(all_edges['edge_danger'])
        features['edge_danger_positive'] = features['avg_edge_danger'] > 0
        features['edge_danger_strong_local'] = features['avg_edge_danger'] >= 5
        features['edge_danger_strong_away'] = features['avg_edge_danger'] <= -5
    else:
        features['avg_edge_danger'] = 0
        features['edge_danger_positive'] = False
        features['edge_danger_strong_local'] = False
        features['edge_danger_strong_away'] = False
    
    # Promedio de edge en tiros a puerta
    if all_edges['edge_sot']:
        features['avg_edge_sot'] = sum(all_edges['edge_sot']) / len(all_edges['edge_sot'])
        features['edge_sot_positive'] = features['avg_edge_sot'] > 0
    else:
        features['avg_edge_sot'] = 0
        features['edge_sot_positive'] = False
    
    # === COBERTURAS ===
    covers = sum(1 for v in cover_data.values() if v == 'COVER')
    no_covers = sum(1 for v in cover_data.values() if v == 'NO_COVER')
    valid = len([v for v in cover_data.values() if v in ['COVER', 'NO_COVER']])
    
    features['covers'] = covers
    features['no_covers'] = no_covers
    features['cover_sources'] = valid
    features['cover_ratio'] = covers / valid if valid > 0 else 0.5
    
    # H2H específico
    features['h2h_stadium_not_cover'] = cover_data.get('h2h_stadium') == 'NO_COVER'
    features['h2h_general_not_cover'] = cover_data.get('h2h_general') == 'NO_COVER'
    features['h2h_both_not_cover'] = (features['h2h_stadium_not_cover'] and 
                                       features['h2h_general_not_cover'])
    
    # === O/U ===
    overs = sum(1 for v in ou_data.values() if v == 'OVER')
    unders = sum(1 for v in ou_data.values() if v == 'UNDER')
    
    features['overs'] = overs  
    features['unders'] = unders
    features['ou_sources'] = len(ou_data)
    features['over_ratio'] = overs / len(ou_data) if ou_data else 0.5
    
    # Goles promedio
    if goals_data:
        features['avg_goals'] = sum(goals_data.values()) / len(goals_data)
        features['high_scoring'] = features['avg_goals'] >= 3
        features['low_scoring'] = features['avg_goals'] <= 2
    else:
        features['avg_goals'] = 2.5
        features['high_scoring'] = False
        features['low_scoring'] = False
    
    # === CONFLICTOS MERCADO VS FORMA ===
    # Conflicto: Visitante favorito pero stats favorecen local
    features['conflict_away_fav_local_stats'] = (
        features['away_fav_market'] and 
        features['avg_edge_danger'] > 0 and
        features['avg_edge_sot'] > 0
    )
    
    # Conflicto: Local favorito pero stats favorecen visitante
    features['conflict_local_fav_away_stats'] = (
        features['local_fav_market'] and 
        features['avg_edge_danger'] < 0 and
        features['avg_edge_sot'] < 0
    )
    
    # Mercado mantiene favorito aunque H2H no cubrió
    features['market_holds_despite_h2h'] = (
        (features['local_fav_market'] and features['h2h_both_not_cover']) or
        (features['away_fav_market'] and cover_data.get('h2h_stadium') == 'COVER' and cover_data.get('h2h_general') == 'COVER')
    )
    
    # Rankings
    try:
        hr = int((match.get('home_standings') or {}).get('ranking', 0) or 0)
        ar = int((match.get('away_standings') or {}).get('ranking', 0) or 0)
        features['rank_diff'] = hr - ar
        features['has_ranks'] = bool(hr and ar)
    except:
        features['rank_diff'] = 0
        features['has_ranks'] = False
    
    return features


# ==================== DEFINICIÓN DE SETUPS ====================

SETUPS = {
    # Setup 1: Anti-intuición - Visitante favorito pero local domina stats
    'ANTI_INTUITION_AWAY_FAV': {
        'conditions': [
            ('away_fav_market', '==', True),
            ('avg_edge_danger', '>=', 3),
            ('avg_edge_sot', '>=', 1),
            ('cover_sources', '>=', 3),
        ],
        'prediction': 'LOCAL',  # Apostar contra el mercado
        'type': 'AH',
        'description': 'Visitante favorito pero local domina en ataques peligrosos'
    },
    
    # Setup 2: Anti-intuición inverso - Local favorito pero visitante domina
    'ANTI_INTUITION_LOCAL_FAV': {
        'conditions': [
            ('local_fav_market', '==', True),
            ('avg_edge_danger', '<=', -3),
            ('avg_edge_sot', '<=', -1),
            ('cover_sources', '>=', 3),
        ],
        'prediction': 'VISITA',
        'type': 'AH',
        'description': 'Local favorito pero visitante domina en ataques peligrosos'
    },
    
    # Setup 3: Mercado mantiene favorito aunque H2H no cubrió
    'MARKET_HOLDS_DESPITE_STADIUM': {
        'conditions': [
            ('local_fav_market', '==', True),
            ('h2h_stadium_not_cover', '==', True),
            ('h2h_general_not_cover', '==', True),
            ('ah_bucket', '>=', 0.5),
        ],
        'prediction': 'VISITA',  # H2H sugiere que favorito no cubre
        'type': 'AH',
        'description': 'Local favorito pero H2H indica que no cubre'
    },
    
    # Setup 4: Edge fuerte en ataques peligrosos - LOCAL
    'DANGER_EDGE_LOCAL_STRONG': {
        'conditions': [
            ('avg_edge_danger', '>=', 8),
            ('avg_edge_sot', '>=', 2),
            ('cover_sources', '>=', 4),
        ],
        'prediction': 'LOCAL',
        'type': 'AH',
        'description': 'Dominio abrumador del local en ataques peligrosos'
    },
    
    # Setup 5: Edge fuerte en ataques peligrosos - VISITA
    'DANGER_EDGE_AWAY_STRONG': {
        'conditions': [
            ('avg_edge_danger', '<=', -8),
            ('avg_edge_sot', '<=', -2),
            ('cover_sources', '>=', 4),
        ],
        'prediction': 'VISITA',
        'type': 'AH',
        'description': 'Dominio abrumador del visitante en ataques peligrosos'
    },
    
    # Setup 6: Consistencia de cobertura - LOCAL
    'CONSISTENT_COVER_LOCAL': {
        'conditions': [
            ('covers', '>=', 5),
            ('no_covers', '<=', 1),
            ('cover_sources', '>=', 5),
        ],
        'prediction': 'LOCAL',
        'type': 'AH',
        'description': 'Consistencia total de cobertura hacia local'
    },
    
    # Setup 7: Consistencia de NO cobertura - VISITA
    'CONSISTENT_NOCOVER_VISITA': {
        'conditions': [
            ('no_covers', '>=', 5),
            ('covers', '<=', 1),
            ('cover_sources', '>=', 5),
        ],
        'prediction': 'VISITA',
        'type': 'AH',
        'description': 'Consistencia total de no cobertura'
    },
    
    # Setup 8: OVER por goles altos
    'OVER_HIGH_SCORING': {
        'conditions': [
            ('avg_goals', '>=', 3.2),
            ('overs', '>=', 4),
            ('ou_sources', '>=', 4),
        ],
        'prediction': 'OVER',
        'type': 'OU',
        'description': 'Historial de partidos con muchos goles'
    },
    
    # Setup 9: UNDER por goles bajos
    'UNDER_LOW_SCORING': {
        'conditions': [
            ('avg_goals', '<=', 2),
            ('unders', '>=', 4),
            ('ou_sources', '>=', 4),
        ],
        'prediction': 'UNDER',
        'type': 'OU',
        'description': 'Historial de partidos con pocos goles'
    },
    
    # Setup 10: OVER cuando edge de ataques es muy alto
    'OVER_DANGER_EDGE': {
        'conditions': [
            ('avg_edge_danger', '>=', 10),
            ('avg_goals', '>=', 2.5),
            ('cover_sources', '>=', 3),
        ],
        'prediction': 'OVER',
        'type': 'OU',
        'description': 'Muchos ataques peligrosos sugieren goles'
    },
    
    # Setup 11: Conflicto resuelto a favor de stats
    'CONFLICT_STATS_WIN': {
        'conditions': [
            ('conflict_away_fav_local_stats', '==', True),
            ('covers', '>=', 3),
            ('avg_edge_danger', '>=', 5),
        ],
        'prediction': 'LOCAL',
        'type': 'AH',
        'description': 'Conflicto mercado/stats resuelto por dominio estadístico'
    },
    
    # Setup 12: Patrón AH bucket específico
    'AH_HALF_BALL_AWAY': {
        'conditions': [
            ('ah_bucket', '==', 0.5),
            ('away_fav_market', '==', True),
            ('h2h_stadium_not_cover', '==', True),
            ('avg_edge_danger', '>=', 0),
        ],
        'prediction': 'LOCAL',
        'type': 'AH',
        'description': 'Línea 0.5 con señales contra visitante'
    },
}


def check_setup_conditions(features: Dict, conditions: List) -> bool:
    """Verifica si las features cumplen todas las condiciones del setup."""
    for feat, op, val in conditions:
        fv = features.get(feat)
        if fv is None:
            return False
        
        try:
            if op == '==' and fv != val:
                return False
            elif op == '>=' and fv < val:
                return False
            elif op == '<=' and fv > val:
                return False
            elif op == '>' and fv <= val:
                return False
            elif op == '<' and fv >= val:
                return False
        except:
            return False
    
    return True


def run_backtest():
    """Ejecuta backtest de todos los setups."""
    print("=" * 70)
    print("🎯 BACKTEST DE SETUPS AVANZADOS")
    print("=" * 70)
    print(f"Setups a evaluar: {len(SETUPS)}")
    print()
    
    # Cargar todos los partidos
    print("📂 Cargando partidos...")
    all_matches = []
    for f in DATA_FILES:
        if not f.exists():
            continue
        try:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                if isinstance(data, list):
                    all_matches.extend(data)
        except Exception as e:
            print(f"  Error en {f.name}: {e}")
    
    # Filtrar partidos con resultado
    valid_matches = [m for m in all_matches if parse_score(m.get('final_score') or m.get('score'))]
    print(f"   Total partidos con resultado: {len(valid_matches)}")
    
    # Inicializar estadísticas por setup
    stats = {name: {'total': 0, 'correct': 0, 'matches': []} for name in SETUPS}
    
    # Evaluar cada partido
    for match in valid_matches:
        score = match.get('final_score') or match.get('score')
        parsed = parse_score(score)
        if not parsed:
            continue
        
        home_goals, away_goals = parsed
        main_odds = match.get('main_match_odds') or {}
        
        try:
            ah = float(main_odds.get('ah_linea', 0) or 0)
            ou = float(main_odds.get('goals_linea', 2.5) or 2.5)
        except:
            continue
        
        ah_result = get_ah_winner(home_goals, away_goals, ah)
        ou_result = get_ou_result(home_goals, away_goals, ou)
        
        features = extract_full_features(match)
        
        # Verificar cada setup
        for setup_name, setup_def in SETUPS.items():
            if not check_setup_conditions(features, setup_def['conditions']):
                continue
            
            stats[setup_name]['total'] += 1
            
            # Verificar si acertó
            predicted = setup_def['prediction']
            
            if setup_def['type'] == 'AH' and ah_result != 'PUSH':
                if predicted == ah_result:
                    stats[setup_name]['correct'] += 1
                    stats[setup_name]['matches'].append({
                        'score': score,
                        'result': 'WIN'
                    })
                else:
                    stats[setup_name]['matches'].append({
                        'score': score,
                        'result': 'LOSS'
                    })
            elif setup_def['type'] == 'OU' and ou_result != 'PUSH':
                if predicted == ou_result:
                    stats[setup_name]['correct'] += 1
                    stats[setup_name]['matches'].append({
                        'score': score,
                        'result': 'WIN'
                    })
                else:
                    stats[setup_name]['matches'].append({
                        'score': score,
                        'result': 'LOSS'
                    })
    
    # Mostrar resultados
    print("\n" + "=" * 70)
    print("📊 RESULTADOS POR SETUP")
    print("=" * 70)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'total_matches': len(valid_matches),
        'setups': []
    }
    
    good_setups = []
    
    for setup_name, setup_def in SETUPS.items():
        s = stats[setup_name]
        if s['total'] == 0:
            continue
        
        accuracy = s['correct'] / s['total'] * 100
        
        # Clasificar
        if accuracy >= 70 and s['total'] >= 30:
            emoji = "🔥"
            good_setups.append(setup_name)
        elif accuracy >= 60 and s['total'] >= 20:
            emoji = "✅"
            good_setups.append(setup_name)
        elif accuracy >= 55:
            emoji = "📊"
        else:
            emoji = "⚠️"
        
        print(f"\n{emoji} {setup_name}")
        print(f"   Tipo: {setup_def['type']} -> {setup_def['prediction']}")
        print(f"   Precisión: {accuracy:.1f}% ({s['correct']}/{s['total']})")
        print(f"   {setup_def['description']}")
        
        results['setups'].append({
            'name': setup_name,
            'type': setup_def['type'],
            'prediction': setup_def['prediction'],
            'conditions': setup_def['conditions'],
            'accuracy': round(accuracy, 2),
            'total': s['total'],
            'correct': s['correct'],
            'description': setup_def['description']
        })
    
    # Guardar resultados
    path = RESULTS_DIR / 'setups_backtest.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Guardado en: {path}")
    
    # Resumen
    print("\n" + "=" * 70)
    print("📌 RESUMEN")
    print("=" * 70)
    print(f"Setups evaluados: {len(SETUPS)}")
    print(f"Setups buenos (>60%): {len(good_setups)}")
    if good_setups:
        print(f"   Mejores: {', '.join(good_setups[:5])}")


if __name__ == '__main__':
    run_backtest()
