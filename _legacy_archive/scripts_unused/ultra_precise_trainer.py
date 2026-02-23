# scripts/ultra_precise_trainer.py
"""
SISTEMA DE ENTRENAMIENTO ULTRA-PRECISO
======================================
Objetivo: Encontrar reglas con >85% de precisión

Características:
1. Analiza FECHAS de partidos - descarta datos >90 días
2. 1000+ generaciones de algoritmo genético
3. Criterios extremadamente estrictos
4. Solo acepta reglas con >80% precisión y 40+ muestras

Uso: py scripts/ultra_precise_trainer.py
"""

import json
import random
import copy
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

DATA_FILES = [
    DATA_DIR / 'data_ah_0.json',
    DATA_DIR / 'data_ah_0.5.json', 
    DATA_DIR / 'data_ah_1.5.json',
    DATA_DIR / 'data_ah_2_plus.json',
    DATA_DIR / 'data_minus_ah_0.5.json',
    DATA_DIR / 'data_minus_ah_1.5.json',
    DATA_DIR / 'data_minus_ah_2_plus.json',
]

# Configuración
MAX_DAYS_OLD = 90  # Máximo días de antigüedad para considerar datos válidos
MIN_ACCURACY = 80  # Precisión mínima requerida
MIN_SAMPLES = 40   # Muestras mínimas para validar regla
GENERATIONS = 1000  # Número de generaciones


def parse_score(score_str) -> Optional[Tuple[int, int]]:
    if not score_str or ':' not in str(score_str):
        return None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except:
        return None


def parse_date(date_str) -> Optional[datetime]:
    """Intenta parsear una fecha de varios formatos."""
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Formatos a intentar
    formats = [
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%d-%m-%Y',
        '%Y/%m/%d',
        '%d.%m.%Y',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str[:10], fmt)
        except:
            continue
    
    # Intentar extraer fecha con regex
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', date_str)
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except:
            pass
    
    return None


def get_days_since(date_str, reference_date=None) -> Optional[int]:
    """Calcula días desde una fecha hasta hoy o fecha de referencia."""
    parsed = parse_date(date_str)
    if not parsed:
        return None
    
    if reference_date:
        ref = parse_date(reference_date) or datetime.now()
    else:
        ref = datetime.now()
    
    delta = ref - parsed
    return delta.days


def get_ah_winner(home_goals, away_goals, ah_line):
    adjusted = (home_goals - away_goals) - ah_line
    if adjusted > 0.25:
        return 'LOCAL'
    elif adjusted < -0.25:
        return 'VISITA'
    return 'PUSH'


def get_ou_result(home_goals, away_goals, ou_line):
    total = home_goals + away_goals
    if total > ou_line + 0.25:
        return 'OVER'
    elif total < ou_line - 0.25:
        return 'UNDER'
    return 'PUSH'


def get_cover_result(home_goals, away_goals, ah_line, perspective='home'):
    if perspective == 'home':
        adjusted = (home_goals - away_goals) - ah_line
    else:
        adjusted = (away_goals - home_goals) + ah_line
    
    if adjusted > 0.25:
        return 'COVER'
    elif adjusted < -0.25:
        return 'NO_COVER'
    return 'PUSH'


def extract_features(match: Dict, main_date: str = None) -> Dict:
    """
    Extrae features avanzadas incluyendo análisis de fechas.
    """
    features = {}
    
    # Fecha del partido principal
    main_match_date = main_date or match.get('date') or match.get('fecha')
    
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
    features['ha_fav'] = 'LOCAL' if ah_line > 0 else ('VISITA' if ah_line < 0 else 'NEUTRO')
    
    # Procesar fuentes con análisis de fechas
    coverData = {}
    ouData = {}
    dateData = {}
    validityData = {}  # Si el dato es "fresco" (<90 días)
    
    def process_source(name, data, perspective):
        if not data:
            return
        
        # Obtener fecha del partido histórico
        src_date = data.get('date') or data.get('fecha') or data.get('match_date')
        days_old = get_days_since(src_date, main_match_date)
        
        # Marcar si es dato fresco
        if days_old is not None:
            dateData[name] = days_old
            validityData[name] = days_old <= MAX_DAYS_OLD
        else:
            # Si no hay fecha, considerarlo potencialmente inválido
            validityData[name] = False
            dateData[name] = 999
        
        # Obtener score
        score = None
        if data.get('goles_home') is not None:
            try:
                score = (int(data['goles_home']), int(data['goles_away']))
            except:
                pass
        elif data.get('score'):
            score = parse_score(data['score'])
        
        if not score:
            return
        
        # Obtener handicap del partido histórico
        try:
            src_ah = float(data.get('handicap_line_raw') or data.get('handicap') or 
                          data.get('ah_line') or data.get('ah') or 0)
        except:
            src_ah = 0
        
        # Calcular cobertura y O/U
        coverData[name] = get_cover_result(score[0], score[1], src_ah, perspective)
        ouData[name] = 'OVER' if (score[0] + score[1]) > 2.5 else 'UNDER'
    
    # Procesar cada fuente
    process_source('prev_home', match.get('last_home_match'), 'home')
    process_source('prev_away', match.get('last_away_match'), 'away')
    process_source('h2h_stadium', match.get('h2h_stadium'), 'home')
    process_source('h2h_general', match.get('h2h_general'), 'home')
    
    if match.get('h2h_col3') and match['h2h_col3'].get('status') == 'found':
        process_source('h2h_col3', match['h2h_col3'], 'home')
    
    comp = match.get('comparativas_indirectas') or {}
    if comp.get('left'):
        process_source('ind_left', comp['left'], 
                      'home' if comp['left'].get('localia') == 'H' else 'away')
    if comp.get('right'):
        process_source('ind_right', comp['right'],
                      'away' if comp['right'].get('localia') == 'A' else 'home')
    
    # === FEATURES DE COBERTURA ===
    # Solo contar datos FRESCOS (< 90 días)
    fresh_covers = sum(1 for k, v in coverData.items() 
                      if v == 'COVER' and validityData.get(k, False))
    fresh_no_covers = sum(1 for k, v in coverData.items() 
                         if v == 'NO_COVER' and validityData.get(k, False))
    fresh_sources = sum(1 for k in coverData.keys() if validityData.get(k, False))
    
    features['fresh_covers'] = fresh_covers
    features['fresh_no_covers'] = fresh_no_covers
    features['fresh_sources'] = fresh_sources
    features['fresh_cover_ratio'] = fresh_covers / fresh_sources if fresh_sources > 0 else 0.5
    
    # Total (incluyendo datos antiguos)
    all_covers = sum(1 for v in coverData.values() if v == 'COVER')
    all_no_covers = sum(1 for v in coverData.values() if v == 'NO_COVER')
    all_valid = len([v for v in coverData.values() if v in ['COVER', 'NO_COVER']])
    
    features['all_covers'] = all_covers
    features['all_no_covers'] = all_no_covers
    features['all_sources'] = all_valid
    features['all_cover_ratio'] = all_covers / all_valid if all_valid > 0 else 0.5
    
    # === FEATURES O/U (solo frescos) ===
    fresh_overs = sum(1 for k, v in ouData.items() 
                     if v == 'OVER' and validityData.get(k, False))
    fresh_unders = sum(1 for k, v in ouData.items() 
                      if v == 'UNDER' and validityData.get(k, False))
    fresh_ou_sources = sum(1 for k in ouData.keys() if validityData.get(k, False))
    
    features['fresh_overs'] = fresh_overs
    features['fresh_unders'] = fresh_unders
    features['fresh_ou_sources'] = fresh_ou_sources
    features['fresh_over_ratio'] = fresh_overs / fresh_ou_sources if fresh_ou_sources > 0 else 0.5
    
    # === FEATURES DE FECHAS ===
    # Días desde prev_home y prev_away
    features['prev_home_days'] = dateData.get('prev_home', 999)
    features['prev_away_days'] = dateData.get('prev_away', 999)
    features['h2h_days'] = min(dateData.get('h2h_stadium', 999), 
                               dateData.get('h2h_general', 999))
    
    # Flags de frescura
    features['prev_home_fresh'] = validityData.get('prev_home', False)
    features['prev_away_fresh'] = validityData.get('prev_away', False)
    features['h2h_fresh'] = validityData.get('h2h_stadium', False) or \
                            validityData.get('h2h_general', False)
    features['all_fresh'] = all(validityData.get(k, False) for k in coverData.keys())
    
    # === FEATURES ESPECÍFICAS ===
    for name in ['prev_home', 'prev_away', 'h2h_stadium', 'h2h_general', 
                 'h2h_col3', 'ind_left', 'ind_right']:
        features[f'{name}_cover'] = coverData.get(name)
        features[f'{name}_ou'] = ouData.get(name)
        features[f'{name}_fresh'] = validityData.get(name, False)
    
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


class Rule:
    def __init__(self, conditions, prediction):
        self.conditions = conditions
        self.prediction = prediction
        self.total = 0
        self.correct = 0
    
    def matches(self, features):
        for feat, op, val in self.conditions:
            fv = features.get(feat)
            
            # Para booleanos
            if isinstance(val, bool):
                if fv != val:
                    return False
                continue
            
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
    
    def accuracy(self):
        return self.correct / self.total * 100 if self.total > 0 else 0
    
    def score(self):
        if self.total < MIN_SAMPLES:
            return 0
        # Priorizar precisión alta, pero también considerar muestras
        return (self.accuracy() ** 2) * min(1, self.total / 100)
    
    def __repr__(self):
        conds = ' & '.join([f"{c[0]}{c[1]}{c[2]}" for c in self.conditions])
        return f"IF {conds} -> {self.prediction} ({self.accuracy():.1f}%, n={self.total})"


def generate_condition(feat):
    """Genera una condición aleatoria para una feature."""
    if feat.endswith('_cover'):
        return (feat, '==', random.choice(['COVER', 'NO_COVER']))
    elif feat.endswith('_ou'):
        return (feat, '==', random.choice(['OVER', 'UNDER']))
    elif feat.endswith('_fresh') or feat == 'all_fresh' or feat == 'has_ranks':
        return (feat, '==', True)  # Solo considerar datos frescos
    elif feat == 'ha_fav':
        return (feat, '==', random.choice(['LOCAL', 'VISITA']))
    elif feat in ['fresh_covers', 'fresh_no_covers', 'fresh_overs', 'fresh_unders', 
                  'fresh_sources', 'fresh_ou_sources']:
        return (feat, random.choice(['>=', '<=']), random.randint(2, 6))
    elif 'ratio' in feat:
        return (feat, random.choice(['>=', '<=']), round(random.uniform(0.2, 0.8), 2))
    elif feat in ['prev_home_days', 'prev_away_days', 'h2h_days']:
        return (feat, '<=', random.choice([30, 45, 60, 90]))  # Máximo días
    elif feat == 'rank_diff':
        return (feat, random.choice(['>', '<']), random.choice([-5, -3, 3, 5]))
    elif feat == 'ah_bucket':
        return (feat, '==', random.choice([0, 0.5, 1, 1.5, 2]))
    return None


def generate_random_rule(features_list, prediction):
    """Genera una regla aleatoria."""
    n = random.randint(3, 6)  # Más condiciones para más especificidad
    conditions = []
    used = set()
    
    # Siempre incluir alguna condición de frescura
    fresh_feats = [f for f in features_list if 'fresh' in f]
    if fresh_feats:
        feat = random.choice(fresh_feats)
        used.add(feat)
        cond = generate_condition(feat)
        if cond:
            conditions.append(cond)
    
    for _ in range(n - 1):
        feat = random.choice(features_list)
        if feat in used:
            continue
        used.add(feat)
        cond = generate_condition(feat)
        if cond:
            conditions.append(cond)
    
    return Rule(conditions, prediction) if len(conditions) >= 2 else None


def mutate(rule, features_list):
    """Muta una regla."""
    new_conds = list(rule.conditions)
    action = random.choice(['add', 'remove', 'modify', 'replace'])
    
    if action == 'add' and len(new_conds) < 7:
        feat = random.choice(features_list)
        cond = generate_condition(feat)
        if cond and not any(c[0] == feat for c in new_conds):
            new_conds.append(cond)
    elif action == 'remove' and len(new_conds) > 2:
        new_conds.pop(random.randint(0, len(new_conds) - 1))
    elif action == 'modify' and new_conds:
        idx = random.randint(0, len(new_conds) - 1)
        feat, op, val = new_conds[idx]
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            val = val * random.uniform(0.8, 1.2)
            val = round(val, 2) if isinstance(val, float) else int(val)
            new_conds[idx] = (feat, op, val)
    elif action == 'replace' and new_conds:
        idx = random.randint(0, len(new_conds) - 1)
        feat = random.choice(features_list)
        cond = generate_condition(feat)
        if cond:
            new_conds[idx] = cond
    
    return Rule(new_conds, rule.prediction)


def evaluate_rules(matches, rules):
    """Evalúa las reglas contra los partidos."""
    for r in rules:
        r.total = 0
        r.correct = 0
    
    for match in matches:
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
        
        features = extract_features(match)
        
        for rule in rules:
            if not rule.matches(features):
                continue
            
            rule.total += 1
            
            if rule.prediction in ['LOCAL', 'VISITA'] and ah_result != 'PUSH':
                if rule.prediction == ah_result:
                    rule.correct += 1
            elif rule.prediction in ['OVER', 'UNDER'] and ou_result != 'PUSH':
                if rule.prediction == ou_result:
                    rule.correct += 1
    
    return rules


def load_matches():
    all_matches = []
    for f in DATA_FILES:
        if not f.exists():
            continue
        try:
            with open(f, 'r', encoding='utf-8') as fp:
                all_matches.extend(json.load(fp))
        except:
            continue
    return [m for m in all_matches if parse_score(m.get('final_score') or m.get('score'))]


def main():
    print("=" * 70)
    print("🎯 SISTEMA DE ENTRENAMIENTO ULTRA-PRECISO")
    print("=" * 70)
    print(f"Objetivo: Precisión > {MIN_ACCURACY}% con mínimo {MIN_SAMPLES} muestras")
    print(f"Generaciones: {GENERATIONS}")
    print(f"Máxima antigüedad de datos: {MAX_DAYS_OLD} días")
    print()
    
    matches = load_matches()
    print(f"Partidos cargados: {len(matches)}")
    
    # Lista de features
    features_list = [
        # Coberturas frescas
        'fresh_covers', 'fresh_no_covers', 'fresh_sources', 'fresh_cover_ratio',
        # O/U frescos
        'fresh_overs', 'fresh_unders', 'fresh_ou_sources', 'fresh_over_ratio',
        # Coberturas totales
        'all_covers', 'all_no_covers', 'all_sources', 'all_cover_ratio',
        # Días de antigüedad
        'prev_home_days', 'prev_away_days', 'h2h_days',
        # Flags de frescura
        'prev_home_fresh', 'prev_away_fresh', 'h2h_fresh', 'all_fresh',
        # Coberturas específicas
        'prev_home_cover', 'prev_away_cover', 'h2h_stadium_cover',
        'h2h_general_cover', 'h2h_col3_cover', 'ind_left_cover', 'ind_right_cover',
        # O/U específicos
        'prev_home_ou', 'prev_away_ou', 'h2h_stadium_ou',
        'h2h_general_ou', 'h2h_col3_ou',
        # Otros
        'ha_fav', 'rank_diff', 'ah_bucket', 'has_ranks'
    ]
    
    # Población inicial grande
    population = []
    for pred in ['LOCAL', 'VISITA', 'OVER', 'UNDER']:
        for _ in range(400):
            r = generate_random_rule(features_list, pred)
            if r:
                population.append(r)
    
    print(f"Población inicial: {len(population)} reglas")
    print("\n" + "-" * 70)
    
    best_rules = {'AH': [], 'OU': []}  # Guardar múltiples mejores
    
    for gen in range(GENERATIONS):
        evaluate_rules(matches, population)
        
        # Filtrar reglas con suficientes muestras
        valid = [r for r in population if r.total >= MIN_SAMPLES]
        
        if not valid:
            # Regenerar si no hay válidas
            population = []
            for pred in ['LOCAL', 'VISITA', 'OVER', 'UNDER']:
                for _ in range(400):
                    r = generate_random_rule(features_list, pred)
                    if r:
                        population.append(r)
            continue
        
        # Ordenar por precisión
        valid.sort(key=lambda r: -r.accuracy())
        
        # Buscar reglas con >80% precisión
        for r in valid[:20]:
            if r.accuracy() < MIN_ACCURACY:
                continue
            
            cat = 'AH' if r.prediction in ['LOCAL', 'VISITA'] else 'OU'
            
            # Verificar si ya existe regla similar
            is_new = True
            for existing in best_rules[cat]:
                if abs(existing.accuracy() - r.accuracy()) < 1 and existing.total == r.total:
                    is_new = False
                    break
            
            if is_new:
                best_rules[cat].append(copy.deepcopy(r))
                print(f"\n🔥 Gen {gen+1} - NUEVA REGLA {cat} con {r.accuracy():.1f}%:")
                print(f"   {r}")
        
        # Progreso
        if (gen + 1) % 100 == 0:
            print(f"\n📊 Gen {gen+1}/{GENERATIONS}")
            print(f"   Mejores AH: {len(best_rules['AH'])} reglas")
            print(f"   Mejores OU: {len(best_rules['OU'])} reglas")
            if best_rules['AH']:
                top = max(best_rules['AH'], key=lambda r: r.accuracy())
                print(f"   Top AH: {top.accuracy():.1f}%")
            if best_rules['OU']:
                top = max(best_rules['OU'], key=lambda r: r.accuracy())
                print(f"   Top OU: {top.accuracy():.1f}%")
        
        # Selección y reproducción
        survivors = valid[:100]
        new_pop = list(survivors)
        
        # Mutación
        while len(new_pop) < 1600:
            parent = random.choice(survivors)
            child = mutate(parent, features_list)
            new_pop.append(child)
        
        # Reglas aleatorias nuevas
        for pred in ['LOCAL', 'VISITA', 'OVER', 'UNDER']:
            for _ in range(50):
                r = generate_random_rule(features_list, pred)
                if r:
                    new_pop.append(r)
        
        population = new_pop
    
    # Resultados finales
    print("\n" + "=" * 70)
    print("📊 RESULTADOS FINALES")
    print("=" * 70)
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'config': {
            'min_accuracy': MIN_ACCURACY,
            'min_samples': MIN_SAMPLES,
            'max_days_old': MAX_DAYS_OLD,
            'generations': GENERATIONS
        },
        'rules': []
    }
    
    for cat in ['AH', 'OU']:
        if best_rules[cat]:
            best_rules[cat].sort(key=lambda r: -r.accuracy())
            print(f"\n🏆 MEJORES REGLAS {cat}:")
            for i, r in enumerate(best_rules[cat][:5], 1):
                print(f"   {i}. {r}")
                results['rules'].append({
                    'type': cat,
                    'conditions': [(c[0], c[1], c[2] if not isinstance(c[2], float) else round(c[2], 3)) 
                                  for c in r.conditions],
                    'prediction': r.prediction,
                    'accuracy': round(r.accuracy(), 2),
                    'samples': r.total
                })
    
    # Guardar
    path = RESULTS_DIR / 'ultra_precise_rules.json'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n💾 Guardado en: {path}")
    
    # Resumen
    total_rules = len(best_rules['AH']) + len(best_rules['OU'])
    if total_rules > 0:
        print(f"\n✅ {total_rules} reglas con >{MIN_ACCURACY}% precisión encontradas!")
    else:
        print(f"\n⚠️ No se encontraron reglas con >{MIN_ACCURACY}% precisión")
        print("   Esto puede indicar que los datos no contienen patrones tan fuertes.")


if __name__ == '__main__':
    main()
