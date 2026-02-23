# scripts/audit_rules.py
"""
AUDITORÍA DE TODAS LAS REGLAS
=============================
Evalúa CADA regla implementada contra TODOS los datos históricos
y genera un informe de calidad real.

Reglas a auditar:
- AH: M1-M8
- O/U: OU1-OU6
"""

import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
DATA_FILES = list(DATA_DIR.glob('data*.json'))


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


def did_cover(home_goals, away_goals, ah_line, is_home: bool) -> str:
    if is_home:
        adjusted = (home_goals - away_goals) - ah_line
    else:
        adjusted = (away_goals - home_goals) + ah_line
    if adjusted > 0.25:
        return 'COVER'
    elif adjusted < -0.25:
        return 'NO_COVER'
    return 'PUSH'


def extract_features(match: Dict) -> Dict:
    """Extrae features necesarias para evaluar las reglas."""
    f = {}
    
    main_odds = match.get('main_match_odds') or {}
    try:
        ah_line = float(main_odds.get('ah_linea', 0) or 0)
        ou_line = float(main_odds.get('goals_linea', 2.5) or 2.5)
    except:
        ah_line, ou_line = 0, 2.5
    
    f['ah_line'] = ah_line
    f['ou_line'] = ou_line
    f['ah_bucket'] = round(abs(ah_line) * 2) / 2
    f['ou_bucket'] = round(ou_line * 2) / 2
    f['ha_fav'] = 'LOCAL' if ah_line > 0 else ('VISITA' if ah_line < 0 else 'NEUTRO')
    
    # Market analysis data
    market = match.get('market_analysis_data') or {}
    stadium_m = market.get('stadium') or {}
    general_m = market.get('general') or {}
    
    f['h2h_stadium_covered'] = stadium_m.get('is_covered')
    f['h2h_general_covered'] = general_m.get('is_covered')
    f['h2h_both_covered'] = f['h2h_stadium_covered'] == True and f['h2h_general_covered'] == True
    
    # Parsear movimiento
    f['stadium_line_increased'] = False
    f['stadium_fav_changed'] = False
    f['stadium_line_change'] = 0
    if stadium_m.get('movement'):
        parts = stadium_m['movement'].replace('→', '->').split('->')
        if len(parts) == 2:
            try:
                before = float(parts[0].strip())
                after = float(parts[1].strip())
                f['stadium_line_change'] = after - before
                f['stadium_fav_changed'] = (before > 0) != (after > 0)
                f['stadium_line_increased'] = after > before
            except:
                pass
    
    # Prev matches
    prev_home = match.get('last_home_match') or {}
    prev_away = match.get('last_away_match') or {}
    
    # Prev away total goals
    prev_away_score = parse_score(prev_away.get('score'))
    f['prev_away_total'] = (prev_away_score[0] + prev_away_score[1]) if prev_away_score else 0
    
    # Prev away ou
    f['prev_away_ou'] = 'OVER' if f['prev_away_total'] > 2.5 else 'UNDER'
    
    # H2H Col3 goals
    h2h_col3 = match.get('h2h_col3') or {}
    if h2h_col3.get('status') == 'found':
        try:
            f['h2h_col3_goals'] = int(h2h_col3.get('goles_home', 0) or 0) + int(h2h_col3.get('goles_away', 0) or 0)
        except:
            f['h2h_col3_goals'] = 0
    else:
        f['h2h_col3_goals'] = 0
    
    # Ind left goals
    comp = match.get('comparativas_indirectas') or {}
    left = comp.get('left') or {}
    left_score = parse_score(left.get('score'))
    f['ind_left_goals'] = (left_score[0] + left_score[1]) if left_score else 0
    
    # Ind right ou
    right = comp.get('right') or {}
    right_score = parse_score(right.get('score'))
    f['ind_right_ou'] = 'OVER' if right_score and (right_score[0] + right_score[1]) > 2.5 else 'UNDER'
    
    # Rankings
    try:
        f['home_rank'] = int((match.get('home_standings') or {}).get('ranking', 0) or 0)
        f['away_rank'] = int((match.get('away_standings') or {}).get('ranking', 0) or 0)
        f['rank_diff'] = f['home_rank'] - f['away_rank']
    except:
        f['home_rank'] = 0
        f['away_rank'] = 0
        f['rank_diff'] = 0
    
    # Covers
    cover_results = []
    
    def get_cover(data, is_home):
        if not data:
            return None
        score = parse_score(data.get('score'))
        if not score:
            return None
        try:
            ah = float(data.get('handicap_line_raw') or data.get('handicap') or 0)
        except:
            ah = 0
        return did_cover(score[0], score[1], ah, is_home)
    
    f['prev_home_cover'] = get_cover(prev_home, True)
    f['prev_away_cover'] = get_cover(prev_away, False)
    if f['prev_home_cover']:
        cover_results.append(f['prev_home_cover'])
    if f['prev_away_cover']:
        cover_results.append(f['prev_away_cover'])
    
    covers = sum(1 for c in cover_results if c == 'COVER')
    no_covers = sum(1 for c in cover_results if c == 'NO_COVER')
    valid = len([c for c in cover_results if c in ['COVER', 'NO_COVER']])
    
    f['covers'] = covers
    f['cover_ratio'] = covers / valid if valid > 0 else 0.5
    f['all_covered'] = covers == valid and valid >= 2
    
    # Over ratio
    ou_results = []
    for name, data, is_home in [
        ('prev_home', prev_home, True),
        ('prev_away', prev_away, False),
    ]:
        if data:
            score = parse_score(data.get('score'))
            if score:
                total = score[0] + score[1]
                ou_results.append('OVER' if total > 2.5 else 'UNDER')
    
    overs = sum(1 for o in ou_results if o == 'OVER')
    f['overs'] = overs
    f['unders'] = len(ou_results) - overs
    f['over_ratio'] = overs / len(ou_results) if ou_results else 0.5
    f['valid_sources'] = valid
    
    return f


# ==================== DEFINICIÓN DE REGLAS ====================

RULES_AH = [
    {
        'name': 'M1',
        'description': 'Cambio de favorito + línea aumentada en AH 0.5',
        'prediction': 'VISITA',
        'claimed_accuracy': 76,
        'conditions': lambda f: (
            f.get('stadium_fav_changed') == True and
            f.get('ah_bucket') == 0.5 and
            f.get('stadium_line_increased') == True and
            f.get('prev_away_total', 99) <= 3 and
            f.get('rank_diff', -99) > -5
        )
    },
    {
        'name': 'M2',
        'description': 'H2H general no cubrió + H2H col3 goles ≥3 + ranking',
        'prediction': 'VISITA',
        'claimed_accuracy': 74,
        'conditions': lambda f: (
            f.get('ah_bucket') == 0.5 and
            f.get('h2h_general_covered') == False and
            f.get('h2h_col3_goals', 0) >= 3 and
            f.get('rank_diff', 0) < 0 and
            f.get('cover_ratio', 0) >= 0.27
        )
    },
    {
        'name': 'M3',
        'description': 'H2H general no cubrió + goles altos + ranking',
        'prediction': 'VISITA',
        'claimed_accuracy': 72,
        'conditions': lambda f: (
            f.get('ah_bucket') == 0.5 and
            f.get('h2h_general_covered') == False and
            f.get('h2h_col3_goals', 0) >= 3 and
            f.get('rank_diff', 0) < 0
        )
    },
    {
        'name': 'M4',
        'description': 'Prev home no cubrió + H2H col3 bajo',
        'prediction': 'LOCAL',
        'claimed_accuracy': 70,
        'conditions': lambda f: (
            f.get('ah_bucket') == 0.5 and
            f.get('prev_away_total', 99) <= 2 and
            f.get('prev_home_cover') == 'NO_COVER' and
            f.get('h2h_col3_goals', 99) <= 1
        )
    },
    {
        'name': 'M5',
        'description': 'H2H mixed + prev away UNDER',
        'prediction': 'VISITA',
        'claimed_accuracy': 69,
        'conditions': lambda f: (
            f.get('h2h_stadium_covered') != f.get('h2h_general_covered') and
            f.get('covers', 0) >= 2 and
            f.get('prev_away_ou') == 'UNDER'
        )
    },
    {
        'name': 'M6',
        'description': 'Cover ratio bajo + prev away pocos goles',
        'prediction': 'LOCAL',
        'claimed_accuracy': 66,
        'conditions': lambda f: (
            f.get('ah_bucket') == 0.5 and
            f.get('prev_away_total', 99) <= 2 and
            f.get('prev_away_cover') == 'NO_COVER' and
            f.get('cover_ratio', 1) <= 0.35
        )
    },
    {
        'name': 'M7',
        'description': 'Ambos H2H cubrieron + HA Local',
        'prediction': 'LOCAL',
        'claimed_accuracy': 65,
        'conditions': lambda f: (
            f.get('h2h_stadium_covered') == True and
            f.get('h2h_general_covered') == True and
            f.get('ha_fav') == 'LOCAL'
        )
    },
    {
        'name': 'M8',
        'description': 'Ambos H2H no cubrieron + HA Visita',
        'prediction': 'VISITA',
        'claimed_accuracy': 65,
        'conditions': lambda f: (
            f.get('h2h_stadium_covered') == False and
            f.get('h2h_general_covered') == False and
            f.get('ha_fav') == 'VISITA'
        )
    },
]

RULES_OU = [
    {
        'name': 'OU1',
        'description': 'h2h_both_covered + ou 2.5 + prev_away UNDER + over_ratio≤0.69 + rank_diff≥-3',
        'prediction': 'UNDER',
        'claimed_accuracy': 87,
        'conditions': lambda f: (
            f.get('h2h_both_covered') == True and
            f.get('ou_bucket') == 2.5 and
            f.get('prev_away_ou') == 'UNDER' and
            not f.get('all_covered', False) and
            f.get('over_ratio', 1) <= 0.69 and
            f.get('rank_diff', -99) >= -3
        )
    },
    {
        'name': 'OU2',
        'description': 'h2h_both_covered + ou 2.5 + prev_away UNDER + rank_diff≥0',
        'prediction': 'UNDER',
        'claimed_accuracy': 86,
        'conditions': lambda f: (
            f.get('h2h_both_covered') == True and
            f.get('ou_bucket') == 2.5 and
            f.get('prev_away_ou') == 'UNDER' and
            f.get('rank_diff', -99) >= 0 and
            f.get('ind_left_goals', 99) <= 5
        )
    },
    {
        'name': 'OU3',
        'description': 'h2h_both_covered + ou 2.5 + not all_covered + rank_diff≥-3',
        'prediction': 'UNDER',
        'claimed_accuracy': 82,
        'conditions': lambda f: (
            f.get('h2h_both_covered') == True and
            f.get('ou_bucket') == 2.5 and
            f.get('prev_away_ou') == 'UNDER' and
            not f.get('all_covered', False) and
            f.get('rank_diff', -99) >= -3
        )
    },
    {
        'name': 'OU4',
        'description': 'h2h_both_covered + ou 2.5 + over_ratio≤0.53',
        'prediction': 'UNDER',
        'claimed_accuracy': 78,
        'conditions': lambda f: (
            f.get('h2h_both_covered') == True and
            f.get('ou_bucket') == 2.5 and
            f.get('prev_away_ou') == 'UNDER' and
            not f.get('all_covered', False) and
            f.get('over_ratio', 1) <= 0.53
        )
    },
    {
        'name': 'OU5',
        'description': 'h2h_both_covered + ou 2.5 + ind_right UNDER',
        'prediction': 'UNDER',
        'claimed_accuracy': 75,
        'conditions': lambda f: (
            f.get('h2h_both_covered') == True and
            f.get('ou_bucket') == 2.5 and
            f.get('prev_away_ou') == 'UNDER' and
            f.get('ind_right_ou') == 'UNDER'
        )
    },
    {
        'name': 'OU6',
        'description': 'Línea disminuyó + visitante favorito',
        'prediction': 'OVER',
        'claimed_accuracy': 66,
        'conditions': lambda f: (
            f.get('stadium_line_change', 0) < 0 and
            f.get('ha_fav') == 'VISITA' and
            f.get('valid_sources', 0) == 2 and
            f.get('home_rank', 0) > 0 and
            f.get('away_rank', 0) > 0
        )
    },
]


def load_all_matches():
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
    return [m for m in all_matches if parse_score(m.get('final_score') or m.get('score'))]


def main():
    print("=" * 70)
    print("🔍 AUDITORÍA DE REGLAS")
    print("=" * 70)
    
    print("\n📂 Cargando partidos...")
    matches = load_all_matches()
    print(f"   Total partidos con resultado: {len(matches)}")
    
    print("\n" + "=" * 70)
    print("📊 EVALUACIÓN DE REGLAS AH")
    print("=" * 70)
    
    results_ah = []
    for rule in RULES_AH:
        total = 0
        correct = 0
        
        for match in matches:
            score = match.get('final_score') or match.get('score')
            parsed = parse_score(score)
            if not parsed:
                continue
            
            main_odds = match.get('main_match_odds') or {}
            try:
                ah = float(main_odds.get('ah_linea', 0) or 0)
            except:
                continue
            
            features = extract_features(match)
            
            if not rule['conditions'](features):
                continue
            
            total += 1
            ah_result = get_ah_winner(parsed[0], parsed[1], ah)
            if ah_result != 'PUSH' and ah_result == rule['prediction']:
                correct += 1
        
        real_acc = (correct / total * 100) if total > 0 else 0
        diff = real_acc - rule['claimed_accuracy']
        
        status = "✅" if real_acc >= rule['claimed_accuracy'] - 3 else "⚠️" if real_acc >= 50 else "❌"
        
        results_ah.append({
            'name': rule['name'],
            'prediction': rule['prediction'],
            'claimed': rule['claimed_accuracy'],
            'real': round(real_acc, 1),
            'diff': round(diff, 1),
            'samples': total,
            'status': status,
            'description': rule['description']
        })
        
        print(f"\n{status} {rule['name']}: {rule['prediction']}")
        print(f"   Declarado: {rule['claimed_accuracy']}% | Real: {real_acc:.1f}% | Dif: {diff:+.1f}%")
        print(f"   Muestras: {total} | {rule['description']}")
    
    print("\n" + "=" * 70)
    print("📊 EVALUACIÓN DE REGLAS O/U")
    print("=" * 70)
    
    results_ou = []
    for rule in RULES_OU:
        total = 0
        correct = 0
        
        for match in matches:
            score = match.get('final_score') or match.get('score')
            parsed = parse_score(score)
            if not parsed:
                continue
            
            main_odds = match.get('main_match_odds') or {}
            try:
                ou = float(main_odds.get('goals_linea', 2.5) or 2.5)
            except:
                continue
            
            features = extract_features(match)
            
            if not rule['conditions'](features):
                continue
            
            total += 1
            ou_result = get_ou_result(parsed[0], parsed[1], ou)
            if ou_result != 'PUSH' and ou_result == rule['prediction']:
                correct += 1
        
        real_acc = (correct / total * 100) if total > 0 else 0
        diff = real_acc - rule['claimed_accuracy']
        
        status = "✅" if real_acc >= rule['claimed_accuracy'] - 3 else "⚠️" if real_acc >= 50 else "❌"
        
        results_ou.append({
            'name': rule['name'],
            'prediction': rule['prediction'],
            'claimed': rule['claimed_accuracy'],
            'real': round(real_acc, 1),
            'diff': round(diff, 1),
            'samples': total,
            'status': status,
            'description': rule['description']
        })
        
        print(f"\n{status} {rule['name']}: {rule['prediction']}")
        print(f"   Declarado: {rule['claimed_accuracy']}% | Real: {real_acc:.1f}% | Dif: {diff:+.1f}%")
        print(f"   Muestras: {total} | {rule['description']}")
    
    # Resumen
    print("\n" + "=" * 70)
    print("📌 RESUMEN DE AUDITORÍA")
    print("=" * 70)
    
    all_results = results_ah + results_ou
    valid_rules = [r for r in all_results if r['samples'] >= 30]
    accurate_rules = [r for r in valid_rules if r['status'] == '✅']
    warning_rules = [r for r in valid_rules if r['status'] == '⚠️']
    failed_rules = [r for r in valid_rules if r['status'] == '❌']
    
    print(f"\nReglas con ≥30 muestras: {len(valid_rules)}")
    print(f"   ✅ Cumplen precisión: {len(accurate_rules)}")
    print(f"   ⚠️ Cerca de objetivo: {len(warning_rules)}")
    print(f"   ❌ No cumplen: {len(failed_rules)}")
    
    if accurate_rules:
        print("\n🏆 REGLAS VALIDADAS:")
        for r in sorted(accurate_rules, key=lambda x: -x['real']):
            print(f"   {r['name']}: {r['real']}% (n={r['samples']}) -> {r['prediction']}")
    
    if warning_rules:
        print("\n⚠️ REGLAS A REVISAR:")
        for r in warning_rules:
            print(f"   {r['name']}: {r['real']}% vs {r['claimed']}% declarado")
    
    if failed_rules:
        print("\n❌ REGLAS FALLIDAS (eliminar):")
        for r in failed_rules:
            print(f"   {r['name']}: {r['real']}% vs {r['claimed']}% declarado")


if __name__ == '__main__':
    main()
