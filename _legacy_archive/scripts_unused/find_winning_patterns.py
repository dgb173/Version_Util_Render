# scripts/find_winning_patterns.py
"""
Busca PATRONES GANADORES específicos en los datos históricos.
En lugar de optimizar pesos, busca condiciones que históricamente
dan >65% de aciertos.

Uso: py scripts/find_winning_patterns.py
"""

import json
from pathlib import Path
from collections import defaultdict
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RESULTS_DIR = PROJECT_ROOT / 'backtest_results'
RESULTS_DIR.mkdir(exist_ok=True)

STATS_KEYS = ['Tiros', 'Tiros a Puerta', 'Ataques', 'Ataques Peligrosos']

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
    if not score_str or ':' not in str(score_str):
        return None
    try:
        parts = str(score_str).replace('-', ':').split(':')
        return int(parts[0]), int(parts[1])
    except:
        return None


def get_ah_winner(home_goals, away_goals, ah_line):
    """Retorna 'LOCAL', 'VISITA', o 'PUSH'."""
    adjusted_diff = (home_goals - away_goals) - ah_line
    if adjusted_diff > 0.25:
        return 'LOCAL'
    elif adjusted_diff < -0.25:
        return 'VISITA'
    return 'PUSH'


def parse_stats(stats_rows):
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


def count_stat_wins(stats_dict, home_perspective=True):
    """Cuenta cuántas de las 4 stats gana cada equipo."""
    local_wins = 0
    visit_wins = 0
    for stat in STATS_KEYS:
        if stat not in stats_dict:
            continue
        if home_perspective:
            local_val = stats_dict[stat]['home']
            visit_val = stats_dict[stat]['away']
        else:
            local_val = stats_dict[stat]['away']
            visit_val = stats_dict[stat]['home']
        if local_val > visit_val:
            local_wins += 1
        elif visit_val > local_val:
            visit_wins += 1
    return local_wins, visit_wins


def analyze_source(source_data, home_perspective=True):
    """Analiza una fuente y devuelve (local_wins, visit_wins)."""
    if not source_data:
        return None
    stats = parse_stats(source_data.get('stats_rows', []))
    if not stats:
        return None
    return count_stat_wins(stats, home_perspective)


def load_all_matches():
    all_matches = []
    for data_file in DATA_FILES:
        if not data_file.exists():
            continue
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                all_matches.extend(json.load(f))
        except:
            continue
    return all_matches


def main():
    print("=" * 70)
    print("🔍 BÚSQUEDA DE PATRONES GANADORES")
    print("=" * 70)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    matches = load_all_matches()
    print(f"Total partidos: {len(matches)}")
    
    # Patrones a analizar
    patterns = defaultdict(lambda: {'total': 0, 'local_wins': 0, 'visit_wins': 0})
    
    for match in matches:
        score = match.get('final_score') or match.get('score')
        parsed = parse_score(score)
        if not parsed:
            continue
        
        home_goals, away_goals = parsed
        main_odds = match.get('main_match_odds') or {}
        try:
            ah_line = float(main_odds.get('ah_linea', 0) or 0)
        except:
            continue
        
        actual_winner = get_ah_winner(home_goals, away_goals, ah_line)
        if actual_winner == 'PUSH':
            continue
        
        # Analizar cada fuente
        sources_analysis = {}
        
        # Prev Home
        result = analyze_source(match.get('last_home_match'), True)
        if result:
            sources_analysis['prev_home'] = result
        
        # Prev Away
        result = analyze_source(match.get('last_away_match'), False)
        if result:
            sources_analysis['prev_away'] = result
        
        # H2H Stadium
        result = analyze_source(match.get('h2h_stadium'), True)
        if result:
            sources_analysis['h2h_stadium'] = result
        
        # H2H General
        result = analyze_source(match.get('h2h_general'), True)
        if result:
            sources_analysis['h2h_general'] = result
        
        # H2H Col3
        result = analyze_source(match.get('h2h_col3'), True)
        if result:
            sources_analysis['h2h_col3'] = result
        
        # Indirectas
        comparativas = match.get('comparativas_indirectas') or {}
        left = comparativas.get('left')
        if left:
            result = analyze_source(left, left.get('localia') == 'H')
            if result:
                sources_analysis['ind_left'] = result
        
        right = comparativas.get('right')
        if right:
            result = analyze_source(right, right.get('localia') != 'A')
            if result:
                sources_analysis['ind_right'] = result
        
        # Calcular totales
        total_local = sum(r[0] for r in sources_analysis.values())
        total_visit = sum(r[1] for r in sources_analysis.values())
        num_sources = len(sources_analysis)
        
        if num_sources == 0:
            continue
        
        # HA favorito
        ha_fav = 'NEUTRO'
        if ah_line > 0:
            ha_fav = 'LOCAL'
        elif ah_line < 0:
            ha_fav = 'VISITA'
        
        # Generar patrones específicos
        
        # Patrón 1: Dominio absoluto (4-0 o 0-4 en alguna fuente)
        for source_name, (l, v) in sources_analysis.items():
            if l == 4 and v == 0:
                pattern = f"DOMINIO_TOTAL_{source_name}_LOCAL"
                patterns[pattern]['total'] += 1
                if actual_winner == 'LOCAL':
                    patterns[pattern]['local_wins'] += 1
            elif v == 4 and l == 0:
                pattern = f"DOMINIO_TOTAL_{source_name}_VISITA"
                patterns[pattern]['total'] += 1
                if actual_winner == 'VISITA':
                    patterns[pattern]['visit_wins'] += 1
        
        # Patrón 2: Múltiples fuentes dominan
        local_sources = sum(1 for l, v in sources_analysis.values() if l > v)
        visit_sources = sum(1 for l, v in sources_analysis.values() if v > l)
        
        if local_sources >= 5 and num_sources >= 5:
            pattern = f"5+_FUENTES_LOCAL_num{num_sources}"
            patterns[pattern]['total'] += 1
            if actual_winner == 'LOCAL':
                patterns[pattern]['local_wins'] += 1
        
        if visit_sources >= 5 and num_sources >= 5:
            pattern = f"5+_FUENTES_VISITA_num{num_sources}"
            patterns[pattern]['total'] += 1
            if actual_winner == 'VISITA':
                patterns[pattern]['visit_wins'] += 1
        
        # Patrón 3: Score total muy desequilibrado
        if total_local >= 20 and total_visit <= 5:
            pattern = "SCORE_LOCAL_20+_vs_5-"
            patterns[pattern]['total'] += 1
            if actual_winner == 'LOCAL':
                patterns[pattern]['local_wins'] += 1
        
        if total_visit >= 20 and total_local <= 5:
            pattern = "SCORE_VISITA_20+_vs_5-"
            patterns[pattern]['total'] += 1
            if actual_winner == 'VISITA':
                patterns[pattern]['visit_wins'] += 1
        
        # Patrón 4: HA + Stats alineados
        stats_fav = 'LOCAL' if total_local > total_visit else ('VISITA' if total_visit > total_local else 'NEUTRO')
        
        if ha_fav == stats_fav and stats_fav != 'NEUTRO':
            # Score diferencial
            diff = abs(total_local - total_visit)
            if diff >= 10:
                pattern = f"HA+STATS_ALINEADOS_diff10+_{stats_fav}"
                patterns[pattern]['total'] += 1
                if actual_winner == stats_fav:
                    patterns[pattern]['local_wins' if stats_fav == 'LOCAL' else 'visit_wins'] += 1
        
        # Patrón 5: H2H Col3 domina completamente + HA coincide
        if 'h2h_col3' in sources_analysis:
            l, v = sources_analysis['h2h_col3']
            if l >= 3 and v == 0 and ha_fav == 'LOCAL':
                pattern = "H2H_COL3_3-0_+_HA_LOCAL"
                patterns[pattern]['total'] += 1
                if actual_winner == 'LOCAL':
                    patterns[pattern]['local_wins'] += 1
            elif v >= 3 and l == 0 and ha_fav == 'VISITA':
                pattern = "H2H_COL3_0-3_+_HA_VISITA"
                patterns[pattern]['total'] += 1
                if actual_winner == 'VISITA':
                    patterns[pattern]['visit_wins'] += 1
        
        # Patrón 6: Prev Home + Prev Away ambos dominan para el mismo equipo
        if 'prev_home' in sources_analysis and 'prev_away' in sources_analysis:
            ph_l, ph_v = sources_analysis['prev_home']
            pa_l, pa_v = sources_analysis['prev_away']
            
            if ph_l >= 3 and pa_l >= 3:
                pattern = "PREV_BOTH_DOMINA_LOCAL_3+"
                patterns[pattern]['total'] += 1
                if actual_winner == 'LOCAL':
                    patterns[pattern]['local_wins'] += 1
            
            if ph_v >= 3 and pa_v >= 3:
                pattern = "PREV_BOTH_DOMINA_VISITA_3+"
                patterns[pattern]['total'] += 1
                if actual_winner == 'VISITA':
                    patterns[pattern]['visit_wins'] += 1
    
    # Mostrar patrones con alta precisión
    print("\n" + "=" * 70)
    print("📊 PATRONES CON ALTA PRECISIÓN (>60%, mínimo 30 muestras)")
    print("=" * 70)
    
    winning_patterns = []
    
    for pattern, data in sorted(patterns.items()):
        total = data['total']
        if total < 30:
            continue
        
        # Calcular precisión
        if 'LOCAL' in pattern:
            correct = data['local_wins']
        elif 'VISITA' in pattern:
            correct = data['visit_wins']
        else:
            correct = max(data['local_wins'], data['visit_wins'])
        
        accuracy = correct / total * 100 if total > 0 else 0
        
        if accuracy >= 55:
            winning_patterns.append({
                'pattern': pattern,
                'accuracy': accuracy,
                'correct': correct,
                'total': total
            })
    
    # Ordenar por precisión
    winning_patterns.sort(key=lambda x: -x['accuracy'])
    
    for p in winning_patterns:
        emoji = "🔥" if p['accuracy'] >= 65 else "✅" if p['accuracy'] >= 60 else "📊"
        print(f"{emoji} {p['pattern']}")
        print(f"   Precisión: {p['accuracy']:.1f}% ({p['correct']}/{p['total']})")
        print()
    
    # Guardar resultados
    results_path = RESULTS_DIR / 'winning_patterns.json'
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump({
            'patterns': winning_patterns,
            'timestamp': datetime.now().isoformat(),
        }, f, indent=2)
    
    print(f"\n💾 Patrones guardados en: {results_path}")
    
    # Resumen
    print("\n" + "=" * 70)
    print("📌 RESUMEN")
    print("=" * 70)
    
    best = [p for p in winning_patterns if p['accuracy'] >= 60]
    if best:
        print(f"\n🎯 {len(best)} patrones con >60% precisión encontrados")
        print("\nMejores patrones para implementar:")
        for p in best[:5]:
            print(f"  - {p['pattern']}: {p['accuracy']:.1f}%")
    else:
        print("\n⚠️ No se encontraron patrones con >60% de precisión")
        print("   Esto indica que las estadísticas de partidos previos")
        print("   no son buenos predictores del resultado del handicap.")


if __name__ == '__main__':
    main()
