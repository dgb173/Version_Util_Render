"""Verifica tablas reales actuales e históricas para cada liga de Precacheo."""
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from modules.nowgoal_tables import get_context


def check(match):
    name = match['league_name']
    current = get_context(match['home_name'], match['away_name'], name)
    seasons = current.get('seasons') or []
    past = get_context(match['home_name'], match['away_name'], name, season_id=seasons[1]['id']) if len(seasons) > 1 else {}
    return dict(league=name, current=current.get('available', False), past=past.get('available', False),
                reason=current.get('reason'), source_url=current.get('source_url'),
                teams=len(current.get('views', {}).get('total', [])),
                ou_matches=current.get('ou', {}).get('matches_analyzed', 0),
                past_teams=len(past.get('views', {}).get('total', [])))


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    matches = json.loads((ROOT / 'data/data_precacheo.json').read_text(encoding='utf-8'))
    leagues = {m['league_name']: m for m in matches}
    results = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for future in as_completed([pool.submit(check, m) for m in leagues.values()]):
            row = future.result()
            results.append(row)
            print(json.dumps(row, ensure_ascii=False), flush=True)
    report = ROOT / 'data' / 'league_tables_audit.json'
    report.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
    print('TOTAL', len(results), 'CURRENT', sum(r['current'] for r in results), 'PAST', sum(r['past'] for r in results))
