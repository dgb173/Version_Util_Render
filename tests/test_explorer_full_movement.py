import ast
import json
import re
from pathlib import Path
from types import SimpleNamespace


def test_movement_filter_matches_stadium_buckets_and_direction():
    path = Path(__file__).resolve().parents[1] / 'src' / 'app.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_explorer_movement_matches')
    namespace = {'re': re}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), 'exec'), namespace)
    matches = namespace['_explorer_movement_matches']
    filters = {'h2h_stadium_start_ah': [1.5], 'h2h_stadium_end_ah': [0.5]}
    assert matches({'h2h_stadium': {'movement': '1.25 → 0.5'}}, filters)
    assert not matches({'h2h_stadium': {'movement': '0.5 → 1.25'}}, filters)
    assert not matches({'h2h_stadium': {'movement': 'N/A'}}, filters)


def test_full_archive_wins_over_partial_sql_bootstrap(tmp_path):
    path = Path(__file__).resolve().parents[1] / 'src' / 'modules' / 'data_manager.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'load_explorer_matches')
    full_path = tmp_path / 'explorer_full.sqlite.gz'
    full_path.touch()
    calls = []
    namespace = {
        'get_bucket_name': lambda ah: 'data_ah_0.5.json',
        '_explorer_cache_ttl': 0,
        'json': json,
        'explorer_archive': SimpleNamespace(
            FULL_PATH=full_path,
            load_page=lambda **kwargs: calls.append(kwargs) or [{'match_id': 'archived'}],
        ),
        'sql_store': SimpleNamespace(fetch_matches=lambda **kwargs: [{'match_id': 'partial'}]),
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), 'exec'), namespace)
    rows = namespace['load_explorer_matches']('0.5', scan_limit=150, offset=300)
    assert rows == [{'match_id': 'archived'}]
    assert calls == [{'bucket': 'data_ah_0.5.json', 'limit': 150, 'offset': 300, 'movement_filters': None}]


def test_indexed_archive_returns_full_movement_group():
    from src.modules import explorer_archive

    if not explorer_archive.FULL_PATH.exists() or not explorer_archive.MOVEMENT_INDEX_PATH.exists():
        return
    rows = explorer_archive.load_page(
        bucket='data_ah_0.5.json', limit=1000,
        movement_filters={'h2h_stadium_start_ah': ['1.5'], 'h2h_stadium_end_ah': ['0.5']},
    )
    assert len(rows) >= 400
    assert {'3111675', '3112883'} <= {str(row['match_id']) for row in rows}
