import ast
import re
from pathlib import Path


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
