from scripts.cloud_cache_pipeline import merge_finished_explorer_recent, read_json, write_json


def test_finished_cache_updates_render_explorer_archive(tmp_path):
    path = tmp_path / 'data/explorer_recent.json'
    write_json(path, [{'match_id': '101', 'match_date': '2026-11-14', '_explorer_bucket': 'data_ah_0.5.json'}])
    row = {
        'match_id': '102', 'match_date': '2026-11-15', 'score': '2:1',
        'main_match_odds': {'ah_linea': 0.5},
        'market_analysis_data': {'stadium': {'movement': '1.5 → 0.5'}},
        'pre_match_context': {'unneeded': True},
    }
    assert merge_finished_explorer_recent(tmp_path, [row]) == 2
    archive = read_json(path)
    assert [item['match_id'] for item in archive] == ['102', '101']
    assert archive[0]['_explorer_bucket'] == 'data_ah_0.5.json'
    assert archive[0]['market_analysis_data']['stadium']['movement'] == '1.5 → 0.5'
    assert 'pre_match_context' not in archive[0]
