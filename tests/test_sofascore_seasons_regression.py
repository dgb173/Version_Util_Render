import requests
from src.modules import sofascore_context as sofa


def test_history_never_uses_latest_cache(monkeypatch):
    latest = {'available': True, 'season_id': 2026, 'views': {'total': [{'team': 'X'}]}}
    monkeypatch.setattr(sofa, '_read_cache', lambda key: latest if key == 'tournament_cache_10770' else None)
    monkeypatch.setattr(sofa, '_write_cache', lambda *args: None)
    monkeypatch.setattr(sofa, '_api_get', lambda *args, **kwargs: {})
    result = sofa.get_league_table_context('Pumas', 'America', tournament_id=10770, season_id=2024)
    assert not result['available']


def test_blocked_provider_reports_verified_league(monkeypatch):
    monkeypatch.setattr(sofa, '_read_cache', lambda key: None)
    response = requests.Response()
    response.status_code = 403
    def blocked(*args, **kwargs):
        raise requests.HTTPError(response=response)
    monkeypatch.setattr(sofa, '_api_get', blocked)
    result = sofa.get_league_table_context('Pumas (W)', 'America (W)', 'Mexico Liga MX Femenil')
    assert result['reason'] == 'provider_blocked'
    assert result['tournament_id'] == 10770
    assert result['source_url'].endswith('/10770')


def test_events_load_beyond_five_pages_and_deduplicate(monkeypatch):
    def get(path):
        page = int(path.rsplit('/', 1)[1])
        return {'events': [{'id': page}, {'id': 99}], 'hasNextPage': page < 6}
    monkeypatch.setattr(sofa, '_api_get', get)
    events = sofa._load_season_events(1, 1)
    assert events.complete
    assert len(events) == 7  # event id 0 is ignored; duplicate id 99 is retained once


def test_interrupted_events_are_marked_partial(monkeypatch):
    def get(path):
        if path.endswith('/1'):
            raise requests.ConnectionError('offline')
        return {'events': [{'id': 9}], 'hasNextPage': True}
    monkeypatch.setattr(sofa, '_api_get', get)
    events = sofa._load_season_events(1, 1)
    assert len(events) == 1
    assert not events.complete


def test_women_league_does_not_match_mens_alias(monkeypatch):
    monkeypatch.setattr(sofa, '_load_league_aliases', lambda: {'mexico liga mx': {'id': 1}})
    assert sofa._lookup_league_alias('Mexico Liga MX Women') is None
