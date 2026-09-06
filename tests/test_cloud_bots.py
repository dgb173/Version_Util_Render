from flask import Flask
from modules import cloud_bots as bots


def client(monkeypatch):
    app = Flask(__name__)
    app.register_blueprint(bots.blueprint)
    monkeypatch.setenv('ACTIONS_TRIGGER_KEY', 'test-key')
    monkeypatch.setenv('GITHUB_ACTIONS_TOKEN', 'test-token')
    bots._last_dispatch.clear()
    return app.test_client()


def test_no_unauthorized_dispatch(monkeypatch):
    c = client(monkeypatch)
    monkeypatch.setattr(bots.requests, 'post', lambda *a, **k: (_ for _ in ()).throw(AssertionError('must not dispatch')))
    assert c.post('/api/cloud_bots/trigger', json={'kind':'upcoming'}).status_code == 401


def test_running_job_is_not_duplicated(monkeypatch):
    c = client(monkeypatch)
    monkeypatch.setattr(bots, 'latest_run', lambda *a: {'status':'in_progress','id':1})
    monkeypatch.setattr(bots.requests, 'post', lambda *a, **k: (_ for _ in ()).throw(AssertionError('duplicate')))
    response = c.post('/api/cloud_bots/trigger', json={'kind':'upcoming'}, headers={'X-Trigger-Key':'test-key'})
    assert response.status_code == 202 and response.json['status'] == 'running'


def test_dispatch_uses_server_token_and_unlimited_defaults(monkeypatch):
    c = client(monkeypatch)
    calls = []
    monkeypatch.setattr(bots, 'latest_run', lambda *a: {'status':'completed'})
    class Response:
        def raise_for_status(self): pass
    monkeypatch.setattr(bots.requests, 'post', lambda *a, **kw: calls.append((a,kw)) or Response())
    response = c.post('/api/cloud_bots/trigger', json={'kind':'upcoming'}, headers={'X-Trigger-Key':'test-key'})
    assert response.status_code == 202
    assert calls[0][1]['json']['inputs'] == {'force_full':'false'}
    assert 'test-token' not in response.get_data(as_text=True)
    c.post('/api/cloud_bots/trigger', json={'kind':'upcoming'}, headers={'X-Trigger-Key':'test-key'})
    assert len(calls) == 1
