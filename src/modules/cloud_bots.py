"""Authenticated, duplicate-aware GitHub Actions controls for Render and local."""
import json
import os
from pathlib import Path
import secrets
import threading
import time
import requests
from flask import Blueprint, jsonify, request

blueprint = Blueprint('cloud_bots', __name__)
WORKFLOWS = {'upcoming': 'actualizar-precacheo.yml', 'finished': 'cachear-terminados.yml',
             'list': 'actualizar-lista-proximos.yml'}
ROOT = Path(__file__).resolve().parents[2]
_lock = threading.Lock()
_last_dispatch = {}


def settings():
    return (os.getenv('GITHUB_REPOSITORY', 'dgb173/Version_Util_Render'),
            os.getenv('GITHUB_ACTIONS_TOKEN') or os.getenv('GH_TOKEN'),
            os.getenv('GITHUB_WORKFLOW_REF', 'main'))


def authorize():
    configured = os.getenv('ACTIONS_TRIGGER_KEY', '')
    provided = request.headers.get('X-Trigger-Key', '')
    if not configured:
        return jsonify(error='La activación online todavía no está configurada.', requires_configuration=True), 503
    if not secrets.compare_digest(configured, provided):
        return jsonify(error='Introduce tu clave de activación.', requires_key=True), 401
    # Same-origin POST plus an explicit non-cookie credential prevents CSRF.
    if request.headers.get('Sec-Fetch-Site') == 'cross-site':
        return jsonify(error='Origen no permitido'), 403
    return None


def headers(token):
    return {'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28'}


def latest_run(kind, repo, token):
    response = requests.get(f'https://api.github.com/repos/{repo}/actions/workflows/{WORKFLOWS[kind]}/runs',
                            headers=headers(token), params={'per_page': 10, 'branch': settings()[2]}, timeout=15)
    response.raise_for_status()
    rows = response.json().get('workflow_runs', [])
    active = next((r for r in rows if r.get('status') != 'completed'), None)
    row = active or (rows[0] if rows else {})
    return {k: row.get(k) for k in ('id', 'status', 'conclusion', 'html_url', 'updated_at', 'created_at')}


@blueprint.get('/api/cloud_bots/status')
def status():
    repo, token, _ = settings()
    configured = bool(token and os.getenv('ACTIONS_TRIGGER_KEY'))
    records = {}
    for kind in WORKFLOWS:
        path = ROOT / f'data/cache_control/{kind}_status.json'
        if path.exists():
            records[kind] = json.loads(path.read_text(encoding='utf-8'))
    return jsonify(configured=configured, published=records, upcoming_limit=400, pending_limit=200,
                   actions_url=f'https://github.com/{repo}/actions')


@blueprint.post('/api/cloud_bots/run_status')
def run_status():
    denied = authorize()
    if denied:
        return denied
    kind = (request.get_json(silent=True) or {}).get('kind', 'upcoming')
    if kind not in WORKFLOWS:
        return jsonify(error='Proceso no válido'), 400
    repo, token, _ = settings()
    if not token:
        return jsonify(error='Falta configurar la conexión con GitHub.'), 503
    try:
        return jsonify(run=latest_run(kind, repo, token))
    except requests.RequestException:
        return jsonify(error='No se pudo consultar GitHub.'), 502


@blueprint.post('/api/cloud_bots/trigger')
def trigger():
    denied = authorize()
    if denied:
        return denied
    data = request.get_json(silent=True) or {}
    kind = data.get('kind')
    if kind not in WORKFLOWS:
        return jsonify(error='Proceso no válido'), 400
    repo, token, ref = settings()
    if not token:
        return jsonify(error='Falta configurar la conexión con GitHub.', requires_configuration=True), 503
    try:
        with _lock:
            run = latest_run(kind, repo, token)
            if run.get('status') and run['status'] != 'completed':
                return jsonify(status='running', message='Ya hay una actualización en curso.', run=run), 202
            if time.monotonic() - _last_dispatch.get(kind, -1000) < 60:
                return jsonify(status='queued', message='Actualización solicitada; esperando a GitHub.'), 202
            inputs = {} if kind == 'list' else {'force_full': 'true' if data.get('force_full') is True else 'false'}
            response = requests.post(f'https://api.github.com/repos/{repo}/actions/workflows/{WORKFLOWS[kind]}/dispatches',
                                     headers=headers(token), json={'ref': ref, 'inputs': inputs}, timeout=20)
            response.raise_for_status()
            _last_dispatch[kind] = time.monotonic()
        return jsonify(status='queued', message='Actualización solicitada.',
                       run_url=f'https://github.com/{repo}/actions/workflows/{WORKFLOWS[kind]}'), 202
    except requests.RequestException:
        return jsonify(error='GitHub no ha aceptado la solicitud. Comprueba la conexión y los permisos.'), 502
