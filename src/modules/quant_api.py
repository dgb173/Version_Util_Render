"""Flask integration. Training is an explicit offline operation, never a GET."""
import hashlib
import json
import sqlite3
from datetime import datetime, timezone

from flask import Blueprint, jsonify

from . import quant_research as quant
from .quant_features import enrich_prior_stats


def predict_from_cache(payload, loader):
    result = quant.predict(payload)
    if result.get('probabilities_1x2'):
        payload = enrich_prior_stats(payload, loader)
        result = quant.predict(payload)
    return payload, result


def append_audit(event_id, payload, result, db=None):
    db = db or quant.ROOT / 'data/quant_v61_audit.sqlite'
    db.parent.mkdir(parents=True, exist_ok=True)
    packet = json.dumps({'event_id': str(event_id), 'snapshot': payload, 'prediction': result},
                        sort_keys=True, ensure_ascii=False, allow_nan=False)
    run_id = hashlib.sha256(packet.encode()).hexdigest()
    with sqlite3.connect(db) as conn:
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS predictions (
                run_id TEXT PRIMARY KEY, event_id TEXT NOT NULL,
                packet TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TRIGGER IF NOT EXISTS predictions_no_update BEFORE UPDATE ON predictions
                BEGIN SELECT RAISE(ABORT, 'append-only'); END;
            CREATE TRIGGER IF NOT EXISTS predictions_no_delete BEFORE DELETE ON predictions
                BEGIN SELECT RAISE(ABORT, 'append-only'); END;
        ''')
        conn.execute('INSERT OR IGNORE INTO predictions VALUES (?,?,?,?)',
                     (run_id, str(event_id), packet, datetime.now(timezone.utc).isoformat()))
    return run_id


def create_blueprint(loader):
    bp = Blueprint('quant_v61', __name__)

    @bp.get('/api/quant/status')
    def status():
        model = quant.load_model()
        return jsonify({'trained': bool(model), 'version': model.get('version') if model else '6.1 RESEARCH',
                        'telemetry_trained': bool(model and model.get('telemetry_model')),
                        'training_end': model.get('training_end') if model else None,
                        'training_count': model.get('training_count') if model else 0,
                        'validation': model.get('validation') if model else None})

    @bp.get('/api/quant/predict/<match_id>')
    @bp.post('/api/quant/audit/<match_id>')
    def prediction(match_id):
        from flask import request
        payload = loader(str(match_id))
        if not isinstance(payload, dict) or not payload:
            return jsonify({'success': False, 'error': 'Partido no encontrado en la base local'}), 404
        payload, result = predict_from_cache(payload, loader)
        if request.method == 'POST':
            result['run_id'] = append_audit(match_id, payload, result)
        return jsonify({'success': True, 'prediction': result, 'match': {
            'home': payload.get('home_name') or payload.get('home_team'),
            'away': payload.get('away_name') or payload.get('away_team'),
            'league': payload.get('league_name')}})

    @bp.post('/api/quant/bulk')
    def bulk():
        from flask import request
        body = request.get_json(silent=True)
        ids = body.get('match_ids') if isinstance(body, dict) else None
        if not isinstance(ids, list) or not ids or len(ids) > 50 or not all(isinstance(i, (str, int)) for i in ids):
            return jsonify(success=False, error='Se requieren entre 1 y 50 IDs de partidos'), 400
        results = {}
        for mid in dict.fromkeys(map(str, ids)):
            payload = loader(mid)
            if not isinstance(payload, dict) or not payload:
                results[mid] = {'status': 'ERROR_DATOS', 'alerts': ['Analiza el partido para obtener datos'], 'selections': {}}
                continue
            payload, result = predict_from_cache(payload, loader)
            result.pop('score_matrix', None)
            result.pop('validation', None)
            if result.get('telemetry'):
                # Bulk rows need a compact summary; full evidence stays in the modal.
                result['telemetry'] = {k: v for k, v in result['telemetry'].items()
                                       if k not in ('validation', 'home', 'away')}
            result['match'] = {'home': payload.get('home_name') or payload.get('home_team'),
                               'away': payload.get('away_name') or payload.get('away_team')}
            results[mid] = result
        return jsonify(success=True, predictions=results)

    return bp
