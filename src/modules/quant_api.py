"""Integración Flask del Motor dual entero-binario v6.2 OPERATIVO.

Endpoints para análisis pre-partido de 12 compuertas, liquidación,
auditoría inmutable SHA-256 y feedback estructurado con Gemini.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
from typing import Callable, Dict, Any, Optional

from flask import Blueprint, jsonify, request

from .quant_v62.adapter import build_snapshot_and_config
from .quant_v62.engine import analyze as analyze_v62
from .quant_v62.audit_log import append_record

PROJECT_ROOT = Path(__file__).resolve().parents[2]
AUDIT_DB_PATH = PROJECT_ROOT / "data" / "quant_v62_audit.sqlite"


def run_quant_v62_analysis(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Ejecuta el análisis v6.2 sobre el payload de la plataforma."""
    snapshot, cfg = build_snapshot_and_config(payload)
    result = analyze_v62(snapshot, cfg)
    return result


def create_blueprint(loader: Callable[[str], Optional[Dict[str, Any]]]) -> Blueprint:
    bp = Blueprint("quant_v62", __name__)
    original_loader = loader

    def loader(mid):
        from .quant_features import enrich_prior_stats
        payload = original_loader(mid)
        return enrich_prior_stats(payload, original_loader) if isinstance(payload, dict) else payload

    @bp.get("/api/quant/status")
    def status():
        return jsonify({
            "success": True,
            "version": "6.2.0-binary",
            "name": "Motor dual entero-binario v6.2 OPERATIVO",
            "description": "Motor determinista de 12 compuertas para hándicap asiático y totales FT90",
            "gates_count": 12,
            "poisson_model": False,
            "status": "ready"
        })

    @bp.get("/api/quant/predict/<match_id>")
    @bp.post("/api/quant/audit/<match_id>")
    def prediction(match_id: str):
        payload = loader(str(match_id))
        if not isinstance(payload, dict) or not payload:
            return jsonify({
                "success": False,
                "error": "Partido no encontrado en la base de datos local. Analiza el partido primero."
            }), 404

        try:
            result = run_quant_v62_analysis(payload)
            if request.method == "POST":
                AUDIT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
                audit_record = {
                    "event_id": str(match_id),
                    "analysis": result,
                }
                try:
                    sha = append_record(AUDIT_DB_PATH, audit_record)
                    result["audit_ledger_sha256"] = sha
                except sqlite3.IntegrityError:
                    from .quant_v62.audit_log import digest
                    result["audit_ledger_sha256"] = digest(audit_record)
                    result["audit_already_exists"] = True

            home = payload.get("home_name") or payload.get("home_team")
            away = payload.get("away_name") or payload.get("away_team")
            league = payload.get("league_name") or payload.get("league")

            return jsonify({
                "success": True,
                "version": "6.2.0-binary",
                "prediction": result,
                "match": {
                    "id": str(match_id),
                    "home": home,
                    "away": away,
                    "league": league,
                    "handicap": payload.get("handicap"),
                    "goal_line": payload.get("goal_line"),
                }
            })
        except Exception as exc:
            import traceback
            traceback.print_exc()
            return jsonify({
                "success": False,
                "error": f"Error calculando motor v6.2: {exc}"
            }), 500

    @bp.post("/api/quant/bulk")
    def bulk():
        body = request.get_json(silent=True)
        ids = body.get("match_ids") if isinstance(body, dict) else None
        if not isinstance(ids, list) or not ids or len(ids) > 60:
            return jsonify(success=False, error="Se requiere lista de entre 1 y 60 IDs"), 400

        results = {}
        for mid in dict.fromkeys(map(str, ids)):
            payload = loader(mid)
            if not isinstance(payload, dict) or not payload:
                results[mid] = {
                    "status": "SIN_DATOS",
                    "label": "Sin datos",
                    "ah_pick": None,
                    "ou_pick": None,
                }
                continue

            try:
                res = run_quant_v62_analysis(payload)
                results[mid] = {
                    "status": "OK",
                    "ah_pick": res.get("ah_pick"),
                    "ou_pick": res.get("ou_pick"),
                    "score_ah": res.get("score_ah"),
                    "score_ou": res.get("score_ou"),
                    "patterns": res.get("patterns", []),
                    "policies": res.get("policy_trace", []),
                    "input_sha256": res.get("input_sha256"),
                    "recent_context": {("home" if res.get("favorite_side") == "home" else "away") if label == "favorite"
                                       else ("away" if res.get("favorite_side") == "home" else "home"):
                                       {key: profile.get(key) for key in ("last_five", "last_five_same_venue", "last_same_venue")}
                                       for label, profile in res.get("profiles", {}).items()},
                }
            except Exception as exc:
                results[mid] = {
                    "status": "ERROR",
                    "error": str(exc),
                    "ah_pick": None,
                    "ou_pick": None,
                }

        return jsonify(success=True, predictions=results, version="6.2.0-binary")

    @bp.post("/api/quant/gemini/<match_id>")
    def gemini_review(match_id: str):
        payload = loader(str(match_id))
        if not isinstance(payload, dict) or not payload:
            return jsonify(success=False, error="Partido no encontrado"), 404

        try:
            analysis = run_quant_v62_analysis(payload)
            from .quant_v62.review_gemini import make_request, validate_review, live_review

            api_key = os.getenv("GEMINI_API_KEY")
            model_name = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

            if api_key:
                review = live_review(analysis, api_key, model_name)
                return jsonify(success=True, live=True, review=review.model_dump())
            else:
                req_json = make_request(analysis)
                return jsonify(
                    success=True,
                    live=False,
                    message="GEMINI_API_KEY no configurada. Se devuelve el esquema de petición validado.",
                    packet_sha256=analysis.get("input_sha256"),
                    request_spec=req_json,
                )
        except Exception as exc:
            import traceback
            traceback.print_exc()
            return jsonify(success=False, error=str(exc)), 500

    return bp
