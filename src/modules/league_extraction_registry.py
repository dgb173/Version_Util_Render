"""Registro persistente de extracciones de liga, separado de la base SQL."""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REGISTRY_PATH = Path(__file__).resolve().parents[2] / "data" / "league_extractions.json"
_LOCK = threading.RLock()


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _empty() -> Dict[str, Any]:
    return {"version": 1, "extractions": {}}


def _default_label(league_id: str, league_name: str, season: str) -> str:
    base = str(league_name or f"Liga {league_id}")
    return f"{base} · {season or 'sin temporada'}"


def _read() -> Dict[str, Any]:
    if not REGISTRY_PATH.exists():
        return _empty()
    try:
        payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _empty()
    if not isinstance(payload, dict) or not isinstance(payload.get("extractions"), dict):
        return _empty()
    return payload


def _write(payload: Dict[str, Any]) -> None:
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = REGISTRY_PATH.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, REGISTRY_PATH)


def create_extraction(
    *,
    league_id: str,
    league_name: str,
    season: str,
    company_id: int,
    target_ah: Optional[float],
    matches: Iterable[Dict[str, Any]],
    label: str = "",
) -> Dict[str, Any]:
    """Crea una extracción y conserva jornada/fecha fuera de SQL."""
    now = _now()
    extraction_id = uuid.uuid4().hex
    registered_matches: List[Dict[str, Any]] = []
    for match in matches:
        registered_matches.append(
            {
                "id": str(match.get("id") or ""),
                "sub_id": str(match.get("sub_id") or "0"),
                "sub_name": str(match.get("sub_name") or ""),
                "round": str(match.get("round") or ""),
                "date": str(match.get("date") or ""),
                "home": str(match.get("home") or ""),
                "away": str(match.get("away") or ""),
                "visible_ah": match.get("visible_ah"),
                "status": "queued",
                "bucket": None,
                "error": None,
                "updated_at": now,
            }
        )

    extraction = {
        "extraction_id": extraction_id,
        "league_id": str(league_id),
        "league_name": str(league_name or f"Liga {league_id}"),
        "label": str(label or _default_label(league_id, league_name, season)),
        "season": str(season or ""),
        "company_id": int(company_id),
        "target_ah": target_ah,
        "status": "queued",
        "created_at": now,
        "updated_at": now,
        "matches": registered_matches,
    }
    with _LOCK:
        payload = _read()
        payload["extractions"][extraction_id] = extraction
        _write(payload)
    return json.loads(json.dumps(extraction, ensure_ascii=False))


def register_existing_league(
    *,
    league_id: str,
    league_name: str,
    season: str,
    company_id: int,
    target_ah: Optional[float],
    matches: Iterable[Dict[str, Any]],
    label: str = "",
) -> Dict[str, Any]:
    """Registra o actualiza un calendario ya scrapeado sin escribir en SQL."""
    now = _now()
    normalized_label = str(label or _default_label(league_id, league_name, season))
    registered_matches: List[Dict[str, Any]] = []
    for match in matches:
        exists = bool(match.get("already_in_sql"))
        registered_matches.append(
            {
                "id": str(match.get("id") or ""),
                "sub_id": str(match.get("sub_id") or "0"),
                "sub_name": str(match.get("sub_name") or ""),
                "round": str(match.get("round") or ""),
                "date": str(match.get("date") or ""),
                "home": str(match.get("home") or ""),
                "away": str(match.get("away") or ""),
                "visible_ah": match.get("visible_ah"),
                "status": "exists" if exists else "missing",
                "bucket": match.get("sql_bucket") if exists else None,
                "error": None,
                "updated_at": now,
            }
        )

    with _LOCK:
        payload = _read()
        existing = next(
            (
                item for item in payload["extractions"].values()
                if str(item.get("league_id")) == str(league_id)
                and str(item.get("season")) == str(season)
                and item.get("target_ah") == target_ah
            ),
            None,
        )
        extraction_id = existing.get("extraction_id") if existing else uuid.uuid4().hex
        extraction = {
            "extraction_id": extraction_id,
            "league_id": str(league_id),
            "league_name": str(league_name or f"Liga {league_id}"),
            "label": normalized_label,
            "season": str(season or ""),
            "company_id": int(company_id),
            "target_ah": target_ah,
            "status": "completed" if registered_matches and all(
                match["status"] == "exists" for match in registered_matches
            ) else "registered",
            "created_at": (existing or {}).get("created_at") or now,
            "updated_at": now,
            "matches": registered_matches,
        }
        payload["extractions"][extraction_id] = extraction
        _write(payload)
    return json.loads(json.dumps(extraction, ensure_ascii=False))


def update_extraction_status(extraction_id: str, status: str) -> None:
    with _LOCK:
        payload = _read()
        extraction = payload["extractions"].get(str(extraction_id))
        if not extraction:
            return
        extraction["status"] = str(status)
        extraction["updated_at"] = _now()
        _write(payload)


def update_match(extraction_id: str, match_id: str, result: Dict[str, Any]) -> None:
    with _LOCK:
        payload = _read()
        extraction = payload["extractions"].get(str(extraction_id))
        if not extraction:
            return
        for match in extraction.get("matches") or []:
            if str(match.get("id")) != str(match_id):
                continue
            match["status"] = str(result.get("status") or "error")
            match["bucket"] = result.get("bucket")
            match["error"] = result.get("error")
            match["updated_at"] = _now()
            break
        extraction["updated_at"] = _now()
        _write(payload)


def get_extraction(extraction_id: str) -> Optional[Dict[str, Any]]:
    with _LOCK:
        extraction = _read()["extractions"].get(str(extraction_id))
        return json.loads(json.dumps(extraction, ensure_ascii=False)) if extraction else None


def list_extractions() -> List[Dict[str, Any]]:
    with _LOCK:
        extractions = list(_read()["extractions"].values())
    summaries = []
    for extraction in extractions:
        matches = extraction.get("matches") or []
        counts: Dict[str, int] = {}
        for match in matches:
            status = str(match.get("status") or "queued")
            counts[status] = counts.get(status, 0) + 1
        summaries.append(
            {
                key: extraction.get(key)
                for key in (
                    "extraction_id",
                    "league_id",
                    "league_name",
                    "label",
                    "season",
                    "company_id",
                    "target_ah",
                    "status",
                    "created_at",
                    "updated_at",
                )
            }
            | {"total": len(matches), "counts": counts}
        )
    summaries.sort(key=lambda row: str(row.get("updated_at") or ""), reverse=True)
    return summaries
