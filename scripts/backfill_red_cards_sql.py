import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules import sql_store  # noqa: E402
from modules.red_cards import normalize_red_card_stats_payload, normalize_red_card_value  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Actualiza tarjetas rojas en SQLite. El modo normalize solo reordena "
            "lo ya guardado; el modo refresh vuelve a consultar Nowgoal por match_id."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("normalize", "refresh"),
        default="normalize",
        help="normalize es rapido; refresh recupera rojas faltantes desde la fuente.",
    )
    parser.add_argument("--state", default=None, help="historical, precacheo, pending_results o all")
    parser.add_argument("--bucket", default=None, help="Bucket concreto, por ejemplo data_precacheo.json")
    parser.add_argument("--match-id", action="append", default=[], help="Limita a uno o varios match_id")
    parser.add_argument("--limit", type=int, default=0, help="Maximo de filas a procesar")
    parser.add_argument("--offset", type=int, default=0, help="Offset SQL para continuar por bloques")
    parser.add_argument("--sleep", type=float, default=0.2, help="Pausa entre refrescos en modo refresh")
    parser.add_argument("--dry-run", action="store_true", help="No escribe cambios")
    parser.add_argument("--export-legacy", action="store_true", help="Exporta JSON legacy de los buckets tocados")
    parser.add_argument("--verbose", action="store_true", help="Muestra una linea por partido")
    parser.add_argument("--progress-every", type=int, default=100, help="Frecuencia de progreso si no hay --verbose")
    return parser.parse_args()


def _clean_filter(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"all", "todos", "none", "null", "-"}:
        return None
    return text


def _fetch_rows(args: argparse.Namespace) -> Iterable[sqlite3.Row]:
    sql_store.ensure_bootstrap()
    query = "SELECT match_id, bucket, state, payload_json FROM matches"
    clauses = []
    params: list[Any] = []

    state = _clean_filter(args.state)
    bucket = _clean_filter(args.bucket)
    match_ids = [str(mid).strip() for mid in args.match_id if str(mid).strip()]

    if state:
        clauses.append("state = ?")
        params.append(state)
    if bucket:
        clauses.append("bucket = ?")
        params.append(bucket)
    if match_ids:
        placeholders = ", ".join("?" for _ in match_ids)
        clauses.append(f"match_id IN ({placeholders})")
        params.extend(match_ids)

    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY updated_at ASC"
    if args.limit and args.limit > 0:
        query += " LIMIT ?"
        params.append(int(args.limit))
    if args.offset and args.offset > 0:
        if not args.limit:
            query += " LIMIT -1"
        query += " OFFSET ?"
        params.append(int(args.offset))

    with sqlite3.connect(sql_store.DB_PATH, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        return list(conn.execute(query, params).fetchall())


def _has_red_value(payload: Any) -> bool:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in {
                "home_red",
                "away_red",
                "home_red_gen",
                "away_red_gen",
                "home_red_stadium",
                "away_red_stadium",
            } and normalize_red_card_value(value):
                return True
            if key == "stats_rows" and isinstance(value, list):
                for row in value:
                    if not isinstance(row, dict):
                        continue
                    label = str(row.get("label") or "").lower()
                    if ("roja" in label or "red card" in label) and (
                        normalize_red_card_value(row.get("home")) or normalize_red_card_value(row.get("away"))
                    ):
                        return True
            if _has_red_value(value):
                return True
    elif isinstance(payload, list):
        return any(_has_red_value(item) for item in payload)
    return False


def _merge_preserved_fields(old_payload: Dict[str, Any], new_payload: Dict[str, Any]) -> Dict[str, Any]:
    for key in ("precacheo_date", "cached_at", "created_at"):
        if key in old_payload and key not in new_payload:
            new_payload[key] = old_payload[key]
    return new_payload


def _load_payload(raw: str) -> Optional[Dict[str, Any]]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def main() -> int:
    args = parse_args()
    analyzer = None
    if args.mode == "refresh":
        from modules.estudio_scraper import analizar_partido_completo  # noqa: WPS433

        analyzer = analizar_partido_completo

    rows = list(_fetch_rows(args))
    if not rows:
        print("No hay filas que procesar.")
        return 0

    print(f"Filas seleccionadas: {len(rows)}")
    print(f"Modo: {args.mode}")
    if args.dry_run:
        print("Dry-run: no se escribiran cambios.")

    processed = 0
    updated = 0
    with_red = 0
    errors = 0
    touched_buckets: set[str] = set()

    for row in rows:
        processed += 1
        match_id = str(row["match_id"])
        bucket = str(row["bucket"])
        state = str(row["state"])
        old_payload = _load_payload(row["payload_json"])
        if not old_payload:
            errors += 1
            print(f"[{processed}/{len(rows)}] {match_id}: payload invalido")
            continue

        try:
            if args.mode == "refresh":
                new_payload = analyzer(match_id, force_refresh=True)
                if not isinstance(new_payload, dict) or new_payload.get("error"):
                    raise RuntimeError(str(new_payload.get("error") if isinstance(new_payload, dict) else new_payload))
                new_payload = _merge_preserved_fields(old_payload, new_payload)
            else:
                new_payload = old_payload

            normalize_red_card_stats_payload(new_payload)
            has_red = _has_red_value(new_payload)
            if has_red:
                with_red += 1

            old_json = json.dumps(old_payload, ensure_ascii=False, sort_keys=True)
            new_json = json.dumps(new_payload, ensure_ascii=False, sort_keys=True)
            changed = old_json != new_json

            if changed and not args.dry_run:
                sql_store.upsert_match(new_payload, bucket=bucket, state=state)
                touched_buckets.add(bucket)
                updated += 1

            marker = "ROJA" if has_red else "sin roja"
            action = "cambiaria" if changed and args.dry_run else ("actualizado" if changed else "igual")
            if args.verbose or changed or has_red or processed == len(rows) or (
                args.progress_every > 0 and processed % args.progress_every == 0
            ):
                print(f"[{processed}/{len(rows)}] {match_id}: {action}, {marker}")

            if args.mode == "refresh" and args.sleep > 0:
                time.sleep(args.sleep)
        except Exception as exc:
            errors += 1
            print(f"[{processed}/{len(rows)}] {match_id}: ERROR {type(exc).__name__}: {exc}")

    if args.export_legacy and touched_buckets and not args.dry_run:
        for bucket in sorted(touched_buckets):
            sql_store.export_bucket_to_json(bucket)

    print(
        f"Resumen: procesados={processed} actualizados={updated} "
        f"con_roja={with_red} errores={errors}"
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
