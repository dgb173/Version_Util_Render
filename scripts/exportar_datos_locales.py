"""Crea dos JSON portables: precacheo completo y terminados completos."""
from __future__ import annotations

import json
from pathlib import Path

try:
    import ijson
except ImportError:
    ijson = None


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUTPUT = ROOT / "exportaciones_cache"


def _iter_rows(paths):
    seen = set()
    for path in paths:
        if not path.is_file():
            continue
        if ijson is not None:
            try:
                with path.open("rb") as handle:
                    for row in ijson.items(handle, "item", use_float=True):
                        if not isinstance(row, dict):
                            continue
                        match_id = str(row.get("match_id") or row.get("id") or "").strip()
                        if not match_id or match_id in seen:
                            continue
                        seen.add(match_id)
                        yield row
                continue
            except Exception:
                pass
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
                rows = payload if isinstance(payload, list) else payload.get("matches", [])
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    match_id = str(row.get("match_id") or row.get("id") or "").strip()
                    if not match_id or match_id in seen:
                        continue
                    seen.add(match_id)
                    yield row
        except Exception:
            pass


def _write_array(path: Path, rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    count = 0
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write("[")
        for row in rows:
            if count:
                handle.write(",\n")
            json.dump(row, handle, ensure_ascii=False, separators=(",", ":"))
            count += 1
        handle.write("]\n")
    temporary.replace(path)
    return count


def main() -> int:
    precache_paths = [DATA / "data_precacheo.json", DATA / "data_pending_results.json", DATA / "data_precacheo_clean.json"]
    finished_paths = [
        *sorted(DATA.glob("data_ah_*.json")),
        *sorted(DATA.glob("data_minus_ah_*.json")),
    ]
    precache_file = OUTPUT / "precacheo_completo.json"
    finished_file = OUTPUT / "terminados_completo.json"
    precache_count = _write_array(precache_file, _iter_rows(precache_paths))
    finished_count = _write_array(finished_file, _iter_rows(finished_paths))
    summary = {
        "precacheo": {"partidos": precache_count, "archivo": str(precache_file)},
        "terminados": {"partidos": finished_count, "archivo": str(finished_file)},
    }
    (OUTPUT / "resumen.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[OK] Precacheo exportado: {precache_count} -> {precache_file}")
    print(f"[OK] Terminados exportados: {finished_count} -> {finished_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
