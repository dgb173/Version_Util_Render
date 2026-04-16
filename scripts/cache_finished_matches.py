#!/usr/bin/env python
import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _configure_stdio() -> None:
    os.environ.setdefault("PYTHONUTF8", "1")
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _normalize_filter(value: str | None):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in {"all", "todos", "none", "null", "-"}:
        return None
    return text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta el cacheo masivo de partidos terminados usando la logica actual de src/app.py."
    )
    parser.add_argument("handicap", nargs="?", default="all", help="Filtro AH o 'all'")
    parser.add_argument("ou", nargs="?", default="all", help="Filtro O/U o 'all'")
    parser.add_argument("workers", nargs="?", type=int, default=10, help="Numero de workers")
    parser.add_argument(
        "flush_every",
        nargs="?",
        type=int,
        default=5,
        help="Export incremental legacy cada N partidos",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Valida imports y argumentos sin ejecutar el cacheo.",
    )
    parser.add_argument(
        "--no-export-legacy",
        action="store_true",
        help="No exporta los JSON legacy durante el proceso.",
    )
    return parser.parse_args()


def main() -> int:
    _configure_stdio()
    args = parse_args()

    handicap_filter = _normalize_filter(args.handicap)
    goal_line_filter = _normalize_filter(args.ou)
    workers = max(1, int(args.workers or 1))
    flush_every = max(0, int(args.flush_every or 0))
    export_legacy = not args.no_export_legacy

    print("cache_finished_matches.py")
    print(f"  handicap={handicap_filter}")
    print(f"  ou={goal_line_filter}")
    print(f"  workers={workers}")
    print(f"  flush_every={flush_every}")
    print(f"  export_legacy={export_legacy}")

    try:
        import app as app_module  # type: ignore
    except Exception as exc:
        print(f"ERROR importando src/app.py: {exc}")
        return 1

    if args.dry_run:
        print("Dry-run correcto. Wrapper listo.")
        return 0

    try:
        app_module.process_all_finished_matches_background(
            handicap_filter=handicap_filter,
            goal_line_filter=goal_line_filter,
            workers=workers,
            flush_every=flush_every,
            export_legacy=export_legacy,
        )
        return 0
    except Exception as exc:
        print(f"ERROR ejecutando cacheo de terminados: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
