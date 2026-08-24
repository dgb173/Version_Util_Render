#!/usr/bin/env python
import argparse
import datetime as dt
import os
import subprocess
import sys
import time
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


def make_git_batch_push_hook():
    def hook(completed_count, total_count):
        print(f"\n[AUTO-PUSH] Lote de 100 alcanzado ({completed_count}/{total_count}). Publicando en GitHub...", flush=True)
        try:
            subprocess.run(
                ["git", "add", "--", "data.json", "data/data.json", "data/data_precacheo.json", "data/data_pending_results.json", "data/data_ah_*.json", "data/data_minus_ah_*.json", "data/league_extractions.json"],
                cwd=str(PROJECT_ROOT),
                check=False
            )
            diff_res = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=str(PROJECT_ROOT))
            if diff_res.returncode != 0:
                stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
                subprocess.run(
                    ["git", "commit", "-m", f"chore: cloud cache finished matches batch {completed_count}/{total_count} ({stamp})"],
                    cwd=str(PROJECT_ROOT),
                    check=False
                )
                for attempt in range(1, 4):
                    print(f"  Pushing batch ({attempt}/3)...", flush=True)
                    subprocess.run(["git", "fetch", "origin", "main", "--depth=1"], cwd=str(PROJECT_ROOT), check=False)
                    subprocess.run(["git", "rebase", "origin/main"], cwd=str(PROJECT_ROOT), check=False)
                    push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=str(PROJECT_ROOT), check=False)
                    if push_res.returncode == 0:
                        print(f"  [OK] Lote de {completed_count} partidos guardado y subido a GitHub exitosamente.\n", flush=True)
                        break
                    time.sleep(2)
            else:
                print("  Sin cambios detectados en este lote.\n", flush=True)
        except Exception as e:
            print(f"  [AVISO] Error al subir lote a GitHub: {e}\n", flush=True)
    return hook


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
        default=100,
        help="Export incremental y guardado en lotes de N partidos (por defecto 100)",
    )
    parser.add_argument(
        "--auto-push-batches",
        action="store_true",
        help="Sube automáticamente a GitHub en main cada lote de N partidos completados.",
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
    flush_every = max(1, int(args.flush_every or 100))
    export_legacy = not args.no_export_legacy
    auto_push = bool(args.auto_push_batches or os.getenv('GITHUB_ACTIONS'))

    print("cache_finished_matches.py")
    print(f"  handicap={handicap_filter}")
    print(f"  ou={goal_line_filter}")
    print(f"  workers={workers}")
    print(f"  flush_every={flush_every}")
    print(f"  export_legacy={export_legacy}")
    print(f"  auto_push_batches={auto_push}")

    try:
        import app as app_module  # type: ignore
    except Exception as exc:
        print(f"ERROR importando src/app.py: {exc}")
        return 1

    if args.dry_run:
        print("Dry-run correcto. Wrapper listo.")
        return 0

    batch_hook = make_git_batch_push_hook() if auto_push else None

    try:
        app_module.process_all_finished_matches_background(
            handicap_filter=handicap_filter,
            goal_line_filter=goal_line_filter,
            workers=workers,
            flush_every=flush_every,
            export_legacy=export_legacy,
            on_batch_hook=batch_hook,
        )
        return 0
    except Exception as exc:
        print(f"ERROR ejecutando cacheo de terminados: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
