import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from modules import data_manager, last_general_context  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrapea y cachea Último General para partidos de precacheo.")
    parser.add_argument("--limit", type=int, default=0, help="Máximo de partidos a procesar. 0 = todos.")
    parser.add_argument("--force", action="store_true", help="Recalcular aunque ya exista caché.")
    parser.add_argument("--match-id", action="append", default=[], help="ID concreto. Puede repetirse.")
    args = parser.parse_args()

    if args.match_id:
        match_ids = args.match_id
    else:
        rows = data_manager.load_precacheo_matches()
        match_ids = [str(m.get("match_id") or m.get("id")) for m in rows if m.get("match_id") or m.get("id")]

    max_items = args.limit if args.limit and args.limit > 0 else None
    result = last_general_context.process_match_ids(match_ids, force_refresh=args.force, max_items=max_items)
    print(
        f"Último General: ok={result['ok']}/{result['total']} "
        f"cache={result['cached']} nuevos={result['scraped']} fallos={len(result['failed'])}"
    )
    if result["failed"]:
        for fail in result["failed"][:20]:
            print(f"  - {fail.get('match_id')}: {fail.get('error')}")
    print(f"Cache: {result['cache_path']}")
    return 0 if not result["failed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
