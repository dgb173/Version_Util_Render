"""Stream complete cache exports without loading all fixtures into memory."""
import argparse
import datetime as dt
import gzip
import json
from pathlib import Path
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def records(root, kind):
    seen = set()
    archive = root / 'data/cache_archive' / kind
    for path in sorted([*archive.glob('*.json'), *archive.glob('*.json.gz')]):
        if path.suffix == '.gz':
            with gzip.open(path, 'rt', encoding='utf-8') as handle:
                row = json.load(handle)
        else:
            row = json.loads(path.read_text(encoding='utf-8'))
        match_id = str(row.get('match_id') or row.get('id'))
        if match_id in seen:
            continue
        seen.add(match_id)
        yield row
    import ijson
    files = [root / 'data/data_precacheo.json', root / 'data/data_pending_results.json'] if kind == 'upcoming' else [*sorted((root / 'data').glob('data_ah_*.json')), *sorted((root / 'data').glob('data_minus_ah_*.json'))]
    for path in files:
        if path.exists():
            with path.open('rb') as handle:
                for row in ijson.items(handle, 'item', use_float=True):
                    mid = str(row.get('match_id') or row.get('id'))
                    if mid and mid not in seen:
                        seen.add(mid)
                        yield row


def export(root, destination):
    destination.mkdir(parents=True, exist_ok=True)
    counts = {}
    for kind, name in [('upcoming', 'precacheo_completo.json'), ('finished', 'terminados_completo.json')]:
        count = 0
        with (destination / name).open('w', encoding='utf-8') as handle:
            handle.write('[')
            for row in records(root, kind):
                if count: handle.write(',\n')
                json.dump(row, handle, ensure_ascii=False, separators=(',', ':'))
                count += 1
            handle.write(']\n')
        counts[kind] = count
    (destination / 'resumen.json').write_text(json.dumps(counts), encoding='utf-8')
    print(f'Exportación completa: {counts} -> {destination}')
    return counts


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--output', type=Path, default=ROOT / 'exports' / dt.datetime.now().strftime('%Y%m%d_%H%M%S'))
    args = parser.parse_args()
    export(args.root, args.output)
