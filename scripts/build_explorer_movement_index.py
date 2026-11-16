"""Generate the small movement index beside the immutable Explorer archive."""
import gzip
import json
import sqlite3
import sys
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from modules.explorer_archive import _database
from modules.explorer_movement_index import index_row


def main():
    destination = ROOT / 'data' / 'explorer_movement_index.json.gz'
    with sqlite3.connect(_database()) as conn, gzip.open(destination, 'wt', encoding='utf-8', compresslevel=6) as output:
        for (payload,) in conn.execute('SELECT payload FROM matches'):
            row = json.loads(zlib.decompress(payload))
            output.write(json.dumps(index_row(row), ensure_ascii=False, separators=(',', ':')) + '\n')
    print(destination, destination.stat().st_size)


if __name__ == '__main__':
    main()
