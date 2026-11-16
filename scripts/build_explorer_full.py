"""Build a compressed, indexed archive of finished Explorer matches."""
import gzip
import json
import sqlite3
import sys
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path(sys.argv[1])
DEST = Path(sys.argv[2])
sys.path.insert(0, str(ROOT / 'scripts'))
from export_explorer_recent import compact_for_archive

temp = DEST.with_suffix('.sqlite')
out = sqlite3.connect(temp)
out.execute('CREATE TABLE matches (match_id TEXT PRIMARY KEY, bucket TEXT, match_date TEXT, payload BLOB)')
source = sqlite3.connect(f'file:{SOURCE.as_posix()}?mode=ro', uri=True)
cursor = source.execute("SELECT match_id,bucket,match_date,coalesce(explorer_json,payload_json) FROM matches WHERE state='historical' AND score IS NOT NULL AND score NOT IN ('?:?','??') ORDER BY match_date DESC")
count = 0
for match_id, bucket, date, payload in cursor:
    try:
        row = compact_for_archive(json.loads(payload))
        score = str(row.get('final_score') or row.get('score') or '')
        if not score or '?' in score:
            continue
        row['match_id'] = str(match_id)
        row['_explorer_bucket'] = bucket
        data = json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        out.execute('INSERT OR IGNORE INTO matches VALUES (?,?,?,?)', (str(match_id),bucket,date,zlib.compress(data, 6)))
        count += 1
        if count % 1000 == 0:
            out.commit()
            print(f'Archived {count}', flush=True)
    except (TypeError, ValueError):
        continue
out.execute('CREATE INDEX match_order ON matches(match_date DESC, match_id DESC)')
out.execute('CREATE INDEX bucket_order ON matches(bucket, match_date DESC, match_id DESC)')
out.commit()
out.close()
source.close()
with temp.open('rb') as src, gzip.open(DEST, 'wb', compresslevel=1) as dst:
    import shutil
    shutil.copyfileobj(src, dst)
temp.unlink()
print(f'Archive: {count} matches, {DEST.stat().st_size} bytes', flush=True)
