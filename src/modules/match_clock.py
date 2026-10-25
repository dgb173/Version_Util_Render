"""Match instants: source UTC/Shanghai -> Europe/Madrid, never fixed offsets."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import json

UTC = timezone.utc
SPAIN = ZoneInfo('Europe/Madrid')
CHINA = ZoneInfo('Asia/Shanghai')


def parse_instant(value, naive_zone=UTC):
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).strip().replace('Z', '+00:00'))
        return (parsed.replace(tzinfo=naive_zone) if parsed.tzinfo is None else parsed).astimezone(UTC)
    except (ValueError, TypeError):
        return None


def source_wall_to_spain(date_value, clock_value, is_utc=True):
    day = None
    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y', '%d-%m-%Y'):
        try:
            day = datetime.strptime(str(date_value).strip(), fmt).date()
            break
        except ValueError:
            pass
    clock = None
    raw = str(clock_value).strip().rstrip(':').upper()
    for fmt in ('%H:%M:%S', '%H:%M', '%I:%M:%S %p', '%I:%M %p', '%I:%M%p'):
        try:
            clock = datetime.strptime(raw, fmt).time()
            break
        except ValueError:
            pass
    if day is None or clock is None:
        return None
    return datetime.combine(day, clock, tzinfo=UTC if is_utc else CHINA).astimezone(SPAIN)


def record_instant(row):
    # start_time is a Spain-local timestamp in this app; legacy time_obj is UTC.
    for key, zone in (('start_time_utc', UTC), ('start_time', SPAIN), ('time_obj', UTC)):
        if row.get(key):
            instant = parse_instant(row[key], zone)
            if instant is not None:
                return instant
    return None


def schedule_fields(row):
    instant = record_instant(row)
    if instant is None:
        return {}
    local = instant.astimezone(SPAIN)
    return {'start_time': local.isoformat(), 'start_time_utc': instant.isoformat(),
            'time': local.strftime('%H:%M'), 'match_date': f'{local.month}/{local.day}/{local.year}',
            'date': local.date().isoformat(), 'schedule_timezone': 'Europe/Madrid'}


def sync_schedule(row, source):
    """Preserve all analysis; copy only the source's complete, consistent clock."""
    return {**row, **schedule_fields(source)}


def sync_stored_schedule(conn, match_id, source, bucket=None):
    """Update payload, explorer views and date index without changing analysis."""
    clock = schedule_fields(source)
    if not clock:
        return False
    columns = {r[1] for r in conn.execute('PRAGMA table_info(matches)')}
    json_columns = [k for k in ('payload_json', 'explorer_json', 'explorer_search_json') if k in columns]
    where = 'match_id=?' + (' AND bucket=?' if bucket is not None else '')
    args = [str(match_id)] + ([bucket] if bucket is not None else [])
    found = conn.execute('SELECT ' + ','.join(json_columns) + ' FROM matches WHERE ' + where, args).fetchone()
    if found is None:
        return False
    updates = {}
    for key, raw in zip(json_columns, found):
        if not raw:
            continue
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            continue
        metadata = clock if key == 'payload_json' else {**clock, 'match_date': clock['date']}
        if any(obj.get(k) != v for k, v in metadata.items()):
            obj.update(metadata)
            updates[key] = json.dumps(obj, ensure_ascii=False)
    if not updates:
        return False
    if 'match_date' in columns:
        updates['match_date'] = clock['date']
    if 'updated_at' in columns:
        updates['updated_at'] = datetime.now(UTC).isoformat()
    guard = ''.join(' AND ' + k + ' IS ?' for k in json_columns)
    result = conn.execute('UPDATE matches SET ' + ','.join(k + '=?' for k in updates) +
                          ' WHERE ' + where + guard, [*updates.values(), *args, *found])
    if result.rowcount != 1:
        raise RuntimeError('El partido cambió durante la sincronización horaria; repetir sin sobrescribirlo')
    return True
