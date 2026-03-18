#!/usr/bin/env python
import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / 'src'
sys.path.insert(0, str(SRC_DIR))

from modules import data_manager, sql_store  # noqa: E402


def cmd_status():
    print(f'DB: {sql_store.get_db_path()}')
    print(f'Legacy sync enabled: {sql_store.LEGACY_SYNC_ENABLED}')
    buckets = sql_store.fetch_distinct_buckets()
    print(f'SQL buckets: {len(buckets)}')
    for b in sorted(buckets):
        count = len(sql_store.fetch_matches(bucket=b))
        print(f'  - {b}: {count}')


def cmd_import(reset: bool):
    print('Importing legacy JSON into SQL...')
    data_manager.import_legacy_json_to_sql(reset_first=reset)
    print('Done.')


def cmd_export():
    print('Exporting SQL data into legacy JSON buckets...')
    paths = data_manager.rebuild_legacy_json_files()
    for p in paths:
        print(f'  - {p}')
    sql_store.history_export_to_json()
    print('Done.')


def main():
    parser = argparse.ArgumentParser(description='Sync SQL storage with legacy JSON files')
    sub = parser.add_subparsers(dest='command', required=True)

    p_import = sub.add_parser('import', help='Import legacy JSON into SQL')
    p_import.add_argument('--reset', action='store_true', help='Drop SQL data before importing')

    sub.add_parser('export', help='Export SQL data into legacy JSON files')
    sub.add_parser('status', help='Show SQL storage status')

    args = parser.parse_args()

    if args.command == 'import':
        cmd_import(reset=args.reset)
    elif args.command == 'export':
        cmd_export()
    elif args.command == 'status':
        cmd_status()


if __name__ == '__main__':
    main()
