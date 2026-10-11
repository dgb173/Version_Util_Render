"""Precacheador de clasificaciones de SofaScore para Render."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from modules import sofascore_context


def main():
    precacheo_file = ROOT / "data" / "data_precacheo.json"
    if not precacheo_file.exists():
        print("No se encontro data_precacheo.json")
        return

    try:
        matches = json.loads(precacheo_file.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Error leyendo data_precacheo.json: {e}")
        return

    leagues = {}
    for m in matches:
        lname = m.get("league_name") or ""
        if lname and lname not in leagues:
            leagues[lname] = m

    print(f"Total de ligas unicas en precacheo: {len(leagues)}")
    cache_data = sofascore_context._load_disk_tables_cache()
    cached_count = 0
    newly_cached = 0

    for idx, (lname, m) in enumerate(leagues.items(), 1):
        alias = sofascore_context._lookup_league_alias(lname)
        if not alias or not alias.get("id"):
            continue

        tid = alias["id"]
        t_key = f"tournament_cache_{tid}"

        if t_key in cache_data:
            cached_count += 1
            continue

        home = m.get("home_name") or ""
        away = m.get("away_name") or ""
        mdate = m.get("date") or m.get("match_date") or ""
        gline = m.get("goal_line") or 2.5

        print(f"[{idx}/{len(leagues)}] Consultando SofaScore para: {lname} ({alias.get('name')})...")
        res = sofascore_context.get_league_table_context(home, away, lname, mdate, gline, tournament_id=tid)

        if res.get("available"):
            cache_data[t_key] = res
            if res.get("season_id"):
                cache_data[f"tournament_cache_{tid}_{res['season_id']}"] = res
            newly_cached += 1
            print(f"  -> OK: guardada clasificacion ({len(res.get('views', {}).get('total', []))} equipos)")
            time.sleep(1.0)
        else:
            print(f"  -> No disponible: {res.get('reason')}")

    cache_file = ROOT / "data" / "sofascore_tables_cache.json"
    cache_file.write_text(json.dumps(cache_data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nFinalizado: {cached_count} ya en cache, {newly_cached} nuevas guardadas en {cache_file.name}")


if __name__ == "__main__":
    main()
