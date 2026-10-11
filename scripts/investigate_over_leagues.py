import sqlite3
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "app_data.db"
PRECACHEO_PATH = ROOT / "data" / "data_precacheo.json"

score_re = re.compile(r"^(\d+)\s*[-:]\s*(\d+)$")

def run():
    stats = defaultdict(lambda: {"total": 0, "o25": 0, "o35": 0, "o45": 0, "o55": 0, "goals": 0})

    # 1. Leer matches de SQLite
    if DB_PATH.exists():
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        print("Consultando SQLite app_data.db...")
        cursor.execute("""
            SELECT json_extract(payload_json, '$.league_name'), score
            FROM matches
            WHERE score IS NOT NULL AND score != '' AND score != 'N/A'
        """)
        count = 0
        for league, score in cursor:
            if not league or not score:
                continue
            m = score_re.match(score.strip())
            if not m:
                continue
            total_g = int(m.group(1)) + int(m.group(2))
            s = stats[league.strip()]
            s["total"] += 1
            s["goals"] += total_g
            if total_g > 2.5: s["o25"] += 1
            if total_g > 3.5: s["o35"] += 1
            if total_g > 4.5: s["o45"] += 1
            if total_g > 5.5: s["o55"] += 1
            count += 1
        conn.close()
        print(f"Partidos procesados de SQLite: {count}")

    # 2. Leer también partidos históricos dentro de pre_match_context de data_precacheo.json
    if PRECACHEO_PATH.exists():
        try:
            print("Analizando historial dentro de data_precacheo.json...")
            data = json.loads(PRECACHEO_PATH.read_text(encoding="utf-8-sig"))
            items = data.values() if isinstance(data, dict) else data
            today_leagues = set()
            for row in items:
                lname = row.get("league_name")
                if lname:
                    today_leagues.add(lname.strip())
                pmc = row.get("pre_match_context") or {}
                current = pmc.get("current") or {}
                # Partidos históricos de home y away
                all_hists = (
                    current.get("home_matches_all", []) +
                    current.get("away_matches_all", []) +
                    current.get("home_matches", []) +
                    current.get("away_matches", [])
                )
                for hm in all_hists:
                    sc = hm.get("score") or hm.get("score_raw")
                    if not sc:
                        continue
                    m = score_re.match(str(sc).strip())
                    if not m:
                        continue
                    # La liga del partido histórico suele ser la misma o league_id_hist
                    total_g = int(m.group(1)) + int(m.group(2))
                    if lname:
                        s = stats[lname.strip()]
                        s["total"] += 1
                        s["goals"] += total_g
                        if total_g > 2.5: s["o25"] += 1
                        if total_g > 3.5: s["o35"] += 1
                        if total_g > 4.5: s["o45"] += 1
                        if total_g > 5.5: s["o55"] += 1
        except Exception as e:
            print(f"Error procesando precacheo: {e}")

    # 3. Filtrar y ordenar
    results = []
    for league, s in stats.items():
        if s["total"] < 4:  # Muestra mínima
            continue
        p35 = round((s["o35"] / s["total"]) * 100, 1)
        p45 = round((s["o45"] / s["total"]) * 100, 1)
        p55 = round((s["o55"] / s["total"]) * 100, 1)
        p25 = round((s["o25"] / s["total"]) * 100, 1)
        avg = round(s["goals"] / s["total"], 2)

        # Regla del usuario:
        # "solo los over y 3.5 y 4,5 y 5,5 que tengan ratio superior al 50%"
        qualifies_55 = p55 >= 50.0
        qualifies_45 = p45 >= 50.0
        qualifies_35 = p35 >= 50.0

        if qualifies_55 or qualifies_45 or qualifies_35:
            # Determinar la línea más alta con >50%
            if qualifies_55:
                badge_line = 5.5
                badge_pct = p55
            elif qualifies_45:
                badge_line = 4.5
                badge_pct = p45
            else:
                badge_line = 3.5
                badge_pct = p35

            results.append({
                "league": league,
                "matches": s["total"],
                "avg_goals": avg,
                "o25_pct": p25,
                "o35_pct": p35,
                "o45_pct": p45,
                "o55_pct": p55,
                "badge_line": badge_line,
                "badge_pct": badge_pct,
                "is_today": league in today_leagues
            })

    results.sort(key=lambda x: (x["badge_line"], x["badge_pct"], x["matches"]), reverse=True)

    out_file = ROOT / "data" / "high_over_leagues.json"
    out_file.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nGuardado en: {out_file}")

    print(f"\n==========================================")
    print(f"LIGAS CON OVER 3.5 / 4.5 / 5.5 > 50% ({len(results)} ligas):")
    print(f"==========================================")
    for r in results:
        today_mark = " [HOY]" if r["is_today"] else ""
        try:
            print(f"{today_mark} {r['league']}: Linea O{r['badge_line']} ({r['badge_pct']}%) | Med: {r['avg_goals']}g | PJ: {r['matches']} | O3.5: {r['o35_pct']}% | O4.5: {r['o45_pct']}% | O5.5: {r['o55_pct']}%")
        except Exception:
            pass

if __name__ == "__main__":
    run()
