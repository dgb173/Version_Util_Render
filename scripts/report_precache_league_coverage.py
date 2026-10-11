"""Write a per-match trace from the latest league audit and cached provider tables."""
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from modules import nowgoal_tables, sofascore_context


def main():
    matches = json.loads((ROOT / "data/data_precacheo.json").read_text(encoding="utf-8"))
    audit = {row["league"]: row for row in json.loads(
        (ROOT / "data/league_tables_audit.json").read_text(encoding="utf-8")
    )}
    rows = []
    for match in matches:
        league = match.get("league_name") or ""
        source = audit.get(league) or {}
        lid = str((match.get("pre_match_context") or {}).get("league_id") or match.get("league_id") or "")
        table_file = ROOT / "data/league_tables" / f"{lid}_latest.json"
        team_names = []
        if source.get("current") and table_file.exists():
            try:
                cached = json.loads(table_file.read_text(encoding="utf-8"))
                views, _ = nowgoal_tables.normalize_tables(cached[1])
                team_names = [row["team"] for row in views.get("total", [])]
            except (OSError, ValueError, KeyError, TypeError):
                pass

        home = match.get("home_name") or ""
        away = match.get("away_name") or ""
        home_score = max((sofascore_context._similarity(home, t) for t in team_names), default=0)
        away_score = max((sofascore_context._similarity(away, t) for t in team_names), default=0)
        verified = bool(team_names and home_score >= .75 and away_score >= .75)
        status = "verified_nowgoal" if verified else (
            "table_found_teams_unverified" if team_names else "unresolved"
        )
        if league == "India Bangalore Super Division":
            context = sofascore_context.get_league_table_context(home, away, league, match.get("match_date"))
            if (context.get("available") and context.get("home_team_id")
                    and context.get("away_team_id")
                    and str(context["home_team_id"]) != str(context["away_team_id"])):
                status = "verified_sofascore"
        rows.append({
            "match_id": match.get("match_id"), "date": match.get("match_date"),
            "league": league, "nowgoal_league_id": lid,
            "home": home, "away": away, "status": status,
            "source_url": "https://www.sofascore.com/football/tournament/india-amateur/bdfa-super-division-league/29341"
                if status == "verified_sofascore" else source.get("source_url"),
            "reason": None if status.startswith("verified") else source.get("reason") or "team_mapping_unverified",
            "home_similarity": round(home_score, 3) if team_names else None,
            "away_similarity": round(away_score, 3) if team_names else None,
        })
    output = ROOT / "artifacts/league_table_match_coverage.json"
    output.parent.mkdir(exist_ok=True)
    output.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_matches": len(matches), "counts": dict(Counter(r["status"] for r in rows)),
        "matches": rows,
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    print(output)
    print(dict(Counter(r["status"] for r in rows)))


if __name__ == "__main__":
    main()
