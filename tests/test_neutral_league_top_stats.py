import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "src" / "templates" / "precacheo.html"


def _render_stats(pc, side="home"):
    template_path = json.dumps(str(TEMPLATE))
    payload = json.dumps(pc)
    node_script = f"""
const fs = require('fs');
const vm = require('vm');
const source = fs.readFileSync({template_path}, 'utf8');
const start = source.indexOf('        function summarizeTeamSimpleLeagueStats(');
const end = source.indexOf('        function mcEscape(', start);
if (start < 0 || end < 0) throw new Error('Could not find top-card stats functions');
const functions = source.slice(start, end);
const sandbox = {{}};
vm.createContext(sandbox);
vm.runInContext(`
let neutrasNombres = ['neutral league'];
function isLigaNeutra(name) {{ return neutrasNombres.includes(String(name || '').trim().toLowerCase()); }}
function isNeutralLeagueMatch(match) {{
  return !!match?.is_neutral_venue || !!match?.pre_match_context?.current?.is_neutral_venue
    || [match?.league_name, match?.league, match?.pre_match_context?.current?.league_name].some(isLigaNeutra);
}}
function parseNumericValue(value) {{
  if (value === undefined || value === null) return null;
  const n = Number(String(value).replace(',', '.').trim());
  return Number.isFinite(n) ? n : null;
}}
function parseScoreValue(raw) {{
  if (!raw) return null;
  const parts = String(raw).replace('-', ':').split(':');
  if (parts.length !== 2) return null;
  const home = Number(parts[0]), away = Number(parts[1]);
  return Number.isFinite(home) && Number.isFinite(away) ? {{home, away, total: home + away}} : null;
}}
function sameTeamName(a, b) {{
  const x = String(a || '').toLowerCase(), y = String(b || '').toLowerCase();
  return !!x && !!y && (x === y || x.includes(y) || y.includes(x));
}}
` + functions, sandbox);
sandbox.pc = {payload};
console.log(vm.runInContext("renderTeamGeneralStats(pc, 'Alpha', '{side}')", sandbox));
"""
    result = subprocess.run(
        ["node", "-e", node_script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return result.stdout


def _match(index, outcome, won):
    subject_home = index % 2 == 0
    if outcome == "D":
        score = "1-1"
    elif won:
        score = "2-0" if subject_home else "0-2"
    else:
        score = "0-2" if subject_home else "2-0"
    return {
        "home": "Alpha" if subject_home else f"Rival {index}",
        "away": f"Rival {index}" if subject_home else "Alpha",
        "score": score,
        "league_id_hist": "2074",
        "ou_result": "OVER" if index < 4 else "UNDER",
    }


def _fixture(neutral):
    last_ten = [
        *[_match(i, "W", True) for i in range(4)],
        *[_match(i + 4, "D", False) for i in range(3)],
        *[_match(i + 7, "L", False) for i in range(3)],
    ]
    newest_first_with_older_rows = last_ten + [_match(10, "W", True), _match(11, "W", True)]
    # Older home-only rows intentionally disagree with the mixed-venue latest ten.
    stale_home_only = [
        _match(i, "W", True) for i in range(3)
    ] + [_match(4, "D", False)] + [_match(5, "L", False), _match(6, "L", False)]
    return {
        "league_name": "Neutral League" if neutral else "Regular League",
        "is_neutral_venue": False,  # Simulate a cached ficha predating the neutral toggle.
        "league_id": "2074",
        "recent_home_matches_same_league_general": newest_first_with_older_rows,
        "recent_home_matches_same_league_specific": stale_home_only,
        "recent_away_matches_same_league_general": newest_first_with_older_rows,
        "recent_away_matches_same_league_specific": stale_home_only,
        "home_ou_stats_specific": {"over": 3, "under": 3, "total": 6},
        "home_ou_stats_general": {"over": 4, "under": 6, "total": 10},
        "home_standings": {
            "total_v": "4", "total_e": "3", "total_d": "3", "total_pj": "10",
            "specific_v": "3", "specific_e": "1", "specific_d": "2", "specific_pj": "6",
        },
    }


def test_neutral_top_card_uses_latest_ten_all_venues_even_for_cached_non_neutral_payload():
    for side in ("home", "away"):
        html = _render_stats(_fixture(neutral=True), side=side)

        assert "N10:" in html
        assert "4-3-3 (10)" in html
        assert "O/U N10:" in html
        assert "O 40%" in html
        assert "U 60%" in html
        assert "L:" not in html


def test_regular_league_top_card_keeps_home_only_statistics():
    html = _render_stats(_fixture(neutral=False))

    assert "L:" in html
    assert "3-1-2 (6)" in html
    assert "N10:" not in html
