#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from analyze_bookie_positioning import analyze_positioning  # noqa: E402
from generate_binary_h2h_html import derive_binary_reading  # noqa: E402


PROFILE = "trained_binary_h2h_system_v1"
TRAINING_FILES = (
    "data_ah_0.json",
    "data_ah_0.5.json",
    "data_ah_1.5.json",
    "data_ah_2_plus.json",
    "data_minus_ah_0.5.json",
    "data_minus_ah_1.5.json",
    "data_minus_ah_2_plus.json",
)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text or text in {"-", "?", "N/A"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_score(value: Any) -> Optional[Tuple[int, int]]:
    text = str(value or "").replace("-", ":").strip()
    if not text or "?" in text:
        return None
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0].strip()), int(parts[1].strip())
    except ValueError:
        return None


def _parse_date(value: Any) -> datetime:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return datetime.min


def _fmt(value: Any) -> str:
    num = _safe_float(value)
    if num is None:
        return "N/A"
    rounded = round(num, 2)
    if abs(rounded - int(rounded)) < 1e-9:
        return str(int(rounded))
    return f"{rounded:.2f}".rstrip("0").rstrip(".")


def _current_ah(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    return _safe_float(odds.get("ah_linea") if odds else match.get("handicap"))


def _current_ou(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds") if isinstance(match.get("main_match_odds"), dict) else {}
    return _safe_float(odds.get("goals_linea") if odds else match.get("goals_line"))


def _ah_family(ah: Optional[float]) -> str:
    if ah is None:
        return "AH_UNKNOWN"
    mag = abs(float(ah))
    if mag < 0.01:
        return "AH_0"
    if mag <= 0.75:
        return "AH_0_25_0_75"
    if mag <= 1.0:
        return "AH_1"
    if mag <= 1.75:
        return "AH_1_25_1_75"
    return "AH_2_PLUS"


def _ou_family(ou: Optional[float]) -> str:
    if ou is None:
        return "OU_UNKNOWN"
    if ou <= 2.25:
        return "OU_LOW"
    if ou <= 2.75:
        return "OU_MID"
    if ou <= 3.25:
        return "OU_HIGH"
    return "OU_EXTREME"


def _fav_side(ah: Optional[float]) -> str:
    if ah is None or abs(float(ah)) < 1e-9:
        return "PICKEM"
    return "HOME" if float(ah) > 0 else "AWAY"


def _actual_favorite_cover(match: Dict[str, Any]) -> Optional[bool]:
    score = _parse_score(match.get("final_score"))
    ah = _current_ah(match)
    side = _fav_side(ah)
    if score is None or ah is None or side == "PICKEM":
        return None
    home_goals, away_goals = score
    fav_margin = home_goals - away_goals if side == "HOME" else away_goals - home_goals
    diff = float(fav_margin) - abs(float(ah))
    if diff > 1e-9:
        return True
    if diff < -1e-9:
        return False
    return None


def _actual_under(match: Dict[str, Any]) -> Optional[bool]:
    score = _parse_score(match.get("final_score"))
    ou = _current_ou(match)
    if score is None or ou is None:
        return None
    total = score[0] + score[1]
    diff = float(total) - float(ou)
    if abs(diff) <= 1e-9:
        return None
    return diff < 0


def _load_finished(project_root: Path, include_unknown: bool = False) -> List[Dict[str, Any]]:
    data_dir = project_root / "data"
    names = list(TRAINING_FILES)
    if include_unknown:
        names.append("data_unknown.json")

    by_id: Dict[str, Dict[str, Any]] = {}
    for name in names:
        path = data_dir / name
        if not path.exists():
            continue
        raw = json.loads(path.read_text(encoding="utf-8"))
        rows = raw if isinstance(raw, list) else raw.get("matches", []) if isinstance(raw, dict) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            score = _parse_score(row.get("final_score"))
            if score is None or _current_ah(row) is None:
                continue
            match_id = str(row.get("match_id") or f"{name}:{len(by_id)}")
            current = by_id.get(match_id)
            if current is None:
                row = dict(row)
                row["_source_file"] = name
                row["_parsed_date"] = _parse_date(row.get("match_date"))
                by_id[match_id] = row
            else:
                # Prefer rows with market/H2H context.
                cur_score = int(bool(current.get("market_analysis_data"))) + int(bool(current.get("h2h_general")))
                new_score = int(bool(row.get("market_analysis_data"))) + int(bool(row.get("h2h_general")))
                if new_score > cur_score:
                    row = dict(row)
                    row["_source_file"] = name
                    row["_parsed_date"] = _parse_date(row.get("match_date"))
                    by_id[match_id] = row
    rows = list(by_id.values())
    rows.sort(key=lambda r: (r.get("_parsed_date") or datetime.min, str(r.get("match_id"))))
    return rows


def _positioning_row(match: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        positioning = analyze_positioning(match)
        binary = derive_binary_reading(positioning)
    except Exception:
        return None

    ah = _current_ah(match)
    ou = _current_ou(match)
    fav_cover = _actual_favorite_cover(match)
    under = _actual_under(match)
    side_signal = binary.get("side_signal")
    goals_signal = binary.get("goals_signal")

    side_hit: Optional[bool] = None
    if fav_cover is not None:
        if side_signal == "FAVORITO":
            side_hit = fav_cover is True
        elif side_signal == "NO_FAVORITO":
            side_hit = fav_cover is False

    goals_hit: Optional[bool] = None
    if under is not None:
        if goals_signal == "CONTROL_BAJO":
            goals_hit = under is True
        elif goals_signal == "RUPTURA_ALTA":
            goals_hit = under is False

    frame = positioning.get("market_frame") if isinstance(positioning.get("market_frame"), dict) else {}
    narrative = positioning.get("narrative") if isinstance(positioning.get("narrative"), dict) else {}
    base_map = narrative.get("base_map") if isinstance(narrative.get("base_map"), dict) else {}
    base = base_map.get("case") if isinstance(base_map.get("case"), dict) else {}
    labels = [str(item.get("id")) for item in positioning.get("labels", []) if isinstance(item, dict)]
    return {
        "match_id": str(match.get("match_id")),
        "date": str(match.get("match_date") or ""),
        "date_sort": (match.get("_parsed_date") or datetime.min).isoformat(),
        "source_file": match.get("_source_file"),
        "home": match.get("home_name"),
        "away": match.get("away_name"),
        "league": match.get("league_name"),
        "final_score": match.get("final_score"),
        "ah": ah,
        "ou": ou,
        "ah_exact": f"AH_{_fmt(ah)}",
        "ah_family": _ah_family(ah),
        "ou_family": _ou_family(ou),
        "fav_side": _fav_side(ah),
        "favorite": frame.get("favorite"),
        "table_positioning": frame.get("table_positioning"),
        "route": binary.get("route"),
        "side_signal": side_signal,
        "goals_signal": goals_signal,
        "confidence": binary.get("confidence"),
        "base_cover": base.get("cover_current_line") or "NO_H2H",
        "pressure_label": base.get("pressure_change_label") or "NO_H2H",
        "base_stats": (base.get("stats_edge_for_current_fav") or {}).get("verdict", "NO_STATS"),
        "labels": labels,
        "actual_fav_cover": fav_cover,
        "actual_under": under,
        "side_hit": side_hit,
        "goals_hit": goals_hit,
    }


def _outcome_hit(row: Dict[str, Any], market: str, direction: str) -> Optional[bool]:
    if market == "side":
        fav_cover = row.get("actual_fav_cover")
        if fav_cover is None:
            return None
        if direction == "FAVORITO":
            return fav_cover is True
        if direction == "NO_FAVORITO":
            return fav_cover is False
        return None
    if market == "goals":
        under = row.get("actual_under")
        if under is None:
            return None
        if direction == "UNDER":
            return under is True
        if direction == "OVER":
            return under is False
        return None
    return None


def _rate(rows: Sequence[Dict[str, Any]], market: str, direction: str) -> Dict[str, Any]:
    vals = [_outcome_hit(row, market, direction) for row in rows]
    valid = [value for value in vals if value is not None]
    wins = sum(1 for value in valid if value is True)
    return {
        "bets": len(valid),
        "wins": wins,
        "hit_rate": round(100.0 * wins / len(valid), 2) if valid else None,
    }


def _split_rows(rows: List[Dict[str, Any]], validation_ratio: float) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not rows:
        return [], []
    cut = int(len(rows) * max(0.05, min(0.8, 1.0 - validation_ratio)))
    cut = max(1, min(len(rows) - 1, cut))
    return rows[:cut], rows[cut:]


def _candidate_keys(row: Dict[str, Any]) -> List[Tuple[str, ...]]:
    labels = row.get("labels") or []
    base = [
        ("ah_family", row["ah_family"]),
        ("ah_exact", row["ah_exact"]),
        ("route", row["route"]),
        ("side_signal", row["side_signal"]),
        ("goals_signal", row["goals_signal"]),
        ("confidence", row["confidence"]),
        ("pressure", row["pressure_label"]),
        ("base_cover", row["base_cover"]),
        ("base_stats", row["base_stats"]),
        ("table", row["table_positioning"]),
        ("ou_family", row["ou_family"]),
    ]
    combos: List[Tuple[str, ...]] = [
        (f"AH={row['ah_family']}", f"ROUTE={row['route']}"),
        (f"AH={row['ah_family']}", f"SIDE={row['side_signal']}"),
        (f"AH={row['ah_family']}", f"GOALS={row['goals_signal']}"),
        (f"AH={row['ah_family']}", f"PRESSURE={row['pressure_label']}"),
        (f"AH={row['ah_family']}", f"COVER={row['base_cover']}"),
        (f"AH={row['ah_family']}", f"ROUTE={row['route']}", f"PRESSURE={row['pressure_label']}"),
        (f"AH={row['ah_family']}", f"ROUTE={row['route']}", f"STATS={row['base_stats']}"),
        (f"AH={row['ah_family']}", f"OU={row['ou_family']}", f"PRESSURE={row['pressure_label']}"),
        (f"AHX={row['ah_exact']}", f"PRESSURE={row['pressure_label']}"),
        (f"AHX={row['ah_exact']}", f"ROUTE={row['route']}"),
        (f"ROUTE={row['route']}", f"PRESSURE={row['pressure_label']}"),
        (f"ROUTE={row['route']}", f"STATS={row['base_stats']}"),
        (f"ROUTE={row['route']}", f"PRESSURE={row['pressure_label']}", f"STATS={row['base_stats']}"),
        (f"OU={row['ou_family']}", f"GOALS={row['goals_signal']}"),
        (f"OU={row['ou_family']}", f"PRESSURE={row['pressure_label']}"),
    ]
    for label in labels:
        combos.append((f"TAG={label}",))
        combos.append((f"AH={row['ah_family']}", f"TAG={label}"))
        combos.append((f"AHX={row['ah_exact']}", f"TAG={label}"))
        combos.append((f"ROUTE={row['route']}", f"TAG={label}"))
        combos.append((f"PRESSURE={row['pressure_label']}", f"TAG={label}"))
        combos.append((f"OU={row['ou_family']}", f"TAG={label}"))
        combos.append((f"AH={row['ah_family']}", f"ROUTE={row['route']}", f"TAG={label}"))
        combos.append((f"AH={row['ah_family']}", f"PRESSURE={row['pressure_label']}", f"TAG={label}"))
        combos.append((f"ROUTE={row['route']}", f"PRESSURE={row['pressure_label']}", f"TAG={label}"))
        combos.append((f"OU={row['ou_family']}", f"PRESSURE={row['pressure_label']}", f"TAG={label}"))
    combos.extend((f"{k}={v}",) for k, v in base if v)
    return combos


def _index_candidates(rows: Sequence[Dict[str, Any]]) -> Dict[Tuple[str, ...], List[Dict[str, Any]]]:
    out: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}
    for row in rows:
        for key in _candidate_keys(row):
            out.setdefault(tuple(key), []).append(row)
    return out


def _filter_by_key(rows: Sequence[Dict[str, Any]], key: Tuple[str, ...]) -> List[Dict[str, Any]]:
    key_set = set(key)
    return [row for row in rows if key_set.issubset(set(_candidate_flat(row)))]


def _candidate_flat(row: Dict[str, Any]) -> List[str]:
    flat: List[str] = []
    for key in _candidate_keys(row):
        flat.extend(key)
    return flat


def _mine_rules(
    train_rows: Sequence[Dict[str, Any]],
    val_rows: Sequence[Dict[str, Any]],
    *,
    market: str,
    min_train: int,
    min_val: int,
    min_hit: float,
) -> List[Dict[str, Any]]:
    train_index = _index_candidates(train_rows)
    val_index = _index_candidates(val_rows)
    rules: List[Dict[str, Any]] = []
    directions = ("FAVORITO", "NO_FAVORITO") if market == "side" else ("UNDER", "OVER")
    for key, subset_train in train_index.items():
        subset_val = val_index.get(key, [])
        for direction in directions:
            train_rate = _rate(subset_train, market, direction)
            if train_rate["bets"] < min_train or train_rate["hit_rate"] is None or train_rate["hit_rate"] < min_hit:
                continue
            val_rate = _rate(subset_val, market, direction)
            if val_rate["bets"] < min_val or val_rate["hit_rate"] is None or val_rate["hit_rate"] < min_hit:
                continue
            rules.append(
                {
                    "key": list(key),
                    "market": market,
                    "direction": direction,
                    "train": train_rate,
                    "validation": val_rate,
                    "stability_gap": round(abs(float(train_rate["hit_rate"]) - float(val_rate["hit_rate"])), 2),
                    "sample_matches": [
                        {
                            "match_id": row["match_id"],
                            "date": row["date"],
                            "home": row["home"],
                            "away": row["away"],
                            "score": row["final_score"],
                            "ah": _fmt(row["ah"]),
                            "ou": _fmt(row["ou"]),
                        }
                        for row in subset_val[:5]
                    ],
                }
            )
    rules.sort(
        key=lambda r: (
            -float(r["validation"]["hit_rate"]),
            -int(r["validation"]["bets"]),
            float(r["stability_gap"]),
            r["key"],
        )
    )
    return rules


def _by_handicap(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(row["ah_family"], []).append(row)
    out: List[Dict[str, Any]] = []
    for family in sorted(buckets):
        subset = buckets[family]
        fav = _rate(subset, "side", "FAVORITO")
        dog = _rate(subset, "side", "NO_FAVORITO")
        under = _rate(subset, "goals", "UNDER")
        over = _rate(subset, "goals", "OVER")
        out.append(
            {
                "ah_family": family,
                "rows": len(subset),
                "fav_cover_bets": fav["bets"],
                "fav_cover_rate": fav["hit_rate"],
                "dog_cover_bets": dog["bets"],
                "dog_cover_rate": dog["hit_rate"],
                "under_bets": under["bets"],
                "under_rate": under["hit_rate"],
                "over_bets": over["bets"],
                "over_rate": over["hit_rate"],
            }
        )
    return out


def _html(payload: Dict[str, Any]) -> str:
    data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Sistema binario H2H entrenado</title>
  <style>
    :root {{ --bg:#f6f7fb; --panel:#fff; --ink:#172033; --muted:#667085; --line:#d9e0ec; --blue:#1d4ed8; --green:#166534; --red:#991b1b; --amber:#92400e; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:Inter,Segoe UI,Arial,sans-serif; background:var(--bg); color:var(--ink); letter-spacing:0; }}
    .wrap {{ max-width:1480px; margin:0 auto; padding:20px; }}
    header {{ background:#fff; border-bottom:1px solid var(--line); position:sticky; top:0; z-index:5; }}
    h1 {{ margin:0 0 4px; font-size:24px; }}
    .sub {{ color:var(--muted); margin:0; font-size:13px; }}
    .stats {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:10px; margin-top:14px; }}
    .stat {{ border:1px solid var(--line); background:#fff; border-radius:8px; padding:10px; }}
    .stat b {{ display:block; font-size:22px; }}
    .stat span {{ color:var(--muted); font-size:12px; }}
    .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; align-items:start; }}
    .panel {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; margin-top:16px; }}
    .panel h2 {{ margin:0 0 10px; font-size:16px; }}
    table {{ width:100%; border-collapse:collapse; font-size:12px; }}
    th,td {{ border-bottom:1px solid var(--line); padding:8px; text-align:left; vertical-align:top; }}
    th {{ color:#334155; background:#f8fafc; position:sticky; top:117px; z-index:1; }}
    .badge {{ display:inline-flex; padding:3px 7px; border-radius:999px; font-size:11px; font-weight:800; border:1px solid transparent; margin:1px; }}
    .ok {{ background:#dcfce7; color:var(--green); border-color:#bbf7d0; }}
    .warn {{ background:#fef3c7; color:var(--amber); border-color:#fde68a; }}
    .bad {{ background:#fee2e2; color:var(--red); border-color:#fecaca; }}
    .info {{ background:#dbeafe; color:var(--blue); border-color:#bfdbfe; }}
    .muted {{ color:var(--muted); }}
    .controls {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }}
    input,select {{ border:1px solid var(--line); border-radius:8px; padding:9px 10px; min-width:180px; }}
    details {{ margin-top:6px; }}
    summary {{ cursor:pointer; font-weight:700; }}
    @media (max-width:900px) {{ .grid,.stats {{ grid-template-columns:1fr; }} th {{ position:static; }} }}
  </style>
</head>
<body>
  <header><div class="wrap">
    <h1>Sistema binario H2H entrenado por handicap</h1>
    <p class="sub">Entrena solo con partidos terminados, separa entrenamiento y validacion temporal, y conserva solo reglas que sobreviven fuera de muestra.</p>
    <div id="stats" class="stats"></div>
    <div class="controls">
      <input id="q" placeholder="Buscar regla, etiqueta o handicap">
      <select id="target"><option value="">Mercado: todos</option><option value="side">Lado AH</option><option value="goals">Goles</option></select>
    </div>
  </div></header>
  <main class="wrap">
    <section class="panel">
      <h2>Resumen por familia AH en validacion</h2>
      <div id="families"></div>
    </section>
    <section class="grid">
      <div class="panel">
        <h2>Reglas elite lado AH</h2>
        <div id="sideRules"></div>
      </div>
      <div class="panel">
        <h2>Reglas elite goles</h2>
        <div id="goalRules"></div>
      </div>
    </section>
  </main>
  <script>
    const DATA = {data_json};
    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));
    function badgeRate(rate) {{
      const cls = rate >= 85 ? 'ok' : rate >= 75 ? 'warn' : 'bad';
      return `<span class="badge ${{cls}}">${{esc(rate)}}%</span>`;
    }}
    function init() {{
      renderStats();
      renderFamilies();
      renderRules();
      document.getElementById('q').addEventListener('input', renderRules);
      document.getElementById('target').addEventListener('input', renderRules);
    }}
    function renderStats() {{
      const s = DATA.summary;
      const items = [
        ['Terminados', s.finished_rows],
        ['Train', s.train_rows],
        ['Validacion', s.validation_rows],
        ['Reglas AH', DATA.rules.side.length],
        ['Reglas goles', DATA.rules.goals.length],
      ];
      document.getElementById('stats').innerHTML = items.map(([k,v]) => `<div class="stat"><b>${{esc(v)}}</b><span>${{esc(k)}}</span></div>`).join('');
    }}
    function renderFamilies() {{
      const rows = DATA.validation_by_handicap || [];
      document.getElementById('families').innerHTML = `<table><thead><tr><th>AH</th><th>Partidos</th><th>Fav cubre</th><th>No fav cubre</th><th>Under</th><th>Over</th></tr></thead><tbody>${{rows.map(r => `<tr><td><span class="badge info">${{esc(r.ah_family)}}</span></td><td>${{esc(r.rows)}}</td><td>${{esc(r.fav_cover_bets)}} / ${{r.fav_cover_rate == null ? '<span class="muted">N/A</span>' : badgeRate(r.fav_cover_rate)}}</td><td>${{esc(r.dog_cover_bets)}} / ${{r.dog_cover_rate == null ? '<span class="muted">N/A</span>' : badgeRate(r.dog_cover_rate)}}</td><td>${{esc(r.under_bets)}} / ${{r.under_rate == null ? '<span class="muted">N/A</span>' : badgeRate(r.under_rate)}}</td><td>${{esc(r.over_bets)}} / ${{r.over_rate == null ? '<span class="muted">N/A</span>' : badgeRate(r.over_rate)}}</td></tr>`).join('')}}</tbody></table>`;
    }}
    function filtered(rules, targetName) {{
      const q = document.getElementById('q').value.toLowerCase().trim();
      const target = document.getElementById('target').value;
      return rules.filter(r => (!target || target === targetName) && (!q || r.key.join(' ').toLowerCase().includes(q)));
    }}
    function renderRuleTable(rules, targetName) {{
      const rows = filtered(rules, targetName);
      if (!rows.length) return '<p class="muted">Sin reglas con estos filtros.</p>';
      return `<table><thead><tr><th>Filtro</th><th>Direccion</th><th>Train</th><th>Validacion</th><th>Gap</th><th>Muestras</th></tr></thead><tbody>${{rows.map(r => `<tr><td>${{r.key.map(x => `<span class="badge info">${{esc(x)}}</span>`).join(' ')}}</td><td><span class="badge ok">${{esc(r.direction)}}</span></td><td>${{esc(r.train.bets)}} / ${{badgeRate(r.train.hit_rate)}}</td><td>${{esc(r.validation.bets)}} / ${{badgeRate(r.validation.hit_rate)}}</td><td>${{esc(r.stability_gap)}}</td><td><details><summary>ver</summary>${{r.sample_matches.map(m => `<div class="muted">${{esc(m.date)}} · ${{esc(m.home)}} vs ${{esc(m.away)}} · ${{esc(m.score)}} · AH ${{esc(m.ah)}} OU ${{esc(m.ou)}}</div>`).join('')}}</details></td></tr>`).join('')}}</tbody></table>`;
    }}
    function renderRules() {{
      document.getElementById('sideRules').innerHTML = renderRuleTable(DATA.rules.side, 'side');
      document.getElementById('goalRules').innerHTML = renderRuleTable(DATA.rules.goals, 'goals');
    }}
    init();
  </script>
</body>
</html>
"""


def train(project_root: Path, args: argparse.Namespace) -> Dict[str, Any]:
    finished = _load_finished(project_root, include_unknown=bool(args.include_unknown))
    rows: List[Dict[str, Any]] = []
    errors = 0
    for match in finished:
        item = _positioning_row(match)
        if item is None:
            errors += 1
        else:
            rows.append(item)

    train_rows, val_rows = _split_rows(rows, float(args.validation_ratio))
    min_hit = float(args.min_hit)
    side_rules = _mine_rules(
        train_rows,
        val_rows,
        market="side",
        min_train=int(args.min_train_support),
        min_val=int(args.min_validation_support),
        min_hit=min_hit,
    )[: int(args.max_rules)]
    goal_rules = _mine_rules(
        train_rows,
        val_rows,
        market="goals",
        min_train=int(args.min_train_support),
        min_val=int(args.min_validation_support),
        min_hit=min_hit,
    )[: int(args.max_rules)]
    return {
        "profile": PROFILE,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "config": {
            "validation_ratio": float(args.validation_ratio),
            "min_train_support": int(args.min_train_support),
            "min_validation_support": int(args.min_validation_support),
            "min_hit": min_hit,
            "include_unknown": bool(args.include_unknown),
        },
        "summary": {
            "finished_rows": len(rows),
            "errors": errors,
            "train_rows": len(train_rows),
            "validation_rows": len(val_rows),
            "train_favorite": _rate(train_rows, "side", "FAVORITO"),
            "validation_favorite": _rate(val_rows, "side", "FAVORITO"),
            "train_non_favorite": _rate(train_rows, "side", "NO_FAVORITO"),
            "validation_non_favorite": _rate(val_rows, "side", "NO_FAVORITO"),
            "train_under": _rate(train_rows, "goals", "UNDER"),
            "validation_under": _rate(val_rows, "goals", "UNDER"),
            "train_over": _rate(train_rows, "goals", "OVER"),
            "validation_over": _rate(val_rows, "goals", "OVER"),
        },
        "validation_by_handicap": _by_handicap(val_rows),
        "rules": {
            "side": side_rules,
            "goals": goal_rules,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Entrena reglas binarias H2H-first con partidos terminados.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-html", default="sistema_binario_h2h_entrenado.html")
    parser.add_argument("--output-json", default="data/sistema_binario_h2h_entrenado.json")
    parser.add_argument("--validation-ratio", type=float, default=0.25)
    parser.add_argument("--min-train-support", type=int, default=35)
    parser.add_argument("--min-validation-support", type=int, default=12)
    parser.add_argument("--min-hit", type=float, default=78.0)
    parser.add_argument("--max-rules", type=int, default=80)
    parser.add_argument("--include-unknown", action="store_true")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    payload = train(project_root, args)

    out_json = Path(args.output_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    out_html = Path(args.output_html)
    out_html.write_text(_html(payload), encoding="utf-8")

    print(f"[OK] JSON: {out_json.resolve()}")
    print(f"[OK] HTML: {out_html.resolve()}")
    print(
        "[INFO] "
        f"finished={payload['summary']['finished_rows']} "
        f"train={payload['summary']['train_rows']} "
        f"validation={payload['summary']['validation_rows']} "
        f"side_rules={len(payload['rules']['side'])} "
        f"goal_rules={len(payload['rules']['goals'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
