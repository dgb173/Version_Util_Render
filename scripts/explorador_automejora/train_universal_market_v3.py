"""Autoentrenador universal AH/O-U con validacion temporal estricta.

Amplia el entrenador v2 con las variables que faltaban en casos como
Grotta-Grindavik: rendimiento especifico casa/fuera, resistencia del no
favorito, posible inflacion por resultados y origen probable de la linea O/U.

El flujo usa cuatro tramos cronologicos:

1. discovery: descubre reglas;
2. validation: selecciona reglas;
3. confirmation: exige que vuelvan a funcionar;
4. audit: mide el conjunto elegido sin volver a seleccionar.

No existe un pronostico infalible. La salida correcta cuando no hay soporte,
estabilidad o acuerdo entre reglas es NO BET.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


PROFILE = "universal_market_v3"


def load_v2(project_root: Path):
    path = project_root / "scripts" / "explorador_automejora" / "train_binary_market_system_v2.py"
    spec = importlib.util.spec_from_file_location("market_v2", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se puede cargar {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def number(node: Any, key: str) -> Optional[float]:
    if not isinstance(node, dict):
        return None
    try:
        value = node.get(key)
        return float(str(value).strip()) if value not in (None, "", "-") else None
    except (TypeError, ValueError):
        return None


def ratio(num: Optional[float], den: Optional[float]) -> Optional[float]:
    return None if num is None or den is None or den <= 0 else num / den


def band(value: Optional[float], low: float, high: float, prefix: str) -> str:
    if value is None:
        return f"{prefix}_UNKNOWN"
    if value < low:
        return f"{prefix}_LOW"
    if value >= high:
        return f"{prefix}_HIGH"
    return f"{prefix}_MID"


def venue_metrics(node: Any) -> Dict[str, Optional[float]]:
    pj = number(node, "specific_pj")
    wins = number(node, "specific_v")
    draws = number(node, "specific_e")
    losses = number(node, "specific_d")
    gf = number(node, "specific_gf")
    gc = number(node, "specific_gc")
    return {
        "matches": pj,
        "win_rate": ratio(wins, pj),
        "draw_rate": ratio(draws, pj),
        "loss_rate": ratio(losses, pj),
        "nonloss_rate": ratio((wins or 0) + (draws or 0), pj),
        "gf_pg": ratio(gf, pj),
        "gc_pg": ratio(gc, pj),
    }


def rank_value(node: Any) -> Optional[int]:
    raw = node.get("ranking") if isinstance(node, dict) else None
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


def enrich_features(match: Dict[str, Any], row: Dict[str, Any], v2: Any) -> None:
    """Anade estructura casa/fuera y etiquetas causales sin usar el resultado actual."""
    features: Set[str] = set(row.get("features") or [])
    home = venue_metrics(match.get("home_standings"))
    away = venue_metrics(match.get("away_standings"))
    side = v2.fav_side(row.get("ah"))
    fav = home if side == "HOME" else away
    dog = away if side == "HOME" else home

    features.update(
        {
            band(home["win_rate"], 0.34, 0.58, "HOME_VENUE_WIN"),
            band(home["loss_rate"], 0.30, 0.55, "HOME_VENUE_LOSS"),
            band(away["nonloss_rate"], 0.50, 0.72, "AWAY_VENUE_NONLOSS"),
            band(away["loss_rate"], 0.28, 0.52, "AWAY_VENUE_LOSS"),
            band(fav["win_rate"], 0.34, 0.58, "FAV_VENUE_WIN"),
            band(dog["nonloss_rate"], 0.50, 0.72, "DOG_VENUE_NONLOSS"),
            band(fav["gf_pg"], 1.10, 1.75, "FAV_VENUE_GF"),
            band(dog["gc_pg"], 1.10, 1.75, "DOG_VENUE_GC"),
        }
    )

    hr = rank_value(match.get("home_standings"))
    ar = rank_value(match.get("away_standings"))
    if hr is not None and ar is not None:
        gap = abs(hr - ar)
        features.add("TABLE_GAP_CLOSE" if gap <= 3 else "TABLE_GAP_MEDIUM" if gap <= 8 else "TABLE_GAP_WIDE")

    if dog["nonloss_rate"] is not None and dog["nonloss_rate"] >= (2.0 / 3.0):
        features.add("DOG_RESILIENT_IN_VENUE")
    if fav["win_rate"] is not None and fav["win_rate"] <= 0.40:
        features.add("FAVORITE_WEAK_IN_VENUE")
    if side == "HOME" and "TABLE_GAP_CLOSE" in features and home["win_rate"] is not None and home["win_rate"] <= 0.40:
        features.add("POSSIBLE_LOCALITY_ONLY_FAVORITE")

    fav_covered = "FAV_RECENT_COVER_COVER" in features
    fav_underlying_weak = bool(
        {"FAV_RECENT_STATS_LEAN_AGAINST", "FAV_RECENT_STATS_STRONG_AGAINST"} & features
    )
    if fav_covered and fav_underlying_weak:
        features.add("FAVORITE_RESULT_OVER_PERFORMANCE")
    if fav_covered and fav_underlying_weak and "FAVORITE_WEAK_IN_VENUE" in features:
        features.add("RESULT_INFLATED_FAVORITE")

    recent_high = "FAV_RECENT_GOALS_4_PLUS" in features or "DOG_RECENT_GOALS_4_PLUS" in features
    low_scoring_structure = (fav["gf_pg"] or 99) < 1.55 and (dog["gc_pg"] or 99) < 1.55
    if recent_high and low_scoring_structure and (row.get("ou") or 0) >= 3.0:
        features.add("OU_INFLATED_BY_RECENT_SCORE")
    if (fav["gf_pg"] or 0) >= 1.75:
        features.add("OU_CAUSE_FAVORITE_ATTACK")
    if (dog["gc_pg"] or 0) >= 1.75:
        features.add("OU_CAUSE_DOG_DEFENSE")
    if (home["gf_pg"] or 0) >= 1.50 and (away["gf_pg"] or 0) >= 1.25:
        features.add("OU_CAUSE_BOTH_ATTACKS")

    row["features"] = sorted(features)
    row["venue"] = {"home": home, "away": away, "favorite": fav, "dog": dog}


def temporal_split(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    n = len(rows)
    cuts = (int(n * 0.50), int(n * 0.70), int(n * 0.85))
    return {
        "discovery": list(rows[: cuts[0]]),
        "validation": list(rows[cuts[0] : cuts[1]]),
        "confirmation": list(rows[cuts[1] : cuts[2]]),
        "audit": list(rows[cuts[2] :]),
    }


def wilson_lower(wins: int, bets: int, z: float = 1.645) -> float:
    if bets <= 0:
        return 0.0
    p = wins / bets
    den = 1.0 + z * z / bets
    center = p + z * z / (2.0 * bets)
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * bets)) / bets)
    return 100.0 * (center - margin) / den


def matches_key(row: Dict[str, Any], key: Sequence[str]) -> bool:
    return set(key).issubset(set(row.get("features") or []))


def stats_for(rows: Sequence[Dict[str, Any]], key: Sequence[str], market: str, direction: str, v2: Any) -> Dict[str, Any]:
    subset = [r for r in rows if matches_key(r, key)]
    stat = v2.rate(subset, market, direction)
    stat["wilson_lower_90"] = round(wilson_lower(int(stat["wins"]), int(stat["bets"])), 2)
    return stat


def candidate_keys(row: Dict[str, Any], v2: Any, max_combo: int) -> Set[Tuple[str, ...]]:
    keys = set(v2.candidate_keys(row, max_combo))
    feats = set(row.get("features") or [])
    venue = sorted(
        f for f in feats if f.startswith(("HOME_VENUE_", "AWAY_VENUE_", "FAV_VENUE_", "DOG_VENUE_", "TABLE_GAP_"))
    )
    causal = sorted(
        f for f in feats if f.startswith(("RESULT_", "FAVORITE_RESULT_", "DOG_RESILIENT_", "FAVORITE_WEAK_", "POSSIBLE_LOCALITY_", "OU_CAUSE_", "OU_INFLATED_"))
    )
    anchors = sorted(f for f in feats if f.startswith(("AH_FAMILY=", "AH_SUPER=", "OU_FAMILY=")))
    for f in venue + causal:
        keys.add((f,))
    for a in anchors:
        for f in venue + causal:
            if a != f:
                keys.add(tuple(sorted((a, f))))
    for c in causal:
        for f in venue:
            if c != f:
                keys.add(tuple(sorted((c, f))))
    return {k for k in keys if len(k) <= max_combo}


def build_index(rows: Sequence[Dict[str, Any]], v2: Any, max_combo: int) -> Dict[Tuple[str, ...], List[Dict[str, Any]]]:
    out: Dict[Tuple[str, ...], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for key in candidate_keys(row, v2, max_combo):
            out[key].append(row)
    return out


def discover_rules(parts: Dict[str, List[Dict[str, Any]]], v2: Any, args: argparse.Namespace, market: str) -> List[Dict[str, Any]]:
    directions = ("FAVORITE", "DOG") if market == "side" else ("UNDER", "OVER")
    di = build_index(parts["discovery"], v2, args.max_combo)
    vi = build_index(parts["validation"], v2, args.max_combo)
    baselines = {d: v2.rate(parts["validation"], market, d)["hit_rate"] or 0 for d in directions}
    found: List[Dict[str, Any]] = []
    for key, drows in di.items():
        vrows = vi.get(key, [])
        for direction in directions:
            ds = v2.rate(drows, market, direction)
            vs = v2.rate(vrows, market, direction)
            if ds["bets"] < args.min_discovery_support or vs["bets"] < args.min_validation_support:
                continue
            if (ds["hit_rate"] or 0) < args.min_hit or (vs["hit_rate"] or 0) < args.min_hit:
                continue
            if (vs["hit_rate"] or 0) < baselines[direction] + args.min_lift:
                continue
            cs = stats_for(parts["confirmation"], key, market, direction, v2)
            if cs["bets"] < args.min_confirmation_support or cs["wilson_lower_90"] < args.min_wilson:
                continue
            aus = stats_for(parts["audit"], key, market, direction, v2)
            found.append({
                "key": list(key), "market": market, "direction": direction,
                "discovery": ds, "validation": vs, "confirmation": cs, "audit": aus,
            })
    found.sort(key=lambda r: (-r["confirmation"]["wilson_lower_90"], -r["confirmation"]["bets"], len(r["key"])))
    return found[: args.max_rules]


def load_current(project_root: Path) -> List[Dict[str, Any]]:
    path = project_root / "data" / "data_precacheo.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, list) else raw.get("matches", [])


def prediction_row(match: Dict[str, Any], args: argparse.Namespace, v2: Any) -> Optional[Dict[str, Any]]:
    clone = dict(match)
    clone["final_score"] = "0:0"  # Solo habilita el extractor; nunca se usa como feature.
    clone["_parsed_date"] = v2.parse_date(clone.get("match_date")) or datetime.max
    row, _ = v2.build_training_row(clone, args)
    if row is None:
        return None
    row["actual_fav_cover"] = None
    row["actual_under"] = None
    enrich_features(match, row, v2)
    return row


def choose_prediction(row: Dict[str, Any], rules: Sequence[Dict[str, Any]], market: str) -> Dict[str, Any]:
    matched = [r for r in rules if r["market"] == market and matches_key(row, r["key"])]
    scores: Dict[str, float] = defaultdict(float)
    for rule in matched:
        c = rule["confirmation"]
        weight = max(0.0, float(c["wilson_lower_90"]) - 50.0) * math.log1p(int(c["bets"]))
        scores[rule["direction"]] += weight
    ranked = sorted(scores.items(), key=lambda x: -x[1])
    if not ranked or ranked[0][1] <= 0:
        return {"pick": "NO BET", "confidence": "NONE", "matched_rules": 0}
    if len(ranked) > 1 and ranked[1][1] >= ranked[0][1] * 0.72:
        return {"pick": "NO BET", "confidence": "CONFLICT", "matched_rules": len(matched), "scores": dict(ranked)}
    supporting = [r for r in matched if r["direction"] == ranked[0][0]]
    best_lower = max(float(r["confirmation"]["wilson_lower_90"]) for r in supporting)
    confidence = "HIGH" if best_lower >= 58 and len(supporting) >= 2 else "MEDIUM" if best_lower >= 54 else "LOW"
    return {"pick": ranked[0][0], "confidence": confidence, "matched_rules": len(matched), "supporting_rules": len(supporting), "best_wilson": best_lower}


def ensemble_audit(rows: Sequence[Dict[str, Any]], rules: Sequence[Dict[str, Any]], market: str, v2: Any) -> Dict[str, Any]:
    bets = wins = 0
    by_confidence: Dict[str, List[int]] = defaultdict(lambda: [0, 0])
    for row in rows:
        pred = choose_prediction(row, rules, market)
        direction = pred.get("pick")
        if direction == "NO BET":
            continue
        result = v2.outcome(row, market, direction)
        if result is None:
            continue
        bets += 1
        wins += int(result is True)
        bucket = by_confidence[str(pred.get("confidence"))]
        bucket[0] += 1
        bucket[1] += int(result is True)
    return {
        "bets": bets,
        "wins": wins,
        "hit_rate": round(100.0 * wins / bets, 2) if bets else None,
        "coverage": round(100.0 * bets / len(rows), 2) if rows else 0.0,
        "by_confidence": {
            key: {"bets": val[0], "wins": val[1], "hit_rate": round(100.0 * val[1] / val[0], 2) if val[0] else None}
            for key, val in sorted(by_confidence.items())
        },
    }


def concrete_pick(prediction: Dict[str, Any], row: Dict[str, Any]) -> str:
    direction = prediction.get("pick")
    if direction == "NO BET":
        return "NO BET"
    if direction in {"UNDER", "OVER"}:
        return f"{direction} {row.get('ou')}"
    team = row.get("favorite") if direction == "FAVORITE" else row.get("dog")
    role = "favorito" if direction == "FAVORITE" else "no favorito"
    return f"{team} ({role}; AH codificado {row.get('ah')})"


def audit_gate(prediction: Dict[str, Any], audit: Dict[str, Any], min_bets: int = 20, min_hit: float = 55.0) -> Dict[str, Any]:
    """Impide publicar niveles de evidencia que no funcionaron en la auditoria."""
    if prediction.get("pick") == "NO BET":
        return prediction
    tier = str(prediction.get("confidence"))
    tier_stats = (audit.get("by_confidence") or {}).get(tier) or {}
    if int(tier_stats.get("bets") or 0) < min_bets or float(tier_stats.get("hit_rate") or 0) < min_hit:
        return {
            "pick": "NO BET",
            "market_pick": "NO BET",
            "confidence": "REJECTED_BY_AUDIT",
            "reason": "El nivel de evidencia no alcanzo soporte/acierto minimo en el tramo audit.",
            "raw_prediction": prediction,
            "audit_tier": tier_stats,
        }
    prediction["audit_tier"] = tier_stats
    return prediction


def markdown(payload: Dict[str, Any]) -> str:
    s = payload["summary"]
    lines = [
        "# Autoentrenamiento universal v3", "",
        "> Ningun modelo es infalible. El sistema prioriza validacion temporal y NO BET.", "",
        f"- Partidos cargados: **{s['loaded']}**", f"- Partidos utilizables: **{s['usable']}**",
        f"- Discovery / validation / confirmation / audit: **{s['split']}**",
        f"- Reglas AH confirmadas: **{s['side_rules']}**", f"- Reglas O/U confirmadas: **{s['goal_rules']}**", "",
        "## Pronosticos actuales", "",
        "| Partido | AH | Conf. | O/U | Conf. |", "|---|---:|---:|---:|---:|",
    ]
    for p in payload["predictions"]:
        lines.append(f"| {p['home']} vs {p['away']} | {p['side']['market_pick']} | {p['side']['confidence']} | {p['goals']['market_pick']} | {p['goals']['confidence']} |")
    lines.extend(["", "## Criterio", "", "Las reglas se descubren en el pasado, se seleccionan en validacion, deben repetirse en confirmacion y se auditan en el tramo mas reciente. Casa/fuera, rival comun, residuales AH, dominancia y causa del O/U se tratan por separado."])
    return "\n".join(lines) + "\n"


def run(project_root: Path, args: argparse.Namespace) -> Dict[str, Any]:
    v2 = load_v2(project_root)
    finished, load_audit = v2.load_finished(project_root, bool(args.include_unknown))
    rows: List[Dict[str, Any]] = []
    rejects = Counter()
    for match in finished:
        row, reasons = v2.build_training_row(match, args)
        if row is None:
            rejects.update(reasons)
            continue
        enrich_features(match, row, v2)
        rows.append(row)
    parts = temporal_split(rows)
    side_rules = discover_rules(parts, v2, args, "side")
    goal_rules = discover_rules(parts, v2, args, "goals")
    side_audit = ensemble_audit(parts["audit"], side_rules, "side", v2)
    goals_audit = ensemble_audit(parts["audit"], goal_rules, "goals", v2)
    predictions = []
    for match in load_current(project_root):
        row = prediction_row(match, args, v2)
        if row is None:
            continue
        predictions.append({
            "match_id": str(match.get("match_id") or ""),
            "home": match.get("home_name"), "away": match.get("away_name"),
            "date": match.get("match_date"), "ah": row.get("ah"), "ou": row.get("ou"),
            "side": choose_prediction(row, side_rules, "side"),
            "goals": choose_prediction(row, goal_rules, "goals"),
        })
        predictions[-1]["side"]["market_pick"] = concrete_pick(predictions[-1]["side"], row)
        predictions[-1]["goals"]["market_pick"] = concrete_pick(predictions[-1]["goals"], row)
        predictions[-1]["side"] = audit_gate(predictions[-1]["side"], side_audit)
        predictions[-1]["goals"] = audit_gate(predictions[-1]["goals"], goals_audit)
    audit_side = [r["audit"]["hit_rate"] for r in side_rules if r["audit"]["bets"] >= args.min_audit_support and r["audit"]["hit_rate"] is not None]
    audit_goals = [r["audit"]["hit_rate"] for r in goal_rules if r["audit"]["bets"] >= args.min_audit_support and r["audit"]["hit_rate"] is not None]
    return {
        "profile": PROFILE, "generated_at": datetime.now().isoformat(timespec="seconds"),
        "config": vars(args),
        "summary": {
            "loaded": len(finished), "usable": len(rows), "rejected": len(finished) - len(rows),
            "split": {k: len(v) for k, v in parts.items()},
            "side_rules": len(side_rules), "goal_rules": len(goal_rules),
            "audit_side_mean_hit": round(sum(audit_side) / len(audit_side), 2) if audit_side else None,
            "audit_goals_mean_hit": round(sum(audit_goals) / len(audit_goals), 2) if audit_goals else None,
            "audit_side_ensemble": side_audit,
            "audit_goals_ensemble": goals_audit,
            "current_side_picks_after_audit_gate": sum(p["side"]["pick"] != "NO BET" for p in predictions),
            "current_goal_picks_after_audit_gate": sum(p["goals"]["pick"] != "NO BET" for p in predictions),
        },
        "load_audit": dict(load_audit), "rejects": dict(rejects),
        "rules": {"side": side_rules, "goals": goal_rules}, "predictions": predictions,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Autoentrenador universal AH/O-U v3")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-json", default="data/universal_market_v3.json")
    parser.add_argument("--output-md", default="UNIVERSAL_MARKET_V3.md")
    parser.add_argument("--min-quality", type=int, default=5)
    parser.add_argument("--max-h2h-days", type=int, default=1100)
    parser.add_argument("--max-recent-days", type=int, default=260)
    parser.add_argument("--max-indirect-days", type=int, default=900)
    parser.add_argument("--min-discovery-support", type=int, default=45)
    parser.add_argument("--min-validation-support", type=int, default=18)
    parser.add_argument("--min-confirmation-support", type=int, default=12)
    parser.add_argument("--min-audit-support", type=int, default=10)
    parser.add_argument("--min-hit", type=float, default=58.0)
    parser.add_argument("--min-lift", type=float, default=3.0)
    parser.add_argument("--min-wilson", type=float, default=50.0)
    parser.add_argument("--max-combo", type=int, default=3)
    parser.add_argument("--max-rules", type=int, default=100)
    parser.add_argument("--include-unknown", action="store_true")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    payload = run(root, args)
    out_json = root / args.output_json
    out_md = root / args.output_md
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(markdown(payload), encoding="utf-8")
    print(json.dumps(payload["summary"], ensure_ascii=False))
    print(f"[OK] {out_json}")
    print(f"[OK] {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
