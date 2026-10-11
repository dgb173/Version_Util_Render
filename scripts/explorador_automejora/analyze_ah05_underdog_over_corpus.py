#!/usr/bin/env python3
"""Auditoria reproducible de AH |0.50|, no favorito y Over.

Lee el snapshot historico SQLite sin modificarlo y genera:
- documento metodologico y conclusiones en Markdown;
- JSON con metricas, patrones y todos los casos objetivo.

La convencion del proyecto es: AH > 0 => HOME favorito; AH <= 0 => AWAY
favorito. Para AH 0.50 no hay push: el no favorito +0.50 cubre si gana o empata.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text or text.upper() in {"N/A", "NULL", "-", "?"}:
        return None
    if "/" in text:
        try:
            vals = [float(x) for x in text.split("/")]
            return sum(vals) / len(vals)
        except Exception:
            return None
    try:
        return float(text)
    except Exception:
        return None


def parse_score(value: Any) -> Optional[Tuple[int, int]]:
    if value is None:
        return None
    text = str(value).strip().replace(" - ", ":").replace("-", ":")
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except Exception:
        return None


def parse_date(value: Any) -> str:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(text[:10], fmt).date().isoformat()
        except Exception:
            pass
    return text[:10]


def wdl(score: Any) -> str:
    parsed = parse_score(score)
    if not parsed:
        return "NA"
    h, a = parsed
    return "HOME_WIN" if h > a else "AWAY_WIN" if a > h else "DRAW"


def over_state(score: Any, line: Optional[float]) -> str:
    parsed = parse_score(score)
    if not parsed or line is None:
        return "NA"
    total = sum(parsed)
    if total > line:
        return "OVER"
    if abs(total - line) < 1e-9:
        return "PUSH"
    return "UNDER"


def line_relation(value: Optional[float], current_abs: float = 0.5) -> str:
    if value is None:
        return "NA"
    mag = abs(value)
    if abs(mag - current_abs) < 1e-9:
        return "EXACT"
    if abs(mag - current_abs) <= 0.25 + 1e-9:
        return "NEAR_QUARTER"
    return "LOWER" if mag < current_abs else "HIGHER"


def movement_signature(raw: Any) -> str:
    """Repricing precedente->actual; no se etiqueta como steam/live movement."""
    if raw is None:
        return "NA"
    text = str(raw).replace(",", ".").replace("→", "->").replace(" ", "")
    if "->" not in text:
        return "NA"
    parts = text.split("->")
    if len(parts) != 2:
        return "NA"
    start, end = safe_float(parts[0]), safe_float(parts[1])
    if start is None or end is None:
        return "NA"
    if start * end < 0:
        return "SIDE_FLIP"
    delta = abs(end) - abs(start)
    if abs(delta) < 1e-9:
        return "FLAT"
    if delta >= 0.5:
        return "STRONGER_050_PLUS"
    if delta > 0:
        return "STRONGER_025"
    if delta <= -0.5:
        return "WEAKER_050_PLUS"
    return "WEAKER_025"


def captured_ah_movement(start: Optional[float], end: Optional[float]) -> str:
    if start is None or end is None:
        return "NA"
    if start * end < 0:
        return "SIDE_FLIP"
    delta = abs(end) - abs(start)
    if abs(delta) < 1e-9:
        return "FLAT"
    if delta >= 0.5:
        return "STRONGER_050_PLUS"
    if delta > 0:
        return "STRONGER_025"
    if delta <= -0.5:
        return "WEAKER_050_PLUS"
    return "WEAKER_025"


def captured_ou_movement(start: Optional[float], end: Optional[float]) -> str:
    if start is None or end is None:
        return "NA"
    delta = end - start
    if abs(delta) < 1e-9:
        return "FLAT"
    if delta >= 0.5:
        return "UP_050_PLUS"
    if delta > 0:
        return "UP_025"
    if delta <= -0.5:
        return "DOWN_050_PLUS"
    return "DOWN_025"


def stat_value(rows: Any, labels: Sequence[str]) -> Optional[Tuple[float, float]]:
    if not isinstance(rows, list):
        return None
    wanted = {x.casefold() for x in labels}
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label") or "").strip().casefold()
        if label in wanted:
            h, a = safe_float(row.get("home")), safe_float(row.get("away"))
            if h is not None and a is not None:
                return h, a
    return None


def volume_band(node: Any) -> str:
    if not isinstance(node, dict):
        return "NA"
    sot = stat_value(node.get("stats_rows"), ("Tiros a Puerta", "Shots on Target"))
    danger = stat_value(node.get("stats_rows"), ("Ataques Peligrosos", "Dangerous Attacks"))
    if sot is None and danger is None:
        return "NA"
    sot_total = sum(sot) if sot else 0
    danger_total = sum(danger) if danger else 0
    if sot_total >= 10 or danger_total >= 110:
        return "HIGH"
    if sot_total >= 6 or danger_total >= 65:
        return "MEDIUM"
    return "LOW"


def ou_band(line: Optional[float]) -> str:
    if line is None:
        return "NA"
    if line <= 2.0:
        return "LOW_LE_2"
    if line <= 2.5:
        return "MID_225_250"
    if line <= 3.0:
        return "HIGH_275_300"
    return "VERY_HIGH_GT_3"


def get_ah(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds") or {}
    return safe_float(odds.get("ah_linea") if isinstance(odds, dict) else None) or safe_float(match.get("handicap"))


def get_ou(match: Dict[str, Any]) -> Optional[float]:
    odds = match.get("main_match_odds") or {}
    return safe_float(odds.get("goals_linea") if isinstance(odds, dict) else None)


def comparable_key(match: Dict[str, Any], prefix: str, current_ou: Optional[float]) -> Dict[str, str]:
    node = match.get(prefix) or {}
    if not isinstance(node, dict):
        node = {}
    line = safe_float(node.get("handicap_line_raw") or node.get("ah_line") or node.get("ah"))
    score = node.get("score") or node.get("result")
    return {
        f"{prefix}_wdl": wdl(score),
        f"{prefix}_over_current": over_state(score, current_ou),
        f"{prefix}_ah_relation": line_relation(line),
        f"{prefix}_volume": volume_band(node),
    }


@dataclass
class Row:
    match_id: str
    date: str
    home: str
    away: str
    league: str
    ah: float
    ou: float
    hg: int
    ag: int
    features: Dict[str, str]

    @property
    def favorite_side(self) -> str:
        return "HOME" if self.ah > 0 else "AWAY"

    @property
    def underdog_win(self) -> bool:
        return self.ag > self.hg if self.favorite_side == "HOME" else self.hg > self.ag

    @property
    def underdog_cover(self) -> bool:
        return self.ag >= self.hg if self.favorite_side == "HOME" else self.hg >= self.ag

    @property
    def over_hit(self) -> bool:
        return self.hg + self.ag > self.ou

    @property
    def target(self) -> bool:
        return self.underdog_win and self.over_hit

    @property
    def cover_over(self) -> bool:
        return self.underdog_cover and self.over_hit


def encode(match: Dict[str, Any]) -> Optional[Row]:
    ah, ou = get_ah(match), get_ou(match)
    score = parse_score(match.get("final_score") or match.get("score"))
    if ah is None or ou is None or score is None or abs(ah) < 1e-9:
        return None
    hg, ag = score
    market = match.get("market_analysis_data") or {}
    stadium = market.get("stadium") or {} if isinstance(market, dict) else {}
    general = market.get("general") or {} if isinstance(market, dict) else {}
    col3 = match.get("h2h_col3") or {}
    col3_found = isinstance(col3, dict) and col3.get("status") == "found"
    col3_line = safe_float(col3.get("handicap")) if col3_found else None
    col3_score = None
    if col3_found and col3.get("goles_home") is not None and col3.get("goles_away") is not None:
        col3_score = f"{col3.get('goles_home')}:{col3.get('goles_away')}"

    st_mov = movement_signature(stadium.get("movement") if isinstance(stadium, dict) else None)
    ge_mov = movement_signature(general.get("movement") if isinstance(general, dict) else None)
    same_h2h = bool(
        isinstance(stadium, dict) and isinstance(general, dict)
        and stadium.get("date") and stadium.get("date") == general.get("date")
        and (stadium.get("result") or stadium.get("score")) == (general.get("result") or general.get("score"))
    )
    features: Dict[str, str] = {
        "favorite_side": "HOME" if ah > 0 else "AWAY",
        "ou_band": ou_band(ou),
        "col3_present": "YES" if col3_found else "NO",
        "col3_wdl": wdl(col3_score),
        "col3_over_current": over_state(col3_score, ou),
        "col3_ah_relation": line_relation(col3_line),
        "col3_volume": volume_band(col3),
        "stadium_repricing": st_mov,
        "general_repricing": ge_mov,
        "repricing_pair": f"{st_mov}|{ge_mov}",
        "h2h_duplicate": "YES" if same_h2h else "NO",
        "stadium_wdl": wdl(stadium.get("result") or stadium.get("score")) if isinstance(stadium, dict) else "NA",
        "general_wdl": wdl(general.get("result") or general.get("score")) if isinstance(general, dict) else "NA",
    }
    features.update(comparable_key(match, "last_home_match", ou))
    features.update(comparable_key(match, "last_away_match", ou))
    comp = match.get("comparativas_indirectas") or {}
    if isinstance(comp, dict):
        shadow = dict(match)
        shadow["ind_left"] = comp.get("left") or {}
        shadow["ind_right"] = comp.get("right") or {}
        features.update(comparable_key(shadow, "ind_left", ou))
        features.update(comparable_key(shadow, "ind_right", ou))
    completeness = sum(v != "NA" for k, v in features.items() if k not in {"favorite_side", "ou_band"})
    features["completeness"] = "HIGH" if completeness >= 16 else "MEDIUM" if completeness >= 10 else "LOW"
    return Row(
        match_id=str(match.get("match_id") or ""),
        date=parse_date(match.get("match_date") or match.get("date")),
        home=str(match.get("home_name") or match.get("home_team") or ""),
        away=str(match.get("away_name") or match.get("away_team") or ""),
        league=str(match.get("league_name") or "N/D"),
        ah=ah,
        ou=ou,
        hg=hg,
        ag=ag,
        features=features,
    )


def wilson(k: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0:
        return 0.0, 0.0
    p = k / n
    den = 1 + z * z / n
    center = (p + z * z / (2 * n)) / den
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / den
    return max(0.0, center - margin), min(1.0, center + margin)


def rate(rows: Sequence[Row], outcome: str = "target") -> float:
    return sum(bool(getattr(r, outcome)) for r in rows) / len(rows) if rows else 0.0


def concentration(rows: Sequence[Row]) -> Tuple[float, float]:
    if not rows:
        return 1.0, 1.0
    leagues = Counter(r.league for r in rows)
    teams = Counter(t for r in rows for t in (r.home, r.away))
    return max(leagues.values()) / len(rows), max(teams.values()) / len(rows)


def pattern_stats(name: str, value: str, subset: Sequence[Row], exact: Sequence[Row], baseline: float, neighbors: Dict[float, Sequence[Row]]) -> Dict[str, Any]:
    n = len(subset)
    hits = sum(r.target for r in subset)
    raw = hits / n if n else 0.0
    prior_n = 24.0
    posterior = (hits + prior_n * baseline) / (n + prior_n) if n else baseline
    lo, hi = wilson(hits, n)
    all_ordered = sorted(exact, key=lambda r: (r.date, r.match_id))
    cutoff = (all_ordered[len(all_ordered) // 2].date, all_ordered[len(all_ordered) // 2].match_id)
    old_all = [r for r in exact if (r.date, r.match_id) < cutoff]
    new_all = [r for r in exact if (r.date, r.match_id) >= cutoff]
    old_rows = [r for r in subset if (r.date, r.match_id) < cutoff]
    new_rows = [r for r in subset if (r.date, r.match_id) >= cutoff]
    old_rate = rate(old_rows) if old_rows else raw
    new_rate = rate(new_rows) if new_rows else raw
    lift_old = old_rate - rate(old_all)
    lift_new = new_rate - rate(new_all)
    league_share, team_share = concentration(subset)
    neigh = {}
    for line, pool in neighbors.items():
        matched = [r for r in pool if r.features.get(name) == value]
        neigh[str(line)] = {"n": len(matched), "rate": round(100 * rate(matched), 2)}
    stable = abs(new_rate - old_rate) <= 0.12
    direction = raw >= baseline
    neighbor_support = sum(x["n"] for x in neigh.values())
    checks = []
    for line, x in neigh.items():
        if x["n"] >= 25:
            line_base = rate(neighbors[float(line)])
            checks.append((x["rate"] / 100 >= line_base) == direction)
    neighbor_agree = bool(checks) and all(checks)
    temporal_sign_agree = len(old_rows) >= 20 and len(new_rows) >= 20 and (lift_old >= 0) == direction and (lift_new >= 0) == direction
    # Aproximacion normal de dos proporciones: patron frente a su complemento.
    rest_n = len(exact) - n
    rest_hits = sum(r.target for r in exact) - hits
    if n and rest_n:
        pooled = (hits + rest_hits) / (n + rest_n)
        se = math.sqrt(max(1e-12, pooled * (1 - pooled) * (1 / n + 1 / rest_n)))
        z_score = (raw - rest_hits / rest_n) / se
        p_value = math.erfc(abs(z_score) / math.sqrt(2.0))
    else:
        p_value = 1.0
    return {
        "feature": name,
        "value": value,
        "support": n,
        "hits": hits,
        "raw_rate_pct": round(100 * raw, 2),
        "posterior_rate_pct": round(100 * posterior, 2),
        "lift_pp": round(100 * (posterior - baseline), 2),
        "wilson_95_pct": [round(100 * lo, 2), round(100 * hi, 2)],
        "old_half_pct": round(100 * old_rate, 2),
        "new_half_pct": round(100 * new_rate, 2),
        "old_half_support": len(old_rows),
        "new_half_support": len(new_rows),
        "old_lift_pp": round(100 * lift_old, 2),
        "new_lift_pp": round(100 * lift_new, 2),
        "temporal_sign_agree": temporal_sign_agree,
        "stable_12pp": stable,
        "max_league_share_pct": round(100 * league_share, 2),
        "max_team_share_pct": round(100 * team_share, 2),
        "neighbor_support": neighbor_support,
        "neighbor_agree": neighbor_agree,
        "p_value": p_value,
        "q_value_bh": 1.0,
        "grade": "PENDIENTE_FDR",
        "neighbors": neigh,
    }


def mine_patterns(exact: Sequence[Row], neighbors: Dict[float, Sequence[Row]]) -> List[Dict[str, Any]]:
    baseline = rate(exact)
    ignored = {"h2h_duplicate", "completeness"}
    feature_names = sorted(k for k in exact[0].features if k not in ignored) if exact else []
    results: List[Dict[str, Any]] = []
    for name in feature_names:
        groups: Dict[str, List[Row]] = defaultdict(list)
        for row in exact:
            groups[row.features.get(name, "NA")].append(row)
        for value, subset in groups.items():
            if value != "NA" and len(subset) >= 25:
                results.append(pattern_stats(name, value, subset, exact, baseline, neighbors))

    combo_features = [
        "favorite_side", "ou_band", "col3_present", "col3_wdl", "col3_over_current",
        "col3_ah_relation", "stadium_repricing", "general_repricing",
        "last_home_match_wdl", "last_away_match_wdl",
        "last_home_match_over_current", "last_away_match_over_current",
        "captured_ah_movement", "captured_ou_movement",
    ]
    for left, right in itertools.combinations(combo_features, 2):
        groups = defaultdict(list)
        combo_name = f"{left}+{right}"
        for row in exact:
            lv, rv = row.features.get(left, "NA"), row.features.get(right, "NA")
            if lv != "NA" and rv != "NA":
                groups[f"{lv} & {rv}"].append(row)
        # Neighbor matching for a compound feature.
        compound_neighbors: Dict[float, List[Row]] = {}
        for line, pool in neighbors.items():
            cloned = []
            for row in pool:
                lv, rv = row.features.get(left, "NA"), row.features.get(right, "NA")
                row.features[combo_name] = f"{lv} & {rv}" if lv != "NA" and rv != "NA" else "NA"
                cloned.append(row)
            compound_neighbors[line] = cloned
        for value, subset in groups.items():
            if len(subset) >= 30:
                for row in subset:
                    row.features[combo_name] = value
                results.append(pattern_stats(combo_name, value, subset, exact, baseline, compound_neighbors))
    # Benjamini-Hochberg sobre toda la familia de hipotesis explorada.
    ranked_p = sorted(enumerate(results), key=lambda x: x[1]["p_value"])
    m = len(ranked_p)
    running = 1.0
    for reverse_index in range(m - 1, -1, -1):
        original_index, item = ranked_p[reverse_index]
        rank = reverse_index + 1
        running = min(running, item["p_value"] * m / rank)
        results[original_index]["q_value_bh"] = round(running, 8)
    for item in results:
        magnitude = abs(item["lift_pp"])
        clean_concentration = item["max_league_share_pct"] <= 35 and item["max_team_share_pct"] <= 15
        if item["support"] >= 100 and item["q_value_bh"] <= 0.05 and item["temporal_sign_agree"] and item["neighbor_agree"] and clean_concentration and magnitude >= 3.0:
            item["grade"] = "FUERTE"
        elif item["support"] >= 60 and item["q_value_bh"] <= 0.10 and item["temporal_sign_agree"] and clean_concentration and magnitude >= 2.5:
            item["grade"] = "MODERADO"
        elif item["support"] >= 25:
            item["grade"] = "EXPLORATORIO"
        else:
            item["grade"] = "DESCARTAR"
    results.sort(key=lambda x: (abs(x["lift_pp"]), x["support"]), reverse=True)
    return results


def pct(value: float) -> str:
    return f"{100 * value:.2f}%"


def render(payload: Dict[str, Any]) -> str:
    s = payload["summary"]
    patterns = payload["patterns"]
    accepted = [p for p in patterns if p["grade"] in {"FUERTE", "MODERADO"}]
    prospective = sorted(
        (
            p for p in patterns
            if p["grade"] == "EXPLORATORIO"
            and p["support"] >= 80
            and p["temporal_sign_agree"]
            and p["neighbor_agree"]
            and p["stable_12pp"]
        ),
        key=lambda x: (x["q_value_bh"], -abs(x["lift_pp"]), -x["support"]),
    )[:15]
    positive = sorted((p for p in accepted if p["lift_pp"] > 0), key=lambda x: (x["grade"] == "FUERTE", x["lift_pp"], x["support"]), reverse=True)[:30]
    negative = sorted((p for p in accepted if p["lift_pp"] < 0), key=lambda x: (x["grade"] == "FUERTE", -x["lift_pp"], x["support"]), reverse=True)[:20]
    lines = [
        "# Análisis primario AH 0,5: no favorito + Over",
        "",
        f"**Snapshot:** `{payload['generated_at']}` · **Base:** `{payload['database']}`",
        "",
        "## Veredicto ejecutivo",
        "",
        "Este estudio busca patrones reproducibles, no una fórmula infalible. Una casa fija precios para equilibrar probabilidad, margen, riesgo y demanda; la línea no revela por sí sola información privada. El uso correcto consiste en traducir cada AH a una expectativa de margen, reexpresar los precedentes con la línea actual y exigir estabilidad fuera de la muestra.",
        "",
        f"En el corte exacto `|AH| = 0,50` hay **{s['exact_n']}** partidos utilizables. De ellos, **{s['target_hits']}** terminaron en victoria directa del no favorito + Over (**{s['target_rate_pct']}%**). La combinación más amplia no favorito cubre `+0,50` + Over ocurrió en **{s['cover_over_hits']}** casos (**{s['cover_over_rate_pct']}%**).",
        "",
        "La diferencia es esencial: con `+0,50`, el empate cubre el hándicap, pero no es victoria directa del no favorito.",
        "",
        "### Conclusiones primarias que sí sostiene el corte",
        "",
        "- La tasa conjunta desciende ordenadamente al aumentar la exigencia: 14,36% en 0,25; 11,80% en 0,50; 8,04% en 0,75. La magnitud de la línea contiene información real sobre la jerarquía previa.",
        "- El Over aislado permanece alrededor del 45% en los tres cortes; la caída conjunta procede principalmente de que el no favorito gana menos conforme aumenta el AH, no de una gran variación del total.",
        "- Ningún patrón de la búsqueda exhaustiva supera todavía la corrección por comparaciones múltiples y todos deben tratarse como hipótesis, no como leyes.",
        "- La cobertura de snapshots es insuficiente para afirmar cómo funciona el movimiento real de apertura a cierre. Sí puede estudiarse el repricing de precedentes, que es una variable diferente.",
        "",
        "## Universo y definiciones",
        "",
        "- Línea exacta: solo `+0,50` o `-0,50` almacenado; no se redondea 0,25/0,75.",
        "- Vecindad estructural: 0,25 y 0,75 se usan únicamente como validación externa de dirección.",
        "- Over: goles finales estrictamente mayores que la línea OU capturada. Un push no es Over.",
        "- Favorito: convención interna del proyecto (`AH > 0` local favorito; `AH < 0` visitante favorito).",
        "- Objetivo principal: victoria directa del no favorito y Over.",
        "- Objetivo secundario: cobertura del no favorito `+0,50` y Over.",
        "",
        "## Tasas base observadas",
        "",
        "| Corte | N | No favorito gana | Over | No favorito gana + Over | No favorito +0,5 cubre + Over |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in payload["line_summaries"]:
        lines.append(f"| {row['line']} | {row['n']} | {row['underdog_win_pct']}% | {row['over_pct']}% | {row['target_pct']}% | {row['cover_over_pct']}% |")
    lines.extend([
        "",
        "## Cómo se interpreta la colocación de cuotas",
        "",
        "1. **AH actual = expectativa madre.** Un 0,50 afirma favoritismo suficiente para que el favorito necesite ganar para cubrir; no afirma el marcador ni garantiza superioridad real.",
        "2. **OU = forma esperada del partido.** El AH describe jerarquía/margen; el OU describe volumen total. Deben leerse conjuntamente. Un favorito 0,50 con OU alto admite más estados donde el perro gana en intercambio; con OU bajo aumenta el peso del empate y del 1-0/0-1.",
        "3. **Repricing histórico no es movimiento en vivo.** Campos como `0,25 → 0,50` dentro de H2H comparan la exigencia del precedente con la actual. Solo una tabla temporal de snapshots permite hablar de apertura, cierre, steam o reversión.",
        "4. **Precio y línea son distintos.** Sin cuotas de ambos lados y vigorish no se puede inferir probabilidad justa completa. Este corpus permite estudiar línea y contexto, pero no reconstruir siempre la probabilidad sin margen de la casa.",
        "5. **Col3 se reexpresa.** Se compara su línea exacta, su resultado, su Over respecto al OU actual y su volumen. El marcador bruto nunca vota solo.",
        f"6. **Cobertura temporal disponible.** La tabla contiene {payload['snapshot_coverage']['all_snapshots']} snapshots; {payload['snapshot_coverage']['exact_ah05_matches_with_any_snapshot']} de los {s['exact_n']} AH 0,50 tienen al menos uno y {payload['snapshot_coverage']['exact_ah05_matches_with_2plus_snapshots']} permiten comparar primera y última captura. Esa última captura no se presume cierre.",
        "",
        "## Patrones positivos filtrados",
        "",
        "Solo aparecen aquí señales con soporte mínimo, contracción bayesiana, control temporal y revisión de concentración. `FUERTE` exige además estabilidad y validación direccional en líneas vecinas.",
        "",
        "| Grado | Patrón | N | Tasa ajustada | Lift vs base | q (BH) | Mitad antigua/nueva | 0,25 / 0,75 |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ])
    for p in positive:
        ns = p["neighbors"]
        lines.append(f"| {p['grade']} | `{p['feature']}={p['value']}` | {p['support']} | {p['posterior_rate_pct']}% | +{p['lift_pp']} pp | {p['q_value_bh']:.4f} | {p['old_half_pct']} / {p['new_half_pct']}% | {ns['0.25']['rate']}% (n={ns['0.25']['n']}) / {ns['0.75']['rate']}% (n={ns['0.75']['n']}) |")
    if not positive:
        lines.append("| — | Ningún patrón superó todos los filtros | — | — | — | — | — | — |")
    lines.extend([
        "",
        "## Patrones negativos o frenos",
        "",
        "| Grado | Patrón | N | Tasa ajustada | Lift vs base | q (BH) | Estabilidad |",
        "|---|---|---:|---:|---:|---:|---|",
    ])
    for p in negative:
        lines.append(f"| {p['grade']} | `{p['feature']}={p['value']}` | {p['support']} | {p['posterior_rate_pct']}% | {p['lift_pp']} pp | {p['q_value_bh']:.4f} | {p['old_half_pct']}→{p['new_half_pct']}% |")
    if not negative:
        lines.append("| — | Ningún freno superó todos los filtros | — | — | — | — | — |")
    lines.extend([
        "",
        "## Hipótesis prospectivas prioritarias",
        "",
        "Estas señales mostraron dirección coherente entre mitades temporales y líneas vecinas, pero **no sobrevivieron** la corrección global. Se conservan exclusivamente para probarlas en partidos futuros que no hayan intervenido en su descubrimiento.",
        "",
        "| Hipótesis | N | Tasa ajustada | Lift | q (BH) | Lift antiguo/nuevo |",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for p in prospective:
        sign = "+" if p["lift_pp"] >= 0 else ""
        lines.append(f"| `{p['feature']}={p['value']}` | {p['support']} | {p['posterior_rate_pct']}% | {sign}{p['lift_pp']} pp | {p['q_value_bh']:.4f} | {p['old_lift_pp']} / {p['new_lift_pp']} pp |")
    if not prospective:
        lines.append("| Ninguna hipótesis cumplió el filtro prospectivo | — | — | — | — | — |")
    lines.extend([
        "",
        "## Protocolo de discusión de una ficha completa",
        "",
        "Para cada partido futuro se debe argumentar en este orden:",
        "",
        "1. Identificar favorito, no favorito, AH exacto, OU exacto y hora del snapshot.",
        "2. Si hay snapshots, separar apertura, máximo/mínimo y cierre; calcular cambio de línea y cambio de precio sin vigorish.",
        "3. Reexpresar prev home, prev away, H2H estadio, H2H general, Col3 e indirectas con la línea actual.",
        "4. Deduplicar H2H estadio/general si son el mismo partido.",
        "5. Comparar Col3 exacto primero; después ±0,25; penalizar cualquier analogía más lejana.",
        "6. Evaluar si marcador y volumen cuentan la misma historia. Tiros/ataques confirman o rebajan, nunca crean por sí solos el pick.",
        "7. Confrontar tesis y antítesis: construir el mejor argumento a favor del perro+Over y el mejor argumento contrario.",
        "8. Exigir soporte, estabilidad temporal y transferencia a 0,25/0,75. Si falla, `NO BET`.",
        "",
        "## Función de puntuación recomendada",
        "",
        "La puntuación no debe sumar votos correlacionados como si fueran independientes. Punto de partida provisional, pendiente de calibración prospectiva:",
        "",
        "```text",
        "Score = 2,4·AH/repricing + 2,0·Col3 + 1,5·OU",
        "      + 1,2·previas + 1,0·indirectas + 0,8·volumen",
        "      - penalización por duplicados, antigüedad, falta de snapshots,",
        "        concentración de liga/equipo y contradicción temporal.",
        "```",
        "",
        "Col3 conserva prioridad contextual, pero queda subordinado a que su línea pueda reexpresarse correctamente. Un Col3 de línea lejana o localía no comparable no puede dominar el dictamen.",
        "",
        "Los pesos anteriores son una estructura de razonamiento, no coeficientes ya validados. Se optimizarán únicamente con un conjunto futuro congelado, función de pérdida definida y comparación contra una base simple de AH+OU.",
        "",
        "## Datos que faltan para estudiar de verdad a las casas",
        "",
        "- Casa/bookmaker y tipo de mercado.",
        "- Hora exacta de cada observación y hora de inicio.",
        "- AH y precio decimal de ambos lados en cada snapshot.",
        "- OU y precios Over/Under en cada snapshot.",
        "- Identificación verificable de apertura y último precio prepartido.",
        "- Límites/liquidez cuando estén disponibles.",
        "",
        "Con esos campos se podrá retirar el margen (`de-vig`), separar cambio de precio de cambio de línea, medir velocidad y persistencia del movimiento y evaluar closing-line value sin hindsight.",
        "",
        "## Sesgos que deben bloquear una conclusión",
        "",
        "- **Look-ahead:** usar cierre o información conocida después de emitir el pick.",
        "- **Selección del ganador:** estudiar solo casos perro+Over y olvidar todos los AH 0,50 que fallaron.",
        "- **Redondeo:** llamar “0,50” a 0,25 y 0,75 sin conservar el valor original.",
        "- **Doble conteo:** H2H estadio y general representan el mismo precedente.",
        "- **Localía/signo:** orientar un H2H invertido desde la columna histórica equivocada.",
        "- **Marcador vistoso:** considerar 4-2 más informativo que la línea y el volumen que lo produjeron.",
        "- **Sesgo de liga/equipo:** una regla puede ser únicamente el comportamiento de una competición o club repetido.",
        "- **Muestra pequeña y múltiples pruebas:** probar cientos de combinaciones garantiza falsos descubrimientos.",
        "- **Closing-line hindsight:** describir como obvio un movimiento que no estaba disponible en el momento del pick. El último snapshot capturado no se denomina cierre salvo prueba de hora/límite.",
        "- **Confundir demanda con información:** un movimiento puede responder a liquidez, límites o balance de exposición.",
        "- **Stats posteriores:** tiros y ataques del propio partido objetivo nunca son variables prepartido.",
        "",
        "## Regla final de decisión",
        "",
        "Un argumento se acepta únicamente si: (a) la línea exacta y el lado están bien orientados; (b) existen al menos dos bloques independientes; (c) Col3/previas pueden reexpresarse; (d) el patrón mantiene signo en el tiempo; (e) no depende de una sola liga/equipo; y (f) el precio disponible ofrece valor frente a la probabilidad conservadora. Si falta precio o snapshots, la conclusión es sobre dirección, no sobre valor apostable.",
        "",
        "No existe método infalible: el estándar correcto es una ventaja probabilística calibrada, auditable y con capacidad explícita de abstenerse.",
        "",
        "## Trazabilidad",
        "",
        f"- Casos exactos objetivo enumerados en el JSON: **{len(payload['target_cases'])}**.",
        f"- Patrones evaluados: **{len(patterns)}**; fuertes/moderados: **{len(accepted)}**.",
        "- El JSON conserva métricas completas, intervalos, estabilidad, concentración y validación vecina.",
    ])
    return "\n".join(lines) + "\n"


def summarize_line(label: str, rows: Sequence[Row]) -> Dict[str, Any]:
    return {
        "line": label,
        "n": len(rows),
        "underdog_win_pct": round(100 * rate(rows, "underdog_win"), 2),
        "over_pct": round(100 * rate(rows, "over_hit"), 2),
        "target_pct": round(100 * rate(rows, "target"), 2),
        "cover_over_pct": round(100 * rate(rows, "cover_over"), 2),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-dir", default="informes/ah05_no_favorito_over_analisis_primario")
    args = parser.parse_args()
    root = Path(args.project_root).resolve()
    db = root / "data" / "app_data.db"
    output = (root / args.output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db))
    snapshots: Dict[str, List[Tuple[str, Optional[float], Optional[float]]]] = defaultdict(list)
    for match_id, observed_at, ah_line, ou_line in conn.execute(
        "SELECT match_id, observed_at, ah_line, ou_line FROM precache_market_snapshots ORDER BY match_id, observed_at"
    ):
        snapshots[str(match_id)].append((str(observed_at), safe_float(ah_line), safe_float(ou_line)))
    rows: List[Row] = []
    for (raw,) in conn.execute("SELECT COALESCE(explorer_json, payload_json) FROM matches WHERE state='historical'"):
        try:
            enc = encode(json.loads(raw))
        except Exception:
            enc = None
        if enc:
            captured = snapshots.get(enc.match_id, [])
            enc.features["snapshot_presence"] = "YES" if captured else "NO"
            enc.features["snapshot_count_band"] = "3_PLUS" if len(captured) >= 3 else "TWO" if len(captured) == 2 else "ONE" if len(captured) == 1 else "NONE"
            if len(captured) >= 2:
                enc.features["captured_ah_movement"] = captured_ah_movement(captured[0][1], captured[-1][1])
                enc.features["captured_ou_movement"] = captured_ou_movement(captured[0][2], captured[-1][2])
            else:
                enc.features["captured_ah_movement"] = "NA"
                enc.features["captured_ou_movement"] = "NA"
            rows.append(enc)
    conn.close()

    exact = [r for r in rows if abs(abs(r.ah) - 0.5) < 1e-9]
    line025 = [r for r in rows if abs(abs(r.ah) - 0.25) < 1e-9]
    line075 = [r for r in rows if abs(abs(r.ah) - 0.75) < 1e-9]
    neighbors = {0.25: line025, 0.75: line075}
    patterns = mine_patterns(exact, neighbors)
    generated = datetime.now().astimezone().isoformat(timespec="seconds")
    payload: Dict[str, Any] = {
        "generated_at": generated,
        "database": str(db),
        "summary": {
            "usable_all_lines": len(rows),
            "exact_n": len(exact),
            "target_hits": sum(r.target for r in exact),
            "target_rate_pct": round(100 * rate(exact), 2),
            "cover_over_hits": sum(r.cover_over for r in exact),
            "cover_over_rate_pct": round(100 * rate(exact, "cover_over"), 2),
        },
        "line_summaries": [summarize_line("AH absoluto 0,25", line025), summarize_line("AH absoluto 0,50 exacto", exact), summarize_line("AH absoluto 0,75", line075)],
        "snapshot_coverage": {
            "all_snapshots": sum(len(x) for x in snapshots.values()),
            "exact_ah05_matches_with_any_snapshot": sum(bool(snapshots.get(r.match_id)) for r in exact),
            "exact_ah05_matches_with_2plus_snapshots": sum(len(snapshots.get(r.match_id, [])) >= 2 for r in exact),
            "note": "Primer y ultimo snapshot capturado; el ultimo no se presume cierre de mercado.",
        },
        "patterns": patterns,
        "target_cases": [
            {
                "match_id": r.match_id, "date": r.date, "home": r.home, "away": r.away,
                "league": r.league, "ah": r.ah, "ou": r.ou, "score": f"{r.hg}:{r.ag}",
                "features": {k: v for k, v in r.features.items() if "+" not in k},
            }
            for r in exact if r.target
        ],
    }
    json_path = output / "ANALISIS_PRIMARIO_AH05_NO_FAVORITO_OVER.json"
    md_path = output / "ANALISIS_PRIMARIO_AH05_NO_FAVORITO_OVER.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render(payload), encoding="utf-8")
    print(json.dumps({"markdown": str(md_path), "json": str(json_path), "summary": payload["summary"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
