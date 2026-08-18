#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from analyze_bookie_positioning import analyze_positioning  # noqa: E402


PROFILE = "binary_h2h_dichotomic_v1"


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


def _fmt(value: Any) -> str:
    num = _safe_float(value)
    if num is None:
        return "N/A"
    rounded = round(num, 2)
    if abs(rounded - int(rounded)) < 1e-9:
        return str(int(rounded))
    return f"{rounded:.2f}".rstrip("0").rstrip(".")


def _step(
    steps: List[Dict[str, Any]],
    *,
    code: str,
    question: str,
    answer: str,
    effect: str,
    side_delta: float = 0.0,
    goals_delta: float = 0.0,
    note: str = "",
) -> Tuple[float, float]:
    steps.append(
        {
            "code": code,
            "question": question,
            "answer": answer,
            "effect": effect,
            "side_delta": round(side_delta, 2),
            "goals_delta": round(goals_delta, 2),
            "note": note,
        }
    )
    return side_delta, goals_delta


def _label_ids(payload: Dict[str, Any]) -> set[str]:
    return {str(item.get("id")) for item in payload.get("labels", []) if isinstance(item, dict)}


def _base_case(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    narrative = payload.get("narrative") if isinstance(payload.get("narrative"), dict) else {}
    base_map = narrative.get("base_map") if isinstance(narrative.get("base_map"), dict) else {}
    case = base_map.get("case")
    return case if isinstance(case, dict) else None


def _recent_favorite(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    frame = payload.get("market_frame") if isinstance(payload.get("market_frame"), dict) else {}
    recent = payload.get("recent") if isinstance(payload.get("recent"), dict) else {}
    if frame.get("fav_side") == "HOME":
        item = recent.get("prev_home")
    elif frame.get("fav_side") == "AWAY":
        item = recent.get("prev_away")
    else:
        item = None
    return item if isinstance(item, dict) else None


def _indirect_favorite(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    frame = payload.get("market_frame") if isinstance(payload.get("market_frame"), dict) else {}
    indirect = payload.get("indirect") if isinstance(payload.get("indirect"), dict) else {}
    if frame.get("fav_side") == "HOME":
        item = indirect.get("left")
    elif frame.get("fav_side") == "AWAY":
        item = indirect.get("right")
    else:
        item = None
    return item if isinstance(item, dict) else None


def _confidence(abs_side: float, abs_goals: float, tension: float, has_h2h: bool) -> Dict[str, Any]:
    raw = max(abs_side, abs_goals) - 0.35 * tension
    if not has_h2h:
        raw -= 0.75
    if raw >= 3.4:
        label = "ALTA"
    elif raw >= 2.0:
        label = "MEDIA"
    else:
        label = "BAJA"
    return {"label": label, "score": round(max(0.0, raw), 2)}


def derive_binary_reading(payload: Dict[str, Any]) -> Dict[str, Any]:
    frame = payload.get("market_frame") if isinstance(payload.get("market_frame"), dict) else {}
    narrative = payload.get("narrative") if isinstance(payload.get("narrative"), dict) else {}
    labels = _label_ids(payload)
    base = _base_case(payload)
    steps: List[Dict[str, Any]] = []
    side_score = 0.0
    goals_score = 0.0
    tension = 0.0
    route = "SIN_H2H_BASE"
    tags = sorted(labels)

    has_h2h = base is not None
    ds, dg = _step(
        steps,
        code="01_H2H_BASE",
        question="Existe ultimo H2H util como mapa base?",
        answer="SI" if has_h2h else "NO",
        effect="H2H primero" if has_h2h else "no se puede abrir pronostico fuerte",
        side_delta=0.6 if has_h2h else -0.6,
        note=(f"{base.get('home')} {base.get('score')} {base.get('away')}" if has_h2h else "sin precedente directo valido"),
    )
    side_score += ds
    goals_score += dg

    if has_h2h:
        cover = str(base.get("cover_current_line") or "UNKNOWN")
        if cover == "COVER":
            delta = 2.0
            route = "FAVORITO_VALIDADO_POR_H2H"
        elif cover == "PUSH_ZONE":
            delta = 0.75
            route = "FAVORITO_EN_ZONA_PUSH_H2H"
        else:
            delta = -1.25
            route = "FAVORITO_CASTIGADO_POR_H2H"
            tension += 0.6
        ds, dg = _step(
            steps,
            code="02_H2H_CUBRE_LINEA",
            question="El favorito actual cubria la linea de hoy en el H2H?",
            answer="SI" if cover == "COVER" else "NO" if cover == "FAIL" else cover,
            effect="memoria pro favorito" if delta > 0 else "memoria contra favorito",
            side_delta=delta,
            note=f"residual={_fmt(base.get('residual_current_line'))}",
        )
        side_score += ds
        goals_score += dg

        stats = base.get("stats_edge_for_current_fav") if isinstance(base.get("stats_edge_for_current_fav"), dict) else {}
        stats_score = float(stats.get("score") or 0.0)
        if stats_score >= 0.30:
            delta = 1.75
            if route == "FAVORITO_CASTIGADO_POR_H2H":
                route = "FAVORITO_POR_CORRECCION_CONTRAINTUITIVA"
            answer = "SI"
            effect = "volumen rescata marcador"
        elif stats_score <= -0.30:
            delta = -1.75
            answer = "NO"
            effect = "volumen confirma veto"
        else:
            delta = 0.0
            answer = "NEUTRO"
            effect = "volumen no decide"
        ds, dg = _step(
            steps,
            code="03_H2H_VOLUMEN",
            question="El volumen del H2H favorece al favorito actual?",
            answer=answer,
            effect=effect,
            side_delta=delta,
            note=(
                f"veredicto={stats.get('verdict', 'N/A')} "
                f"SOT={_fmt(stats.get('sot_diff'))} DA={_fmt(stats.get('danger_diff'))}"
            ),
        )
        side_score += ds
        goals_score += dg

        pressure_label = str(base.get("pressure_change_label") or "UNKNOWN")
        if pressure_label == "NEW_FAVORITE_STATUS":
            delta = 1.1 if stats_score >= 0.30 else -0.4
            effect = "la casa crea favorito nuevo con soporte" if delta > 0 else "favorito nuevo sin soporte completo"
        elif pressure_label == "RAISE_PRESSURE_KEEP_FAVORITE":
            delta = 1.0 if stats_score >= 0.30 else -0.8
            effect = "sube presion con soporte" if delta > 0 else "sube presion sin soporte"
        elif pressure_label == "LOWER_PRESSURE_KEEP_FAVORITE":
            delta = 0.75 if stats_score >= 0.12 else 0.15
            effect = "baja precio pero mantiene favorito"
        elif pressure_label == "FAVORITE_STATUS_REMOVED":
            delta = -1.1
            effect = "la casa retira favorito historico"
        else:
            delta = 0.0
            effect = "presion no concluyente"
        ds, dg = _step(
            steps,
            code="04_MOVIMIENTO",
            question="Que hace la casa con la presion del favorito desde el H2H?",
            answer=pressure_label,
            effect=effect,
            side_delta=delta,
            note=f"delta presion={_fmt(base.get('favorite_pressure_delta'))}",
        )
        side_score += ds
        goals_score += dg

        ou_line = _safe_float(frame.get("ou_line"))
        total = _safe_float(base.get("total_goals"))
        if ou_line is not None and total is not None:
            if total < ou_line:
                gdelta = -1.05
                answer = "UNDER"
                effect = "memoria fria"
            elif total > ou_line:
                gdelta = 0.85
                answer = "OVER"
                effect = "memoria abierta"
            else:
                gdelta = -0.45
                answer = "PUSH"
                effect = "techo justo"
            ds, dg = _step(
                steps,
                code="05_TOTAL_H2H",
                question="El total del H2H rompe el O/U actual?",
                answer=answer,
                effect=effect,
                goals_delta=gdelta,
                note=f"H2H goles={_fmt(total)} vs OU={_fmt(ou_line)}",
            )
            side_score += ds
            goals_score += dg

    if frame.get("table_positioning") == "LINE_AGAINST_TABLE":
        delta = 0.65 if "VOLUMEN_PERDONA_RESULTADO" in labels else -0.35
        effect = "linea especial con soporte" if delta > 0 else "linea contra tabla sin soporte suficiente"
    elif frame.get("table_positioning") == "TABLE_ALIGNED":
        delta = 0.45
        effect = "tabla acompana favorito"
    else:
        delta = 0.0
        effect = "tabla neutral"
    ds, dg = _step(
        steps,
        code="06_TABLA",
        question="La linea contradice la clasificacion?",
        answer=str(frame.get("table_positioning") or "UNKNOWN"),
        effect=effect,
        side_delta=delta,
        note=f"fav_rank={frame.get('favorite_rank')} dog_rank={frame.get('non_favorite_rank')}",
    )
    side_score += ds
    goals_score += dg

    recent_fav = _recent_favorite(payload)
    if recent_fav:
        recent_cover = str(recent_fav.get("cover_label") or "UNKNOWN")
        recent_stats = recent_fav.get("stats_edge") if isinstance(recent_fav.get("stats_edge"), dict) else {}
        recent_stats_score = float(recent_stats.get("score") or 0.0)
        delta = 0.55 if recent_cover == "COVER" else -0.45 if recent_cover == "FAIL" else 0.0
        if recent_stats_score >= 0.30:
            delta += 0.45
        elif recent_stats_score <= -0.30:
            delta -= 0.65
            tension += 0.45
        ds, dg = _step(
            steps,
            code="07_PREVIA_FAVORITO",
            question="La previa del favorito cubre y sostiene proceso?",
            answer=recent_cover,
            effect="forma suma" if delta > 0 else "forma resta" if delta < 0 else "forma neutral",
            side_delta=delta,
            note=f"stats={recent_stats.get('verdict', 'N/A')} residual={_fmt(recent_fav.get('residual'))}",
        )
        side_score += ds
        goals_score += dg

    fav_ind = _indirect_favorite(payload)
    if fav_ind:
        ind_stats = fav_ind.get("stats_edge") if isinstance(fav_ind.get("stats_edge"), dict) else {}
        ind_score = float(ind_stats.get("score") or 0.0)
        if ind_score >= 0.30:
            delta = 0.70
            answer = "SI"
            effect = "indirecta confirma"
        elif ind_score <= -0.30 or "INDIRECTA_DEBILITA_FAVORITO" in labels:
            delta = -0.85
            answer = "NO"
            effect = "indirecta contradice"
            tension += 0.5
        else:
            delta = 0.0
            answer = "NEUTRO"
            effect = "indirecta no decide"
        ds, dg = _step(
            steps,
            code="08_INDIRECTA",
            question="La indirecta del favorito confirma la colocacion?",
            answer=answer,
            effect=effect,
            side_delta=delta,
            note=f"stats={ind_stats.get('verdict', 'N/A')} residual={_fmt(fav_ind.get('residual'))}",
        )
        side_score += ds
        goals_score += dg

    ou_line = _safe_float(frame.get("ou_line"))
    if ou_line is not None:
        if ou_line <= 2.25:
            gdelta = -1.45
            answer = "SI"
            effect = "techo bajo, margen corto"
        elif ou_line >= 3.0:
            gdelta = 0.70
            answer = "NO"
            effect = "techo alto permite ruptura"
        else:
            gdelta = 0.0
            answer = "MEDIO"
            effect = "total intermedio"
        if "GOLEADA_NO_PERSEGUIDA" in labels:
            gdelta -= 0.95
            tension += 0.35
        if "MEMORIA_H2H_UNDER" in labels:
            gdelta -= 0.70
        ds, dg = _step(
            steps,
            code="09_OU_CAPADO",
            question="El O/U capado obliga a guion de control?",
            answer=answer,
            effect=effect,
            goals_delta=gdelta,
            note=f"OU={_fmt(ou_line)}",
        )
        side_score += ds
        goals_score += dg

    if "RESULTADO_MEJOR_QUE_PROCESO" in labels:
        side_score -= 0.55
        tension += 0.4
    if "H2H_PARTIDO_DIVIDIDO" in labels:
        tension += 0.55

    if not has_h2h:
        side_signal = "SIN_H2H"
        route = "SIN_H2H_BASE"
    elif side_score >= 2.25:
        side_signal = "FAVORITO"
    elif side_score <= -1.65:
        side_signal = "NO_FAVORITO"
    else:
        side_signal = "NO_DECISION"

    if goals_score <= -1.65:
        goals_signal = "CONTROL_BAJO"
    elif goals_score >= 1.55:
        goals_signal = "RUPTURA_ALTA"
    else:
        goals_signal = "NEUTRO"

    conf = _confidence(abs(side_score), abs(goals_score), tension, has_h2h)
    return {
        "profile": PROFILE,
        "route": route,
        "side_signal": side_signal,
        "goals_signal": goals_signal,
        "confidence": conf["label"],
        "confidence_score": conf["score"],
        "side_score": round(side_score, 2),
        "goals_score": round(goals_score, 2),
        "tension": round(tension, 2),
        "tags": tags,
        "steps": steps,
    }


def _compact_match(payload: Dict[str, Any], binary: Dict[str, Any]) -> Dict[str, Any]:
    frame = payload.get("market_frame") if isinstance(payload.get("market_frame"), dict) else {}
    match = payload.get("match") if isinstance(payload.get("match"), dict) else {}
    narrative = payload.get("narrative") if isinstance(payload.get("narrative"), dict) else {}
    base = _base_case(payload)
    return {
        "match_id": match.get("match_id"),
        "home": match.get("home"),
        "away": match.get("away"),
        "league": match.get("league"),
        "date": match.get("date"),
        "time": match.get("time"),
        "ah": _fmt(frame.get("home_line")),
        "ou": _fmt(frame.get("ou_line")),
        "favorite": frame.get("favorite"),
        "non_favorite": frame.get("non_favorite"),
        "fav_side": frame.get("fav_side"),
        "table_positioning": frame.get("table_positioning"),
        "base": {
            "home": base.get("home") if base else None,
            "away": base.get("away") if base else None,
            "score": base.get("score") if base else None,
            "date": base.get("date") if base else None,
            "pressure_then": _fmt(base.get("current_fav_pressure_then")) if base else "N/A",
            "pressure_now": _fmt(base.get("current_fav_pressure_now")) if base else "N/A",
            "pressure_label": base.get("pressure_change_label") if base else "NO_H2H",
            "residual": _fmt(base.get("residual_current_line")) if base else "N/A",
            "cover": base.get("cover_current_line") if base else "NO_H2H",
            "stats": base.get("stats_edge_for_current_fav") if base else {},
            "total_goals": base.get("total_goals") if base else None,
        },
        "labels": payload.get("labels", []),
        "why_yes": narrative.get("why_bookie_can_place_it", []),
        "why_no": narrative.get("why_it_is_counterintuitive", []),
        "ou_notes": narrative.get("ou_positioning", []),
        "binary": binary,
    }


def _summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    def count_where(fn) -> int:
        return sum(1 for row in rows if fn(row))

    tags: Dict[str, int] = {}
    for row in rows:
        for tag in row["binary"].get("tags", []):
            tags[tag] = tags.get(tag, 0) + 1
    top_tags = sorted(tags.items(), key=lambda item: item[1], reverse=True)[:18]
    return {
        "total": len(rows),
        "with_h2h": count_where(lambda r: r["binary"]["side_signal"] != "SIN_H2H"),
        "favorite": count_where(lambda r: r["binary"]["side_signal"] == "FAVORITO"),
        "non_favorite": count_where(lambda r: r["binary"]["side_signal"] == "NO_FAVORITO"),
        "no_decision": count_where(lambda r: r["binary"]["side_signal"] in {"NO_DECISION", "SIN_H2H"}),
        "control_bajo": count_where(lambda r: r["binary"]["goals_signal"] == "CONTROL_BAJO"),
        "ruptura_alta": count_where(lambda r: r["binary"]["goals_signal"] == "RUPTURA_ALTA"),
        "high_conf": count_where(lambda r: r["binary"]["confidence"] == "ALTA"),
        "top_tags": top_tags,
    }


def _html(payload: Dict[str, Any]) -> str:
    data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Clave dicotomica H2H binaria</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --panel: #ffffff;
      --ink: #172033;
      --muted: #667085;
      --line: #d9e0ec;
      --blue: #2563eb;
      --green: #15803d;
      --red: #b42318;
      --amber: #b45309;
      --violet: #6d28d9;
      --slate: #334155;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, Segoe UI, Arial, sans-serif;
      background: var(--bg);
      color: var(--ink);
      letter-spacing: 0;
    }}
    .app-header {{
      position: sticky;
      top: 0;
      z-index: 10;
      background: rgba(245, 247, 251, 0.96);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(10px);
    }}
    .wrap {{ max-width: 1480px; margin: 0 auto; padding: 18px 22px; }}
    h1 {{ margin: 0 0 4px; font-size: 24px; line-height: 1.15; }}
    .subtitle {{ margin: 0; color: var(--muted); font-size: 13px; }}
    .grid {{
      display: grid;
      grid-template-columns: 360px minmax(0, 1fr);
      gap: 16px;
      align-items: start;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06);
    }}
    .panel-pad {{ padding: 14px; }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }}
    .stat {{
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
    }}
    .stat b {{ display: block; font-size: 22px; }}
    .stat span {{ color: var(--muted); font-size: 12px; }}
    .controls {{
      display: grid;
      grid-template-columns: 1.7fr repeat(4, minmax(130px, 1fr));
      gap: 10px;
      margin-top: 14px;
    }}
    input, select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px 11px;
      background: #fff;
      color: var(--ink);
      font-size: 13px;
    }}
    .key-title {{ font-size: 15px; margin: 0 0 10px; }}
    .tree {{ display: grid; gap: 8px; }}
    .node {{
      border: 1px solid var(--line);
      border-left: 4px solid var(--blue);
      border-radius: 8px;
      padding: 10px;
      background: #fbfdff;
    }}
    .node strong {{ display: block; font-size: 13px; margin-bottom: 4px; }}
    .node p {{ margin: 0; color: var(--muted); font-size: 12px; line-height: 1.35; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; }}
    .badge {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 24px;
      padding: 4px 8px;
      border-radius: 999px;
      border: 1px solid transparent;
      font-size: 11px;
      font-weight: 700;
      white-space: nowrap;
    }}
    .b-fav {{ color: #0f3b8f; background: #dbeafe; border-color: #bfdbfe; }}
    .b-dog {{ color: #7f1d1d; background: #fee2e2; border-color: #fecaca; }}
    .b-low {{ color: #14532d; background: #dcfce7; border-color: #bbf7d0; }}
    .b-high {{ color: #7c2d12; background: #ffedd5; border-color: #fed7aa; }}
    .b-neutral {{ color: #334155; background: #e2e8f0; border-color: #cbd5e1; }}
    .b-conf {{ color: #3b0764; background: #ede9fe; border-color: #ddd6fe; }}
    .cards {{ display: grid; gap: 12px; margin-top: 14px; }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }}
    .card-head {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      padding: 13px 14px;
      border-bottom: 1px solid var(--line);
      background: #fbfcff;
    }}
    .teams {{ font-size: 16px; font-weight: 800; }}
    .meta {{ margin-top: 4px; color: var(--muted); font-size: 12px; display: flex; flex-wrap: wrap; gap: 8px; }}
    .signals {{ display: flex; flex-wrap: wrap; gap: 6px; justify-content: flex-end; align-content: start; }}
    .card-body {{ padding: 14px; }}
    .split {{
      display: grid;
      grid-template-columns: 1.1fr 0.9fr;
      gap: 12px;
    }}
    .base-box {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #fff;
    }}
    .base-box h3, .path h3 {{ margin: 0 0 8px; font-size: 13px; }}
    .kv {{
      display: grid;
      grid-template-columns: 140px minmax(0, 1fr);
      gap: 6px;
      font-size: 12px;
      line-height: 1.35;
    }}
    .kv span:nth-child(odd) {{ color: var(--muted); }}
    .steps {{ display: grid; gap: 7px; }}
    .step {{
      display: grid;
      grid-template-columns: 54px minmax(0, 1fr) auto;
      gap: 8px;
      align-items: start;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 8px;
      background: #fff;
    }}
    .step-code {{ font-weight: 800; color: var(--blue); font-size: 11px; }}
    .step-q {{ font-weight: 700; font-size: 12px; }}
    .step-note {{ color: var(--muted); font-size: 11px; margin-top: 2px; }}
    .impact {{ font-size: 11px; color: var(--muted); text-align: right; }}
    details {{
      margin-top: 12px;
      border-top: 1px solid var(--line);
      padding-top: 10px;
    }}
    summary {{ cursor: pointer; font-size: 13px; font-weight: 800; }}
    .cols {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 10px;
    }}
    .small-list {{ margin: 0; padding-left: 18px; color: var(--muted); font-size: 12px; line-height: 1.4; }}
    .tag-cloud {{ display: flex; flex-wrap: wrap; gap: 5px; margin-top: 10px; }}
    .empty {{ padding: 30px; text-align: center; color: var(--muted); }}
    @media (max-width: 1100px) {{
      .grid, .split, .cols {{ grid-template-columns: 1fr; }}
      .controls {{ grid-template-columns: 1fr 1fr; }}
      .stats {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .card-head {{ grid-template-columns: 1fr; }}
      .signals {{ justify-content: flex-start; }}
    }}
    @media (max-width: 640px) {{
      .wrap {{ padding: 14px; }}
      .controls, .stats {{ grid-template-columns: 1fr; }}
      .step {{ grid-template-columns: 1fr; }}
      .impact {{ text-align: left; }}
    }}
  </style>
</head>
<body>
  <header class="app-header">
    <div class="wrap">
      <h1>Clave dicotomica H2H-first para pronostico binario</h1>
      <p class="subtitle">Mapa experimental basado en ultimo enfrentamiento directo, movimiento AH, O/U y soportes indirectos. No es una garantia; es una lectura mecanica entrenable.</p>
      <div class="stats" id="stats"></div>
      <div class="controls">
        <input id="q" type="search" placeholder="Buscar equipo, liga, ID o etiqueta">
        <select id="side">
          <option value="">Lado binario: todos</option>
          <option>FAVORITO</option>
          <option>NO_FAVORITO</option>
          <option>NO_DECISION</option>
          <option>SIN_H2H</option>
        </select>
        <select id="goals">
          <option value="">Goles: todos</option>
          <option>CONTROL_BAJO</option>
          <option>RUPTURA_ALTA</option>
          <option>NEUTRO</option>
        </select>
        <select id="conf">
          <option value="">Confianza: todas</option>
          <option>ALTA</option>
          <option>MEDIA</option>
          <option>BAJA</option>
        </select>
        <select id="tag">
          <option value="">Etiqueta: todas</option>
        </select>
      </div>
    </div>
  </header>

  <main class="wrap grid">
    <aside class="panel panel-pad">
      <h2 class="key-title">Clave dicotomica operativa</h2>
      <div class="tree">
        <div class="node"><strong>1. Hay H2H directo?</strong><p>Si no hay H2H, el partido queda en lectura secundaria: Col3, previa e indirectas no sustituyen la memoria del cruce.</p></div>
        <div class="node"><strong>2. El favorito actual cubria la linea de hoy?</strong><p>Si cubria, hay memoria pro-favorito. Si no cubria, se activa veto inicial y se pregunta por volumen.</p></div>
        <div class="node"><strong>3. El volumen contradice el marcador?</strong><p>Si el favorito fallo marcador pero domino tiros/SOT/ataques peligrosos, nace la lectura contraintuitiva.</p></div>
        <div class="node"><strong>4. La casa sube, baja o mantiene presion?</strong><p>Favorito nuevo, subida de presion o favorito mantenido tras fallo son senales de posicionamiento.</p></div>
        <div class="node"><strong>5. El O/U capado cambia el guion?</strong><p>OU 2/2.25 prioriza margen corto, empate y under-control aunque el lado tenga ventaja.</p></div>
        <div class="node"><strong>6. Indirectas y previa confirman?</strong><p>Solo sirven como validacion. Si contradicen al H2H, bajan confianza o bloquean decision.</p></div>
      </div>
      <div class="legend">
        <span class="badge b-fav">FAVORITO</span>
        <span class="badge b-dog">NO_FAVORITO</span>
        <span class="badge b-low">CONTROL_BAJO</span>
        <span class="badge b-high">RUPTURA_ALTA</span>
        <span class="badge b-neutral">NO_DECISION</span>
      </div>
      <div class="tag-cloud" id="topTags"></div>
    </aside>
    <section>
      <div id="cards" class="cards"></div>
    </section>
  </main>

  <script>
    const PAYLOAD = {data_json};
    const rows = PAYLOAD.rows || [];
    const summary = PAYLOAD.summary || {{}};

    const $ = (id) => document.getElementById(id);
    const esc = (v) => String(v ?? '').replace(/[&<>"']/g, ch => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[ch]));
    const clsSide = (v) => v === 'FAVORITO' ? 'b-fav' : v === 'NO_FAVORITO' ? 'b-dog' : 'b-neutral';
    const clsGoals = (v) => v === 'CONTROL_BAJO' ? 'b-low' : v === 'RUPTURA_ALTA' ? 'b-high' : 'b-neutral';

    function init() {{
      renderStats();
      renderTags();
      fillTagFilter();
      ['q','side','goals','conf','tag'].forEach(id => $(id).addEventListener('input', renderCards));
      renderCards();
    }}

    function renderStats() {{
      const items = [
        ['Partidos', summary.total],
        ['Con H2H util', summary.with_h2h],
        ['Binario favorito', summary.favorite],
        ['Control bajo', summary.control_bajo],
      ];
      $('stats').innerHTML = items.map(([label, value]) => `<div class="stat"><b>${{esc(value)}}</b><span>${{esc(label)}}</span></div>`).join('');
    }}

    function renderTags() {{
      $('topTags').innerHTML = (summary.top_tags || []).map(([tag, n]) => `<span class="badge b-neutral">${{esc(tag)}} · ${{esc(n)}}</span>`).join('');
    }}

    function fillTagFilter() {{
      const tags = new Set();
      rows.forEach(r => (r.binary.tags || []).forEach(t => tags.add(t)));
      $('tag').innerHTML += [...tags].sort().map(t => `<option value="${{esc(t)}}">${{esc(t)}}</option>`).join('');
    }}

    function filteredRows() {{
      const q = $('q').value.trim().toLowerCase();
      const side = $('side').value;
      const goals = $('goals').value;
      const conf = $('conf').value;
      const tag = $('tag').value;
      return rows.filter(r => {{
        const hay = [
          r.match_id, r.home, r.away, r.league, r.favorite,
          r.binary.route, r.binary.side_signal, r.binary.goals_signal,
          ...(r.binary.tags || [])
        ].join(' ').toLowerCase();
        return (!q || hay.includes(q))
          && (!side || r.binary.side_signal === side)
          && (!goals || r.binary.goals_signal === goals)
          && (!conf || r.binary.confidence === conf)
          && (!tag || (r.binary.tags || []).includes(tag));
      }});
    }}

    function renderCards() {{
      const out = filteredRows();
      $('cards').innerHTML = out.length ? out.map(renderCard).join('') : '<div class="panel empty">Sin partidos con esos filtros.</div>';
    }}

    function renderCard(r) {{
      const b = r.binary;
      const base = r.base || {{}};
      const labels = (r.labels || []).slice(0, 8).map(l => `<span class="badge b-neutral">${{esc(l.id)}}</span>`).join('');
      return `<article class="card">
        <div class="card-head">
          <div>
            <div class="teams">${{esc(r.home)}} vs ${{esc(r.away)}}</div>
            <div class="meta">
              <span>ID ${{esc(r.match_id)}}</span>
              <span>${{esc(r.league)}}</span>
              <span>${{esc(r.date)}} ${{esc(r.time)}}</span>
              <span>AH ${{esc(r.ah)}} · OU ${{esc(r.ou)}}</span>
              <span>Favorito: ${{esc(r.favorite)}}</span>
            </div>
          </div>
          <div class="signals">
            <span class="badge ${{clsSide(b.side_signal)}}">${{esc(b.side_signal)}}</span>
            <span class="badge ${{clsGoals(b.goals_signal)}}">${{esc(b.goals_signal)}}</span>
            <span class="badge b-conf">${{esc(b.confidence)}} · ${{esc(b.confidence_score)}}</span>
          </div>
        </div>
        <div class="card-body">
          <div class="split">
            <div class="base-box">
              <h3>Mapa base H2H</h3>
              <div class="kv">
                <span>Partido</span><span>${{esc(base.home)}} ${{esc(base.score)}} ${{esc(base.away)}}</span>
                <span>Fecha</span><span>${{esc(base.date)}}</span>
                <span>Presion</span><span>antes ${{esc(base.pressure_then)}} · ahora ${{esc(base.pressure_now)}} · ${{esc(base.pressure_label)}}</span>
                <span>Residual</span><span>${{esc(base.residual)}} · ${{esc(base.cover)}}</span>
                <span>Volumen</span><span>${{esc(base.stats?.verdict)}} · SOT ${{esc(base.stats?.sot_diff)}} · DA ${{esc(base.stats?.danger_diff)}}</span>
                <span>Ruta</span><span>${{esc(b.route)}} · SideScore ${{esc(b.side_score)}} · GoalScore ${{esc(b.goals_score)}} · Tension ${{esc(b.tension)}}</span>
              </div>
              <div class="tag-cloud">${{labels}}</div>
            </div>
            <div class="path">
              <h3>Ruta SI/NO</h3>
              <div class="steps">${{b.steps.map(renderStep).join('')}}</div>
            </div>
          </div>
          <details>
            <summary>Narrativa y frenos</summary>
            <div class="cols">
              <div><h3>Por que si</h3><ul class="small-list">${{list(r.why_yes)}}</ul></div>
              <div><h3>Por que no</h3><ul class="small-list">${{list(r.why_no)}}</ul></div>
              <div><h3>O/U en el mapa</h3><ul class="small-list">${{list(r.ou_notes)}}</ul></div>
            </div>
          </details>
        </div>
      </article>`;
    }}

    function renderStep(s) {{
      const answerClass = s.answer === 'SI' || s.answer === 'COVER' || s.answer === 'UNDER' ? 'b-low'
        : s.answer === 'NO' || s.answer === 'FAIL' ? 'b-dog'
        : 'b-neutral';
      return `<div class="step">
        <div class="step-code">${{esc(s.code.split('_')[0])}}</div>
        <div>
          <div class="step-q">${{esc(s.question)}} <span class="badge ${{answerClass}}">${{esc(s.answer)}}</span></div>
          <div class="step-note">${{esc(s.effect)}}. ${{esc(s.note)}}</div>
        </div>
        <div class="impact">Lado ${{esc(s.side_delta)}}<br>Goles ${{esc(s.goals_delta)}}</div>
      </div>`;
    }}

    function list(items) {{
      return (items && items.length ? items : ['Sin senal fuerte.']).map(x => `<li>${{esc(x)}}</li>`).join('');
    }}

    init();
  </script>
</body>
</html>
"""


def build_payload(project_root: Path, limit: int = 0) -> Dict[str, Any]:
    data_path = project_root / "data" / "data_precacheo.json"
    rows_raw = json.loads(data_path.read_text(encoding="utf-8"))
    if not isinstance(rows_raw, list):
        raise SystemExit("[ERROR] data_precacheo.json no es una lista.")
    if limit > 0:
        rows_raw = rows_raw[:limit]

    rows: List[Dict[str, Any]] = []
    errors = 0
    for match in rows_raw:
        if not isinstance(match, dict):
            continue
        try:
            positioning = analyze_positioning(match)
            binary = derive_binary_reading(positioning)
            rows.append(_compact_match(positioning, binary))
        except Exception:
            errors += 1

    rows.sort(
        key=lambda row: (
            row["binary"]["confidence"] != "ALTA",
            -float(row["binary"]["confidence_score"]),
            row.get("time") or "",
        )
    )
    return {
        "profile": PROFILE,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": str(data_path),
        "errors": errors,
        "summary": _summary(rows),
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera HTML de clave dicotomica H2H-first para pronostico binario.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-html", default="analisis_binario_h2h.html")
    parser.add_argument("--output-json", default="")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    payload = build_payload(project_root, limit=max(0, int(args.limit)))

    output_html = Path(args.output_html)
    output_html.write_text(_html(payload), encoding="utf-8")
    print(f"[OK] HTML: {output_html.resolve()}")

    if args.output_json:
        output_json = Path(args.output_json)
        output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] JSON: {output_json.resolve()}")

    print(
        "[INFO] "
        f"partidos={payload['summary']['total']} "
        f"h2h={payload['summary']['with_h2h']} "
        f"favorito={payload['summary']['favorite']} "
        f"no_favorito={payload['summary']['non_favorite']} "
        f"control_bajo={payload['summary']['control_bajo']} "
        f"errors={payload['errors']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
