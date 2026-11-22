"""Búsqueda Patrón 2.0 y Etiquetado de Equipos (Local, Visitante y Cruce).

Módulo 100% aditivo y desacoplado respecto a pattern_search.py y /api/explorer_search:
- Lee SQLite únicamente en modo solo lectura (app_data.db y explorer_cards.sqlite).
- Combina similitud estructural de mercado (0-55 pts) + perfil de rendimiento/etiquetas
  de ambos equipos y rivales comunes (0-45 pts).
- Incluye evaluación en lote paralelo para etiquetar filas del Explorador o fichas
  individuales bajo demanda.
"""
from __future__ import annotations

import concurrent.futures
import datetime
import gc
import json
import math
import os
import re
import sqlite3
import tempfile
import threading
import time
import unicodedata
import zlib
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .pattern_search import normalize_ah_bucket, explore_matches
try:
    from . import explorer_archive
except Exception:
    explorer_archive = None

PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_DB_PATH = Path(os.getenv("APP_SQLITE_PATH", str(PROJECT_ROOT / "data" / "app_data.db")))
CARDS_DB_PATH = Path(os.getenv("EXPLORER_CARDS_PATH", str(PROJECT_ROOT / "data" / "explorer_cards.sqlite")))
_IS_RENDER = bool(os.getenv("RENDER")) or os.getenv("PV2_LOW_RAM", "").strip().lower() in ("1", "true", "yes")
PV2_ARCHIVE_INDEX_PATH = PROJECT_ROOT / "data" / ".precacheo_fast" / "pv2_archive_index.sqlite"
PV2_ARCHIVE_INDEX_TMP_PATH = Path(tempfile.gettempdir()) / "pv2_archive_index.sqlite"

_CARDS_LOCK = threading.Lock()
_CARDS_BY_BUCKET: Dict[float, List[Tuple[str, Dict[str, Any]]]] = {}
_CARDS_ALL: List[Tuple[str, float, Dict[str, Any]]] = []
_OU_BY_MATCH_ID: Dict[str, float] = {}
_CARDS_META: Dict[str, Any] = {"count": None, "max_updated": None, "checked_at": 0.0, "source": None}
_CARDS_TTL_SECONDS = 45.0

_ARCHIVE_IDX_LOCK = threading.Lock()
_ARCHIVE_IDX_PATH_READY: Optional[Path] = None

_PROFILE_LOCK = threading.Lock()
_PROFILE_CACHE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_PROFILE_CACHE_MAX = 250 if _IS_RENDER else 6000

_DEEP_CTX_LOCK = threading.Lock()
_DEEP_CTX_CACHE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_DEEP_CTX_CACHE_MAX = 60 if _IS_RENDER else 800


def _read_only_sqlite(path: Path) -> Optional[sqlite3.Connection]:
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=15.0)
    conn.row_factory = sqlite3.Row
    return conn


def _norm_team(name: Any) -> str:
    raw = str(name or "").strip().lower()
    if not raw:
        return ""
    raw = unicodedata.normalize("NFKD", raw)
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    raw = re.sub(r"[^a-z0-9\s]", " ", raw)
    return re.sub(r"\s+", " ", raw).strip()


def _same_team(left: Any, right: Any) -> bool:
    a = _norm_team(left)
    b = _norm_team(right)
    if not a or not b:
        return False
    if a == b:
        return True
    if len(a) >= 4 and len(b) >= 4:
        return a in b or b in a
    return False


def _parse_score(raw: Any) -> Optional[Tuple[int, int]]:
    if raw is None:
        return None
    txt = str(raw).strip().replace("-", ":")
    if not txt or "?" in txt or ":" not in txt:
        return None
    parts = [p.strip() for p in txt.split(":")]
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except (TypeError, ValueError):
        return None


def _parse_ah(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        val = float(raw)
        return None if math.isnan(val) else val
    s = str(raw).strip().replace(",", ".").replace(" ", "")
    if not s or s in ("-", "?", "N/A", "null", "None", "undefined"):
        return None
    try:
        if "/" in s:
            p1, p2 = s.split("/", 1)
            v1 = float(p1)
            v2 = float(p2)
            if (v1 < 0 or s.startswith("-0")) and not p2.startswith("-") and v2 > 0:
                v2 = -abs(v2)
            return (v1 + v2) / 2.0
        return float(s)
    except (TypeError, ValueError):
        return None


def _parse_ou(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    val = _parse_ah(raw)
    if val is None or val <= 0.4 or val > 10.0:
        return None
    return val


def _format_ah(val: Optional[float]) -> str:
    if val is None:
        return "-"
    if abs(val) < 0.01:
        return "0"
    if abs(val - round(val)) < 0.01:
        return str(int(round(val)))
    return f"{val:.2f}".rstrip("0").rstrip(".")


def _safe_int(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    m = re.search(r"-?\d+", str(raw))
    if not m:
        return None
    try:
        return int(m.group(0))
    except (TypeError, ValueError):
        return None


def _get_favorite_side(ah: Optional[float]) -> str:
    if ah is None:
        return "EVEN"
    if ah > 0.01:
        return "HOME"
    if ah < -0.01:
        return "AWAY"
    return "EVEN"


def _eval_split_net(goal_diff: int, line_given: float) -> Tuple[str, float]:
    """Evalúa cobertura asiática para `goal_diff - line_given` soportando cuartos (.25/.75)."""
    abs_mod = abs(line_given) % 0.5
    if abs(abs_mod - 0.25) < 0.02:
        sub_lines = (line_given - 0.25, line_given + 0.25)
    else:
        sub_lines = (line_given,)

    votes: List[float] = []
    for sub in sub_lines:
        diff = goal_diff - sub
        if diff > 1e-6:
            votes.append(1.0)
        elif diff < -1e-6:
            votes.append(-1.0)
        else:
            votes.append(0.0)

    avg = sum(votes) / len(votes)
    if avg >= 0.99:
        return "COVER", avg
    if avg > 0.01:
        return "HALF_WIN", avg
    if abs(avg) <= 0.01:
        return "PUSH", 0.0
    if avg > -0.99:
        return "HALF_LOSS", avg
    return "NO_COVER", avg


def _eval_team_ah(hg: int, ag: int, match_ah: Optional[float], team_is_home: bool) -> Tuple[str, Optional[float]]:
    if match_ah is None:
        return "SIN_DATO", None
    goal_diff = (hg - ag) if team_is_home else (ag - hg)
    # Convención NowGoal: match_ah > 0 => Local da match_ah goles; Visitante recibe match_ah.
    team_gives_line = match_ah if team_is_home else -match_ah
    return _eval_split_net(goal_diff, team_gives_line)


def _eval_match_fav_cover(hg: int, ag: int, match_ah: Optional[float]) -> Tuple[str, Optional[float]]:
    if match_ah is None:
        return "UNKNOWN", None
    if match_ah < -0.01:
        return _eval_split_net(ag - hg, abs(match_ah))
    return _eval_split_net(hg - ag, max(0.0, match_ah))


def _collapse_cover_3(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    c = str(code).upper().strip()
    if c in ("COVER", "HALF_WIN", "WIN"):
        return "COVER"
    if c in ("NO_COVER", "HALF_LOSS", "LOSS"):
        return "NO_COVER"
    if c in ("PUSH", "DRAW"):
        return "PUSH"
    return None


def _extract_stats_metrics(stats_rows: Any) -> Dict[str, Tuple[int, int]]:
    out: Dict[str, Tuple[int, int]] = {}
    if not isinstance(stats_rows, list):
        return out
    for row in stats_rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get("label") or row.get("name") or "").lower()
        hv = _safe_int(row.get("home"))
        av = _safe_int(row.get("away"))
        if hv is None or av is None:
            continue
        if "puerta" in label or "target" in label:
            out["sot"] = (hv, av)
        elif "tiro" in label or "shot" in label:
            out["shots"] = (hv, av)
        elif "peligros" in label or "dangerous" in label:
            out["danger"] = (hv, av)
        elif "ataque" in label or "attack" in label:
            out["attacks"] = (hv, av)
    return out


def _detect_hidden_dominance(prev_match: Any, team_name: str, default_is_home: bool) -> Optional[str]:
    if not isinstance(prev_match, dict):
        return None
    score = _parse_score(prev_match.get("score") or prev_match.get("final_score"))
    if not score:
        return None
    hg, ag = score
    p_home = prev_match.get("home_team") or prev_match.get("home") or ""
    p_away = prev_match.get("away_team") or prev_match.get("away") or ""
    if _same_team(team_name, p_home):
        is_home = True
    elif _same_team(team_name, p_away):
        is_home = False
    else:
        is_home = default_is_home

    team_diff = (hg - ag) if is_home else (ag - hg)
    if team_diff > 0:
        return None  # Solo interesa superioridad oculta en empate o derrota

    metrics = _extract_stats_metrics(prev_match.get("stats_rows"))
    if not metrics:
        return None

    idx_team = 0 if is_home else 1
    idx_opp = 1 - idx_team
    reasons: List[str] = []
    if "shots" in metrics and (metrics["shots"][idx_team] - metrics["shots"][idx_opp]) >= 4:
        reasons.append(f"tiros {metrics['shots'][idx_team]}-{metrics['shots'][idx_opp]}")
    if "sot" in metrics and (metrics["sot"][idx_team] - metrics["sot"][idx_opp]) >= 2:
        reasons.append(f"puerta {metrics['sot'][idx_team]}-{metrics['sot'][idx_opp]}")
    if "danger" in metrics and (metrics["danger"][idx_team] - metrics["danger"][idx_opp]) >= 12:
        reasons.append(f"peligrosos {metrics['danger'][idx_team]}-{metrics['danger'][idx_opp]}")

    if len(reasons) >= 2 or (len(reasons) == 1 and team_diff == 0):
        return ", ".join(reasons)
    return None


def _extract_col3_status(match_payload: Dict[str, Any], current_ah: Optional[float]) -> Dict[str, Any]:
    col3 = match_payload.get("h2h_col3")
    if not isinstance(col3, dict) or col3.get("status") != "found":
        return {"status": None, "type": None}

    if col3.get("goles_home") is None or col3.get("goles_away") is None:
        return {"status": None, "type": None}
    try:
        c_hg = int(col3["goles_home"])
        c_ag = int(col3["goles_away"])
    except (TypeError, ValueError):
        return {"status": None, "type": None}

    home_name = str(match_payload.get("home_name") or match_payload.get("home_team") or "")
    away_name = str(match_payload.get("away_name") or match_payload.get("away_team") or "")
    fav_is_local = (current_ah is None) or (current_ah > 0)

    def _wdl_rank(hg: int, ag: int, is_home: bool) -> int:
        d = (hg - ag) if is_home else (ag - hg)
        return 3 if d > 0 else (1 if d == 0 else 0)

    fav_wdl = None
    mirror_team = ""
    lhm = match_payload.get("last_home_match") or {}
    lam = match_payload.get("last_away_match") or {}

    if fav_is_local and isinstance(lhm, dict) and lhm.get("score"):
        sc = _parse_score(lhm.get("score"))
        if sc:
            fav_is_h = _same_team(home_name, lhm.get("home_team") or lhm.get("home") or "")
            fav_wdl = _wdl_rank(sc[0], sc[1], fav_is_h)
            if isinstance(lam, dict):
                la_h = lam.get("home_team") or lam.get("home") or ""
                la_a = lam.get("away_team") or lam.get("away") or ""
                mirror_team = la_a if _same_team(away_name, la_h) else la_h
    elif (not fav_is_local) and isinstance(lam, dict) and lam.get("score"):
        sc = _parse_score(lam.get("score"))
        if sc:
            fav_is_h = _same_team(away_name, lam.get("home_team") or lam.get("home") or "")
            fav_wdl = _wdl_rank(sc[0], sc[1], fav_is_h)
            if isinstance(lhm, dict):
                lh_h = lhm.get("home_team") or lhm.get("home") or ""
                lh_a = lhm.get("away_team") or lhm.get("away") or ""
                mirror_team = lh_a if _same_team(home_name, lh_h) else lh_h

    c_home = col3.get("h2h_home_team_name") or ""
    c_away = col3.get("h2h_away_team_name") or ""
    comp_type = None
    mirror_wdl = None
    if mirror_team:
        m_is_h = _same_team(mirror_team, c_home)
        m_is_a = _same_team(mirror_team, c_away)
        if m_is_h:
            mirror_wdl = _wdl_rank(c_hg, c_ag, True)
            base_type = "directa"
        elif m_is_a:
            mirror_wdl = _wdl_rank(c_hg, c_ag, False)
            base_type = "inversa"
        else:
            base_type = None
        if base_type:
            comp_type = ("inversa" if base_type == "directa" else "directa") if not fav_is_local else base_type

    status = None
    if fav_wdl is not None and mirror_wdl is not None:
        if fav_wdl > mirror_wdl:
            status = "MEJORA"
        elif fav_wdl == mirror_wdl:
            status = "IGUALA"
        else:
            status = "EMPEORA"

    return {
        "status": status,
        "type": comp_type,
        "ah": _parse_ah(col3.get("handicap")),
    }


def _collect_recent_matches(
    payload: Dict[str, Any],
    team_name: str,
    side: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Devuelve (generales_max10, en_su_localia_max10) para home o away."""
    ctx_curr = ((payload.get("pre_match_context") or {}).get("current") or {})
    if not isinstance(ctx_curr, dict):
        ctx_curr = {}
    if side == "home":
        all_list = (
            payload.get("recent_home_matches_all")
            or payload.get("recent_home_matches_same_league_general")
            or payload.get("recent_home_matches")
            or ctx_curr.get("home_matches_all")
            or ctx_curr.get("home_matches")
            or []
        )
        spec_list = (
            payload.get("recent_home_matches_same_league_specific")
            or payload.get("recent_home_matches")
            or ctx_curr.get("home_matches")
            or []
        )
    else:
        all_list = (
            payload.get("recent_away_matches_all")
            or payload.get("recent_away_matches_same_league_general")
            or payload.get("recent_away_matches")
            or ctx_curr.get("away_matches_all")
            or ctx_curr.get("away_matches")
            or []
        )
        spec_list = (
            payload.get("recent_away_matches_same_league_specific")
            or payload.get("recent_away_matches")
            or ctx_curr.get("away_matches")
            or []
        )

    def _clean_rows(rows: Any) -> List[Dict[str, Any]]:
        cleaned: List[Dict[str, Any]] = []
        seen = set()
        if not isinstance(rows, list):
            return cleaned
        for item in rows:
            if not isinstance(item, dict):
                continue
            m_home = str(item.get("home") or item.get("home_team") or "").strip()
            m_away = str(item.get("away") or item.get("away_team") or "").strip()
            m_date = str(item.get("date") or item.get("match_date") or "").strip()
            sc = _parse_score(item.get("score") or item.get("score_raw") or item.get("result"))
            if not sc:
                continue
            dedup = f"{m_date}:{_norm_team(m_home)}:{_norm_team(m_away)}"
            if dedup in seen:
                continue
            seen.add(dedup)

            is_home = _same_team(team_name, m_home)
            is_away = _same_team(team_name, m_away)
            if not is_home and not is_away:
                # Si el nombre viene abreviado/distinto en un registro antiguo, inferir solo si spec_list
                continue

            ah_val = _parse_ah(
                item.get("ahLine")
                or item.get("ah_line")
                or item.get("ah")
                or item.get("handicap")
                or item.get("handicap_line_raw")
            )
            ou_val = _parse_ou(item.get("ouLine") or item.get("ou_line") or item.get("ou"))
            rival = m_away if is_home else m_home
            cleaned.append({
                "date": m_date,
                "home": m_home,
                "away": m_away,
                "rival": rival,
                "rival_norm": _norm_team(rival),
                "is_home": is_home,
                "hg": sc[0],
                "ag": sc[1],
                "score": f"{sc[0]}:{sc[1]}",
                "ah": ah_val,
                "ou": ou_val,
            })
        return cleaned

    general = _clean_rows(all_list)[:10]
    if not general and spec_list:
        general = _clean_rows(spec_list)[:10]

    want_home = (side == "home")
    venue = [r for r in general if r["is_home"] == want_home]
    if len(venue) < 4 and spec_list:
        spec_cleaned = [r for r in _clean_rows(spec_list) if r["is_home"] == want_home]
        if len(spec_cleaned) > len(venue):
            venue = spec_cleaned
    return general[:10], venue[:10]


def _summarize_rows(
    rows: List[Dict[str, Any]],
    ref_ou: float,
    ref_ah: Optional[float] = None,
) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    n = len(rows)
    w = d = l = gf = gc = 0
    ah_n = 0
    ah_win_eq = 0.0
    close_n = 0
    close_cov_eq = 0.0
    close_unb = 0
    sim_ah_n = 0
    sim_ah_cov_eq = 0.0
    ou_n = 0
    over_cnt = 0
    under_cnt = 0
    ou_lines_sum = 0.0
    ou_lines_cnt = 0
    rivals_map: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        tg = r["hg"] if r["is_home"] else r["ag"]
        og = r["ag"] if r["is_home"] else r["hg"]
        gf += tg
        gc += og
        if tg > og:
            w += 1
            pts = 3
        elif tg == og:
            d += 1
            pts = 1
        else:
            l += 1
            pts = 0

        ah_val = r.get("ah")
        ah_code, ah_net = _eval_team_ah(r["hg"], r["ag"], ah_val, r["is_home"])
        if ah_net is not None:
            ah_n += 1
            if ah_net > 0:
                ah_win_eq += 1.0 if ah_code == "COVER" else 0.5
            elif ah_code == "PUSH":
                ah_win_eq += 0.5

            if abs(ah_val) <= 0.76:
                close_n += 1
                if tg >= og:
                    close_unb += 1
                if ah_net > 0:
                    close_cov_eq += 1.0 if ah_code == "COVER" else 0.5
                elif ah_code == "PUSH":
                    close_cov_eq += 0.5

            if ref_ah is not None:
                row_persp = ah_val if r["is_home"] else -ah_val
                same_role = (
                    (abs(ref_ah) <= 0.26 and abs(row_persp) <= 0.51)
                    or (ref_ah > 0.26 and row_persp > 0.01)
                    or (ref_ah < -0.26 and row_persp < -0.01)
                )
                if same_role and abs(row_persp - ref_ah) <= 0.51:
                    sim_ah_n += 1
                    if ah_net > 0:
                        sim_ah_cov_eq += 1.0 if ah_code == "COVER" else 0.5
                    elif ah_code == "PUSH":
                        sim_ah_cov_eq += 0.5

        raw_row_ou = r.get("ou")
        if raw_row_ou:
            ou_lines_sum += raw_row_ou
            ou_lines_cnt += 1
        ou_line = raw_row_ou or ref_ou
        total_goals = r["hg"] + r["ag"]
        if ou_line:
            ou_n += 1
            if total_goals > ou_line + 0.01:
                over_cnt += 1
            elif total_goals < ou_line - 0.01:
                under_cnt += 1

        rn = r.get("rival_norm")
        if rn and rn not in rivals_map:
            rivals_map[rn] = {
                "rival": r["rival"],
                "score": r["score"],
                "ah": _format_ah(ah_val),
                "pts": pts,
                "gd": tg - og,
                "is_home": r["is_home"],
                "date": r.get("date", ""),
            }

    stats = {
        "n": n,
        "w": w,
        "d": d,
        "l": l,
        "gf": gf,
        "gc": gc,
        "win_pct": round((w / n) * 100.0, 1) if n else None,
        "draw_pct": round((d / n) * 100.0, 1) if n else None,
        "unbeaten_pct": round(((w + d) / n) * 100.0, 1) if n else None,
        "loss_pct": round((l / n) * 100.0, 1) if n else None,
        "avg_gf": round((gf / n), 2) if n else None,
        "avg_gc": round((gc / n), 2) if n else None,
        "avg_goals_total": round(((gf + gc) / n), 2) if n else None,
        "avg_ou_line": round((ou_lines_sum / ou_lines_cnt), 2) if ou_lines_cnt else None,
        "ah_n": ah_n,
        "ah_cover_pct": round((ah_win_eq / ah_n) * 100.0, 1) if ah_n else None,
        "close_n": close_n,
        "close_cover_pct": round((close_cov_eq / close_n) * 100.0, 1) if close_n else None,
        "close_unbeaten_pct": round((close_unb / close_n) * 100.0, 1) if close_n else None,
        "sim_ah_n": sim_ah_n,
        "sim_ah_cover_pct": round((sim_ah_cov_eq / sim_ah_n) * 100.0, 1) if sim_ah_n else None,
        "over_pct": round((over_cnt / ou_n) * 100.0, 1) if ou_n else None,
        "under_pct": round((under_cnt / ou_n) * 100.0, 1) if ou_n else None,
    }
    return stats, rivals_map


def _stats_from_standings(standings: Any) -> Tuple[Dict[str, Any], Dict[str, Any], Optional[int]]:
    if not isinstance(standings, dict):
        return {"n": 0}, {"n": 0}, None
    ranking = _safe_int(standings.get("ranking"))

    def _build(prefix: str) -> Dict[str, Any]:
        pj = _safe_int(standings.get(f"{prefix}_pj")) or 0
        v = _safe_int(standings.get(f"{prefix}_v")) or 0
        e = _safe_int(standings.get(f"{prefix}_e")) or 0
        d = _safe_int(standings.get(f"{prefix}_d")) or 0
        gf = _safe_int(standings.get(f"{prefix}_gf")) or 0
        gc = _safe_int(standings.get(f"{prefix}_gc")) or 0
        if pj <= 0:
            pj = v + e + d
        if pj <= 0:
            return {"n": 0, "w": 0, "d": 0, "l": 0, "gf": 0, "gc": 0,
                    "win_pct": None, "draw_pct": None, "unbeaten_pct": None, "loss_pct": None,
                    "avg_gf": None, "avg_gc": None, "avg_goals_total": None, "avg_ou_line": None,
                    "ah_n": 0, "ah_cover_pct": None, "close_n": 0, "close_cover_pct": None,
                    "close_unbeaten_pct": None, "sim_ah_n": 0, "sim_ah_cover_pct": None,
                    "over_pct": None, "under_pct": None}
        return {
            "n": pj, "w": v, "d": e, "l": d, "gf": gf, "gc": gc,
            "win_pct": round((v / pj) * 100.0, 1),
            "draw_pct": round((e / pj) * 100.0, 1),
            "unbeaten_pct": round(((v + e) / pj) * 100.0, 1),
            "loss_pct": round((d / pj) * 100.0, 1),
            "avg_gf": round((gf / pj), 2),
            "avg_gc": round((gc / pj), 2),
            "avg_goals_total": round(((gf + gc) / pj), 2),
            "avg_ou_line": None,
            "ah_n": 0, "ah_cover_pct": None,
            "close_n": 0, "close_cover_pct": None, "close_unbeaten_pct": None,
            "sim_ah_n": 0, "sim_ah_cover_pct": None,
            "over_pct": None, "under_pct": None,
        }

    return _build("total"), _build("specific"), ranking


def _make_tag(code: str, label: str, tone: str, reason: str) -> Dict[str, str]:
    return {"code": code, "label": label, "tone": tone, "reason": reason}


def compute_team_profile_and_tags(
    match_payload: Dict[str, Any],
    match_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Calcula perfil estadístico, etiquetas de Local/Visitante/Cruce y rivales comunes."""
    mid = str(match_id or match_payload.get("match_id") or match_payload.get("id") or "")
    home_name = str(match_payload.get("home_name") or match_payload.get("home_team") or "").strip()
    away_name = str(match_payload.get("away_name") or match_payload.get("away_team") or "").strip()
    odds = match_payload.get("main_match_odds") or {}
    ah = _parse_ah(odds.get("ah_linea") if isinstance(odds, dict) else None)
    if ah is None:
        ah = _parse_ah(match_payload.get("handicap") or match_payload.get("ah_line"))
    ou = _parse_ou(odds.get("goals_linea") if isinstance(odds, dict) else None)
    if ou is None:
        ou = _parse_ou(match_payload.get("goal_line") or match_payload.get("ou_line"))
    fav_side = _get_favorite_side(ah)

    ref_ou = ou or 2.5
    h_all_rows, h_ven_rows = _collect_recent_matches(match_payload, home_name, "home")
    a_all_rows, a_ven_rows = _collect_recent_matches(match_payload, away_name, "away")

    h_st_all, h_st_ven, h_rank = _stats_from_standings(match_payload.get("home_standings"))
    a_st_all, a_st_ven, a_rank = _stats_from_standings(match_payload.get("away_standings"))

    away_ref_ah = (-ah) if ah is not None else None
    h_all_stats, h_rivals = _summarize_rows(h_all_rows, ref_ou, ref_ah=ah)
    h_ven_stats, _ = _summarize_rows(h_ven_rows, ref_ou, ref_ah=ah)
    a_all_stats, a_rivals = _summarize_rows(a_all_rows, ref_ou, ref_ah=away_ref_ah)
    a_ven_stats, _ = _summarize_rows(a_ven_rows, ref_ou, ref_ah=away_ref_ah)

    used_standings = False
    if h_all_stats["n"] < 3 and h_st_all.get("n", 0) >= 3:
        h_all_stats = h_st_all
        used_standings = True
    if h_ven_stats["n"] < 3 and h_st_ven.get("n", 0) >= 3:
        h_ven_stats = h_st_ven
        used_standings = True
    if a_all_stats["n"] < 3 and a_st_all.get("n", 0) >= 3:
        a_all_stats = a_st_all
        used_standings = True
    if a_ven_stats["n"] < 3 and a_st_ven.get("n", 0) >= 3:
        a_ven_stats = a_st_ven
        used_standings = True

    if h_all_rows and a_all_rows and min(len(h_all_rows), len(a_all_rows)) >= 5:
        profile_quality = "full"
    elif used_standings or (h_all_stats.get("n", 0) >= 3 and a_all_stats.get("n", 0) >= 3):
        profile_quality = "standings" if used_standings else "partial"
    else:
        profile_quality = "minimal"

    # Rivales comunes entre últimos 10 del Local y del Visitante
    common_keys = [k for k in h_rivals.keys() if k in a_rivals]
    common_samples: List[Dict[str, Any]] = []
    c_home_pts = 0
    c_away_pts = 0
    c_home_gd = 0
    c_away_gd = 0
    for rk in common_keys:
        hr = h_rivals[rk]
        ar = a_rivals[rk]
        c_home_pts += hr["pts"]
        c_away_pts += ar["pts"]
        c_home_gd += hr["gd"]
        c_away_gd += ar["gd"]
        if len(common_samples) < 6:
            common_samples.append({
                "rival": hr["rival"],
                "home_score": hr["score"],
                "home_ah": hr["ah"],
                "away_score": ar["score"],
                "away_ah": ar["ah"],
            })

    common_edge = "EVEN"
    if len(common_keys) >= 2:
        pts_diff = c_home_pts - c_away_pts
        gd_diff = c_home_gd - c_away_gd
        if pts_diff >= 3 or (pts_diff >= 1 and gd_diff >= 3):
            common_edge = "HOME"
        elif pts_diff <= -3 or (pts_diff <= -1 and gd_diff <= -3):
            common_edge = "AWAY"

    common_rivals = {
        "count": len(common_keys),
        "home_pts": c_home_pts,
        "away_pts": c_away_pts,
        "home_gd": c_home_gd,
        "away_gd": c_away_gd,
        "edge": common_edge,
        "samples": common_samples,
    }

    hidden_dom_home = _detect_hidden_dominance(match_payload.get("last_home_match"), home_name, True)
    hidden_dom_away = _detect_hidden_dominance(match_payload.get("last_away_match"), away_name, False)
    col3_info = _extract_col3_status(match_payload, ah)

    # --- Etiquetas del Local ---
    home_tags: List[Dict[str, str]] = []
    ha, hv = h_all_stats, h_ven_stats
    if ha["n"] >= 5 and (
        (ha["win_pct"] or 0) >= 65.0
        or ((ha["unbeaten_pct"] or 0) >= 80.0 and (ha["win_pct"] or 0) >= 50.0)
    ):
        home_tags.append(_make_tag(
            "LOCAL_DURO_GEN", "Local Duro", "strong",
            f"General: {ha['w']}V-{ha['d']}E-{ha['l']}D ({ha['win_pct']}% V)"
        ))
    if hv["n"] >= 4 and (
        (hv["win_pct"] or 0) >= 65.0 or (hv["unbeaten_pct"] or 0) >= 80.0
    ):
        home_tags.append(_make_tag(
            "LOCAL_DURO_CASA", "Fortín Casa", "good",
            f"En casa: {hv['w']}V-{hv['d']}E-{hv['l']}D ({hv['unbeaten_pct']}% invicto)"
        ))
    if ha.get("ah_n", 0) >= 4 and (ha.get("ah_cover_pct") or 0) >= 60.0:
        home_tags.append(_make_tag(
            "LOCAL_CUBRE_AH", "Local Cubre AH", "good",
            f"Cubre AH en {ha['ah_cover_pct']}% ({ha['ah_n']} partidos)"
        ))
    if ha["n"] >= 5 and ((ha["loss_pct"] or 0) >= 50.0 or (ha["win_pct"] or 0) <= 25.0):
        home_tags.append(_make_tag(
            "LOCAL_FLOJO", "Local Flojo", "danger",
            f"Solo {ha['win_pct']}% V y {ha['loss_pct']}% D ({ha['w']}V-{ha['d']}E-{ha['l']}D)"
        ))
    if (ha["win_pct"] or 0) >= 55.0 and ha.get("close_n", 0) >= 3 and (ha.get("close_cover_pct") or 100) <= 35.0:
        home_tags.append(_make_tag(
            "LOCAL_FALLA_PARES", "Pincha ante Pares", "warn",
            f"Cubre solo {ha['close_cover_pct']}% en líneas cortas |AH|<=0.75 ({ha['close_n']}p)"
        ))
    if hidden_dom_home:
        home_tags.append(_make_tag(
            "DOMINIO_OCULTO_LOC", "Dominio Oculto Loc", "info",
            f"Último partido sin victoria pero superior ({hidden_dom_home})"
        ))

    # --- Etiquetas del Visitante ---
    away_tags: List[Dict[str, str]] = []
    aa, av = a_all_stats, a_ven_stats
    if aa["n"] >= 5 and (
        (aa["win_pct"] or 0) >= 60.0
        or ((aa["unbeaten_pct"] or 0) >= 75.0 and (aa["win_pct"] or 0) >= 50.0)
    ):
        away_tags.append(_make_tag(
            "VISITANTE_DURO_GEN", "Visitante Duro", "strong",
            f"General: {aa['w']}V-{aa['d']}E-{aa['l']}D ({aa['win_pct']}% V)"
        ))
    if av["n"] >= 4 and (
        (av["win_pct"] or 0) >= 55.0 or (av["unbeaten_pct"] or 0) >= 75.0
    ):
        away_tags.append(_make_tag(
            "VISITANTE_DURO_FUERA", "Fuerte Fuera", "good",
            f"Fuera: {av['w']}V-{av['d']}E-{av['l']}D ({av['unbeaten_pct']}% invicto)"
        ))
    if (av["n"] >= 4 and ((av["loss_pct"] or 0) >= 55.0 or (av["win_pct"] or 0) <= 20.0)) or (
        aa["n"] >= 5 and (aa["loss_pct"] or 0) >= 55.0
    ):
        detail = (
            f"Fuera: {av['w']}V-{av['d']}E-{av['l']}D ({av['loss_pct']}% D)"
            if av["n"] >= 4 else
            f"General: {aa['w']}V-{aa['d']}E-{aa['l']}D ({aa['loss_pct']}% D)"
        )
        away_tags.append(_make_tag("MAL_VISITANTE", "Mal Visitante", "danger", detail))
    if aa.get("ah_n", 0) >= 4 and (aa.get("ah_cover_pct") or 100) <= 35.0:
        away_tags.append(_make_tag(
            "VISITANTE_BLANDO_AH", "Vis. Blando AH", "warn",
            f"Cubre AH solo {aa['ah_cover_pct']}% ({aa['ah_n']} partidos)"
        ))
    if (aa["win_pct"] or 0) >= 50.0 and av["n"] >= 4 and (av["loss_pct"] or 0) >= 50.0:
        away_tags.append(_make_tag(
            "VISITANTE_MAQUILLADO", "Vis. Maquillado", "warn",
            f"{aa['win_pct']}% V general pero {av['loss_pct']}% D fuera ({av['w']}V-{av['d']}E-{av['l']}D)"
        ))
    if aa.get("ah_n", 0) >= 4 and (aa.get("ah_cover_pct") or 0) >= 60.0:
        away_tags.append(_make_tag(
            "VISITANTE_CUBRE_AH", "Vis. Cubre AH", "good",
            f"Cubre AH en {aa['ah_cover_pct']}% ({aa['ah_n']} partidos)"
        ))
    if hidden_dom_away:
        away_tags.append(_make_tag(
            "DOMINIO_OCULTO_VIS", "Dominio Oculto Vis", "info",
            f"Último partido sin victoria pero superior ({hidden_dom_away})"
        ))

    # --- Etiquetas de Cruce y Justificación del Hándicap ---
    h_codes = {t["code"] for t in home_tags}
    a_codes = {t["code"] for t in away_tags}
    home_is_hard = bool({"LOCAL_DURO_GEN", "LOCAL_DURO_CASA"} & h_codes)
    away_is_hard = bool({"VISITANTE_DURO_GEN", "VISITANTE_DURO_FUERA"} & a_codes)
    away_is_weak = bool({"MAL_VISITANTE", "VISITANTE_BLANDO_AH"} & a_codes)
    home_is_weak = "LOCAL_FLOJO" in h_codes

    matchup_tags: List[Dict[str, str]] = []
    if home_is_hard and away_is_hard:
        matchup_tags.append(_make_tag(
            "CHOQUE_DE_DUROS", "Choque de Duros", "warn",
            f"Local ({ha['w']}V-{ha['d']}E-{ha['l']}D) y Visitante ({aa['w']}V-{aa['d']}E-{aa['l']}D) llegan fuertes"
        ))

    if fav_side == "HOME":
        if home_is_hard and away_is_weak:
            matchup_tags.append(_make_tag(
                "DOBLE_FUERZA_FAV", "Doble Fuerza Fav", "strong",
                "Local duro frente a visitante débil fuera/AH"
            ))
        elif home_is_hard and not away_is_hard:
            matchup_tags.append(_make_tag(
                "FAV_POR_LOCAL_DURO", "Fav por Local Duro", "good",
                f"AH Local respaldado por racha local ({ha['w']}V en {ha['n']}p)"
            ))
        elif (not home_is_hard) and away_is_weak:
            matchup_tags.append(_make_tag(
                "FAV_POR_MAL_VISITANTE", "Fav por Mal Visitante", "info",
                "El favoritismo del local se apoya sobre todo en la debilidad del visitante"
            ))
    elif fav_side == "AWAY":
        if away_is_hard and home_is_weak:
            matchup_tags.append(_make_tag(
                "DOBLE_FUERZA_FAV", "Doble Fuerza Fav", "strong",
                "Visitante duro frente a local flojo"
            ))
        elif away_is_hard and not home_is_hard:
            matchup_tags.append(_make_tag(
                "FAV_POR_VIS_DURO", "Fav por Vis. Duro", "good",
                f"AH Visitante respaldado por su solidez ({aa['w']}V en {aa['n']}p)"
            ))
        elif (not away_is_hard) and home_is_weak:
            matchup_tags.append(_make_tag(
                "FAV_POR_LOCAL_FLOJO", "Fav por Local Flojo", "info",
                "El favoritismo visitante nace de la debilidad del local"
            ))

    trap_reasons: List[str] = []
    if fav_side == "HOME" and "LOCAL_FALLA_PARES" in h_codes:
        trap_reasons.append("Local pincha en líneas cortas")
    if home_is_hard and away_is_hard and ah is not None and abs(ah) >= 0.49:
        trap_reasons.append(f"Choque de duros con línea exigente ({_format_ah(ah)})")
    if common_rivals["count"] >= 3:
        if fav_side == "HOME" and common_edge == "AWAY":
            trap_reasons.append(f"Visitante domina rivales comunes ({c_away_pts} vs {c_home_pts} pts)")
        elif fav_side == "AWAY" and common_edge == "HOME":
            trap_reasons.append(f"Local domina rivales comunes ({c_home_pts} vs {c_away_pts} pts)")
    if trap_reasons:
        matchup_tags.append(_make_tag(
            "AH_ALERTA_TRAMPA", "Alerta AH Inflado", "danger",
            " · ".join(trap_reasons)
        ))

    if common_rivals["count"] >= 3:
        if common_edge == "HOME":
            matchup_tags.append(_make_tag(
                "VENTAJA_COMUNES_LOC", "Comunes: +Local", "good",
                f"{common_rivals['count']} rivales comunes: Local {c_home_pts} pts ({c_home_gd:+d}) vs Vis {c_away_pts} pts ({c_away_gd:+d})"
            ))
        elif common_edge == "AWAY":
            matchup_tags.append(_make_tag(
                "VENTAJA_COMUNES_VIS", "Comunes: +Visitante", "warn",
                f"{common_rivals['count']} rivales comunes: Vis {c_away_pts} pts ({c_away_gd:+d}) vs Local {c_home_pts} pts ({c_home_gd:+d})"
            ))
        else:
            matchup_tags.append(_make_tag(
                "COMUNES_PAREJOS", "Comunes Parejos", "neutral",
                f"{common_rivals['count']} rivales comunes parejos ({c_home_pts} vs {c_away_pts} pts)"
            ))

    h_over = ha.get("over_pct")
    a_over = aa.get("over_pct")
    h_under = ha.get("under_pct")
    a_under = aa.get("under_pct")
    if h_over is not None and a_over is not None:
        avg_over = (h_over + a_over) / 2.0
        avg_under = ((h_under or 0) + (a_under or 0)) / 2.0
        if (h_over >= 60.0 and a_over >= 60.0) or avg_over >= 68.0:
            matchup_tags.append(_make_tag(
                "TENDENCIA_OVER", "Tendencia Over", "info",
                f"Over reciente: Local {h_over}% · Vis {a_over}%"
            ))
        elif ((h_under or 0) >= 60.0 and (a_under or 0) >= 60.0) or avg_under >= 68.0:
            matchup_tags.append(_make_tag(
                "TENDENCIA_UNDER", "Tendencia Under", "neutral",
                f"Under reciente: Local {h_under}% · Vis {a_under}%"
            ))

    # Frase ejecutiva de diagnóstico
    m_codes = {t["code"] for t in matchup_tags}
    if "AH_ALERTA_TRAMPA" in m_codes:
        headline_reason = f"Línea {_format_ah(ah)} bajo vigilancia: {trap_reasons[0]}."
    elif "DOBLE_FUERZA_FAV" in m_codes:
        headline_reason = f"Hándicap {_format_ah(ah)} doblemente respaldado (favorito duro y rival débil)."
    elif "FAV_POR_MAL_VISITANTE" in m_codes:
        headline_reason = f"Local favorito ({_format_ah(ah)}) más por debilidad del visitante que por racha propia."
    elif "FAV_POR_LOCAL_DURO" in m_codes:
        headline_reason = f"Local favorito ({_format_ah(ah)}) justificado por su solidez ({ha['w']}V en {ha['n']}p)."
    elif "CHOQUE_DE_DUROS" in m_codes:
        headline_reason = f"Choque de duros: ambos superan el corte de forma ({ha['w']}V vs {aa['w']}V)."
    elif "FAV_POR_VIS_DURO" in m_codes:
        headline_reason = f"Visitante favorito ({_format_ah(ah)}) respaldado por su buen rendimiento ({aa['w']}V en {aa['n']}p)."
    elif "FAV_POR_LOCAL_FLOJO" in m_codes:
        headline_reason = f"Visitante favorito ({_format_ah(ah)}) impulsado por mal momento del local."
    elif ah is not None and abs(ah) < 0.01:
        headline_reason = "Línea pareja (0 / PK): ambos equipos en equilibrio según mercado."
    else:
        headline_reason = "Perfil equilibrado sin desequilibrio extremo en los últimos 10 partidos."

    raw_date = str(match_payload.get("match_date") or match_payload.get("date") or match_payload.get("cached_at") or "").split(" ")[0]
    raw_score = str(match_payload.get("final_score") or match_payload.get("score") or "?:?").replace(" - ", ":").replace("-", ":")

    return {
        "match_id": mid,
        "date": raw_date,
        "league": str(match_payload.get("league_name") or match_payload.get("league") or ""),
        "home": home_name,
        "away": away_name,
        "score": raw_score,
        "ah": ah,
        "ou": ou,
        "fav_side": fav_side,
        "profile_quality": profile_quality,
        "home_rank": h_rank,
        "away_rank": a_rank,
        "home_stats": {"all": ha, "venue": hv},
        "away_stats": {"all": aa, "venue": av},
        "home_tags": home_tags,
        "away_tags": away_tags,
        "matchup_tags": matchup_tags,
        "common_rivals": common_rivals,
        "hidden_dom": {"home": hidden_dom_home, "away": hidden_dom_away},
        "col3": col3_info,
        "headline_reason": headline_reason,
    }


def _get_cached_profile(match_id: str) -> Optional[Dict[str, Any]]:
    with _PROFILE_LOCK:
        prof = _PROFILE_CACHE.get(match_id)
        if prof is not None:
            _PROFILE_CACHE.move_to_end(match_id)
        return prof


def _set_cached_profile(match_id: str, profile: Dict[str, Any]) -> None:
    if not match_id:
        return
    with _PROFILE_LOCK:
        _PROFILE_CACHE[match_id] = profile
        _PROFILE_CACHE.move_to_end(match_id)
        while len(_PROFILE_CACHE) > _PROFILE_CACHE_MAX:
            _PROFILE_CACHE.popitem(last=False)


def _get_explorer_archive_db() -> Optional[str]:
    """Devuelve ruta SQLite descomprimida de `explorer_full.sqlite.gz` si existe en este entorno."""
    if explorer_archive is None:
        return None
    full_path = getattr(explorer_archive, "FULL_PATH", None)
    db_fn = getattr(explorer_archive, "_database", None)
    if not full_path or not callable(db_fn) or not Path(full_path).exists():
        return None
    try:
        return db_fn()
    except Exception:
        return None


def get_archive_match_by_id(match_id: Any) -> Optional[Dict[str, Any]]:
    """Lee un único partido histórico desde `explorer_archive` (solo lectura)."""
    mid = str(match_id or "").strip()
    if not mid:
        return None
    db_path = _get_explorer_archive_db()
    if not db_path:
        return None
    conn = _read_only_sqlite(Path(db_path))
    if conn is None:
        return None
    try:
        row = conn.execute("SELECT payload FROM matches WHERE match_id = ? LIMIT 1", (mid,)).fetchone()
        if not row or not row["payload"]:
            return None
        obj = json.loads(zlib.decompress(row["payload"]))
        if isinstance(obj, dict):
            obj.setdefault("match_id", mid)
            return obj
    except Exception:
        return None
    finally:
        conn.close()
    return None


def _fetch_archive_payloads_bulk(match_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Lee en lote payloads históricos desde `explorer_archive` sin cargar todo en RAM."""
    ids = [str(m).strip() for m in match_ids if str(m).strip()]
    if not ids:
        return {}
    db_path = _get_explorer_archive_db()
    if not db_path:
        return {}
    conn = _read_only_sqlite(Path(db_path))
    if conn is None:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    try:
        chunk_size = 400
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i:i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT match_id, payload FROM matches WHERE match_id IN ({placeholders})",
                chunk,
            ).fetchall()
            for r in rows:
                mid = str(r["match_id"])
                try:
                    obj = json.loads(zlib.decompress(r["payload"]))
                    if isinstance(obj, dict):
                        obj.setdefault("match_id", mid)
                        out[mid] = obj
                except Exception:
                    continue
    finally:
        conn.close()
    return out


def _is_valid_archive_index(path: Path, src_mtime_ns: int) -> bool:
    if not path.is_file():
        return False
    try:
        if path.stat().st_mtime_ns < src_mtime_ns:
            return False
    except OSError:
        return False
    conn = _read_only_sqlite(path)
    if conn is None:
        return False
    try:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM sigs").fetchone()
        return bool(row and int(row["cnt"] or 0) > 0)
    except Exception:
        return False
    finally:
        conn.close()


def build_archive_signature_index(dest_path: Optional[Path] = None, force: bool = False) -> Optional[Path]:
    """Construye un índice SQLite compacto por `ah_bucket` desde `explorer_full.sqlite.gz` (Low-RAM)."""
    global _ARCHIVE_IDX_PATH_READY
    if explorer_archive is None or not getattr(explorer_archive, "FULL_PATH", None):
        return None
    full_path = Path(explorer_archive.FULL_PATH)
    if not full_path.exists():
        return None
    try:
        src_mtime_ns = full_path.stat().st_mtime_ns
    except OSError:
        return None

    candidates = [Path(dest_path)] if dest_path else [PV2_ARCHIVE_INDEX_PATH, PV2_ARCHIVE_INDEX_TMP_PATH]
    if not force:
        for cand in candidates:
            if _is_valid_archive_index(cand, src_mtime_ns):
                _ARCHIVE_IDX_PATH_READY = cand
                return cand

    db_path = _get_explorer_archive_db()
    if not db_path:
        return None

    src_conn = _read_only_sqlite(Path(db_path))
    if src_conn is None:
        return None

    target_path = None
    for cand in candidates:
        try:
            cand.parent.mkdir(parents=True, exist_ok=True)
            tmp_probe = cand.with_suffix(".probe")
            tmp_probe.write_bytes(b"")
            tmp_probe.unlink(missing_ok=True)
            target_path = cand
            break
        except OSError:
            continue

    if target_path is None:
        src_conn.close()
        return None

    tmp_path = target_path.with_suffix(".sqlite.tmp")
    dst_conn: Optional[sqlite3.Connection] = None
    total_valid = 0
    try:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        dst_conn = sqlite3.connect(str(tmp_path), timeout=30.0)
        dst_conn.execute("PRAGMA journal_mode=OFF")
        dst_conn.execute("PRAGMA synchronous=OFF")
        dst_conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        dst_conn.execute("CREATE TABLE sigs (match_id TEXT PRIMARY KEY, ah_bucket REAL NOT NULL, ou REAL, sig_json TEXT NOT NULL)")

        cur = src_conn.execute("SELECT match_id, payload FROM matches")
        processed = 0
        batch_rows: List[Tuple[str, float, Optional[float], str]] = []
        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for r in rows:
                processed += 1
                try:
                    obj = json.loads(zlib.decompress(r["payload"]))
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                sc = _parse_score(obj.get("final_score") or obj.get("score"))
                if not sc:
                    continue
                sig = _extract_target_signature(obj)
                if sig.get("ah") is None:
                    continue
                cov, _ = _eval_match_fav_cover(sc[0], sc[1], sig["ah"])
                sig["result"] = "W" if sc[0] > sc[1] else ("L" if sc[0] < sc[1] else "D")
                sig["cover_result"] = _collapse_cover_3(cov) or "PUSH"
                b_key = float(normalize_ah_bucket(sig["ah"]))
                ou_v = float(sig["ou"]) if sig.get("ou") is not None else None
                batch_rows.append((str(r["match_id"]), b_key, ou_v, json.dumps(sig, ensure_ascii=False, separators=(",", ":"))))
                total_valid += 1
            if batch_rows:
                dst_conn.executemany("INSERT OR REPLACE INTO sigs VALUES (?,?,?,?)", batch_rows)
                batch_rows.clear()
            if processed % 3000 == 0:
                dst_conn.commit()
                gc.collect()

        dst_conn.execute("CREATE INDEX idx_sigs_bucket ON sigs(ah_bucket)")
        dst_conn.execute("INSERT OR REPLACE INTO meta VALUES ('total_count', ?)", (str(total_valid),))
        dst_conn.execute("INSERT OR REPLACE INTO meta VALUES ('source_mtime_ns', ?)", (str(src_mtime_ns),))
        dst_conn.commit()
        dst_conn.close()
        dst_conn = None
        os.replace(tmp_path, target_path)
        _ARCHIVE_IDX_PATH_READY = target_path
        return target_path
    except Exception:
        if dst_conn is not None:
            try:
                dst_conn.close()
            except Exception:
                pass
        if tmp_path.exists():
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        return None
    finally:
        src_conn.close()


def _ensure_archive_signature_index() -> Optional[Path]:
    global _ARCHIVE_IDX_PATH_READY
    if explorer_archive is None or not getattr(explorer_archive, "FULL_PATH", None):
        return None
    full_path = Path(explorer_archive.FULL_PATH)
    if not full_path.exists():
        return None
    try:
        src_mtime_ns = full_path.stat().st_mtime_ns
    except OSError:
        return None

    with _ARCHIVE_IDX_LOCK:
        for cand in (PV2_ARCHIVE_INDEX_PATH, PV2_ARCHIVE_INDEX_TMP_PATH):
            if _is_valid_archive_index(cand, src_mtime_ns):
                _ARCHIVE_IDX_PATH_READY = cand
                return cand
        return build_archive_signature_index()


def _query_archive_sigs_by_bucket(
    idx_path: Path,
    target_bucket: float,
    target_ah: float,
) -> Tuple[List[Tuple[str, Dict[str, Any]]], int]:
    conn = _read_only_sqlite(idx_path)
    if conn is None:
        return [], 0
    pool_raw: List[Tuple[str, Dict[str, Any]]] = []
    total_scanned = 0
    try:
        meta_row = conn.execute("SELECT value FROM meta WHERE key = 'total_count' LIMIT 1").fetchone()
        if meta_row and meta_row["value"]:
            total_scanned = int(meta_row["value"])
        else:
            cnt_row = conn.execute("SELECT COUNT(*) AS cnt FROM sigs").fetchone()
            total_scanned = int(cnt_row["cnt"] or 0) if cnt_row else 0

        rows = conn.execute(
            "SELECT match_id, sig_json FROM sigs WHERE ah_bucket = ?",
            (target_bucket,),
        ).fetchall()
        for r in rows:
            try:
                pool_raw.append((str(r["match_id"]), json.loads(r["sig_json"])))
            except Exception:
                continue

        if len(pool_raw) < 35:
            avail_buckets = [
                float(row["ah_bucket"])
                for row in conn.execute("SELECT DISTINCT ah_bucket FROM sigs").fetchall()
                if row["ah_bucket"] is not None
            ]
            same_sign_buckets = sorted(
                (b for b in avail_buckets if b != target_bucket and (
                    (target_ah > 0.01 and b > 0)
                    or (target_ah < -0.01 and b < 0)
                    or (abs(target_ah) <= 0.01 and abs(b) <= 0.51)
                )),
                key=lambda b: abs(b - target_bucket),
            )
            for b in same_sign_buckets:
                extra_rows = conn.execute(
                    "SELECT match_id, sig_json FROM sigs WHERE ah_bucket = ?",
                    (b,),
                ).fetchall()
                for r in extra_rows:
                    try:
                        pool_raw.append((str(r["match_id"]), json.loads(r["sig_json"])))
                    except Exception:
                        continue
                if len(pool_raw) >= 500:
                    break
    finally:
        conn.close()
    return pool_raw, total_scanned


def _lookup_ou_from_archive_index(match_id: str) -> Optional[float]:
    idx_path = _ARCHIVE_IDX_PATH_READY
    if not match_id or idx_path is None or not idx_path.is_file():
        return None
    conn = _read_only_sqlite(idx_path)
    if conn is None:
        return None
    try:
        row = conn.execute("SELECT ou FROM sigs WHERE match_id = ? LIMIT 1", (str(match_id),)).fetchone()
        if row and row["ou"] is not None:
            return float(row["ou"])
    except Exception:
        return None
    finally:
        conn.close()
    return None


def _fetch_match_payloads_bulk(match_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    ids = [str(m).strip() for m in match_ids if str(m).strip()]
    if not ids:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    conn = _read_only_sqlite(APP_DB_PATH)
    if conn is not None:
        try:
            chunk_size = 400
            for i in range(0, len(ids), chunk_size):
                chunk = ids[i:i + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT match_id, score, match_date, handicap, payload_json FROM matches WHERE match_id IN ({placeholders})",
                    chunk,
                ).fetchall()
                for r in rows:
                    mid = str(r["match_id"])
                    try:
                        payload = json.loads(r["payload_json"])
                        if isinstance(payload, dict):
                            if not payload.get("final_score") and r["score"]:
                                payload["final_score"] = r["score"]
                            if not payload.get("match_date") and r["match_date"]:
                                payload["match_date"] = r["match_date"]
                            out[mid] = payload
                    except (TypeError, ValueError):
                        continue
        finally:
            conn.close()

    still_missing = [mid for mid in ids if mid not in out]
    if still_missing:
        out.update(_fetch_archive_payloads_bulk(still_missing))
    return out


def _fetch_h2h_payloads_bulk(match_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Lee en lote `h2h_stadium`/`h2h_general` desde `app_data.db` para resolver líneas O/U históricas."""
    ids = [str(m).strip() for m in match_ids if str(m).strip()]
    if not ids:
        return {}
    conn = _read_only_sqlite(APP_DB_PATH)
    if conn is None:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    try:
        chunk_size = 200
        for i in range(0, len(ids), chunk_size):
            chunk = ids[i:i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            try:
                rows = conn.execute(
                    f"SELECT match_id, "
                    f"COALESCE(json_extract(payload_json, '$.h2h_stadium'), json_extract(payload_json, '$.h2h_general')) AS h2h_raw "
                    f"FROM matches WHERE match_id IN ({placeholders})",
                    chunk,
                ).fetchall()
            except Exception:
                return out
            for r in rows:
                mid = str(r["match_id"])
                raw = r["h2h_raw"]
                if isinstance(raw, dict):
                    out[mid] = raw
                elif isinstance(raw, str) and raw.strip():
                    try:
                        parsed = json.loads(raw)
                        if isinstance(parsed, dict):
                            out[mid] = parsed
                    except (TypeError, ValueError):
                        continue
    finally:
        conn.close()
    return out


def _fetch_explorer_cards_bulk(
    match_ids: List[str],
    fallback_payloads: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Any]]:
    """Lee en bloque tarjetas nativas de explorer_cards.sqlite con fallback a explore_matches."""
    ids = [str(m).strip() for m in match_ids if str(m).strip()]
    if not ids:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    conn = _read_only_sqlite(CARDS_DB_PATH)
    if conn is not None:
        try:
            chunk_size = 400
            for i in range(0, len(ids), chunk_size):
                chunk = ids[i:i + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT match_id, result FROM cards WHERE match_id IN ({placeholders}) AND result IS NOT NULL",
                    chunk,
                ).fetchall()
                for r in rows:
                    mid = str(r["match_id"])
                    try:
                        card = json.loads(zlib.decompress(r["result"]))
                        if isinstance(card, dict) and isinstance(card.get("candidate"), dict):
                            out[mid] = card
                    except Exception:
                        continue
        finally:
            conn.close()

    missing = [mid for mid in ids if mid not in out]
    if missing:
        pl_map = dict(fallback_payloads or {})
        still_missing = [mid for mid in missing if mid not in pl_map]
        if still_missing:
            pl_map.update(_fetch_match_payloads_bulk(still_missing))

        batch_pls = [pl_map[mid] for mid in missing if mid in pl_map]
        if batch_pls:
            try:
                built_list = explore_matches(batch_pls, {"limit": len(batch_pls), "include_stats": False})
                for built_card in (built_list or []):
                    if isinstance(built_card, dict):
                        cid = str(built_card.get("match_id") or (built_card.get("candidate") or {}).get("match_id") or "").strip()
                        if cid:
                            out[cid] = built_card
            except Exception:
                pass

        for mid in missing:
            if mid in out:
                continue
            pl = pl_map.get(mid)
            if not pl:
                continue
            try:
                built = explore_matches([pl], {"limit": 1, "include_stats": False})
                if built and isinstance(built[0], dict):
                    out[mid] = built[0]
            except Exception:
                continue
    return out


def _build_cards_cache() -> None:
    global _CARDS_BY_BUCKET, _CARDS_ALL, _CARDS_META, _OU_BY_MATCH_ID
    now = time.monotonic()
    with _CARDS_LOCK:
        if _CARDS_ALL and (now - _CARDS_META["checked_at"]) < _CARDS_TTL_SECONDS:
            return

        if CARDS_DB_PATH.exists():
            conn = _read_only_sqlite(CARDS_DB_PATH)
            if conn is not None:
                try:
                    row_ver = conn.execute(
                        "SELECT COUNT(*) AS cnt, MAX(updated) AS mx FROM cards WHERE result IS NOT NULL"
                    ).fetchone()
                    cnt = int(row_ver["cnt"]) if row_ver else 0
                    mx = row_ver["mx"] if row_ver else None
                    if (
                        _CARDS_ALL
                        and _CARDS_META["source"] == "cards"
                        and _CARDS_META["count"] == cnt
                        and _CARDS_META["max_updated"] == mx
                    ):
                        _CARDS_META["checked_at"] = now
                        return

                    by_bucket: Dict[float, List[Tuple[str, Dict[str, Any]]]] = {}
                    all_rows: List[Tuple[str, float, Dict[str, Any]]] = []
                    ou_by_id: Dict[str, float] = {}
                    cur = conn.execute("SELECT match_id, doc FROM cards WHERE result IS NOT NULL")
                    while True:
                        batch = cur.fetchmany(2000)
                        if not batch:
                            break
                        for r in batch:
                            try:
                                doc = json.loads(r["doc"])
                            except (TypeError, ValueError):
                                continue
                            if not isinstance(doc, dict):
                                continue
                            mid = str(r["match_id"])
                            ou_val = _parse_ou(doc.get("ou"))
                            if ou_val is not None:
                                ou_by_id[mid] = ou_val
                            if doc.get("result") not in ("W", "D", "L"):
                                continue
                            if doc.get("cover_result") not in ("COVER", "NO_COVER", "PUSH"):
                                continue
                            ah_val = _parse_ah(doc.get("ah"))
                            if ah_val is None:
                                continue
                            b_key = float(normalize_ah_bucket(ah_val))
                            by_bucket.setdefault(b_key, []).append((mid, doc))
                            all_rows.append((mid, b_key, doc))

                    _CARDS_BY_BUCKET = by_bucket
                    _CARDS_ALL = all_rows
                    _OU_BY_MATCH_ID = ou_by_id
                    _CARDS_META = {"count": cnt, "max_updated": mx, "checked_at": now, "source": "cards"}
                    return
                finally:
                    conn.close()

        # Fallback si no existe explorer_cards.sqlite: construir caché ligera desde app_data.db
        conn = _read_only_sqlite(APP_DB_PATH)
        if conn is None:
            return
        try:
            row_ver = conn.execute(
                "SELECT COUNT(*) AS cnt, MAX(updated_at) AS mx FROM matches WHERE state='historical'"
            ).fetchone()
            cnt = int(row_ver["cnt"]) if row_ver else 0
            mx = row_ver["mx"] if row_ver else None
            if _CARDS_ALL and _CARDS_META["source"] == "sql" and _CARDS_META["count"] == cnt and _CARDS_META["max_updated"] == mx:
                _CARDS_META["checked_at"] = now
                return

            by_bucket = {}
            all_rows = []
            ou_by_id = {}
            cur = conn.execute(
                "SELECT match_id, score, handicap, COALESCE(explorer_search_json, explorer_json, payload_json) AS raw "
                "FROM matches WHERE state='historical' AND score IS NOT NULL AND handicap IS NOT NULL"
            )
            while True:
                batch = cur.fetchmany(1000)
                if not batch:
                    break
                for r in batch:
                    sc = _parse_score(r["score"])
                    if not sc:
                        continue
                    try:
                        m = json.loads(r["raw"])
                    except (TypeError, ValueError):
                        continue
                    if not isinstance(m, dict):
                        continue
                    sig = _extract_target_signature(m)
                    mid = str(r["match_id"])
                    if sig.get("ou") is not None:
                        ou_by_id[mid] = float(sig["ou"])
                    if sig.get("ah") is None:
                        continue
                    cov, _ = _eval_match_fav_cover(sc[0], sc[1], sig["ah"])
                    sig["result"] = "W" if sc[0] > sc[1] else ("L" if sc[0] < sc[1] else "D")
                    sig["cover_result"] = _collapse_cover_3(cov) or "PUSH"
                    b_key = float(normalize_ah_bucket(sig["ah"]))
                    by_bucket.setdefault(b_key, []).append((mid, sig))
                    all_rows.append((mid, b_key, sig))

            _CARDS_BY_BUCKET = by_bucket
            _CARDS_ALL = all_rows
            _OU_BY_MATCH_ID = ou_by_id
            _CARDS_META = {"count": cnt, "max_updated": mx, "checked_at": now, "source": "sql"}
        finally:
            conn.close()


def _parse_mov_direction(raw_mov: Any, is_general: bool = False) -> Optional[str]:
    if not raw_mov or raw_mov == "N/A":
        return None
    s = str(raw_mov).replace("→", "->").replace(",", ".").replace(" ", "")
    if "->" not in s:
        return None
    p1, p2 = s.split("->", 1)
    v1 = _parse_ah(p1)
    v2 = _parse_ah(p2)
    if v1 is None or v2 is None:
        return None
    b1 = normalize_ah_bucket(v1)
    b2 = normalize_ah_bucket(v2)
    if is_general:
        a1, a2 = abs(b1), abs(b2)
        return "UP" if a2 > a1 else ("DOWN" if a2 < a1 else "SAME")
    return "UP" if b2 > b1 else ("DOWN" if b2 < b1 else "SAME")


def _extract_target_signature(match_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Extrae firma de mercado compatible con los docs de explorer_cards.sqlite."""
    odds = match_payload.get("main_match_odds") or {}
    ah = _parse_ah(odds.get("ah_linea") if isinstance(odds, dict) else None)
    if ah is None:
        ah = _parse_ah(match_payload.get("handicap") or match_payload.get("ah_line"))
    ou = _parse_ou(odds.get("goals_linea") if isinstance(odds, dict) else None)
    if ou is None:
        ou = _parse_ou(match_payload.get("goal_line") or match_payload.get("ou_line"))

    home_name = str(match_payload.get("home_name") or match_payload.get("home_team") or "")
    away_name = str(match_payload.get("away_name") or match_payload.get("away_team") or "")
    fav_side = _get_favorite_side(ah)

    def _build_prev(node: Any, team_name: str, default_is_home: bool) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict) or not node.get("score"):
            return None
        sc = _parse_score(node.get("score"))
        if not sc:
            return None
        p_ah = _parse_ah(node.get("handicap_line_raw") or node.get("ah") or node.get("handicap"))
        nh = node.get("home_team") or node.get("home") or ""
        na = node.get("away_team") or node.get("away") or ""
        if _same_team(team_name, nh):
            is_h = True
        elif _same_team(team_name, na):
            is_h = False
        else:
            is_h = default_is_home
        cov_code, _ = _eval_team_ah(sc[0], sc[1], p_ah, is_h)
        td = (sc[0] - sc[1]) if is_h else (sc[1] - sc[0])
        wdl = "HOME_WIN" if td > 0 else ("AWAY_WIN" if td < 0 else "DRAW")
        return {
            "ah": p_ah,
            "cover_result": _collapse_cover_3(cov_code),
            "wdl": wdl,
            "team_is_home": is_h,
        }

    prev_home = _build_prev(match_payload.get("last_home_match"), home_name, True)
    prev_away = _build_prev(match_payload.get("last_away_match"), away_name, False)

    fav_prev = None
    if fav_side == "HOME" and prev_home and prev_home.get("ah") is not None and ah is not None:
        fml = prev_home["ah"] if prev_home["team_is_home"] else -prev_home["ah"]
        mov = "SAME" if abs(abs(ah) - fml) < 0.01 else ("UP" if abs(ah) > fml else "DOWN")
        fav_prev = {"team_market_line": fml, "cover_result": prev_home["cover_result"], "movement_direction": mov}
    elif fav_side == "AWAY" and prev_away and prev_away.get("ah") is not None and ah is not None:
        fml = (-prev_away["ah"]) if not prev_away["team_is_home"] else prev_away["ah"]
        mov = "SAME" if abs(abs(ah) - fml) < 0.01 else ("UP" if abs(ah) > fml else "DOWN")
        fav_prev = {"team_market_line": fml, "cover_result": prev_away["cover_result"], "movement_direction": mov}

    def _build_h2h_node(key: str, is_gen: bool) -> Optional[Dict[str, Any]]:
        mad = match_payload.get("market_analysis_data")
        if isinstance(mad, dict) and isinstance(mad.get("stadium" if not is_gen else "general"), dict):
            node = mad["stadium" if not is_gen else "general"]
            mov_raw = node.get("movement")
            sc_raw = node.get("result") or node.get("score")
            sc = _parse_score(sc_raw)
            real_wdl = None
            if sc and ah is not None:
                fav_loc = ah > 0
                diff = ((sc[1] - sc[0]) if fav_loc else (sc[0] - sc[1])) if is_gen else ((sc[0] - sc[1]) if fav_loc else (sc[1] - sc[0]))
                real_wdl = "COVER" if diff > abs(ah) else ("NO_COVER" if diff < abs(ah) else "PUSH")
            return {
                "mov_direction": _parse_mov_direction(mov_raw, is_general=is_gen),
                "real_wdl": real_wdl,
            }
        raw_node = match_payload.get(key)
        if isinstance(raw_node, dict):
            sc_raw = raw_node.get("res1") or raw_node.get("res6") or raw_node.get("score")
            ah_raw = _parse_ah(raw_node.get("ah1") or raw_node.get("ah6") or raw_node.get("ah"))
            sc = _parse_score(sc_raw)
            mov_dir = None
            if ah_raw is not None and ah is not None:
                mov_dir = _parse_mov_direction(f"{ah_raw}->{ah}", is_general=is_gen)
            real_wdl = None
            if sc and ah is not None:
                fav_loc = ah > 0
                diff = ((sc[1] - sc[0]) if fav_loc else (sc[0] - sc[1])) if is_gen else ((sc[0] - sc[1]) if fav_loc else (sc[1] - sc[0]))
                real_wdl = "COVER" if diff > abs(ah) else ("NO_COVER" if diff < abs(ah) else "PUSH")
            if mov_dir or real_wdl:
                return {"mov_direction": mov_dir, "real_wdl": real_wdl}
        return None

    col3 = match_payload.get("h2h_col3")
    col3_ah = _parse_ah(col3.get("handicap") or col3.get("ah")) if isinstance(col3, dict) and col3.get("status") == "found" else None
    ci = match_payload.get("comparativas_indirectas") or {}
    ind_l = ci.get("left") if isinstance(ci, dict) else None
    ind_r = ci.get("right") if isinstance(ci, dict) else None
    ind_l_ah = _parse_ah(ind_l.get("ah_line") or ind_l.get("ah")) if isinstance(ind_l, dict) else None
    ind_r_ah = _parse_ah(ind_r.get("ah_line") or ind_r.get("ah")) if isinstance(ind_r, dict) else None

    return {
        "ah": ah,
        "ou": ou,
        "prev_home": prev_home,
        "prev_away": prev_away,
        "favorite_previous": fav_prev,
        "h2h_stadium": _build_h2h_node("h2h_stadium", False),
        "h2h_general": _build_h2h_node("h2h_general", True),
        "h2h_col3": {"ah": col3_ah} if col3_ah is not None else None,
        "ind_local": {"ah": ind_l_ah} if ind_l_ah is not None else None,
        "ind_visitante": {"ah": ind_r_ah} if ind_r_ah is not None else None,
    }


def _parse_movement_pair(raw_mov: Any) -> Tuple[Optional[float], Optional[float]]:
    if not raw_mov or raw_mov == "N/A":
        return None, None
    s = str(raw_mov).replace(",", ".")
    m = re.search(r"([+-]?\d*\.?\d+)\s*(?:→|->|\ba\b|\bto\b)\s*([+-]?\d*\.?\d+)", s)
    if not m:
        return None, None
    return _parse_ah(m.group(1)), _parse_ah(m.group(2))


def _resolve_node_ou(node: Any, fallback_match_id: Optional[str] = None) -> Optional[float]:
    """Resuelve la línea O/U de un nodo usando campos directos o el índice en RAM."""
    if isinstance(node, dict):
        for k in ("ou", "ou_line", "over_under_line_raw", "ouLine", "goals_line", "goal_line"):
            val = _parse_ou(node.get(k))
            if val is not None:
                return val
        mid = str(node.get("match_id") or node.get("matchIndex") or fallback_match_id or "").strip()
    else:
        mid = str(fallback_match_id or "").strip()
    if mid:
        return _OU_BY_MATCH_ID.get(mid)
    return None


def _extract_h2h_ou_from_h2h_raw(
    h2h_raw: Any,
    is_general: bool,
    fallback_date: str = "",
    fallback_score: str = "",
) -> Tuple[Optional[float], Optional[str]]:
    """Extrae `(ou_line, match_id)` de un nodo crudo `h2h_stadium`/`h2h_general` de `app_data.db`."""
    if not isinstance(h2h_raw, dict):
        return None, None

    mid_key = "match6_id" if is_general else "match1_id"
    ou_key = "ou6" if is_general else "ou1"
    date_key = "date6" if is_general else "date1"

    m_id = str(h2h_raw.get(mid_key) or h2h_raw.get("match_id") or "").strip()
    h_ou = _parse_ou(h2h_raw.get(ou_key) or h2h_raw.get("ou") or h2h_raw.get("ou_line"))

    all_matches = h2h_raw.get("all_matches")
    if h_ou is None and isinstance(all_matches, list) and all_matches:
        matched_entry = None
        if m_id:
            for entry in all_matches:
                if isinstance(entry, dict) and str(entry.get("matchIndex") or "").strip() == m_id:
                    matched_entry = entry
                    break
        if matched_entry is None:
            target_date = str(h2h_raw.get(date_key) or fallback_date or "").strip()[:10]
            target_score = str(fallback_score or "").strip().replace("-", ":")
            if target_date and target_date != "N/A":
                date_matches = [
                    entry for entry in all_matches
                    if isinstance(entry, dict) and str(entry.get("date") or "").strip()[:10] == target_date
                ]
                if len(date_matches) == 1:
                    matched_entry = date_matches[0]
                elif len(date_matches) > 1:
                    if target_score:
                        for entry in date_matches:
                            e_score = str(entry.get("score") or "").strip().replace("-", ":")
                            if e_score == target_score:
                                matched_entry = entry
                                break
                    if matched_entry is None:
                        matched_entry = date_matches[0]
        if isinstance(matched_entry, dict):
            if not m_id:
                m_id = str(matched_entry.get("matchIndex") or "").strip()
            h_ou = _parse_ou(
                matched_entry.get("ouLine")
                or matched_entry.get("ou")
                or matched_entry.get("over_under_line_raw")
            )

    if h_ou is None and m_id:
        h_ou = _OU_BY_MATCH_ID.get(m_id)
        if h_ou is None:
            h_ou = _lookup_ou_from_archive_index(m_id)

    return h_ou, (m_id or None)


def _apply_ou_enrichment_to_node(
    node: Dict[str, Any],
    own_ou: Optional[float],
    curr_ou: Optional[float],
    include_vs_curr: bool = False,
) -> Dict[str, Any]:
    """Calcula movimiento y resultado O/U sobre una copia del nodo de previa o H2H."""
    if own_ou is not None:
        node["ou"] = _format_ah(own_ou)
        if curr_ou is not None:
            delta = round(curr_ou - own_ou, 2)
            mov_dir = "IGUAL" if abs(delta) <= 0.05 else ("SUBE" if delta > 0 else "BAJA")
            node["ou_delta"] = delta
            node["ou_mov_dir"] = mov_dir
            node["ou_movement"] = f"{_format_ah(own_ou)} → {_format_ah(curr_ou)}"

    sc = _parse_score(node.get("score"))
    if sc:
        total_g = sc[0] + sc[1]
        ou_ref = own_ou if own_ou is not None else curr_ou
        if ou_ref:
            node["ou_res"] = "OVER" if total_g > ou_ref + 0.01 else ("UNDER" if total_g < ou_ref - 0.01 else "PUSH")
            node["ou_ref_source"] = "own" if own_ou is not None else "curr"
        if include_vs_curr and curr_ou:
            node["ou_res_vs_curr"] = "OVER" if total_g > curr_ou + 0.01 else ("UNDER" if total_g < curr_ou - 0.01 else "PUSH")
    return node


def _enrich_card_ou(
    card: Dict[str, Any],
    target_payload: Optional[Dict[str, Any]] = None,
    h2h_raw: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Enriquece prev_home/prev_away y h2h_stadium/h2h_general con línea O/U, movimiento y resultado O/U."""
    if not isinstance(card, dict):
        return card
    card_copy = dict(card)
    cand = card_copy.get("candidate") or {}
    curr_ou = _parse_ou(cand.get("ou_line") if isinstance(cand, dict) else None)
    pl = target_payload if isinstance(target_payload, dict) else None

    for card_key, last_key, rec_key in (
        ("prev_home", "last_home_match", "recent_home_matches_all"),
        ("prev_away", "last_away_match", "recent_away_matches_all"),
    ):
        raw_node = card_copy.get(card_key)
        if not isinstance(raw_node, dict):
            continue
        node = dict(raw_node)
        p_ou = _resolve_node_ou(node)

        if p_ou is None and pl is not None:
            last_obj = pl.get(last_key)
            if isinstance(last_obj, dict):
                p_ou = _resolve_node_ou(last_obj, node.get("match_id"))
            if p_ou is None:
                rec_list = pl.get(rec_key) or []
                if isinstance(rec_list, list) and rec_list:
                    n_date = str(node.get("date") or "").strip()
                    for r in rec_list[:4]:
                        if isinstance(r, dict) and (not n_date or str(r.get("date") or "").strip() == n_date):
                            p_ou = _resolve_node_ou(r, node.get("match_id"))
                            if p_ou is not None:
                                break

        if p_ou is None:
            mid = str(node.get("match_id") or "").strip()
            if mid:
                p_ou = _OU_BY_MATCH_ID.get(mid)
                if p_ou is None:
                    p_ou = _lookup_ou_from_archive_index(mid)

        card_copy[card_key] = _apply_ou_enrichment_to_node(node, p_ou, curr_ou, include_vs_curr=False)

    for card_key, is_gen in (("h2h_stadium", False), ("h2h_general", True)):
        raw_node = card_copy.get(card_key)
        if not isinstance(raw_node, dict):
            continue
        mov_val = str(raw_node.get("movement") or "").strip()
        sc_val = str(raw_node.get("score") or "").strip()
        if (not mov_val or mov_val == "N/A") and (not sc_val or sc_val in ("?:?", "?-?", "N/A")):
            continue

        node = dict(raw_node)
        h_ou = _resolve_node_ou(node)
        if h_ou is None:
            raw_src = h2h_raw if isinstance(h2h_raw, dict) else None
            if raw_src is None and pl is not None:
                raw_src = pl.get(card_key) or pl.get("h2h_stadium") or pl.get("h2h_general")
            if isinstance(raw_src, dict):
                h_ou, found_mid = _extract_h2h_ou_from_h2h_raw(
                    raw_src,
                    is_gen,
                    fallback_date=str(node.get("date") or ""),
                    fallback_score=sc_val,
                )
                if found_mid and not node.get("match_id"):
                    node["match_id"] = found_mid

        if h_ou is None:
            mid = str(node.get("match_id") or "").strip()
            if mid:
                h_ou = _OU_BY_MATCH_ID.get(mid)
                if h_ou is None:
                    h_ou = _lookup_ou_from_archive_index(mid)

        card_copy[card_key] = _apply_ou_enrichment_to_node(node, h_ou, curr_ou, include_vs_curr=True)

    return card_copy


def _extract_card_features(card: Dict[str, Any]) -> Dict[str, Any]:
    """Extrae todas las dimensiones comparables visibles en una fila del Explorador."""
    cand = card.get("candidate") or {}
    ah = _parse_ah(cand.get("ah_real"))
    ah_bucket = float(normalize_ah_bucket(ah)) if ah is not None else None
    ou = _parse_ou(cand.get("ou_line"))
    fav_side = _get_favorite_side(ah)
    home_name = str(cand.get("home") or cand.get("home_team") or "").strip()
    away_name = str(cand.get("away") or cand.get("away_team") or "").strip()

    def _feat_prev(node: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict) or (not node.get("score") and node.get("ah") is None):
            return None
        p_ah = _parse_ah(node.get("ah"))
        p_bucket = float(normalize_ah_bucket(p_ah)) if p_ah is not None else None
        cov = _collapse_cover_3(node.get("cover_result"))
        mov_dir = _parse_mov_direction(node.get("movement"), is_general=False)
        if not mov_dir and p_ah is not None and ah is not None:
            mov_dir = _parse_mov_direction(f"{p_ah}->{ah}", is_general=False)
        p_ou = _resolve_node_ou(node)
        ou_delta = node.get("ou_delta")
        ou_mov_dir = node.get("ou_mov_dir")
        if p_ou is not None and ou is not None and ou_delta is None:
            ou_delta = round(ou - p_ou, 2)
            ou_mov_dir = "IGUAL" if abs(ou_delta) <= 0.05 else ("SUBE" if ou_delta > 0 else "BAJA")
        sc = _parse_score(node.get("score"))
        total_g = (sc[0] + sc[1]) if sc else None
        ou_res = node.get("ou_res")
        if not ou_res and total_g is not None:
            ou_ref = p_ou if p_ou is not None else ou
            if ou_ref:
                ou_res = "OVER" if total_g > ou_ref + 0.01 else ("UNDER" if total_g < ou_ref - 0.01 else "PUSH")
        return {
            "ah": p_ah,
            "ah_bucket": p_bucket,
            "cover": cov,
            "wdl": node.get("wdl"),
            "mov_dir": mov_dir,
            "score": node.get("score"),
            "home_team": node.get("home_team"),
            "away_team": node.get("away_team"),
            "ou": p_ou,
            "ou_delta": ou_delta,
            "ou_mov_dir": ou_mov_dir,
            "ou_res": ou_res,
            "total_goals": total_g,
        }

    prev_home = _feat_prev(card.get("prev_home"))
    prev_away = _feat_prev(card.get("prev_away"))

    st_raw = card.get("h2h_stadium")
    gen_raw = card.get("h2h_general")
    is_same_h2h = bool(
        isinstance(st_raw, dict) and isinstance(gen_raw, dict)
        and st_raw.get("date") and st_raw.get("date") == gen_raw.get("date")
        and st_raw.get("score") and st_raw.get("score") == gen_raw.get("score")
    )

    def _feat_h2h(node: Any, is_gen: bool) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None
        mov = node.get("movement")
        sc_raw = node.get("score")
        if (not mov or mov == "N/A") and (not sc_raw or sc_raw in ("?:?", "?-?", "N/A")):
            return None
        s_ah, e_ah = _parse_movement_pair(mov)
        s_bucket = float(normalize_ah_bucket(s_ah)) if s_ah is not None else None
        e_bucket = float(normalize_ah_bucket(e_ah)) if e_ah is not None else None
        mov_dir = node.get("mov_direction") or _parse_mov_direction(mov, is_general=is_gen)
        sc = _parse_score(sc_raw)
        cov = _collapse_cover_3(node.get("real_wdl"))
        real_res = None
        total_g = (sc[0] + sc[1]) if sc else None
        h_ou = _resolve_node_ou(node)
        ou_delta = node.get("ou_delta")
        ou_mov_dir = node.get("ou_mov_dir")
        ou_movement = node.get("ou_movement")
        if h_ou is not None and ou is not None and ou_delta is None:
            ou_delta = round(ou - h_ou, 2)
            ou_mov_dir = "IGUAL" if abs(ou_delta) <= 0.05 else ("SUBE" if ou_delta > 0 else "BAJA")
            ou_movement = ou_movement or f"{_format_ah(h_ou)} → {_format_ah(ou)}"
        ou_res = node.get("ou_res")
        ou_ref_source = node.get("ou_ref_source")
        if not ou_res and total_g is not None:
            ou_ref = h_ou if h_ou is not None else ou
            if ou_ref:
                ou_res = "OVER" if total_g > ou_ref + 0.01 else ("UNDER" if total_g < ou_ref - 0.01 else "PUSH")
                ou_ref_source = ou_ref_source or ("own" if h_ou is not None else "curr")
        ou_vs_curr = node.get("ou_res_vs_curr")
        if sc:
            fav_is_loc = (ah is not None and ah > 0)
            if is_gen:
                fav_diff = (sc[1] - sc[0]) if fav_is_loc else (sc[0] - sc[1])
            else:
                fav_diff = (sc[0] - sc[1]) if fav_is_loc else (sc[1] - sc[0])
            real_res = "FAV_WIN" if fav_diff > 0 else ("DOG_WIN" if fav_diff < 0 else "DRAW")
            if not cov and ah is not None:
                cov = "COVER" if fav_diff > abs(ah) else ("NO_COVER" if fav_diff < abs(ah) else "PUSH")
            if not ou_vs_curr and ou and total_g is not None:
                ou_vs_curr = "OVER" if total_g > ou + 0.01 else ("UNDER" if total_g < ou - 0.01 else "PUSH")
        return {
            "start_ah": s_ah,
            "start_bucket": s_bucket,
            "end_ah": e_ah,
            "end_bucket": e_bucket,
            "mov_dir": mov_dir,
            "cover": cov,
            "real_result": real_res,
            "movement": mov,
            "score": sc_raw,
            "total_goals": total_g,
            "ou": h_ou,
            "ou_delta": ou_delta,
            "ou_mov_dir": ou_mov_dir,
            "ou_movement": ou_movement,
            "ou_res": ou_res,
            "ou_ref_source": ou_ref_source,
            "ou_vs_curr": ou_vs_curr,
        }

    h2h_stadium = _feat_h2h(st_raw, False)
    h2h_general = None if is_same_h2h else _feat_h2h(gen_raw, True)

    # H2H Col3 (MEJORA / IGUALA / EMPEORA + directa / inversa + cover propio + mov AH)
    col3_feat = None
    col3_raw = card.get("h2h_col3")
    if isinstance(col3_raw, dict) and (col3_raw.get("score") or col3_raw.get("ah") is not None or col3_raw.get("handicap") is not None):
        c3_ah = _parse_ah(col3_raw.get("ah") if col3_raw.get("ah") is not None else (col3_raw.get("handicap") or col3_raw.get("ah_line")))
        c3_bucket = float(normalize_ah_bucket(c3_ah)) if c3_ah is not None else None
        c3_sc = _parse_score(col3_raw.get("score"))
        c3_cov = None
        if c3_sc and c3_ah is not None:
            raw_cov, _ = _eval_match_fav_cover(c3_sc[0], c3_sc[1], c3_ah)
            c3_cov = _collapse_cover_3(raw_cov)
        c3_mov = None
        if c3_bucket is not None and ah_bucket is not None:
            diff_abs = abs(ah_bucket) - abs(c3_bucket)
            c3_mov = "IGUAL" if abs(diff_abs) < 0.01 else ("SUBE" if diff_abs > 0 else "BAJA")

        def _wdl_rank(sc_tuple: Optional[Tuple[int, int]], is_h: bool) -> Optional[int]:
            if not sc_tuple:
                return None
            d = (sc_tuple[0] - sc_tuple[1]) if is_h else (sc_tuple[1] - sc_tuple[0])
            return 2 if d > 0 else (1 if d == 0 else 0)

        fav_is_vis = (ah is None) or (ah <= 0)
        fav_wdl = None
        mirror_team = ""
        if not fav_is_vis and prev_home and prev_home.get("score"):
            ph_sc = _parse_score(prev_home["score"])
            fav_played_h = _same_team(home_name, prev_home.get("home_team") or "")
            fav_wdl = _wdl_rank(ph_sc, fav_played_h)
            if prev_away:
                pa_h = str(prev_away.get("home_team") or "")
                pa_a = str(prev_away.get("away_team") or "")
                mirror_team = pa_a if _same_team(away_name, pa_h) else pa_h
        elif fav_is_vis and prev_away and prev_away.get("score"):
            pa_sc = _parse_score(prev_away["score"])
            fav_played_h = _same_team(away_name, prev_away.get("home_team") or "")
            fav_wdl = _wdl_rank(pa_sc, fav_played_h)
            if prev_home:
                ph_h = str(prev_home.get("home_team") or "")
                ph_a = str(prev_home.get("away_team") or "")
                mirror_team = ph_a if _same_team(home_name, ph_h) else ph_h

        c3_home = str(col3_raw.get("home_team") or col3_raw.get("h2h_home_team_name") or "")
        c3_away = str(col3_raw.get("away_team") or col3_raw.get("h2h_away_team_name") or "")
        comp_type = None
        mirror_wdl = None
        if mirror_team:
            m_is_h = _same_team(mirror_team, c3_home)
            m_is_a = _same_team(mirror_team, c3_away)
            base_t = "directa" if m_is_h else ("inversa" if m_is_a else None)
            if base_t:
                comp_type = ("inversa" if base_t == "directa" else "directa") if fav_is_vis else base_t
            if c3_sc and (m_is_h or m_is_a):
                mirror_wdl = _wdl_rank(c3_sc, m_is_h)

        perf = None
        if fav_wdl is not None and mirror_wdl is not None:
            perf = "MEJORA" if fav_wdl > mirror_wdl else ("IGUALA" if fav_wdl == mirror_wdl else "EMPEORA")

        c3_total_g = (c3_sc[0] + c3_sc[1]) if c3_sc else None
        c3_ou = _resolve_node_ou(col3_raw)
        c3_ou_vs_curr = None
        if c3_total_g is not None and ou:
            c3_ou_vs_curr = "OVER" if c3_total_g > ou + 0.01 else ("UNDER" if c3_total_g < ou - 0.01 else "PUSH")

        col3_feat = {
            "ah": c3_ah,
            "ah_bucket": c3_bucket,
            "perf": perf,
            "comp_type": comp_type,
            "cover": c3_cov,
            "ah_mov": c3_mov,
            "score": col3_raw.get("score"),
            "total_goals": c3_total_g,
            "ou": c3_ou,
            "ou_vs_curr": c3_ou_vs_curr,
        }

    # Comparativas Indirectas (Ind. Local e Ind. Visitante)
    def _feat_ind(node: Any, is_subject_home: bool) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None
        i_ah = _parse_ah(node.get("ah_line") if node.get("ah_line") is not None else node.get("ah"))
        i_sc = _parse_score(node.get("score"))
        if i_ah is None and not i_sc:
            return None
        raw_loc = str(node.get("localia") or "").strip()
        loc = None
        if raw_loc:
            loc = "H" if raw_loc in ("L", "H", "Local", "Home") else "A"
        i_bucket = float(normalize_ah_bucket(i_ah)) if i_ah is not None else None
        cov = None
        real_wdl = None
        i_total_g = (i_sc[0] + i_sc[1]) if i_sc else None
        i_ou = _resolve_node_ou(node)
        i_ou_vs_curr = None
        if i_sc:
            is_subj_local = (loc == "H")
            tg = i_sc[0] if is_subj_local else i_sc[1]
            og = i_sc[1] if is_subj_local else i_sc[0]
            real_wdl = "W" if tg > og else ("L" if tg < og else "D")
            if ah is not None and loc is not None:
                subj_persp_ah = -ah if is_subject_home else ah
                net = (tg - og) + subj_persp_ah
                cov = "COVER" if net > 0.01 else ("NO_COVER" if net < -0.01 else "PUSH")
            if i_total_g is not None and ou:
                i_ou_vs_curr = "OVER" if i_total_g > ou + 0.01 else ("UNDER" if i_total_g < ou - 0.01 else "PUSH")
        if not cov and node.get("cover_status"):
            cov = _collapse_cover_3(str(node.get("cover_status")).replace("CUBIERTO", "COVER").replace("NO CUBIERTO", "NO_COVER"))
        own_cov = None
        if i_sc and i_ah is not None and loc is not None:
            raw_own, _ = _eval_team_ah(i_sc[0], i_sc[1], i_ah, loc == "H")
            own_cov = _collapse_cover_3(raw_own)
        return {
            "ah": i_ah,
            "ah_bucket": i_bucket,
            "localia": loc,
            "cover": cov,
            "own_cover": own_cov,
            "real_wdl": real_wdl,
            "score": node.get("score"),
            "total_goals": i_total_g,
            "ou": i_ou,
            "ou_vs_curr": i_ou_vs_curr,
        }

    ind_local = _feat_ind(card.get("ind_local"), True)
    ind_visitante = _feat_ind(card.get("ind_visitante"), False)

    h_rank = _safe_int((card.get("home_standings") or {}).get("ranking"))
    a_rank = _safe_int((card.get("away_standings") or {}).get("ranking"))

    return {
        "ah": ah,
        "ah_bucket": ah_bucket,
        "ou": ou,
        "fav_side": fav_side,
        "prev_home": prev_home,
        "prev_away": prev_away,
        "h2h_stadium": h2h_stadium,
        "h2h_general": h2h_general,
        "h2h_col3": col3_feat,
        "ind_local": ind_local,
        "ind_visitante": ind_visitante,
        "rank_gap": (h_rank - a_rank) if (h_rank is not None and a_rank is not None) else None,
    }


def build_explorer_card(match_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Construye una tarjeta nativa del Explorador para cualquier partido (p.ej. partido objetivo)."""
    if not isinstance(match_payload, dict):
        return None
    tmp = dict(match_payload)
    if not tmp.get("final_score") and not tmp.get("score"):
        tmp["final_score"] = "?:?"
    elif not tmp.get("final_score"):
        tmp["final_score"] = tmp.get("score")
    try:
        built = explore_matches([tmp], {"limit": 1, "include_stats": False})
    except Exception:
        built = []
    if not built or not isinstance(built[0], dict):
        return None
    card = dict(built[0])
    card["match_id"] = str(card.get("match_id") or tmp.get("match_id") or tmp.get("id") or "")

    # Fallback si h2h_stadium / h2h_general venían solo como ah1/res1 o ah6/res6 sin market_analysis_data
    ah_val = _parse_ah((card.get("candidate") or {}).get("ah_real"))
    for key, ah_k, res_k, date_k in (
        ("h2h_stadium", "ah1", "res1", "date1"),
        ("h2h_general", "ah6", "res6", "date6"),
    ):
        if card.get(key) is None and isinstance(tmp.get(key), dict):
            raw_h = tmp[key]
            sc_raw = str(raw_h.get(res_k) or raw_h.get("score") or "").replace("-", ":").strip()
            h_ah = _parse_ah(raw_h.get(ah_k) or raw_h.get("ah"))
            if sc_raw or h_ah is not None:
                is_gen = (key == "h2h_general")
                mov_str = f"{_format_ah(h_ah)} -> {_format_ah(ah_val)}" if (h_ah is not None and ah_val is not None) else "N/A"
                card[key] = {
                    "movement": mov_str,
                    "score": sc_raw,
                    "mov_direction": _parse_mov_direction(mov_str, is_general=is_gen),
                    "date": raw_h.get(date_k) or raw_h.get("date"),
                }
    return _enrich_card_ou(card, target_payload=tmp)


def _select_pattern_candidates(
    target_sig: Dict[str, Any],
    target_feat: Dict[str, Any],
    pool_raw: List[Tuple[str, Dict[str, Any]]],
    target_id: str,
    candidate_pool: int = 180,
) -> Tuple[List[Tuple[int, float, str, Dict[str, Any]]], int]:
    """Selección en cascada por Patrón Base (AH + Prev Home + Prev Away) antes del scoring fino."""
    t_ph = target_feat.get("prev_home") or target_sig.get("prev_home")
    t_pa = target_feat.get("prev_away") or target_sig.get("prev_away")
    t_ph_b = t_ph.get("ah_bucket") if isinstance(t_ph, dict) else None
    t_ph_c = t_ph.get("cover") or t_ph.get("cover_result") if isinstance(t_ph, dict) else None
    t_pa_b = t_pa.get("ah_bucket") if isinstance(t_pa, dict) else None
    t_pa_c = t_pa.get("cover") or t_pa.get("cover_result") if isinstance(t_pa, dict) else None
    if t_ph_b is None and isinstance(t_ph, dict) and t_ph.get("ah") is not None:
        t_ph_b = float(normalize_ah_bucket(_parse_ah(t_ph.get("ah"))))
    if t_pa_b is None and isinstance(t_pa, dict) and t_pa.get("ah") is not None:
        t_pa_b = float(normalize_ah_bucket(_parse_ah(t_pa.get("ah"))))

    t_ah = target_feat.get("ah")
    t_ou = target_feat.get("ou")
    t_st = target_feat.get("h2h_stadium")
    t_ge = target_feat.get("h2h_general")
    t_c3 = target_feat.get("h2h_col3")
    t_il = target_feat.get("ind_local")
    t_iv = target_feat.get("ind_visitante")

    tier1: List[Tuple[int, float, str, Dict[str, Any]]] = []
    tier2: List[Tuple[int, float, str, Dict[str, Any]]] = []
    tier3: List[Tuple[int, float, str, Dict[str, Any]]] = []

    for mid, doc in pool_raw:
        if target_id and mid == target_id:
            continue

        c_ph = doc.get("prev_home") if isinstance(doc.get("prev_home"), dict) else {}
        c_pa = doc.get("prev_away") if isinstance(doc.get("prev_away"), dict) else {}
        c_ph_ah = _parse_ah(c_ph.get("ah"))
        c_pa_ah = _parse_ah(c_pa.get("ah"))
        c_ph_b = float(normalize_ah_bucket(c_ph_ah)) if c_ph_ah is not None else None
        c_pa_b = float(normalize_ah_bucket(c_pa_ah)) if c_pa_ah is not None else None
        c_ph_c = _collapse_cover_3(c_ph.get("cover_result"))
        c_pa_c = _collapse_cover_3(c_pa.get("cover_result"))

        ph_ah_ok = (t_ph_b is not None and c_ph_b is not None and abs(t_ph_b - c_ph_b) < 0.01)
        ph_cov_ok = (t_ph_c is not None and c_ph_c == t_ph_c)
        pa_ah_ok = (t_pa_b is not None and c_pa_b is not None and abs(t_pa_b - c_pa_b) < 0.01)
        pa_cov_ok = (t_pa_c is not None and c_pa_c == t_pa_c)

        has_t_ph = (t_ph_b is not None or t_ph_c is not None)
        has_t_pa = (t_pa_b is not None or t_pa_c is not None)
        ph_full = (not has_t_ph) or ((t_ph_b is None or ph_ah_ok) and (t_ph_c is None or ph_cov_ok))
        pa_full = (not has_t_pa) or ((t_pa_b is None or pa_ah_ok) and (t_pa_c is None or pa_cov_ok))

        c_ah = _parse_ah(doc.get("ah"))
        exact_ah = (t_ah is not None and c_ah is not None and abs(t_ah - c_ah) < 0.01)

        if has_t_ph and has_t_pa:
            if ph_full and pa_full:
                tier = 1
            elif (ph_full and (pa_ah_ok or pa_cov_ok)) or (pa_full and (ph_ah_ok or ph_cov_ok)) or (ph_cov_ok and pa_cov_ok):
                tier = 2
            else:
                tier = 3
        elif has_t_ph or has_t_pa:
            if ph_full and pa_full:
                tier = 1
            elif ph_cov_ok or pa_cov_ok or ph_ah_ok or pa_ah_ok:
                tier = 2
            else:
                tier = 3
        else:
            tier = 1 if exact_ah else 2

        # Afinidad preliminar rápida en Fase 1 para ordenar dentro de cada Tier
        pre = 0.0
        if exact_ah:
            pre += 10.0
        if t_ou is not None:
            c_ou = _parse_ou(doc.get("ou"))
            if c_ou is not None:
                if abs(t_ou - c_ou) <= 0.05:
                    pre += 4.0
                elif abs(t_ou - c_ou) <= 0.26:
                    pre += 2.0
        if ph_ah_ok:
            pre += 5.0
        if ph_cov_ok:
            pre += 5.0
        if pa_ah_ok:
            pre += 5.0
        if pa_cov_ok:
            pre += 5.0

        c_st = doc.get("h2h_stadium") if isinstance(doc.get("h2h_stadium"), dict) else {}
        c_ge = doc.get("h2h_general") if isinstance(doc.get("h2h_general"), dict) else {}
        if t_st:
            if t_st.get("mov_dir") and c_st.get("mov_direction") == t_st["mov_dir"]:
                pre += 4.0
            if t_st.get("cover") and _collapse_cover_3(c_st.get("real_wdl")) == t_st["cover"]:
                pre += 4.0
        if t_ge:
            if t_ge.get("mov_dir") and c_ge.get("mov_direction") == t_ge["mov_dir"]:
                pre += 3.0
            if t_ge.get("cover") and _collapse_cover_3(c_ge.get("real_wdl")) == t_ge["cover"]:
                pre += 3.0

        for xkey, tf in (("h2h_col3", t_c3), ("ind_local", t_il), ("ind_visitante", t_iv)):
            if tf and tf.get("ah_bucket") is not None:
                cx = doc.get(xkey)
                cx_ah = _parse_ah(cx.get("ah")) if isinstance(cx, dict) else None
                if cx_ah is not None and abs(tf["ah_bucket"] - float(normalize_ah_bucket(cx_ah))) < 0.01:
                    pre += 3.5

        item = (tier, pre, mid, doc)
        if tier == 1:
            tier1.append(item)
        elif tier == 2:
            tier2.append(item)
        else:
            tier3.append(item)

    tier1.sort(key=lambda x: x[1], reverse=True)
    tier2.sort(key=lambda x: x[1], reverse=True)
    tier3.sort(key=lambda x: x[1], reverse=True)

    combined = (tier1 + tier2 + tier3)[:candidate_pool]
    return combined, len(tier1)


def _score_deep_similarity(
    target_feat: Dict[str, Any],
    cand_feat: Dict[str, Any],
    target_prof: Dict[str, Any],
    cand_prof: Optional[Dict[str, Any]],
    pattern_tier: int,
    ficha_hydrated: bool = False,
) -> Dict[str, Any]:
    """Calcula Máxima Similitud en 6 pilares: Núcleo+Previos, Líneas O/U, H2H, Indirectas, Col3 y Ficha (si hidratada)."""
    reasons: List[str] = []
    divergences: List[str] = []
    col_hits: Dict[str, str] = {
        "ah": "miss",
        "ou": "miss",
        "prev_home": "miss",
        "prev_away": "miss",
        "h2h_stadium": "miss",
        "h2h_general": "miss",
        "h2h_col3": "miss",
        "ind_local": "miss",
        "ind_visitante": "miss",
    }

    # --- PILAR 1: Núcleo AH + Prev Home + Prev Away (peso 24) ---
    p1_e = 0.0
    p1_p = 0.0
    t_ah, c_ah = target_feat.get("ah"), cand_feat.get("ah")
    if t_ah is not None and c_ah is not None:
        p1_p += 6.0
        if abs(t_ah - c_ah) < 0.01:
            p1_e += 6.0
            col_hits["ah"] = "exact"
            reasons.append(f"AH {_format_ah(c_ah)}")
        elif target_feat.get("ah_bucket") == cand_feat.get("ah_bucket"):
            p1_e += 3.5
            col_hits["ah"] = "partial"

    def _eval_prev_block(tp: Optional[Dict[str, Any]], cp: Optional[Dict[str, Any]], hit_key: str, lbl: str) -> None:
        nonlocal p1_e, p1_p
        if not tp:
            return
        local_e = 0.0
        local_p = 0.0
        if tp.get("ah_bucket") is not None:
            local_p += 3.5
            p1_p += 3.5
            if cp and cp.get("ah") is not None and tp.get("ah") is not None and abs(tp["ah"] - cp["ah"]) < 0.01:
                local_e += 3.5
                p1_e += 3.5
            elif cp and cp.get("ah_bucket") == tp.get("ah_bucket"):
                local_e += 3.0
                p1_e += 3.0
        if tp.get("cover"):
            local_p += 3.0
            p1_p += 3.0
            if cp and cp.get("cover") == tp["cover"]:
                local_e += 3.0
                p1_e += 3.0
        if tp.get("wdl"):
            local_p += 1.5
            p1_p += 1.5
            if cp and cp.get("wdl") == tp["wdl"]:
                local_e += 1.5
                p1_e += 1.5
        if tp.get("mov_dir"):
            local_p += 1.0
            p1_p += 1.0
            if cp and cp.get("mov_dir") == tp["mov_dir"]:
                local_e += 1.0
                p1_e += 1.0
        if local_p > 0:
            ratio = local_e / local_p
            if ratio >= 0.78:
                col_hits[hit_key] = "exact"
                reasons.append(f"{lbl} ✓")
            elif ratio >= 0.48:
                col_hits[hit_key] = "partial"

    _eval_prev_block(target_feat.get("prev_home"), cand_feat.get("prev_home"), "prev_home", "Prev Loc")
    _eval_prev_block(target_feat.get("prev_away"), cand_feat.get("prev_away"), "prev_away", "Prev Vis")

    if pattern_tier == 1:
        p1_p += 2.0
        p1_e += 2.0
        reasons.insert(0, "Patrón Base ✓")
    elif pattern_tier == 2:
        p1_p += 2.0
        p1_e += 1.0

    core_pct = round((p1_e / p1_p) * 100.0, 1) if p1_p > 0 else None

    # --- PILAR 2: Líneas y Movimientos O/U (Goles) (peso 18) ---
    p_ou_e = 0.0
    p_ou_p = 0.0
    t_ou, c_ou = target_feat.get("ou"), cand_feat.get("ou")
    if t_ou is not None:
        p_ou_p += 6.0
        if c_ou is not None:
            diff_ou = abs(t_ou - c_ou)
            if diff_ou <= 0.05:
                p_ou_e += 6.0
                reasons.append(f"O/U {_format_ah(c_ou)}")
            elif diff_ou <= 0.26:
                p_ou_e += 4.2
            elif diff_ou <= 0.51:
                p_ou_e += 2.0

    mov_dir_hits = 0
    mov_dir_exact = 0
    mov_dir_total = 0

    def _eval_prev_ou(tp: Optional[Dict[str, Any]], cp: Optional[Dict[str, Any]], lbl: str) -> None:
        nonlocal p_ou_e, p_ou_p, mov_dir_hits, mov_dir_exact, mov_dir_total
        if not tp:
            return
        if tp.get("ou_mov_dir"):
            mov_dir_total += 1
            p_ou_p += 5.0
            if cp and cp.get("ou_mov_dir") == tp["ou_mov_dir"]:
                p_ou_e += 3.5
                mov_dir_hits += 1
                td = tp.get("ou_delta")
                cd = cp.get("ou_delta")
                if td is not None and cd is not None:
                    d_diff = abs(float(td) - float(cd))
                    if d_diff <= 0.05:
                        p_ou_e += 1.5
                        mov_dir_exact += 1
                        sign = "+" if float(cd) > 0 else ""
                        reasons.append(f"Mov O/U {lbl} {sign}{cd:g} ✓")
                    elif d_diff <= 0.26:
                        p_ou_e += 0.75
                        mov_dir_exact += 1
            elif cp and cp.get("ou_mov_dir") and cp.get("ou_mov_dir") != tp.get("ou_mov_dir"):
                divergences.append(f"Mov O/U {lbl} distinto")
        if tp.get("ou_res"):
            p_ou_p += 1.5
            if cp and cp.get("ou_res") == tp["ou_res"]:
                p_ou_e += 1.5

    _eval_prev_ou(target_feat.get("prev_home"), cand_feat.get("prev_home"), "Loc")
    _eval_prev_ou(target_feat.get("prev_away"), cand_feat.get("prev_away"), "Vis")

    for node_k in ("h2h_stadium", "h2h_general", "ind_local", "ind_visitante"):
        tn = target_feat.get(node_k)
        cn = cand_feat.get(node_k)
        if tn and tn.get("ou_vs_curr"):
            p_ou_p += 1.0
            if cn and cn.get("ou_vs_curr") == tn["ou_vs_curr"]:
                p_ou_e += 1.0

    ou_pct = round((p_ou_e / p_ou_p) * 100.0, 1) if p_ou_p > 0 else None
    if ou_pct is not None:
        if ou_pct >= 72.0:
            col_hits["ou"] = "exact"
        elif ou_pct >= 45.0:
            col_hits["ou"] = "partial"
    ou_mov_match = bool(
        mov_dir_exact >= 1
        or (mov_dir_total >= 2 and mov_dir_hits >= 2)
        or (mov_dir_total == 1 and mov_dir_hits >= 1)
    )

    # --- PILAR 3: Movimiento de Hándicap y Resultado H2H (peso 18) ---
    p2_e = 0.0
    p2_p = 0.0

    def _eval_h2h_block(th: Optional[Dict[str, Any]], ch: Optional[Dict[str, Any]], w_scale: float, hit_key: str, lbl: str) -> None:
        nonlocal p2_e, p2_p
        if not th:
            return
        local_e = 0.0
        local_p = 0.0
        if th.get("start_bucket") is not None:
            local_p += 4.5 * w_scale
            p2_p += 4.5 * w_scale
            if ch and ch.get("start_ah") is not None and th.get("start_ah") is not None and abs(th["start_ah"] - ch["start_ah"]) < 0.01:
                local_e += 4.5 * w_scale
                p2_e += 4.5 * w_scale
            elif ch and ch.get("start_bucket") == th.get("start_bucket"):
                local_e += 3.6 * w_scale
                p2_e += 3.6 * w_scale
        if th.get("mov_dir"):
            local_p += 3.0 * w_scale
            p2_p += 3.0 * w_scale
            if ch and ch.get("mov_dir") == th["mov_dir"]:
                local_e += 3.0 * w_scale
                p2_e += 3.0 * w_scale
        if th.get("cover"):
            local_p += 4.0 * w_scale
            p2_p += 4.0 * w_scale
            if ch and ch.get("cover") == th["cover"]:
                local_e += 4.0 * w_scale
                p2_e += 4.0 * w_scale
        if th.get("real_result"):
            local_p += 1.5 * w_scale
            p2_p += 1.5 * w_scale
            if ch and ch.get("real_result") == th["real_result"]:
                local_e += 1.5 * w_scale
                p2_e += 1.5 * w_scale
        if local_p > 0:
            ratio = local_e / local_p
            if ratio >= 0.72:
                col_hits[hit_key] = "exact"
                mov_txt = ch.get("movement") if ch else ""
                reasons.append(f"{lbl} {mov_txt or th.get('mov_dir') or ''}".strip())
            elif ratio >= 0.42:
                col_hits[hit_key] = "partial"
            elif ch and th.get("mov_dir") and ch.get("mov_dir") != th.get("mov_dir"):
                divergences.append(f"{lbl} mov {ch.get('mov_dir')}")

    _eval_h2h_block(target_feat.get("h2h_stadium"), cand_feat.get("h2h_stadium"), 1.0, "h2h_stadium", "H2H Est")
    _eval_h2h_block(target_feat.get("h2h_general"), cand_feat.get("h2h_general"), 0.7, "h2h_general", "H2H Gen")
    h2h_pct = round((p2_e / p2_p) * 100.0, 1) if p2_p > 0 else None

    # --- PILAR 4: Comparativas Indirectas Ind. Local + Ind. Visitante (peso 16) ---
    p3_e = 0.0
    p3_p = 0.0

    def _eval_ind_block(ti: Optional[Dict[str, Any]], ci: Optional[Dict[str, Any]], hit_key: str, lbl: str) -> None:
        nonlocal p3_e, p3_p
        if not ti:
            return
        local_e = 0.0
        local_p = 0.0
        if ti.get("ah_bucket") is not None:
            local_p += 4.0
            p3_p += 4.0
            if ci and ci.get("ah") is not None and ti.get("ah") is not None and abs(ti["ah"] - ci["ah"]) < 0.01:
                local_e += 4.0
                p3_e += 4.0
            elif ci and ci.get("ah_bucket") == ti.get("ah_bucket"):
                local_e += 3.0
                p3_e += 3.0
        if ti.get("cover"):
            local_p += 3.0
            p3_p += 3.0
            if ci and ci.get("cover") == ti["cover"]:
                local_e += 3.0
                p3_e += 3.0
        if ti.get("localia"):
            local_p += 2.0
            p3_p += 2.0
            if ci and ci.get("localia") == ti["localia"]:
                local_e += 2.0
                p3_e += 2.0
        if ti.get("real_wdl"):
            local_p += 1.0
            p3_p += 1.0
            if ci and ci.get("real_wdl") == ti["real_wdl"]:
                local_e += 1.0
                p3_e += 1.0
        if local_p > 0:
            ratio = local_e / local_p
            if ratio >= 0.72:
                col_hits[hit_key] = "exact"
                reasons.append(f"{lbl} {ci.get('localia') or ''} {ci.get('cover') or ''}".strip())
            elif ratio >= 0.45:
                col_hits[hit_key] = "partial"

    _eval_ind_block(target_feat.get("ind_local"), cand_feat.get("ind_local"), "ind_local", "Ind.Loc")
    _eval_ind_block(target_feat.get("ind_visitante"), cand_feat.get("ind_visitante"), "ind_visitante", "Ind.Vis")
    ind_pct = round((p3_e / p3_p) * 100.0, 1) if p3_p > 0 else None

    # --- PILAR 5: H2H Col3 Espejo (peso 10) ---
    p4_e = 0.0
    p4_p = 0.0
    tc3 = target_feat.get("h2h_col3")
    cc3 = cand_feat.get("h2h_col3")
    if tc3:
        if tc3.get("perf"):
            p4_p += 4.0
            if cc3 and cc3.get("perf") == tc3["perf"]:
                p4_e += 4.0
        if tc3.get("comp_type"):
            p4_p += 2.5
            if cc3 and cc3.get("comp_type") == tc3["comp_type"]:
                p4_e += 2.5
        if tc3.get("cover"):
            p4_p += 2.5
            if cc3 and cc3.get("cover") == tc3["cover"]:
                p4_e += 2.5
        if tc3.get("ah_bucket") is not None:
            p4_p += 2.0
            if cc3 and cc3.get("ah_bucket") == tc3["ah_bucket"]:
                p4_e += 2.0
        if tc3.get("ah_mov"):
            p4_p += 1.0
            if cc3 and cc3.get("ah_mov") == tc3["ah_mov"]:
                p4_e += 1.0
        if p4_p > 0:
            ratio = p4_e / p4_p
            if ratio >= 0.72:
                col_hits["h2h_col3"] = "exact"
                reasons.append(f"Col3 {cc3.get('perf') or ''} {cc3.get('comp_type') or ''}".strip())
            elif ratio >= 0.45:
                col_hits["h2h_col3"] = "partial"
    col3_pct = round((p4_e / p4_p) * 100.0, 1) if p4_p > 0 else None

    # --- PILAR 6: Similitud en Ficha Completa 10p + O/U ficha + Etiquetas verificadas (peso 14, SOLO si ficha_hydrated) ---
    ficha_pct: Optional[float] = None
    if ficha_hydrated and isinstance(cand_prof, dict) and cand_prof.get("profile_quality") == "full":
        p6_e = 0.0
        p6_p = 0.0

        t_ha = (target_prof.get("home_stats") or {}).get("all") or {}
        t_hv = (target_prof.get("home_stats") or {}).get("venue") or {}
        t_aa = (target_prof.get("away_stats") or {}).get("all") or {}
        t_av = (target_prof.get("away_stats") or {}).get("venue") or {}
        c_ha = (cand_prof.get("home_stats") or {}).get("all") or {}
        c_hv = (cand_prof.get("home_stats") or {}).get("venue") or {}
        c_aa = (cand_prof.get("away_stats") or {}).get("all") or {}
        c_av = (cand_prof.get("away_stats") or {}).get("venue") or {}

        # Sub-bloque A (4.5 pts): Forma específica Casa (Local) y Fuera (Visitante)
        ven_e = 0.0
        ven_p = 0.0
        for tv, cv, w in (
            (t_hv.get("win_pct"), c_hv.get("win_pct"), 1.25),
            (t_hv.get("unbeaten_pct"), c_hv.get("unbeaten_pct"), 1.0),
            (t_av.get("win_pct"), c_av.get("win_pct"), 1.0),
            (t_av.get("draw_pct"), c_av.get("draw_pct"), 0.65),
            (t_av.get("loss_pct"), c_av.get("loss_pct"), 0.60),
        ):
            if tv is not None and cv is not None:
                ven_p += w
                ven_e += max(0.0, 1.0 - abs(float(tv) - float(cv)) / 55.0) * w
        if ven_p > 0:
            p6_p += 4.5
            p6_e += (ven_e / ven_p) * 4.5

        # Sub-bloque B (3.5 pts): Forma general 10p + AH cover% + líneas similares en ficha
        gen_e = 0.0
        gen_p = 0.0
        for tv, cv, w in (
            (t_ha.get("win_pct"), c_ha.get("win_pct"), 0.9),
            (t_aa.get("win_pct"), c_aa.get("win_pct"), 0.9),
            (t_ha.get("ah_cover_pct"), c_ha.get("ah_cover_pct"), 0.65),
            (t_aa.get("ah_cover_pct"), c_aa.get("ah_cover_pct"), 0.65),
            (t_ha.get("sim_ah_cover_pct"), c_ha.get("sim_ah_cover_pct"), 0.45),
            (t_aa.get("sim_ah_cover_pct"), c_aa.get("sim_ah_cover_pct"), 0.45),
        ):
            if tv is not None and cv is not None:
                gen_p += w
                gen_e += max(0.0, 1.0 - abs(float(tv) - float(cv)) / 55.0) * w
        if gen_p > 0:
            p6_p += 3.5
            p6_e += (gen_e / gen_p) * 3.5

        # Sub-bloque C (3.0 pts): Perfil de goles O/U dentro de la ficha
        fou_e = 0.0
        fou_p = 0.0
        for tv, cv, scale, w in (
            (t_ha.get("over_pct"), c_ha.get("over_pct"), 55.0, 0.9),
            (t_aa.get("over_pct"), c_aa.get("over_pct"), 55.0, 0.9),
            (t_hv.get("avg_goals_total"), c_hv.get("avg_goals_total"), 1.6, 0.5),
            (t_av.get("avg_goals_total"), c_av.get("avg_goals_total"), 1.6, 0.5),
            (t_ha.get("avg_ou_line"), c_ha.get("avg_ou_line"), 0.75, 0.2),
        ):
            if tv is not None and cv is not None:
                fou_p += w
                fou_e += max(0.0, 1.0 - abs(float(tv) - float(cv)) / scale) * w
        if fou_p > 0:
            p6_p += 3.0
            p6_e += (fou_e / fou_p) * 3.0

        # Sub-bloque D (1.0 pts): Rivales comunes verificados (sin etiquetas de equipo)
        t_cr = target_prof.get("common_rivals") or {}
        c_cr = cand_prof.get("common_rivals") or {}
        if t_cr.get("count", 0) >= 3:
            p6_p += 1.0
            if c_cr.get("count", 0) >= 3 and c_cr.get("edge") == t_cr.get("edge"):
                p6_e += 1.0
                reasons.append("Comunes =")

        ficha_pct = round((p6_e / p6_p) * 100.0, 1) if p6_p > 0 else None

    # Normalización ponderada total sobre los pilares disponibles
    weight_map = [
        (core_pct, 24.0),
        (ou_pct, 18.0),
        (h2h_pct, 18.0),
        (ind_pct, 16.0),
        (col3_pct, 10.0),
        (ficha_pct, 14.0),
    ]
    total_w = sum(w for pct, w in weight_map if pct is not None)
    weighted_sum = sum((pct / 100.0) * w for pct, w in weight_map if pct is not None)
    similarity_pct = round((weighted_sum / total_w) * 100.0, 1) if total_w > 0 else 0.0

    # Retro-compatibilidad con Matriz 2x2: Mercado (Core+OU+H2H+Ind+Col3) vs Ficha
    mkt_parts = [(pct, w) for pct, w in weight_map[:5] if pct is not None]
    mkt_w = sum(w for _, w in mkt_parts)
    market_pct = round((sum((pct / 100.0) * w for pct, w in mkt_parts) / mkt_w) * 100.0, 1) if mkt_w > 0 else 50.0
    profile_pct = ficha_pct

    # Deduplicar reasons preservando orden
    dedup_reasons: List[str] = []
    for r in reasons:
        if r and r not in dedup_reasons:
            dedup_reasons.append(r)

    return {
        "similarity_pct": similarity_pct,
        "market_pct": market_pct,
        "profile_pct": profile_pct,
        "market_score": round((market_pct / 100.0) * 86.0, 1),
        "profile_score": round(((profile_pct or 0.0) / 100.0) * 14.0, 1),
        "ou_mov_match": ou_mov_match,
        "pillars": {
            "core_pct": core_pct,
            "ou_pct": ou_pct,
            "h2h_pct": h2h_pct,
            "ind_pct": ind_pct,
            "col3_pct": col3_pct,
            "ficha_pct": ficha_pct,
            "profile_pct": profile_pct,
        },
        "col_hits": col_hits,
        "matched_reasons": dedup_reasons[:8],
        "divergences": divergences[:4],
    }


def _aggregate_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(records)
    if n == 0:
        return {
            "n": 0, "fav_cover_pct": 0.0, "dog_cover_pct": 0.0, "push_pct": 0.0,
            "home_pct": 0.0, "draw_pct": 0.0, "away_pct": 0.0,
            "over_pct": 0.0, "under_pct": 0.0, "avg_goals": 0.0,
        }
    fav_eq = 0.0
    dog_eq = 0.0
    push_cnt = 0
    h_cnt = d_cnt = a_cnt = 0
    over_cnt = under_cnt = ou_n = 0
    total_goals = 0
    goals_n = 0

    for rec in records:
        fc = rec.get("fav_cover")
        if fc == "COVER":
            fav_eq += 1.0
        elif fc == "HALF_WIN":
            fav_eq += 0.5
            push_cnt += 0.5
        elif fc == "PUSH":
            push_cnt += 1.0
        elif fc == "HALF_LOSS":
            dog_eq += 0.5
            push_cnt += 0.5
        elif fc == "NO_COVER":
            dog_eq += 1.0

        wdl = rec.get("wdl_1x2")
        if wdl == "1":
            h_cnt += 1
        elif wdl == "X":
            d_cnt += 1
        elif wdl == "2":
            a_cnt += 1

        if rec.get("total_goals") is not None:
            total_goals += rec["total_goals"]
            goals_n += 1
        if rec.get("ou_res") == "OVER":
            over_cnt += 1
            ou_n += 1
        elif rec.get("ou_res") == "UNDER":
            under_cnt += 1
            ou_n += 1

    return {
        "n": n,
        "fav_cover_pct": round((fav_eq / n) * 100.0, 1),
        "dog_cover_pct": round((dog_eq / n) * 100.0, 1),
        "push_pct": round((push_cnt / n) * 100.0, 1),
        "home_pct": round((h_cnt / n) * 100.0, 1),
        "draw_pct": round((d_cnt / n) * 100.0, 1),
        "away_pct": round((a_cnt / n) * 100.0, 1),
        "over_pct": round((over_cnt / ou_n) * 100.0, 1) if ou_n else 0.0,
        "under_pct": round((under_cnt / ou_n) * 100.0, 1) if ou_n else 0.0,
        "avg_goals": round(total_goals / goals_n, 2) if goals_n else 0.0,
    }


def _build_verdict(cohort: List[Dict[str, Any]], target_prof: Dict[str, Any]) -> Dict[str, Any]:
    stats = _aggregate_records(cohort)
    n = stats["n"]
    fav_side = target_prof.get("fav_side", "HOME")
    fav_label = "Local" if fav_side == "HOME" else ("Visitante" if fav_side == "AWAY" else "Local (PK)")
    dog_label = "Visitante" if fav_side == "HOME" else ("Local" if fav_side == "AWAY" else "Visitante (PK)")

    if n < 4:
        return {
            "signal": "MUESTRA INSUFICIENTE",
            "signal_code": "NO_BET",
            "confidence": "BAJA",
            "tone": "neutral",
            "summary": f"Solo {n} precedentes con similitud suficiente para emitir señal fuerte.",
            **stats,
        }

    fav_p = stats["fav_cover_pct"]
    dog_p = stats["dog_cover_pct"]
    over_p = stats["over_pct"]
    under_p = stats["under_pct"]

    if fav_p >= 62.0:
        conf = "ALTA" if (n >= 10 and fav_p >= 68.0) else "MEDIA"
        return {
            "signal": f"SEÑAL FAV ({fav_label})",
            "signal_code": "FAV",
            "confidence": conf,
            "tone": "good",
            "summary": f"En {n} partidos de máxima similitud, el favorito ({fav_label}) cubre en el {fav_p}%.",
            **stats,
        }
    if dog_p >= 62.0:
        conf = "ALTA" if (n >= 10 and dog_p >= 68.0) else "MEDIA"
        return {
            "signal": f"SEÑAL RIVAL/DOG ({dog_label})",
            "signal_code": "DOG",
            "confidence": conf,
            "tone": "warn",
            "summary": f"En {n} similares, el no-favorito ({dog_label}) cubre o puntúa en el {dog_p}%.",
            **stats,
        }
    if over_p >= 68.0:
        return {
            "signal": "SEÑAL OVER GOLES",
            "signal_code": "OVER",
            "confidence": "MEDIA",
            "tone": "info",
            "summary": f"AH equilibrado ({fav_p}%/{dog_p}%), pero el Over alcanza el {over_p}% ({stats['avg_goals']} goles/p).",
            **stats,
        }
    if under_p >= 68.0:
        return {
            "signal": "SEÑAL UNDER GOLES",
            "signal_code": "UNDER",
            "confidence": "MEDIA",
            "tone": "info",
            "summary": f"AH equilibrado ({fav_p}%/{dog_p}%), pero el Under domina con el {under_p}% ({stats['avg_goals']} goles/p).",
            **stats,
        }
    return {
        "signal": "ZONA MIXTA / NO BET",
        "signal_code": "NO_BET",
        "confidence": "BAJA",
        "tone": "neutral",
        "summary": f"División pareja en {n} similares: Fav {fav_p}% vs Rival {dog_p}% (Push {stats['push_pct']}%).",
        **stats,
    }


def _build_ou_verdict(
    cohort: List[Dict[str, Any]],
    all_enriched: List[Dict[str, Any]],
    target_feat: Dict[str, Any],
) -> Dict[str, Any]:
    """Construye el veredicto de Goles (Over/Under) y su resumen de movimientos O/U."""
    stats = _aggregate_records(cohort)
    n = stats["n"]
    settled_ou = [r for r in cohort if r.get("ou_res") in ("OVER", "UNDER", "PUSH")]
    ou_push_cnt = sum(1 for r in settled_ou if r.get("ou_res") == "PUSH")
    ou_push_pct = round((ou_push_cnt / len(settled_ou)) * 100.0, 1) if settled_ou else 0.0

    same_mov = [r for r in all_enriched if r.get("ou_mov_match")]
    sm_agg = _aggregate_records(same_mov)

    t_ou = target_feat.get("ou")
    parts: List[str] = []
    if t_ou is not None:
        parts.append(f"O/U {_format_ah(t_ou)}")
    for lbl, node in (
        ("Prev Loc", target_feat.get("prev_home")),
        ("Prev Vis", target_feat.get("prev_away")),
        ("H2H Est", target_feat.get("h2h_stadium")),
        ("H2H Gen", target_feat.get("h2h_general")),
    ):
        if isinstance(node, dict) and node.get("ou") is not None and t_ou is not None:
            delta = node.get("ou_delta")
            d_txt = f"{delta:+.2g}" if isinstance(delta, (int, float)) else ""
            res_c = (node.get("ou_res") or "")[:1]
            extra = ", ".join(x for x in (d_txt, res_c) if x)
            parts.append(f"{lbl}: {_format_ah(node['ou'])}→{_format_ah(t_ou)}" + (f" ({extra})" if extra else ""))
        elif isinstance(node, dict) and node.get("ou_res"):
            parts.append(f"{lbl}: {node['ou_res']}")
    target_ou_mov_text = " · ".join(parts)

    over_p = stats["over_pct"]
    under_p = stats["under_pct"]
    avg_g = stats["avg_goals"]

    if n < 4 or not settled_ou:
        signal, code, conf, tone = "MUESTRA O/U INSUFICIENTE", "NO_BET", "BAJA", "neutral"
        summary = f"Solo {len(settled_ou)} precedentes con línea O/U definida."
    elif over_p >= 60.0:
        conf = "ALTA" if (len(settled_ou) >= 10 and over_p >= 66.0) else "MEDIA"
        signal, code, tone = "SEÑAL OVER GOLES", "OVER", "good"
        summary = f"El Over se cumple en el {over_p}% (Under {under_p}% · Push {ou_push_pct}%) con {avg_g} goles/p."
    elif under_p >= 60.0:
        conf = "ALTA" if (len(settled_ou) >= 10 and under_p >= 66.0) else "MEDIA"
        signal, code, tone = "SEÑAL UNDER GOLES", "UNDER", "warn"
        summary = f"El Under domina en el {under_p}% (Over {over_p}% · Push {ou_push_pct}%) con {avg_g} goles/p."
    else:
        conf, tone, signal, code = "BAJA", "neutral", "ZONA NEUTRA O/U", "NO_BET"
        summary = f"Goles equilibrados en similares: Over {over_p}% vs Under {under_p}% ({avg_g} goles/p)."

    return {
        "signal": signal,
        "signal_code": code,
        "confidence": conf,
        "tone": tone,
        "summary": summary,
        "over_pct": over_p,
        "under_pct": under_p,
        "ou_push_pct": ou_push_pct,
        "avg_goals": avg_g,
        "n": len(settled_ou),
        "same_ou_mov_n": sm_agg["n"],
        "same_ou_mov_over_pct": sm_agg["over_pct"],
        "same_ou_mov_under_pct": sm_agg["under_pct"],
        "same_ou_mov_avg_goals": sm_agg["avg_goals"],
        "target_ou_mov_text": target_ou_mov_text,
    }


def _build_anti_trap_checklist(
    target_prof: Dict[str, Any],
    top_cohort: List[Dict[str, Any]],
    tag_cohorts: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    m_tags = {t["code"]: t for t in target_prof.get("matchup_tags", [])}
    h_tags = {t["code"]: t for t in target_prof.get("home_tags", [])}
    a_tags = {t["code"]: t for t in target_prof.get("away_tags", [])}
    cr = target_prof.get("common_rivals") or {}

    if "CHOQUE_DE_DUROS" in m_tags:
        items.append({
            "kind": "alert",
            "title": "Choque de Duros detectado",
            "detail": m_tags["CHOQUE_DE_DUROS"]["reason"] + " · Cuidado con asumir victoria fácil del favorito.",
        })
    if "AH_ALERTA_TRAMPA" in m_tags:
        items.append({
            "kind": "danger",
            "title": "Alerta de Hándicap Inflado",
            "detail": m_tags["AH_ALERTA_TRAMPA"]["reason"],
        })
    if "DOBLE_FUERZA_FAV" in m_tags:
        items.append({
            "kind": "pro",
            "title": "Doble Fuerza a favor del Hándicap",
            "detail": m_tags["DOBLE_FUERZA_FAV"]["reason"],
        })
    if "FAV_POR_MAL_VISITANTE" in m_tags:
        items.append({
            "kind": "info",
            "title": "Favoritismo por debilidad del Visitante",
            "detail": f"{m_tags['FAV_POR_MAL_VISITANTE']['reason']}. Conviene verificar cómo cubren en historiales similares.",
        })
    if "VISITANTE_MAQUILLADO" in a_tags:
        items.append({
            "kind": "pro",
            "title": "Visitante maquilla números en casa",
            "detail": a_tags["VISITANTE_MAQUILLADO"]["reason"],
        })
    if "LOCAL_FALLA_PARES" in h_tags:
        items.append({
            "kind": "alert",
            "title": "Local sufre con líneas cortas",
            "detail": h_tags["LOCAL_FALLA_PARES"]["reason"],
        })
    if cr.get("count", 0) >= 3:
        items.append({
            "kind": "info" if cr.get("edge") == "EVEN" else ("pro" if cr.get("edge") == target_prof.get("fav_side") else "danger"),
            "title": f"{cr['count']} Rivales Comunes recientes",
            "detail": f"Local sumó {cr['home_pts']} pts ({cr['home_gd']:+d} DG) vs Visitante {cr['away_pts']} pts ({cr['away_gd']:+d} DG).",
        })
    if "DOMINIO_OCULTO_LOC" in h_tags:
        items.append({"kind": "info", "title": "Dominio Oculto Local", "detail": h_tags["DOMINIO_OCULTO_LOC"]["reason"]})
    if "DOMINIO_OCULTO_VIS" in a_tags:
        items.append({"kind": "info", "title": "Dominio Oculto Visitante", "detail": a_tags["DOMINIO_OCULTO_VIS"]["reason"]})

    # Añadir contraste desde la cohorte histórica
    if top_cohort:
        st = _aggregate_records(top_cohort)
        items.append({
            "kind": "pro" if st["fav_cover_pct"] >= 60 else ("danger" if st["dog_cover_pct"] >= 60 else "info"),
            "title": f"Histórico de Máxima Similitud (N={st['n']})",
            "detail": f"Fav cubre {st['fav_cover_pct']}% · Rival cubre {st['dog_cover_pct']}% · Over {st['over_pct']}% · Promedio {st['avg_goals']} goles.",
        })
    return items


def search_pattern_v2(
    target_match: Dict[str, Any],
    top_n: int = 80,
    candidate_pool: int = 180,
) -> Dict[str, Any]:
    """Fase 1 rápida: Patrón Base + Tarjetas Explorador + 5 Pilares de mercado/O-U (SIN etiquetas)."""
    t0 = time.perf_counter()
    use_archive_index = (not CARDS_DB_PATH.exists()) and (_get_explorer_archive_db() is not None)
    idx_path = _ensure_archive_signature_index() if use_archive_index else None
    if idx_path is None:
        use_archive_index = False
        _build_cards_cache()

    target_id = str(target_match.get("match_id") or target_match.get("id") or "")
    target_prof = compute_team_profile_and_tags(target_match, match_id=target_id)
    target_sig = _extract_target_signature(target_match)
    target_ah = target_sig.get("ah")
    target_pc = _build_pre_match_context_from_payload(target_match, target_id)

    if target_ah is None:
        return {
            "status": "error",
            "error": "El partido actual no tiene línea de hándicap asiático válida.",
            "target": target_prof,
        }

    target_card = build_explorer_card(target_match)
    if target_card is None:
        target_card = _enrich_card_ou({
            "match_id": target_id,
            "candidate": {
                "date": target_prof.get("date", ""),
                "league": target_prof.get("league", ""),
                "home": target_prof.get("home", ""),
                "away": target_prof.get("away", ""),
                "score": target_prof.get("score", "?:?"),
                "ah_real": target_ah,
                "ou_line": target_sig.get("ou"),
            },
            "prev_home": target_match.get("last_home_match"),
            "prev_away": target_match.get("last_away_match"),
            "h2h_stadium": target_match.get("h2h_stadium"),
            "h2h_general": target_match.get("h2h_general"),
            "h2h_col3": target_match.get("h2h_col3"),
            "ind_local": (target_match.get("comparativas_indirectas") or {}).get("left"),
            "ind_visitante": (target_match.get("comparativas_indirectas") or {}).get("right"),
            "home_standings": target_match.get("home_standings"),
            "away_standings": target_match.get("away_standings"),
        }, target_payload=target_match)
    target_feat = _extract_card_features(target_card)

    target_bucket = float(normalize_ah_bucket(target_ah))
    if use_archive_index and idx_path is not None:
        pool_raw, total_scanned = _query_archive_sigs_by_bucket(idx_path, target_bucket, target_ah)
    else:
        with _CARDS_LOCK:
            total_scanned = len(_CARDS_ALL)
            pool_raw = list(_CARDS_BY_BUCKET.get(target_bucket, []))
            # Si el bucket exacto tiene pocos candidatos, ampliar a buckets del mismo signo
            if len(pool_raw) < 35:
                same_sign_buckets = sorted(
                    (b for b in _CARDS_BY_BUCKET.keys()
                     if b != target_bucket and (
                         (target_ah > 0.01 and b > 0)
                         or (target_ah < -0.01 and b < 0)
                         or (abs(target_ah) <= 0.01 and abs(b) <= 0.51)
                     )),
                    key=lambda b: abs(b - target_bucket),
                )
                for b in same_sign_buckets:
                    pool_raw.extend(_CARDS_BY_BUCKET[b])
                    if len(pool_raw) >= 500:
                        break

    shortlist, tier1_total = _select_pattern_candidates(
        target_sig=target_sig,
        target_feat=target_feat,
        pool_raw=pool_raw,
        target_id=target_id,
        candidate_pool=max(top_n, candidate_pool),
    )
    shortlist_ids = [mid for _, _, mid, _ in shortlist]

    # Fase 1: Solo leer tarjetas nativas de explorer_cards.sqlite (cero etiquetas ni lectura masiva de fichas)
    cards_map = _fetch_explorer_cards_bulk(shortlist_ids)
    h2h_raw_map = _fetch_h2h_payloads_bulk(shortlist_ids)

    enriched: List[Dict[str, Any]] = []
    enriched_cards_map: Dict[str, Dict[str, Any]] = {}
    target_ou = target_sig.get("ou")
    for p_tier, _pre, mid, doc in shortlist:
        card = cards_map.get(mid)
        if not card:
            continue
        card_enriched = _enrich_card_ou(card, h2h_raw=h2h_raw_map.get(mid))
        cand = card_enriched.get("candidate") or {}
        sc = _parse_score(cand.get("score"))
        if not sc:
            continue
        hg, ag = sc
        cand_ah = _parse_ah(cand.get("ah_real"))
        if cand_ah is None:
            cand_ah = _parse_ah(doc.get("ah"))
        fav_cov, _ = _eval_match_fav_cover(hg, ag, cand_ah)
        wdl_1x2 = "1" if hg > ag else ("2" if ag > hg else "X")
        cand_ou = _parse_ou(cand.get("ou_line")) or _parse_ou(doc.get("ou")) or target_ou
        total_g = hg + ag
        ou_res = None
        if cand_ou:
            if total_g > cand_ou + 0.01:
                ou_res = "OVER"
            elif total_g < cand_ou - 0.01:
                ou_res = "UNDER"
            else:
                ou_res = "PUSH"

        cand_feat = _extract_card_features(card_enriched)
        deep = _score_deep_similarity(target_feat, cand_feat, target_prof, None, p_tier, ficha_hydrated=False)
        sim_pct = deep["similarity_pct"]
        m_pct = deep["market_pct"]
        pillars = deep["pillars"]
        exact_ah = (cand_ah is not None and abs(cand_ah - target_ah) < 0.01)

        q_key = "other_prof_high_mkt" if m_pct >= 65.0 else "other_prof_part_mkt"
        raw_date = str(cand.get("date") or doc.get("date") or "").split(" ")[0]

        enriched_cards_map[mid] = card_enriched
        enriched.append({
            "match_id": mid,
            "date": raw_date,
            "league": str(cand.get("league") or ""),
            "home": str(cand.get("home") or doc.get("home") or ""),
            "away": str(cand.get("away") or doc.get("away") or ""),
            "score": str(cand.get("score") or f"{hg}:{ag}"),
            "total_goals": total_g,
            "ah": cand_ah,
            "ou": cand_ou,
            "fav_side": _get_favorite_side(cand_ah),
            "fav_cover": fav_cov,
            "wdl_1x2": wdl_1x2,
            "ou_res": ou_res,
            "exact_ah": exact_ah,
            "pattern_tier": p_tier,
            "ficha_hydrated": False,
            "has_full_ficha": False,
            "similarity_pct": sim_pct,
            "market_score": deep["market_score"],
            "profile_score": deep["profile_score"],
            "market_pct": m_pct,
            "profile_pct": None,
            "pillars": pillars,
            "col_hits": deep["col_hits"],
            "ou_match": (pillars.get("ou_pct") or 0) >= 60.0,
            "ou_mov_match": deep["ou_mov_match"],
            "h2h_match": (pillars.get("h2h_pct") or 0) >= 60.0,
            "ind_match": (pillars.get("ind_pct") or 0) >= 60.0,
            "col3_match": (pillars.get("col3_pct") or 0) >= 60.0,
            "ficha_match": False,
            "quadrant": q_key,
            "matched_reasons": deep["matched_reasons"],
            "divergences": deep["divergences"],
            "home_tags": [],
            "away_tags": [],
            "matchup_tags": [],
            "home_venue_summary": "",
            "away_venue_summary": "",
            "common_rivals_count": 0,
            "profile_quality": "card_only",
            "headline_reason": "",
        })

    enriched.sort(
        key=lambda r: (
            r["similarity_pct"],
            -r["pattern_tier"],
            r["pillars"].get("ou_pct") or 0.0,
            r["pillars"].get("h2h_pct") or 0.0,
            r["pillars"].get("ind_pct") or 0.0,
        ),
        reverse=True,
    )

    # Cohorte principal de alta similitud para el Veredicto Dual (AH + O/U)
    high_sim = [r for r in enriched if r["similarity_pct"] >= 70.0]
    verdict_cohort = high_sim if len(high_sim) >= 6 else enriched[:15]
    verdict = _build_verdict(verdict_cohort, target_prof)
    ou_verdict = _build_ou_verdict(verdict_cohort, enriched, target_feat)

    # Escalera de convergencia (6 escalones)
    ladder_defs = [
        ("Top 5 Máxima Similitud", enriched[:5]),
        ("Top 10 Más Similares", enriched[:10]),
        ("Top 20 Más Similares", enriched[:20]),
        ("Similitud >= 75%", [r for r in enriched if r["similarity_pct"] >= 75.0]),
        ("Similitud >= 65%", [r for r in enriched if r["similarity_pct"] >= 65.0]),
        (f"Muestra Base (Top {len(enriched)})", enriched),
    ]
    similarity_ladder = []
    for label, sub in ladder_defs:
        agg = _aggregate_records(sub)
        similarity_ladder.append({"label": label, **agg})

    # Fase 1: Sin etiquetas hasta que se hidraten las fichas en 2º plano
    tag_cohorts: List[Dict[str, Any]] = []

    # Matriz 2x2: Perfil Equipos x Mercado
    m_cells: Dict[str, List[Dict[str, Any]]] = {
        "high_prof_high_mkt": [],
        "high_prof_part_mkt": [],
        "other_prof_high_mkt": [],
        "other_prof_part_mkt": [],
    }
    for r in enriched:
        m_cells[r["quadrant"]].append(r)

    quadrant_matrix = {
        key: _aggregate_records(rows)
        for key, rows in m_cells.items()
    }

    anti_trap_checklist = _build_anti_trap_checklist(target_prof, verdict_cohort, tag_cohorts)

    top_matches = enriched[:max(1, min(top_n, 150))]
    explorer_results: List[Dict[str, Any]] = []
    missing_ficha_ids: List[str] = []

    for rank_idx, r in enumerate(top_matches, start=1):
        card = enriched_cards_map.get(r["match_id"])
        if not card:
            continue
        missing_ficha_ids.append(r["match_id"])
        card_copy = dict(card)
        cand = dict(card_copy.get("candidate") or {})
        if not cand.get("score") or "?" in str(cand.get("score")) or cand.get("score") == "-":
            cand["score"] = r["score"]
        card_copy["candidate"] = cand
        card_copy["match_id"] = card_copy.get("match_id") or r["match_id"]
        card_copy["_pv2"] = {
            "rank": rank_idx,
            "similarity_pct": r["similarity_pct"],
            "market_pct": r["market_pct"],
            "profile_pct": None,
            "pillars": r["pillars"],
            "col_hits": r["col_hits"],
            "pattern_tier": r["pattern_tier"],
            "ficha_hydrated": False,
            "has_full_ficha": False,
            "ou_match": r["ou_match"],
            "ou_mov_match": r["ou_mov_match"],
            "h2h_match": r["h2h_match"],
            "ind_match": r["ind_match"],
            "col3_match": r["col3_match"],
            "ficha_match": False,
            "exact_ah": r["exact_ah"],
            "fav_cover": r["fav_cover"],
            "wdl_1x2": r["wdl_1x2"],
            "ou_res": r["ou_res"],
            "total_goals": r["total_goals"],
            "matched_reasons": r["matched_reasons"],
            "divergences": r["divergences"],
            "home_tags": [],
            "away_tags": [],
            "matchup_tags": [],
            "home_venue_summary": "",
            "away_venue_summary": "",
            "headline_reason": "",
            "quadrant": r["quadrant"],
            "common_rivals_count": 0,
        }
        explorer_results.append(card_copy)

    elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 1)
    return {
        "status": "success",
        "elapsed_ms": elapsed_ms,
        "total_scanned": total_scanned,
        "total_candidates_scanned": len(pool_raw),
        "shortlist_size": len(enriched),
        "runtime_profile": {
            "is_render": _IS_RENDER,
            "source": "archive_index" if use_archive_index else (_CARDS_META.get("source") or "cards"),
            "bg_target_total": 12 if _IS_RENDER else 20,
            "bg_chunk_size": 2 if _IS_RENDER else 4,
        },
        "tier1_count": tier1_total,
        "hydrated_ficha_count": 0,
        "full_ficha_count": 0,
        "missing_ficha_ids": missing_ficha_ids,
        "target": target_prof,
        "target_explorer_card": target_card,
        "target_pre_match_context": target_pc,
        "target_ou_summary": {
            "ou": target_feat.get("ou"),
            "prev_home": target_feat.get("prev_home"),
            "prev_away": target_feat.get("prev_away"),
            "h2h_stadium": target_feat.get("h2h_stadium"),
            "h2h_general": target_feat.get("h2h_general"),
            "text": ou_verdict.get("target_ou_mov_text", ""),
        },
        "verdict": verdict,
        "ou_verdict": ou_verdict,
        "similarity_ladder": similarity_ladder,
        "tag_cohorts": tag_cohorts,
        "quadrant_matrix": quadrant_matrix,
        "anti_trap_checklist": anti_trap_checklist,
        "matches": top_matches,
        "explorer_results": explorer_results,
    }


def _build_pre_match_context_from_payload(payload: Dict[str, Any], mid: str) -> Optional[Dict[str, Any]]:
    """Construye el objeto pre_match_context compatible con renderPreMatchContextCard desde SQLite."""
    if not isinstance(payload, dict):
        return None
    h_all = payload.get("recent_home_matches_all") or payload.get("recent_home_matches_same_league_general") or []
    a_all = payload.get("recent_away_matches_all") or payload.get("recent_away_matches_same_league_general") or []
    if len(h_all) < 5 or len(a_all) < 5:
        return None
    home_name = payload.get("home_name") or payload.get("home_team") or ""
    away_name = payload.get("away_name") or payload.get("away_team") or ""
    odds = payload.get("main_match_odds") or {}
    ah = odds.get("ah_linea") if isinstance(odds, dict) else payload.get("handicap")
    existing_ctx = payload.get("pre_match_context")
    if isinstance(existing_ctx, dict) and isinstance(existing_ctx.get("current"), dict):
        pc = dict(existing_ctx)
        pc["match_id"] = mid
        pc["home_name"] = home_name
        pc["away_name"] = away_name
        pc["handicap"] = ah
        pc["last_home_match"] = payload.get("last_home_match")
        pc["last_away_match"] = payload.get("last_away_match")
        return pc

    return {
        "match_id": mid,
        "home_name": home_name,
        "away_name": away_name,
        "handicap": ah,
        "last_home_match": payload.get("last_home_match"),
        "last_away_match": payload.get("last_away_match"),
        "current": {
            "date": payload.get("match_date") or payload.get("date") or "",
            "home_name": home_name,
            "away_name": away_name,
            "league_id": payload.get("league_id") or "",
            "league_name": payload.get("league_name") or "",
            "ah_line": ah,
            "home_matches": payload.get("recent_home_matches") or h_all,
            "away_matches": payload.get("recent_away_matches") or a_all,
            "home_matches_all": h_all,
            "away_matches_all": a_all,
        },
        "previous": None,
    }


def deep_download_and_rescore(
    target_match: Dict[str, Any],
    candidate_ids: List[str],
    force_refresh: bool = False,
    sqlite_only: bool = False,
) -> Dict[str, Any]:
    """Hidrata fichas completas (SQLite o descarga paralela) y calcula similitud 6-pilares + etiquetas."""
    t0 = time.perf_counter()
    from .estudio_scraper import analizar_contexto_previo_rapido

    max_ids = 40 if sqlite_only else 20
    clean_ids = [str(c).strip() for c in (candidate_ids or []) if str(c).strip()][:max_ids]
    if not clean_ids:
        return {
            "status": "success",
            "updated": {},
            "downloaded_count": 0,
            "sqlite_count": 0,
            "needs_download_ids": [],
            "failed_ids": [],
            "elapsed_ms": 0.0,
        }

    target_id = str(target_match.get("match_id") or target_match.get("id") or "")
    target_prof = compute_team_profile_and_tags(target_match, match_id=target_id)
    target_card = build_explorer_card(target_match) or {}
    target_feat = _extract_card_features(target_card)
    target_sig = _extract_target_signature(target_match)
    target_ah = target_feat.get("ah")

    cards_map = _fetch_explorer_cards_bulk(clean_ids)
    payloads = _fetch_match_payloads_bulk(clean_ids)

    updated: Dict[str, Dict[str, Any]] = {}
    downloaded_count = 0
    sqlite_count = 0
    needs_download_ids: List[str] = []
    failed_ids: List[str] = []

    def _hydrate_one(cid: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]], str]:
        if not force_refresh:
            with _DEEP_CTX_LOCK:
                cached = _DEEP_CTX_CACHE.get(cid)
                if cached is not None:
                    _DEEP_CTX_CACHE.move_to_end(cid)
                    return cid, cached["payload"], cached["pc"], "cache"

        base_pl = dict(payloads.get(cid) or {})
        pc_sqlite = _build_pre_match_context_from_payload(base_pl, cid)
        if pc_sqlite is not None and not force_refresh:
            with _DEEP_CTX_LOCK:
                _DEEP_CTX_CACHE[cid] = {"payload": base_pl, "pc": pc_sqlite}
                _DEEP_CTX_CACHE.move_to_end(cid)
                while len(_DEEP_CTX_CACHE) > _DEEP_CTX_CACHE_MAX:
                    _DEEP_CTX_CACHE.popitem(last=False)
            return cid, base_pl, pc_sqlite, "sqlite"

        if sqlite_only:
            return cid, None, None, "needs_download"

        # Descargar contexto previo completo en tiempo real sin modificar SQLite
        odds = base_pl.get("main_match_odds") or {}
        c_ah = odds.get("ah_linea") if isinstance(odds, dict) else base_pl.get("handicap")
        c_ou = odds.get("goals_linea") if isinstance(odds, dict) else base_pl.get("goal_line")
        try:
            ctx = analizar_contexto_previo_rapido(
                cid,
                current_ah=c_ah,
                current_goal_line=c_ou,
                is_neutral_venue=True if base_pl.get("is_neutral_venue") is True else None,
            )
        except Exception:
            ctx = None

        if not isinstance(ctx, dict) or not isinstance(ctx.get("current"), dict):
            return cid, base_pl if base_pl else None, None, "failed"

        curr = ctx["current"]
        h_all = curr.get("home_matches_all") or curr.get("home_matches") or []
        a_all = curr.get("away_matches_all") or curr.get("away_matches") or []
        enriched_pl = dict(base_pl)
        enriched_pl["match_id"] = cid
        enriched_pl["home_name"] = enriched_pl.get("home_name") or curr.get("home_name") or ""
        enriched_pl["away_name"] = enriched_pl.get("away_name") or curr.get("away_name") or ""
        if h_all:
            enriched_pl["recent_home_matches_all"] = h_all
            enriched_pl["recent_home_matches"] = curr.get("home_matches") or h_all
            if not enriched_pl.get("last_home_match"):
                enriched_pl["last_home_match"] = h_all[0]
        if a_all:
            enriched_pl["recent_away_matches_all"] = a_all
            enriched_pl["recent_away_matches"] = curr.get("away_matches") or a_all
            if not enriched_pl.get("last_away_match"):
                enriched_pl["last_away_match"] = a_all[0]
        if isinstance(ctx.get("h2h_data"), dict):
            existing_hs = enriched_pl.get("h2h_stadium")
            if not (isinstance(existing_hs, dict) and existing_hs.get("all_matches")):
                enriched_pl["h2h_stadium"] = ctx["h2h_data"]
                enriched_pl["h2h_general"] = ctx["h2h_data"]

        pc = dict(ctx)
        pc["match_id"] = cid
        pc["home_name"] = enriched_pl["home_name"]
        pc["away_name"] = enriched_pl["away_name"]
        pc["handicap"] = curr.get("ah_line") or c_ah or ""
        pc["last_home_match"] = enriched_pl.get("last_home_match")
        pc["last_away_match"] = enriched_pl.get("last_away_match")

        with _DEEP_CTX_LOCK:
            _DEEP_CTX_CACHE[cid] = {"payload": enriched_pl, "pc": pc}
            _DEEP_CTX_CACHE.move_to_end(cid)
            while len(_DEEP_CTX_CACHE) > _DEEP_CTX_CACHE_MAX:
                _DEEP_CTX_CACHE.popitem(last=False)
        return cid, enriched_pl, pc, "downloaded"

    hydrated: List[Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]], str]] = []
    if sqlite_only:
        for cid in clean_ids:
            hydrated.append(_hydrate_one(cid))
    else:
        pool_workers = 2 if _IS_RENDER else 4
        with concurrent.futures.ThreadPoolExecutor(max_workers=pool_workers) as executor:
            for item in executor.map(_hydrate_one, clean_ids):
                hydrated.append(item)

    # Preparar target signature para determinar pattern_tier
    t_ph = target_feat.get("prev_home") or target_sig.get("prev_home")
    t_pa = target_feat.get("prev_away") or target_sig.get("prev_away")

    for cid, pl, pc, src in hydrated:
        if src == "needs_download":
            needs_download_ids.append(cid)
            continue
        if src == "downloaded":
            downloaded_count += 1
        elif src in ("sqlite", "cache"):
            sqlite_count += 1
        else:
            failed_ids.append(cid)

        card = cards_map.get(cid)
        if not card and pl:
            card = build_explorer_card(pl)
        if not pl or not card:
            continue

        card = _enrich_card_ou(card, target_payload=pl)
        prof = compute_team_profile_and_tags(pl, match_id=cid)
        _set_cached_profile(cid, prof)

        is_full = (prof.get("profile_quality") == "full")
        cand_feat = _extract_card_features(card)
        c_ph = cand_feat.get("prev_home")
        c_pa = cand_feat.get("prev_away")

        # Calcular pattern_tier exacto
        ph_ok = (not t_ph) or (
            c_ph is not None
            and (t_ph.get("ah_bucket") is None or c_ph.get("ah_bucket") == t_ph.get("ah_bucket"))
            and (t_ph.get("cover") is None or c_ph.get("cover") == t_ph.get("cover"))
        )
        pa_ok = (not t_pa) or (
            c_pa is not None
            and (t_pa.get("ah_bucket") is None or c_pa.get("ah_bucket") == t_pa.get("ah_bucket"))
            and (t_pa.get("cover") is None or c_pa.get("cover") == t_pa.get("cover"))
        )
        p_tier = 1 if (ph_ok and pa_ok) else (2 if (ph_ok or pa_ok) else 3)

        deep = _score_deep_similarity(
            target_feat,
            cand_feat,
            target_prof,
            prof if is_full else None,
            p_tier,
            ficha_hydrated=is_full,
        )
        pillars = deep["pillars"]
        m_pct = deep["market_pct"]
        f_pct = pillars.get("ficha_pct")
        if is_full and f_pct is not None and f_pct >= 65.0 and m_pct >= 65.0:
            q_key = "high_prof_high_mkt"
        elif is_full and f_pct is not None and f_pct >= 65.0:
            q_key = "high_prof_part_mkt"
        elif m_pct >= 65.0:
            q_key = "other_prof_high_mkt"
        else:
            q_key = "other_prof_part_mkt"

        sc = _parse_score(prof.get("score") or (card.get("candidate") or {}).get("score"))
        cand_ah = prof.get("ah")
        fav_cov, _ = _eval_match_fav_cover(sc[0], sc[1], cand_ah) if sc else ("UNKNOWN", None)
        wdl_1x2 = ("1" if sc[0] > sc[1] else ("2" if sc[1] > sc[0] else "X")) if sc else None
        cand_ou = prof.get("ou") or target_feat.get("ou")
        total_g = (sc[0] + sc[1]) if sc else None
        ou_res = None
        if total_g is not None and cand_ou:
            ou_res = "OVER" if total_g > cand_ou + 0.01 else ("UNDER" if total_g < cand_ou - 0.01 else "PUSH")

        hv = (prof.get("home_stats") or {}).get("venue") or {}
        av = (prof.get("away_stats") or {}).get("venue") or {}
        h_ven_sum = f"Casa V{hv.get('w', 0)} E{hv.get('d', 0)} D{hv.get('l', 0)}" if (is_full and hv.get("n")) else ""
        a_ven_sum = f"Fuera V{av.get('w', 0)} E{av.get('d', 0)} D{av.get('l', 0)}" if (is_full and av.get("n")) else ""

        updated[cid] = {
            "source": src,
            "pre_match_context": pc,
            "prev_home": card.get("prev_home"),
            "prev_away": card.get("prev_away"),
            "prev_home_ou": (card.get("prev_home") or {}).get("ou"),
            "prev_away_ou": (card.get("prev_away") or {}).get("ou"),
            "h2h_stadium": card.get("h2h_stadium"),
            "h2h_general": card.get("h2h_general"),
            "_pv2": {
                "rank": 0,
                "similarity_pct": deep["similarity_pct"],
                "market_pct": m_pct,
                "profile_pct": f_pct,
                "pillars": pillars,
                "col_hits": deep["col_hits"],
                "pattern_tier": p_tier,
                "ficha_hydrated": is_full,
                "has_full_ficha": is_full,
                "ou_match": (pillars.get("ou_pct") or 0) >= 60.0,
                "ou_mov_match": deep["ou_mov_match"],
                "h2h_match": (pillars.get("h2h_pct") or 0) >= 60.0,
                "ind_match": (pillars.get("ind_pct") or 0) >= 60.0,
                "col3_match": (pillars.get("col3_pct") or 0) >= 60.0,
                "ficha_match": bool(is_full and (f_pct or 0) >= 65.0),
                "exact_ah": (cand_ah is not None and target_ah is not None and abs(cand_ah - target_ah) < 0.01),
                "fav_cover": fav_cov,
                "wdl_1x2": wdl_1x2,
                "ou_res": ou_res,
                "total_goals": total_g,
                "matched_reasons": deep["matched_reasons"],
                "divergences": deep["divergences"],
                "home_tags": [],
                "away_tags": [],
                "matchup_tags": [],
                "home_venue_summary": h_ven_sum,
                "away_venue_summary": a_ven_sum,
                "headline_reason": "",
                "quadrant": q_key,
                "common_rivals_count": (prof.get("common_rivals") or {}).get("count", 0) if is_full else 0,
            },
        }

    return {
        "status": "success",
        "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 1),
        "updated": updated,
        "downloaded_count": downloaded_count,
        "sqlite_count": sqlite_count,
        "needs_download_ids": needs_download_ids,
        "failed_ids": failed_ids,
    }


def tag_matches_batch(
    match_ids: Optional[List[str]] = None,
    inline_payloads: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Etiqueta en paralelo 1 o varios partidos (ficha individual o filas del Explorador)."""
    t0 = time.perf_counter()
    ids = [str(m).strip() for m in (match_ids or []) if str(m).strip()][:120]
    inline_map = inline_payloads or {}

    items: Dict[str, Dict[str, Any]] = {}
    need_fetch: List[str] = []

    for mid in ids:
        if mid in inline_map and isinstance(inline_map[mid], dict):
            prof = compute_team_profile_and_tags(inline_map[mid], match_id=mid)
            items[mid] = prof
            continue
        cached = _get_cached_profile(mid)
        if cached is not None:
            items[mid] = cached
        else:
            need_fetch.append(mid)

    if need_fetch:
        fetched = _fetch_match_payloads_bulk(need_fetch)

        def _worker(mid: str) -> Tuple[str, Optional[Dict[str, Any]]]:
            pl = fetched.get(mid)
            if not pl:
                return mid, None
            return mid, compute_team_profile_and_tags(pl, match_id=mid)

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            for mid, prof in executor.map(_worker, need_fetch):
                if prof is not None:
                    _set_cached_profile(mid, prof)
                    items[mid] = prof

    # Resumen por etiquetas para los chips de filtro del Explorador
    summary_map: Dict[str, Dict[str, Any]] = {}
    for mid, prof in items.items():
        sc = _parse_score(prof.get("score"))
        fav_cov = None
        if sc and prof.get("ah") is not None:
            fav_cov, _ = _eval_match_fav_cover(sc[0], sc[1], prof.get("ah"))

        seen_codes = set()
        for t in (prof.get("matchup_tags", []) + prof.get("home_tags", []) + prof.get("away_tags", [])):
            code = t["code"]
            if code in seen_codes:
                continue
            seen_codes.add(code)
            entry = summary_map.setdefault(code, {
                "code": code,
                "label": t["label"],
                "tone": t["tone"],
                "count": 0,
                "fav_covers": 0.0,
                "dog_covers": 0.0,
                "settled": 0,
                "match_ids": [],
            })
            entry["count"] += 1
            entry["match_ids"].append(mid)
            if fav_cov in ("COVER", "HALF_WIN", "NO_COVER", "HALF_LOSS", "PUSH"):
                entry["settled"] += 1
                if fav_cov == "COVER":
                    entry["fav_covers"] += 1.0
                elif fav_cov == "HALF_WIN":
                    entry["fav_covers"] += 0.5
                elif fav_cov == "NO_COVER":
                    entry["dog_covers"] += 1.0
                elif fav_cov == "HALF_LOSS":
                    entry["dog_covers"] += 0.5

    tag_summary = []
    for entry in summary_map.values():
        settled = entry["settled"]
        tag_summary.append({
            "code": entry["code"],
            "label": entry["label"],
            "tone": entry["tone"],
            "count": entry["count"],
            "fav_cover_pct": round((entry["fav_covers"] / settled) * 100.0, 1) if settled else None,
            "dog_cover_pct": round((entry["dog_covers"] / settled) * 100.0, 1) if settled else None,
            "match_ids": entry["match_ids"],
        })
    tag_summary.sort(key=lambda x: (-x["count"], x["label"]))

    return {
        "status": "success",
        "elapsed_ms": round((time.perf_counter() - t0) * 1000.0, 1),
        "items": items,
        "tag_summary": tag_summary,
    }
