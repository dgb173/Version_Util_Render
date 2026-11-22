"""Módulo backend para la vista dedicada de análisis `/ficha-completa/<match_id>`.

Prepara en un solo payload el estudio completo del partido objetivo:
- Fila 0 en formato Patrón 2.0 / Explorador (`card`)
- Tablas normalizadas Home/Away (sede específica hasta 20 y todas las sedes hasta 20)
- Cruces de rivales comunes (sede específica y general)
- Filtrado y resúmenes en líneas AH (±0.5) y O/U (±0.25) similares
- Lista de equipos seleccionables para el comparador rápido A vs B

No realiza escrituras en SQLite ni altera los caches históricos de la app.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from modules import estudio_scraper
from modules import pattern_search_v2 as ps2


def _clean_match_id(raw: Any) -> str:
    return "".join(ch for ch in str(raw or "") if ch.isdigit())


def _safe_str(val: Any, default: str = "") -> str:
    if val is None:
        return default
    txt = str(val).strip()
    return txt if txt else default


def _date_sort_key(raw_date: Any) -> Tuple[int, int, int]:
    txt = _safe_str(raw_date)
    if not txt:
        return (1900, 1, 1)
    m_iso = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", txt)
    if m_iso:
        return (int(m_iso.group(1)), int(m_iso.group(2)), int(m_iso.group(3)))
    m_lat4 = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{4})", txt)
    if m_lat4:
        return (int(m_lat4.group(3)), int(m_lat4.group(2)), int(m_lat4.group(1)))
    m_lat2 = re.search(r"(\d{1,2})[-/](\d{1,2})[-/](\d{2})(?!\d)", txt)
    if m_lat2:
        yy = int(m_lat2.group(3))
        year = (2000 + yy) if yy < 80 else (1900 + yy)
        return (year, int(m_lat2.group(2)), int(m_lat2.group(1)))
    return (1900, 1, 1)


def _short_date(raw_date: Any) -> str:
    txt = _safe_str(raw_date, "-")
    m_iso = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})(.*)$", txt)
    if m_iso:
        return f"{m_iso.group(3).zfill(2)}/{m_iso.group(2).zfill(2)}/{m_iso.group(1)[-2:]}{m_iso.group(4)}"
    return txt


def _norm_league_id(raw_val: Any) -> str:
    txt = _safe_str(raw_val)
    if not txt or txt == "-":
        return ""
    digits = "".join(ch for ch in txt if ch.isdigit())
    return digits or txt.lower()


def _parse_int_or_none(raw_val: Any) -> Optional[int]:
    if raw_val in (None, "", "-", "N/A", "null", "None"):
        return None
    try:
        return int(float(str(raw_val).strip()))
    except (TypeError, ValueError):
        return None


def _eval_ou_result(hg: int, ag: int, ou_val: Optional[float]) -> str:
    if ou_val is None:
        return "SIN_DATO"
    diff = (hg + ag) - ou_val
    if diff > 0.01:
        return "OVER"
    if diff < -0.01:
        return "UNDER"
    return "PUSH"


def _compute_standings_wdl_text(standings: Any) -> Dict[str, Any]:
    """Normaliza la estructura de standings para el resumen de Fila 0."""
    if not isinstance(standings, dict):
        return {}
    return {
        "ranking": _safe_str(standings.get("ranking") or standings.get("rank")),
        "total_pj": _safe_str(standings.get("total_pj")),
        "total_v": _safe_str(standings.get("total_v")),
        "total_e": _safe_str(standings.get("total_e")),
        "total_d": _safe_str(standings.get("total_d")),
        "total_gf": _safe_str(standings.get("total_gf")),
        "total_gc": _safe_str(standings.get("total_gc")),
        "specific_pj": _safe_str(standings.get("specific_pj")),
        "specific_v": _safe_str(standings.get("specific_v")),
        "specific_e": _safe_str(standings.get("specific_e")),
        "specific_d": _safe_str(standings.get("specific_d")),
        "specific_gf": _safe_str(standings.get("specific_gf")),
        "specific_gc": _safe_str(standings.get("specific_gc")),
        "specific_type": _safe_str(standings.get("specific_type")),
    }


def _extract_target_odds(payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    odds = payload.get("main_match_odds") if isinstance(payload.get("main_match_odds"), dict) else {}
    ah_raw = (
        odds.get("ah_linea")
        or payload.get("ah")
        or payload.get("ah_line")
        or payload.get("handicap")
    )
    ou_raw = (
        odds.get("goals_linea")
        or payload.get("ou")
        or payload.get("ou_line")
        or payload.get("goal_line")
    )
    return ps2._parse_ah(ah_raw), ps2._parse_ou(ou_raw)


def _has_sufficient_context(pre_context: Any) -> bool:
    if not isinstance(pre_context, dict):
        return False
    cur = pre_context.get("current")
    if not isinstance(cur, dict):
        return False
    hm = cur.get("home_matches") or []
    hma = cur.get("home_matches_all") or []
    am = cur.get("away_matches") or []
    ama = cur.get("away_matches_all") or []
    return max(len(hm), len(hma)) >= 5 and max(len(am), len(ama)) >= 5


def _ensure_pre_match_context(
    payload: Dict[str, Any],
    match_id: str,
    force_refresh: bool = False,
) -> Tuple[Dict[str, Any], bool]:
    """Hidrata `pre_match_context` en RAM si falta o se pide refresco, sin escribir SQLite."""
    working = dict(payload or {})
    existing_pre = working.get("pre_match_context")
    if not force_refresh and _has_sufficient_context(existing_pre):
        return working, False

    ah_val, ou_val = _extract_target_odds(working)
    cur_existing = (existing_pre.get("current") if isinstance(existing_pre, dict) else {}) or {}
    is_neutral = cur_existing.get("is_neutral_venue")
    try:
        fresh = estudio_scraper.analizar_contexto_previo_rapido(
            match_id,
            current_ah=ah_val,
            current_goal_line=ou_val,
            is_neutral_venue=is_neutral,
        )
        if isinstance(fresh, dict) and not fresh.get("error") and isinstance(fresh.get("current"), dict):
            working["pre_match_context"] = fresh
            return working, True
    except Exception as exc:
        logging.warning("ficha_completa: fallo hidratando pre_match_context (%s): %s", match_id, exc)
    return working, False


def _build_fallback_card(payload: Dict[str, Any], match_id: str) -> Dict[str, Any]:
    """Construye una card compatible con Explorador si `build_explorer_card` devuelve None."""
    pre_cur = ((payload.get("pre_match_context") or {}).get("current") or {}) if isinstance(payload.get("pre_match_context"), dict) else {}
    ah_val, ou_val = _extract_target_odds(payload)
    home_name = _safe_str(payload.get("home_name") or pre_cur.get("home_name"), "Local")
    away_name = _safe_str(payload.get("away_name") or pre_cur.get("away_name"), "Visitante")
    score_val = _safe_str(payload.get("final_score") or payload.get("score"), "?:?")
    date_val = _safe_str(payload.get("date") or payload.get("match_date") or payload.get("time") or pre_cur.get("date"))
    league_val = _safe_str(payload.get("league_name") or pre_cur.get("league_name") or payload.get("league_id"))

    card: Dict[str, Any] = {
        "match_id": match_id,
        "league": league_val,
        "date": date_val,
        "candidate": {
            "match_id": match_id,
            "home": home_name,
            "away": away_name,
            "home_id": _clean_match_id(payload.get("home_id")),
            "away_id": _clean_match_id(payload.get("away_id")),
            "score": score_val,
            "ah_real": ps2._format_ah(ah_val) if ah_val is not None else "-",
            "ou_line": ps2._format_ah(ou_val) if ou_val is not None else "-",
            "home_standings": _compute_standings_wdl_text(payload.get("home_standings")),
            "away_standings": _compute_standings_wdl_text(payload.get("away_standings")),
        },
        "prev_home": payload.get("last_home_match"),
        "prev_away": payload.get("last_away_match"),
        "h2h_stadium": payload.get("h2h_stadium"),
        "h2h_general": payload.get("h2h_general"),
        "h2h_col3": payload.get("h2h_col3"),
        "ind_local": None,
        "ind_visitante": None,
    }
    comp = payload.get("comparative_indirect")
    if isinstance(comp, dict):
        card["ind_local"] = comp.get("left")
        card["ind_visitante"] = comp.get("right")
    try:
        return ps2._enrich_card_ou(card, target_payload=payload)
    except Exception:
        return card


def _extract_raw_rows_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    pre = payload.get("pre_match_context") if isinstance(payload.get("pre_match_context"), dict) else {}
    cur = pre.get("current") if isinstance(pre.get("current"), dict) else {}

    home_venue = list(cur.get("home_matches") or [])
    home_all = list(cur.get("home_matches_all") or [])
    away_venue = list(cur.get("away_matches") or [])
    away_all = list(cur.get("away_matches_all") or [])

    if not home_all and home_venue:
        home_all = list(home_venue)
    if not away_all and away_venue:
        away_all = list(away_venue)

    return {
        "current": cur,
        "previous": pre.get("previous"),
        "home_venue": home_venue,
        "home_all": home_all,
        "away_venue": away_venue,
        "away_all": away_all,
    }


def _normalize_row(
    item: Dict[str, Any],
    team_name: str,
    target_team_ah: Optional[float],
    target_ou: Optional[float],
    target_league_id: str,
    ou_lookup_by_mid: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    row_home = _safe_str(item.get("home") or item.get("home_team"))
    row_away = _safe_str(item.get("away") or item.get("away_team"))
    if not row_home and not row_away:
        return None

    is_home = ps2._same_team(row_home, team_name)
    is_away = ps2._same_team(row_away, team_name)
    if not is_home and not is_away:
        # Si el alias del equipo difiere ligeramente, inferir si coincide menos con alguno o asumir Local si venía de table_v1
        is_home = False

    rival = row_away if is_home else row_home
    rival_norm = ps2._norm_team(rival)
    row_mid = _clean_match_id(item.get("match_id") or item.get("matchIndex") or item.get("id"))
    raw_date = _safe_str(item.get("date") or item.get("match_date") or item.get("date_txt") or item.get("date_str"), "-")
    raw_league = _safe_str(item.get("league_id_hist") or item.get("league_id") or item.get("league") or item.get("league_name"), "-")
    same_league = bool(target_league_id and _norm_league_id(raw_league) == target_league_id)

    raw_score = _safe_str(item.get("score") or item.get("score_raw") or item.get("result"), "-")
    score_tuple = ps2._parse_score(raw_score)
    goals_for: Optional[int] = None
    goals_against: Optional[int] = None
    total_goals: Optional[int] = None
    wdl = "N/A"
    res_letter = "-"
    if score_tuple is not None:
        hg, ag = score_tuple
        goals_for = hg if is_home else ag
        goals_against = ag if is_home else hg
        total_goals = hg + ag
        if goals_for > goals_against:
            wdl, res_letter = "W", "V"
        elif goals_for < goals_against:
            wdl, res_letter = "L", "D"
        else:
            wdl, res_letter = "D", "E"

    raw_ah = next(
        (
            item.get(k)
            for k in ("ahLine", "ahLine_raw", "ah_line", "ah", "handicap")
            if item.get(k) not in (None, "", "N/A", "null")
        ),
        None,
    )
    ah_num = ps2._parse_ah(raw_ah)
    ah_str = ps2._format_ah(ah_num) if ah_num is not None else (_safe_str(raw_ah, "-") or "-")
    team_ah: Optional[float] = None
    if ah_num is not None:
        team_ah = ah_num if is_home else -ah_num

    ah_cover = "SIN_DATO"
    ah_cover_detail = "SIN_DATO"
    if score_tuple is not None and ah_num is not None:
        cover_code, _ = ps2._eval_team_ah(score_tuple[0], score_tuple[1], ah_num, is_home)
        ah_cover_detail = cover_code
        collapsed = ps2._collapse_cover_3(cover_code)
        ah_cover = collapsed or "SIN_DATO"

    raw_ou = next(
        (
            item.get(k)
            for k in ("ouLine", "ou_line", "ou", "goals_line", "over_under_line_raw")
            if item.get(k) not in (None, "", "-", "N/A", "null")
        ),
        None,
    )
    if not raw_ou and row_mid and ou_lookup_by_mid and row_mid in ou_lookup_by_mid:
        raw_ou = ou_lookup_by_mid[row_mid]
    ou_num = ps2._parse_ou(raw_ou)
    ou_str = ps2._format_ah(ou_num) if ou_num is not None else (_safe_str(raw_ou, "-") or "-")
    ou_res = _eval_ou_result(score_tuple[0], score_tuple[1], ou_num) if score_tuple is not None else "SIN_DATO"

    is_similar_ah = bool(
        target_team_ah is not None
        and team_ah is not None
        and abs(team_ah - target_team_ah) <= 0.5 + 1e-6
    )
    is_exact_ah = bool(
        target_team_ah is not None
        and team_ah is not None
        and abs(team_ah - target_team_ah) <= 0.01
    )
    is_similar_ou = bool(
        target_ou is not None
        and ou_num is not None
        and abs(ou_num - target_ou) <= 0.25 + 1e-6
    )

    home_red = _parse_int_or_none(item.get("home_red") if item.get("home_red") is not None else item.get("homeRed"))
    away_red = _parse_int_or_none(item.get("away_red") if item.get("away_red") is not None else item.get("awayRed"))

    team_role = "N/D"
    if team_ah is not None:
        if team_ah > 0.01:
            team_role = "FAV"
        elif team_ah < -0.01:
            team_role = "DOG"
        else:
            team_role = "PK"

    ah_delta_vs_target = (
        round(team_ah - target_team_ah, 2)
        if (team_ah is not None and target_team_ah is not None)
        else None
    )

    return {
        "match_id": row_mid,
        "date": raw_date,
        "short_date": _short_date(raw_date),
        "league_id": raw_league,
        "same_league": same_league,
        "home_team": row_home,
        "away_team": row_away,
        "home_id": _clean_match_id(item.get("home_id")),
        "away_id": _clean_match_id(item.get("away_id")),
        "is_home": is_home,
        "venue": "home" if is_home else "away",
        "rival": rival or "-",
        "rival_norm": rival_norm,
        "score": raw_score,
        "goals_for": goals_for,
        "goals_against": goals_against,
        "total_goals": total_goals,
        "wdl": wdl,
        "res_letter": res_letter,
        "ah": ah_str,
        "ah_num": round(ah_num, 2) if ah_num is not None else None,
        "team_ah": round(team_ah, 2) if team_ah is not None else None,
        "team_role": team_role,
        "ah_delta_vs_target": ah_delta_vs_target,
        "ah_cover": ah_cover,
        "ah_cover_detail": ah_cover_detail,
        "ou": ou_str,
        "ou_num": round(ou_num, 2) if ou_num is not None else None,
        "ou_res": ou_res,
        "home_red": home_red,
        "away_red": away_red,
        "is_similar_ah": is_similar_ah,
        "is_exact_ah": is_exact_ah,
        "is_similar_ou": is_similar_ou,
        "is_common_all": False,
        "is_common_venue": False,
        "other_matches_against_rival": [],
        "_sort_key": _date_sort_key(raw_date),
    }


def _normalize_team_list(
    raw_list: List[Dict[str, Any]],
    team_name: str,
    target_team_ah: Optional[float],
    target_ou: Optional[float],
    target_league_id: str,
    ou_lookup_by_mid: Optional[Dict[str, str]] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, str, str, str]] = set()
    for raw in raw_list or []:
        row = _normalize_row(
            raw,
            team_name=team_name,
            target_team_ah=target_team_ah,
            target_ou=target_ou,
            target_league_id=target_league_id,
            ou_lookup_by_mid=ou_lookup_by_mid,
        )
        if not row:
            continue
        dedup_key = (
            row["match_id"] or row["date"],
            ps2._norm_team(row["home_team"]),
            ps2._norm_team(row["away_team"]),
            row["score"],
        )
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        normalized.append(row)

    normalized.sort(key=lambda r: r["_sort_key"], reverse=True)
    for row in normalized:
        row.pop("_sort_key", None)
    return normalized[:limit]


def _compact_cross_ref(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "match_id": row.get("match_id", ""),
        "date": row.get("date", "-"),
        "short_date": row.get("short_date", "-"),
        "league_id": row.get("league_id", "-"),
        "home_team": row.get("home_team", ""),
        "away_team": row.get("away_team", ""),
        "home_id": row.get("home_id", ""),
        "away_id": row.get("away_id", ""),
        "venue": row.get("venue", ""),
        "is_home": row.get("is_home", False),
        "score": row.get("score", "-"),
        "wdl": row.get("wdl", "N/A"),
        "ah": row.get("ah", "-"),
        "team_ah": row.get("team_ah"),
        "ah_cover": row.get("ah_cover", "SIN_DATO"),
        "ou": row.get("ou", "-"),
        "ou_res": row.get("ou_res", "SIN_DATO"),
    }


def _annotate_common_rivals(
    home_venue: List[Dict[str, Any]],
    home_all: List[Dict[str, Any]],
    away_venue: List[Dict[str, Any]],
    away_all: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Marca rivales comunes en todas las listas y devuelve `common_rivals` consolidado."""
    home_all_by_rival: Dict[str, List[Dict[str, Any]]] = {}
    home_venue_by_rival: Dict[str, List[Dict[str, Any]]] = {}
    away_all_by_rival: Dict[str, List[Dict[str, Any]]] = {}
    away_venue_by_rival: Dict[str, List[Dict[str, Any]]] = {}

    for row in home_all:
        if row["rival_norm"]:
            home_all_by_rival.setdefault(row["rival_norm"], []).append(row)
    for row in home_venue:
        if row["rival_norm"]:
            home_venue_by_rival.setdefault(row["rival_norm"], []).append(row)
    for row in away_all:
        if row["rival_norm"]:
            away_all_by_rival.setdefault(row["rival_norm"], []).append(row)
    for row in away_venue:
        if row["rival_norm"]:
            away_venue_by_rival.setdefault(row["rival_norm"], []).append(row)

    common_all_keys = set(home_all_by_rival.keys()) & set(away_all_by_rival.keys())
    common_venue_keys = set(home_venue_by_rival.keys()) & set(away_venue_by_rival.keys())

    for row in home_venue + home_all:
        rn = row["rival_norm"]
        row["is_common_all"] = rn in common_all_keys
        row["is_common_venue"] = rn in common_venue_keys
        if rn in away_all_by_rival:
            row["other_matches_against_rival"] = [_compact_cross_ref(r) for r in away_all_by_rival[rn][:3]]

    for row in away_venue + away_all:
        rn = row["rival_norm"]
        row["is_common_all"] = rn in common_all_keys
        row["is_common_venue"] = rn in common_venue_keys
        if rn in home_all_by_rival:
            row["other_matches_against_rival"] = [_compact_cross_ref(r) for r in home_all_by_rival[rn][:3]]

    def _merge_unique(primary: List[Dict[str, Any]], secondary: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: List[Dict[str, Any]] = []
        seen_ids: Set[Tuple[str, str, str]] = set()
        for r in (primary + secondary):
            k = (r.get("match_id") or r.get("date", ""), r.get("home_team", ""), r.get("away_team", ""))
            if k in seen_ids:
                continue
            seen_ids.add(k)
            merged.append(r)
        merged.sort(key=lambda x: _date_sort_key(x.get("date")), reverse=True)
        return merged

    home_merged_by_rival: Dict[str, List[Dict[str, Any]]] = {}
    away_merged_by_rival: Dict[str, List[Dict[str, Any]]] = {}
    for rn in common_all_keys | common_venue_keys:
        home_merged_by_rival[rn] = _merge_unique(home_venue_by_rival.get(rn, []), home_all_by_rival.get(rn, []))
        away_merged_by_rival[rn] = _merge_unique(away_venue_by_rival.get(rn, []), away_all_by_rival.get(rn, []))

    common_rivals: List[Dict[str, Any]] = []
    for rn in sorted(
        common_all_keys | common_venue_keys,
        key=lambda k: (
            0 if k in common_venue_keys else 1,
            -len(home_merged_by_rival.get(k, [])) - len(away_merged_by_rival.get(k, [])),
            k,
        ),
    ):
        h_list = home_merged_by_rival.get(rn, [])
        a_list = away_merged_by_rival.get(rn, [])
        display_name = (h_list[0]["rival"] if h_list else (a_list[0]["rival"] if a_list else rn))
        h_sum = _calc_subset_summary(h_list)
        a_sum = _calc_subset_summary(a_list)
        common_rivals.append({
            "rival": display_name,
            "rival_norm": rn,
            "in_venue": rn in common_venue_keys,
            "home_matches": h_list,
            "away_matches": a_list,
            "summary": {
                "home": h_sum,
                "away": a_sum,
            },
        })

    return common_rivals, []


def _calc_subset_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    wins = sum(1 for r in rows if r.get("wdl") == "W")
    draws = sum(1 for r in rows if r.get("wdl") == "D")
    losses = sum(1 for r in rows if r.get("wdl") == "L")

    gf_vals = [r["goals_for"] for r in rows if isinstance(r.get("goals_for"), int)]
    ga_vals = [r["goals_against"] for r in rows if isinstance(r.get("goals_against"), int)]
    gf_total = sum(gf_vals)
    ga_total = sum(ga_vals)
    scored_n = len(gf_vals)

    cover_w = sum(1 for r in rows if r.get("ah_cover") == "COVER")
    cover_l = sum(1 for r in rows if r.get("ah_cover") == "NO_COVER")
    cover_p = sum(1 for r in rows if r.get("ah_cover") == "PUSH")
    ah_decided = cover_w + cover_l

    overs = sum(1 for r in rows if r.get("ou_res") == "OVER")
    unders = sum(1 for r in rows if r.get("ou_res") == "UNDER")
    ou_push = sum(1 for r in rows if r.get("ou_res") == "PUSH")
    ou_decided = overs + unders

    fav_rows = [r for r in rows if r.get("team_role") == "FAV"]
    fav_cw = sum(1 for r in fav_rows if r.get("ah_cover") == "COVER")
    fav_cl = sum(1 for r in fav_rows if r.get("ah_cover") == "NO_COVER")
    fav_dec = fav_cw + fav_cl

    dog_rows = [r for r in rows if r.get("team_role") == "DOG"]
    dog_cw = sum(1 for r in dog_rows if r.get("ah_cover") == "COVER")
    dog_cl = sum(1 for r in dog_rows if r.get("ah_cover") == "NO_COVER")
    dog_dec = dog_cw + dog_cl

    return {
        "total": total,
        "wins": wins,
        "draws": draws,
        "losses": losses,
        "goals_for": gf_total,
        "goals_against": ga_total,
        "goals_for_avg": round(gf_total / scored_n, 2) if scored_n else None,
        "goals_against_avg": round(ga_total / scored_n, 2) if scored_n else None,
        "cover_wins": cover_w,
        "cover_losses": cover_l,
        "cover_push": cover_p,
        "cover_pct": round(cover_w * 100.0 / ah_decided, 1) if ah_decided else None,
        "overs": overs,
        "unders": unders,
        "ou_push": ou_push,
        "over_pct": round(overs * 100.0 / ou_decided, 1) if ou_decided else None,
        "fav_n": len(fav_rows),
        "fav_cover_wins": fav_cw,
        "fav_cover_pct": round(fav_cw * 100.0 / fav_dec, 1) if fav_dec else None,
        "dog_n": len(dog_rows),
        "dog_cover_wins": dog_cw,
        "dog_cover_pct": round(dog_cw * 100.0 / dog_dec, 1) if dog_dec else None,
    }


def _extract_year(raw_date: str, sort_key: str) -> str:
    m = re.search(r"(20\d{2}|19\d{2})", str(raw_date or ""))
    if m:
        return m.group(1)
    if sort_key and len(sort_key) >= 4 and sort_key[:4].isdigit():
        return sort_key[:4]
    return ""


def _build_h2h_history_list(
    working: Dict[str, Any],
    card: Dict[str, Any],
    home_name: str,
    away_name: str,
    target_ah: Optional[float],
    target_ou: Optional[float],
    target_match_id: str = "",
    ou_lookup_by_mid: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Construye el historial cronologico H2H normalizado a la perspectiva del Local de hoy."""
    raw_entries: List[Tuple[Dict[str, Any], Optional[bool]]] = []

    pre_ctx = working.get("pre_match_context") if isinstance(working.get("pre_match_context"), dict) else {}
    pre_h2h = pre_ctx.get("h2h_data") if isinstance(pre_ctx.get("h2h_data"), dict) else {}
    h2h_stad = working.get("h2h_stadium") if isinstance(working.get("h2h_stadium"), dict) else {}
    h2h_gen = working.get("h2h_general") if isinstance(working.get("h2h_general"), dict) else {}
    h2h_data = working.get("h2h_data") if isinstance(working.get("h2h_data"), dict) else {}

    for block in (h2h_stad, pre_h2h, h2h_gen, h2h_data):
        matches_list = block.get("all_matches") if isinstance(block.get("all_matches"), list) else []
        for m in matches_list:
            if isinstance(m, dict):
                raw_entries.append((m, None))

        # Agregar entradas escalares (ah1/res1/date1 y ah6/res6/date6) por si no vinieron en all_matches
        d1 = _safe_str(block.get("date1"))
        r1 = _safe_str(block.get("res1_raw") or block.get("res1"))
        a1 = _safe_str(block.get("ah1"))
        m1_id = _safe_str(block.get("match1_id"))
        if d1 or (r1 and r1 not in ("-", "?:?", "N/A")):
            raw_entries.append((
                {
                    "match_id": m1_id,
                    "date": d1,
                    "home": _safe_str(block.get("h2h_stad_home") or block.get("home_team") or home_name),
                    "away": _safe_str(block.get("h2h_stad_away") or block.get("away_team") or away_name),
                    "score": r1,
                    "ah": a1,
                    "ou": _safe_str(block.get("ou1")),
                    "league": _safe_str(block.get("league1") or block.get("league")),
                },
                True,
            ))

        d6 = _safe_str(block.get("date6"))
        r6 = _safe_str(block.get("res6_raw") or block.get("res6"))
        a6 = _safe_str(block.get("ah6"))
        m6_id = _safe_str(block.get("match6_id"))
        same_as_d1 = bool(
            (d6 and d1 and d6 == d1 and r6 == r1)
            or (m6_id and m1_id and m6_id == m1_id)
        )
        if not same_as_d1 and (d6 or (r6 and r6 not in ("-", "?:?", "N/A"))):
            g_home = _safe_str(block.get("h2h_gen_home") or away_name)
            g_away = _safe_str(block.get("h2h_gen_away") or home_name)
            raw_entries.append((
                {
                    "match_id": m6_id,
                    "date": d6,
                    "home": g_home,
                    "away": g_away,
                    "score": r6,
                    "ah": a6,
                    "ou": _safe_str(block.get("ou6")),
                    "league": _safe_str(block.get("league6") or block.get("league")),
                },
                None,
            ))

    # Fallback desde card (row0) por si solo quedaron ahi los escalares
    if isinstance(card, dict):
        stad_blk = card.get("h2h_stadium") if isinstance(card.get("h2h_stadium"), dict) else {}
        stad_date = _safe_str(stad_blk.get("date"))
        stad_score = _safe_str(stad_blk.get("score"))
        for key, hint in (("h2h_stadium", True), ("h2h_general", None)):
            blk = card.get(key) if isinstance(card.get(key), dict) else {}
            b_date = _safe_str(blk.get("date"))
            b_score = _safe_str(blk.get("score"))
            if key == "h2h_general" and stad_date and b_date == stad_date and b_score == stad_score:
                continue
            if b_date or (b_score and b_score not in ("-", "?:?", "N/A")):
                raw_entries.append((
                    {
                        "match_id": _safe_str(blk.get("match_id")),
                        "date": b_date,
                        "home": _safe_str(blk.get("home_team") or blk.get("home") or (home_name if hint else away_name)),
                        "away": _safe_str(blk.get("away_team") or blk.get("away") or (away_name if hint else home_name)),
                        "score": b_score,
                        "ah": _safe_str(blk.get("ah")),
                        "ou": _safe_str(blk.get("ou")),
                    },
                    hint,
                ))

    seen_mids: Set[str] = set()
    seen_sigs: Set[Tuple[str, bool, str]] = set()
    seen_date_scores: Set[Tuple[str, str]] = set()
    parsed_items: List[Dict[str, Any]] = []

    for item, stadium_hint in raw_entries:
        row_mid = _clean_match_id(item.get("matchIndex") or item.get("match_id") or item.get("id"))
        if target_match_id and row_mid and row_mid == target_match_id:
            continue

        raw_date = _safe_str(
            item.get("date") or item.get("date_raw") or item.get("match_time") or item.get("time")
        )
        raw_home = _safe_str(item.get("home") or item.get("home_team") or item.get("HomeName"))
        raw_away = _safe_str(item.get("away") or item.get("away_team") or item.get("AwayName"))
        raw_score = _safe_str(
            item.get("score") or item.get("score_raw") or item.get("res") or item.get("res_raw"),
            "-",
        ).replace("-", ":")

        # Detectar si fue en el mismo estadio (Local hoy = Home entonces) o estadio invertido
        if raw_home and raw_away and home_name and away_name:
            if ps2._same_team(raw_home, home_name) and ps2._same_team(raw_away, away_name):
                is_same_stadium = True
            elif ps2._same_team(raw_home, away_name) and ps2._same_team(raw_away, home_name):
                is_same_stadium = False
            elif ps2._same_team(raw_home, home_name):
                is_same_stadium = True
            elif ps2._same_team(raw_home, away_name):
                is_same_stadium = False
            else:
                is_same_stadium = bool(stadium_hint)
        else:
            is_same_stadium = bool(stadium_hint)

        score_tuple = ps2._parse_score(raw_score)
        if not raw_date and score_tuple is None:
            continue

        raw_ah = next(
            (
                item.get(k)
                for k in ("ah", "ahLine", "ah_line", "ahLine_raw", "handicap")
                if item.get(k) not in (None, "", "-", "N/A", "?", "null")
            ),
            None,
        )
        ah_num = ps2._parse_ah(raw_ah)
        ah_str = ps2._format_ah(ah_num) if ah_num is not None else (_safe_str(raw_ah, "-") or "-")

        raw_ou = next(
            (
                item.get(k)
                for k in ("ou", "ouLine", "ou_line", "ouLine_raw", "goals_line")
                if item.get(k) not in (None, "", "-", "N/A", "?", "null")
            ),
            None,
        )
        if not raw_ou and row_mid and ou_lookup_by_mid and row_mid in ou_lookup_by_mid:
            raw_ou = ou_lookup_by_mid[row_mid]
        ou_num = ps2._parse_ou(raw_ou)
        ou_str = ps2._format_ah(ou_num) if ou_num is not None else (_safe_str(raw_ou, "-") or "-")

        sort_key = _date_sort_key(raw_date)
        score_norm = f"{score_tuple[0]}:{score_tuple[1]}" if score_tuple else raw_score
        sig = (sort_key or raw_date, is_same_stadium, score_norm)
        if row_mid and row_mid in seen_mids:
            continue
        if sig in seen_sigs:
            continue
        if sort_key and (sort_key, score_norm) in seen_date_scores:
            continue
        if row_mid:
            seen_mids.add(row_mid)
        seen_sigs.add(sig)
        if sort_key:
            seen_date_scores.add((sort_key, score_norm))

        home_persp_ah: Optional[float] = None
        home_persp_role = "N/D"
        if ah_num is not None:
            home_persp_ah = round(ah_num if is_same_stadium else -ah_num, 2)
            if home_persp_ah > 0.01:
                home_persp_role = "FAV"
            elif home_persp_ah < -0.01:
                home_persp_role = "DOG"
            else:
                home_persp_role = "PK"

        ah_delta_vs_today: Optional[float] = None
        ah_mov_dir_vs_today = ""
        ah_movement_vs_today = ""
        if home_persp_ah is not None and target_ah is not None:
            ah_delta_vs_today = round(target_ah - home_persp_ah, 2)
            if ah_delta_vs_today > 0.01:
                ah_mov_dir_vs_today = "SUBE"
            elif ah_delta_vs_today < -0.01:
                ah_mov_dir_vs_today = "BAJA"
            else:
                ah_mov_dir_vs_today = "IGUAL"
            ah_movement_vs_today = f"{ps2._format_ah(home_persp_ah)} \u2192 {ps2._format_ah(target_ah)}"

        cover_own = "SIN_DATO"
        cover_today = "SIN_DATO"
        wdl_local = "SIN_DATO"
        goals_home: Optional[int] = None
        goals_away: Optional[int] = None
        ou_res = "SIN_DATO"
        ou_res_vs_today = "SIN_DATO"

        if score_tuple is not None:
            hg, ag = score_tuple
            goals_home, goals_away = hg, ag
            if ah_num is not None:
                c_own_code, _ = ps2._eval_team_ah(hg, ag, ah_num, is_same_stadium)
                cover_own = ps2._collapse_cover_3(c_own_code) or "SIN_DATO"
            local_g = hg if is_same_stadium else ag
            away_g = ag if is_same_stadium else hg
            if local_g > away_g:
                wdl_local = "W"
            elif local_g < away_g:
                wdl_local = "L"
            else:
                wdl_local = "D"
            if target_ah is not None:
                c_today_code, _ = ps2._eval_team_ah(local_g, away_g, target_ah, True)
                cover_today = ps2._collapse_cover_3(c_today_code) or "SIN_DATO"
            ou_res = _eval_ou_result(hg, ag, ou_num)
            ou_res_vs_today = _eval_ou_result(hg, ag, target_ou)

        ou_delta_vs_today: Optional[float] = None
        ou_mov_dir_vs_today = ""
        ou_movement_vs_today = ""
        if ou_num is not None and target_ou is not None:
            ou_delta_vs_today = round(target_ou - ou_num, 2)
            if ou_delta_vs_today > 0.01:
                ou_mov_dir_vs_today = "SUBE"
            elif ou_delta_vs_today < -0.01:
                ou_mov_dir_vs_today = "BAJA"
            else:
                ou_mov_dir_vs_today = "IGUAL"
            ou_movement_vs_today = f"{ps2._format_ah(ou_num)} \u2192 {ps2._format_ah(target_ou)}"

        parsed_items.append({
            "match_id": row_mid,
            "date": raw_date,
            "short_date": _short_date(raw_date),
            "year": _extract_year(raw_date, sort_key),
            "league": _safe_str(item.get("league") or item.get("league_id") or ""),
            "home_team": raw_home or (home_name if is_same_stadium else away_name),
            "away_team": raw_away or (away_name if is_same_stadium else home_name),
            "is_same_stadium": is_same_stadium,
            "score": score_norm if score_tuple else raw_score,
            "goals_home": goals_home,
            "goals_away": goals_away,
            "wdl_local": wdl_local,
            "ah": ah_str,
            "ah_num": round(ah_num, 2) if ah_num is not None else None,
            "home_persp_ah": home_persp_ah,
            "home_persp_role": home_persp_role,
            "ah_delta_vs_today": ah_delta_vs_today,
            "ah_mov_dir_vs_today": ah_mov_dir_vs_today,
            "ah_movement_vs_today": ah_movement_vs_today,
            "ah_delta_vs_prev": None,
            "cover_own": cover_own,
            "cover_today": cover_today,
            "ou": ou_str,
            "ou_num": round(ou_num, 2) if ou_num is not None else None,
            "ou_delta_vs_today": ou_delta_vs_today,
            "ou_mov_dir_vs_today": ou_mov_dir_vs_today,
            "ou_movement_vs_today": ou_movement_vs_today,
            "ou_delta_vs_prev": None,
            "ou_res": ou_res,
            "ou_res_vs_today": ou_res_vs_today,
            "_sort_key": sort_key,
        })

    parsed_items.sort(key=lambda r: r.get("_sort_key") or "", reverse=True)
    for idx, entry in enumerate(parsed_items):
        if idx + 1 < len(parsed_items):
            prev = parsed_items[idx + 1]
            if entry["home_persp_ah"] is not None and prev["home_persp_ah"] is not None:
                entry["ah_delta_vs_prev"] = round(entry["home_persp_ah"] - prev["home_persp_ah"], 2)
            if entry["ou_num"] is not None and prev["ou_num"] is not None:
                entry["ou_delta_vs_prev"] = round(entry["ou_num"] - prev["ou_num"], 2)
        entry.pop("_sort_key", None)

    return parsed_items


def _collect_selectable_teams(
    home_name: str,
    away_name: str,
    home_rows: List[Dict[str, Any]],
    away_rows: List[Dict[str, Any]],
    common_rivals: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    selectable: List[Dict[str, Any]] = []
    seen_norms: Set[str] = set()

    def _add(name: str, role: str, count: int) -> None:
        clean = _safe_str(name)
        norm = ps2._norm_team(clean)
        if not norm or norm in seen_norms:
            return
        seen_norms.add(norm)
        selectable.append({
            "name": clean,
            "norm": norm,
            "role": role,
            "count": count,
        })

    _add(home_name, "target_home", len(home_rows))
    _add(away_name, "target_away", len(away_rows))

    for cr in common_rivals:
        cnt = len(cr.get("home_matches", [])) + len(cr.get("away_matches", []))
        _add(cr.get("rival", ""), "common_rival", cnt)

    rival_counts: Dict[str, Tuple[str, int]] = {}
    for r in home_rows + away_rows:
        rn = r.get("rival_norm") or ""
        if not rn or rn in seen_norms:
            continue
        prev = rival_counts.get(rn)
        rival_counts[rn] = (prev[0] if prev else r.get("rival", ""), (prev[1] if prev else 0) + 1)

    for rn, (display, cnt) in sorted(rival_counts.items(), key=lambda item: (-item[1][1], item[1][0])):
        _add(display, "rival", cnt)

    return selectable


def _build_ou_lookup_from_payload(payload: Dict[str, Any], card: Dict[str, Any]) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for src in (
        card.get("prev_home"),
        card.get("prev_away"),
        card.get("h2h_stadium"),
        card.get("h2h_general"),
        card.get("h2h_col3"),
        card.get("ind_local"),
        card.get("ind_visitante"),
        payload.get("last_home_match"),
        payload.get("last_away_match"),
        payload.get("h2h_col3"),
    ):
        if not isinstance(src, dict):
            continue
        mid = _clean_match_id(src.get("match_id") or src.get("matchIndex"))
        if not mid:
            continue
        ou_val = src.get("ou") or src.get("ou_line") or src.get("ouLine") or src.get("over_under_line_raw")
        if ou_val not in (None, "", "-", "N/A"):
            lookup[mid] = _safe_str(ou_val)
    return lookup


def build_ficha_completa_payload(
    match_payload: Dict[str, Any],
    match_id: str,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """Construye el JSON completo consumido por `/ficha-completa/<match_id>`."""
    clean_id = _clean_match_id(match_id) or _safe_str(match_id)
    working, hydrated_from_live = _ensure_pre_match_context(match_payload, clean_id, force_refresh=force_refresh)

    pre_ctx = working.get("pre_match_context") if isinstance(working.get("pre_match_context"), dict) else {}
    pre_h2h = pre_ctx.get("h2h_data") if isinstance(pre_ctx.get("h2h_data"), dict) else {}
    if pre_h2h:
        working = dict(working)
        h2h_stad = dict(working.get("h2h_stadium") or {}) if isinstance(working.get("h2h_stadium"), dict) else {}
        if not h2h_stad.get("all_matches") and pre_h2h.get("all_matches"):
            h2h_stad["all_matches"] = pre_h2h.get("all_matches")
        for key in ("ou1", "ou6", "ah1", "ah6", "res1", "res6", "date1", "date6", "match1_id", "match6_id"):
            if h2h_stad.get(key) in (None, "", "-", "?") and pre_h2h.get(key) not in (None, "", "-", "?"):
                h2h_stad[key] = pre_h2h.get(key)
        working["h2h_stadium"] = h2h_stad

    card: Optional[Dict[str, Any]] = None
    try:
        card = ps2.build_explorer_card(working)
    except Exception as exc:
        logging.warning("ficha_completa: build_explorer_card fallo para %s: %s", clean_id, exc)
    if not isinstance(card, dict):
        card = _build_fallback_card(working, clean_id)
    else:
        cand = card.setdefault("candidate", {})
        if not isinstance(cand.get("home_standings"), dict) or not cand.get("home_standings"):
            cand["home_standings"] = _compute_standings_wdl_text(working.get("home_standings"))
        if not isinstance(cand.get("away_standings"), dict) or not cand.get("away_standings"):
            cand["away_standings"] = _compute_standings_wdl_text(working.get("away_standings"))

    cand = card.get("candidate") if isinstance(card.get("candidate"), dict) else {}
    ah_val, ou_val = _extract_target_odds(working)
    if ah_val is None:
        ah_val = ps2._parse_ah(cand.get("ah_real"))
    if ou_val is None:
        ou_val = ps2._parse_ou(cand.get("ou_line"))

    raw_ctx = _extract_raw_rows_from_payload(working)
    cur_ctx = raw_ctx.get("current") if isinstance(raw_ctx.get("current"), dict) else {}

    home_name = _safe_str(cand.get("home") or cur_ctx.get("home_name") or working.get("home_name"), "Local")
    away_name = _safe_str(cand.get("away") or cur_ctx.get("away_name") or working.get("away_name"), "Visitante")
    target_league_id = _norm_league_id(
        cur_ctx.get("league_id") or working.get("league_id") or card.get("league")
    )
    target_home_ah = ah_val
    target_away_ah = (-ah_val) if ah_val is not None else None

    ou_lookup = _build_ou_lookup_from_payload(working, card)

    home_venue = _normalize_team_list(
        raw_ctx["home_venue"],
        team_name=home_name,
        target_team_ah=target_home_ah,
        target_ou=ou_val,
        target_league_id=target_league_id,
        ou_lookup_by_mid=ou_lookup,
        limit=20,
    )
    home_all = _normalize_team_list(
        raw_ctx["home_all"],
        team_name=home_name,
        target_team_ah=target_home_ah,
        target_ou=ou_val,
        target_league_id=target_league_id,
        ou_lookup_by_mid=ou_lookup,
        limit=20,
    )
    away_venue = _normalize_team_list(
        raw_ctx["away_venue"],
        team_name=away_name,
        target_team_ah=target_away_ah,
        target_ou=ou_val,
        target_league_id=target_league_id,
        ou_lookup_by_mid=ou_lookup,
        limit=20,
    )
    away_all = _normalize_team_list(
        raw_ctx["away_all"],
        team_name=away_name,
        target_team_ah=target_away_ah,
        target_ou=ou_val,
        target_league_id=target_league_id,
        ou_lookup_by_mid=ou_lookup,
        limit=20,
    )

    common_rivals, _ = _annotate_common_rivals(home_venue, home_all, away_venue, away_all)

    home_ah_rows = [r for r in home_all if r.get("is_similar_ah")]
    away_ah_rows = [r for r in away_all if r.get("is_similar_ah")]
    home_ou_rows = [r for r in home_all if r.get("is_similar_ou")]
    away_ou_rows = [r for r in away_all if r.get("is_similar_ou")]

    selectable_teams = _collect_selectable_teams(home_name, away_name, home_all, away_all, common_rivals)
    h2h_history = _build_h2h_history_list(
        working,
        card,
        home_name=home_name,
        away_name=away_name,
        target_ah=target_home_ah,
        target_ou=ou_val,
        target_match_id=clean_id,
        ou_lookup_by_mid=ou_lookup,
    )

    return {
        "status": "ok",
        "match_id": clean_id,
        "hydrated_from_live": hydrated_from_live,
        "target": {
            "match_id": clean_id,
            "home": home_name,
            "away": away_name,
            "home_id": _clean_match_id(cand.get("home_id") or working.get("home_id")),
            "away_id": _clean_match_id(cand.get("away_id") or working.get("away_id")),
            "league_id": _safe_str(cur_ctx.get("league_id") or working.get("league_id")),
            "league_name": _safe_str(cur_ctx.get("league_name") or working.get("league_name") or card.get("league")),
            "date": _safe_str(cand.get("date") or card.get("date") or working.get("time")),
            "score": _safe_str(cand.get("score") or working.get("final_score"), "?:?"),
            "ah": ps2._format_ah(ah_val) if ah_val is not None else "-",
            "ah_num": round(ah_val, 2) if ah_val is not None else None,
            "home_target_ah": round(target_home_ah, 2) if target_home_ah is not None else None,
            "away_target_ah": round(target_away_ah, 2) if target_away_ah is not None else None,
            "fav_side": ps2._get_favorite_side(ah_val),
            "ou": ps2._format_ah(ou_val) if ou_val is not None else "-",
            "ou_num": round(ou_val, 2) if ou_val is not None else None,
            "is_neutral_venue": bool(cur_ctx.get("is_neutral_venue")),
        },
        "row0": card,
        "h2h_history": h2h_history,
        "tables": {
            "home_venue": home_venue,
            "home_all": home_all,
            "away_venue": away_venue,
            "away_all": away_all,
            "summary": {
                "home_venue": _calc_subset_summary(home_venue),
                "home_all": _calc_subset_summary(home_all),
                "away_venue": _calc_subset_summary(away_venue),
                "away_all": _calc_subset_summary(away_all),
            },
        },
        "common_rivals": common_rivals,
        "similar_lines": {
            "ah_threshold": 0.5,
            "ou_threshold": 0.25,
            "home_ah": {
                "target_line": round(target_home_ah, 2) if target_home_ah is not None else None,
                "summary": _calc_subset_summary(home_ah_rows),
                "rows": home_ah_rows,
            },
            "away_ah": {
                "target_line": round(target_away_ah, 2) if target_away_ah is not None else None,
                "summary": _calc_subset_summary(away_ah_rows),
                "rows": away_ah_rows,
            },
            "home_ou": {
                "target_line": round(ou_val, 2) if ou_val is not None else None,
                "summary": _calc_subset_summary(home_ou_rows),
                "rows": home_ou_rows,
            },
            "away_ou": {
                "target_line": round(ou_val, 2) if ou_val is not None else None,
                "summary": _calc_subset_summary(away_ou_rows),
                "rows": away_ou_rows,
            },
        },
        "selectable_teams": selectable_teams,
        "pre_context_raw_meta": {
            "has_previous_h2h": bool(raw_ctx.get("previous")),
            "previous_h2h": raw_ctx.get("previous"),
        },
    }
