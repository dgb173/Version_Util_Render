"""Comparativa manual del rival actual mediante rivales historicos del local.

El modulo no se ejecuta durante la carga del explorador. Solo lo llama el
endpoint asociado al icono de comparativa AH.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from . import estudio_scraper as es


def _same_team(left: str, right: str) -> bool:
    a = es._normalize_team_name(left or "")
    b = es._normalize_team_name(right or "")
    return bool(a and b and (a == b or a in b or b in a))


def _score_parts(value: str) -> Optional[Tuple[int, int]]:
    text = str(value or "").replace(":", "-").strip()
    if "?" in text:
        return None
    parts = text.split("-")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except (TypeError, ValueError):
        return None


def _stats_rows(match_id) -> List[Dict]:
    if not match_id:
        return []
    return es._df_to_rows(es.get_match_progression_stats_data(str(match_id)))


def _league_matches(rows: List[Dict], league_id: str) -> List[Dict]:
    league = str(league_id or "")
    return [row for row in rows if str(row.get("league_id_hist") or "") == league]


def _market_line(row: Dict) -> Optional[float]:
    return es.parse_ah_to_number_of(str(row.get("ahLine_raw") or row.get("ahLine") or ""))


def _home_ah_result(row: Dict) -> Dict:
    """Evalua el AH desde el equipo local historico.

    En este proyecto AH positivo significa favorito local. Por ello el handicap
    aplicado al local es siempre el inverso de la linea de mercado mostrada.
    """
    score = _score_parts(row.get("score_raw") or row.get("score"))
    line = _market_line(row)
    if not score or line is None:
        return {"code": "SIN_DATO", "label": "Sin dato", "covered": False, "net": None}
    net = (score[0] - score[1]) - line
    if net > 0.25 + 1e-9:
        code, label = "COVER", "Cubre"
    elif abs(net - 0.25) <= 1e-9:
        code, label = "HALF_WIN", "Media ganancia"
    elif abs(net) <= 1e-9:
        code, label = "PUSH", "Nulo"
    elif abs(net + 0.25) <= 1e-9:
        code, label = "HALF_LOSS", "Media perdida"
    else:
        code, label = "NO_COVER", "No cubre"
    return {"code": code, "label": label, "covered": net > 0, "net": net}


def _team_result(row: Dict, team_name: str) -> Dict:
    score = _score_parts(row.get("score_raw") or row.get("score"))
    if not score:
        return {"wdl": "?", "rank": None, "goals_for": None, "goals_against": None}
    is_home = _same_team(row.get("home") or row.get("home_team"), team_name)
    is_away = _same_team(row.get("away") or row.get("away_team"), team_name)
    if not (is_home or is_away):
        return {"wdl": "?", "rank": None, "goals_for": None, "goals_against": None}
    gf, ga = (score[0], score[1]) if is_home else (score[1], score[0])
    if gf > ga:
        return {"wdl": "V", "rank": 2, "goals_for": gf, "goals_against": ga}
    if gf == ga:
        return {"wdl": "E", "rank": 1, "goals_for": gf, "goals_against": ga}
    return {"wdl": "D", "rank": 0, "goals_for": gf, "goals_against": ga}


def _row_payload(row: Dict, subject_name: Optional[str] = None, with_stats: bool = True) -> Dict:
    match_id = row.get("matchIndex") or row.get("match_id")
    payload = {
        "match_id": str(match_id or ""),
        "date": row.get("date") or "N/A",
        "league_id": str(row.get("league_id_hist") or ""),
        "home_team": row.get("home") or row.get("home_team") or "",
        "away_team": row.get("away") or row.get("away_team") or "",
        "home_id": row.get("home_id"),
        "away_id": row.get("away_id"),
        "score": str(row.get("score_raw") or row.get("score") or "?:?").replace("-", ":"),
        "ah": es.format_ah_as_decimal_string_of(str(row.get("ahLine_raw") or row.get("ahLine") or "-")),
        "stats_rows": _stats_rows(match_id) if with_stats else [],
    }
    if subject_name:
        payload["subject_result"] = _team_result(row, subject_name)
    return payload


def _opponent(row: Dict, team_name: str) -> Tuple[str, Optional[str]]:
    if _same_team(row.get("home"), team_name):
        return row.get("away") or "", row.get("away_id")
    return row.get("home") or "", row.get("home_id")


def _find_pair(rows: List[Dict], first_name: str, first_id, second_name: str, second_id, league_id: str) -> List[Dict]:
    found = []
    seen = set()
    for row in _league_matches(rows, league_id):
        home_id = str(row.get("home_id") or "")
        away_id = str(row.get("away_id") or "")
        ids_ok = bool(first_id and second_id and {home_id, away_id} == {str(first_id), str(second_id)})
        names_ok = (
            (_same_team(row.get("home"), first_name) and _same_team(row.get("away"), second_name))
            or (_same_team(row.get("home"), second_name) and _same_team(row.get("away"), first_name))
        )
        if not (ids_ok or names_ok):
            continue
        match_id = str(row.get("matchIndex") or "")
        if not match_id or match_id in seen:
            continue
        seen.add(match_id)
        found.append(row)
    return found


def _bridge_matches(reference: Dict, rival_name: str, rival_id, common_name: str, common_id, league_id: str) -> List[Dict]:
    reference_id = reference.get("matchIndex")
    if not reference_id:
        return []
    try:
        soup = es._load_main_match_soup(str(reference_id))
        odds = es.extract_vs_odds(soup)
        rows = es.extract_recent_matches(
            soup, "table_v2", rival_name, None, False, odds,
            limit=100, is_neutral_venue=True,
        )
        return _find_pair(rows, rival_name, rival_id, common_name, common_id, league_id)
    except Exception:
        return []


def _comparison_status(current_result: Dict, historic_result: Dict) -> str:
    current_rank = current_result.get("rank")
    historic_rank = historic_result.get("rank")
    if current_rank is None or historic_rank is None:
        return "SIN_DATO"
    if current_rank > historic_rank:
        return "MEJORA"
    if current_rank < historic_rank:
        return "EMPEORA"
    return "IGUALA"


def _decorate_sample(reference: Dict, home_name: str, away_name: str, common_name: str, common_id, league_id: str, base_result: Dict) -> Dict:
    rival_name, rival_id = _opponent(reference, home_name)
    ah_result = _home_ah_result(reference)
    bridges = _bridge_matches(reference, rival_name, rival_id, common_name, common_id, league_id)
    bridge_payloads = []
    statuses = []
    for bridge in bridges:
        rival_result = _team_result(bridge, rival_name)
        status = _comparison_status(base_result, rival_result)
        statuses.append(status)
        item = _row_payload(bridge, rival_name, with_stats=True)
        item["comparison_status"] = status
        bridge_payloads.append(item)

    sample = _row_payload(reference, home_name, with_stats=True)
    sample.update({
        "rival_name": rival_name,
        "rival_id": rival_id,
        "home_ah_result": ah_result,
        "bridges": bridge_payloads,
        "comparison_summary": {
            "mejora": statuses.count("MEJORA"),
            "iguala": statuses.count("IGUALA"),
            "empeora": statuses.count("EMPEORA"),
            "sin_dato": statuses.count("SIN_DATO"),
        },
    })
    return sample


def analyze(match_id: str) -> Dict:
    main_id = "".join(filter(str.isdigit, str(match_id or "")))
    if not main_id:
        return {"error": "ID de partido invalido."}

    soup = es._load_main_match_soup(main_id)
    home_id, away_id, league_id, home_name, away_name, league_name = es.get_team_league_info_from_script_of(soup)
    odds_map = es.extract_vs_odds(soup)
    current_odds = es.extract_bet365_initial_odds_of(soup, main_id)
    current_ah_raw = str(current_odds.get("ah_linea_raw") or "")
    current_ah = es.parse_ah_to_number_of(current_ah_raw)
    if current_ah is None:
        return {"error": "El partido actual no tiene un handicap valido."}

    home_rows = es.extract_recent_matches(
        soup, "table_v1", home_name, None, True, odds_map,
        limit=100, is_neutral_venue=False,
    )
    away_rows = es.extract_recent_matches(
        soup, "table_v2", away_name, None, False, odds_map,
        limit=100, is_neutral_venue=False,
    )
    home_rows = _league_matches(home_rows, str(league_id or ""))
    away_rows = _league_matches(away_rows, str(league_id or ""))
    if not away_rows:
        return {"error": f"No se encontro un partido fuera de {away_name} en la misma liga."}

    base = away_rows[0]
    common_name, common_id = _opponent(base, away_name)
    base_result = _team_result(base, away_name)
    base_payload = _row_payload(base, away_name, with_stats=True)

    candidates = []
    for row in home_rows:
        rival_name, _ = _opponent(row, home_name)
        if _same_team(rival_name, away_name):
            continue
        candidates.append(row)

    exact_rows = [row for row in candidates if _market_line(row) is not None and abs(_market_line(row) - current_ah) <= 1e-9]
    covered_rows = [row for row in candidates if _home_ah_result(row).get("covered")]

    exact_samples = [
        _decorate_sample(row, home_name, away_name, common_name, common_id, str(league_id or ""), base_result)
        for row in exact_rows
    ]
    covered_samples = [
        _decorate_sample(row, home_name, away_name, common_name, common_id, str(league_id or ""), base_result)
        for row in covered_rows
    ]

    return {
        "match_id": main_id,
        "home_name": home_name,
        "home_id": home_id,
        "away_name": away_name,
        "away_id": away_id,
        "league_id": str(league_id or ""),
        "league_name": league_name,
        "current_ah": es.format_ah_as_decimal_string_of(current_ah_raw),
        "common_opponent": {"name": common_name, "id": common_id},
        "current_rival_base": base_payload,
        "exact_ah_samples": exact_samples,
        "covered_any_ah_samples": covered_samples,
        "manual_scrape": True,
    }
