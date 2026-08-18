import copy
import json
import os
import threading
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from . import estudio_scraper as es
from .red_cards import normalize_red_card_stats_payload


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
CACHE_PATH = DATA_DIR / "last_general_precacheo.json"
COLUMN3_CACHE_PATH = DATA_DIR / "pre_context_row_col3.json"
CACHE_SCHEMA_VERSION = 3
COL3_CACHE_SCHEMA_VERSION = 3

_cache_lock = threading.Lock()
_col3_cache_lock = threading.Lock()


def _read_cache() -> Dict[str, Dict]:
    if not CACHE_PATH.exists():
        return {}
    try:
        with CACHE_PATH.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
        if isinstance(raw, dict) and isinstance(raw.get("matches"), dict):
            return raw["matches"]
        if isinstance(raw, dict):
            return raw
    except Exception:
        return {}
    return {}


def _write_cache(cache: Dict[str, Dict]) -> None:
    payload = {
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "matches": cache,
    }
    try:
        tmp_path = CACHE_PATH.with_suffix(".json.tmp")
        with tmp_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        tmp_path.replace(CACHE_PATH)
    except Exception as e:
        import logging
        logging.warning(f"No se pudo escribir la cache de ultimo general: {e}")


def _read_col3_cache() -> Dict[str, Dict]:
    if not COLUMN3_CACHE_PATH.exists():
        return {}
    try:
        with COLUMN3_CACHE_PATH.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
        if isinstance(raw, dict) and isinstance(raw.get("matches"), dict):
            return raw["matches"]
    except Exception:
        return {}
    return {}


def _write_col3_cache(cache: Dict[str, Dict]) -> None:
    payload = {
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "matches": cache,
    }
    temp_path = COLUMN3_CACHE_PATH.with_suffix(
        f".{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        with temp_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
        temp_path.replace(COLUMN3_CACHE_PATH)
    except Exception as exc:
        import logging
        logging.warning(f"No se pudo escribir la cache ligera Col3: {exc}")
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _norm(value: str) -> str:
    return es._normalize_team_name(value or "")


def _same_team(a: str, b: str) -> bool:
    left = _norm(a)
    right = _norm(b)
    return bool(left and right and (left == right or left in right or right in left))


def _score_parts(score: str) -> Optional[Tuple[int, int]]:
    text = str(score or "").replace(":", "-").strip()
    if "?" in text:
        return None
    parts = text.split("-")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except Exception:
        return None


def _get_stats_rows(match_id_value) -> List[Dict]:
    if not match_id_value:
        return []
    df = es.get_match_progression_stats_data(str(match_id_value))
    return es._df_to_rows(df)


def _row_to_match(details: Dict, team_name: str, league_id: str) -> Optional[Dict]:
    if not details:
        return None
    home = details.get("home") or ""
    away = details.get("away") or ""
    is_home = _same_team(home, team_name)
    is_away = _same_team(away, team_name)
    if not is_home and not is_away:
        return None
    subject_is_home = bool(is_home)
    rival_name = away if subject_is_home else home
    rival_id = details.get("away_id") if subject_is_home else details.get("home_id")
    league_hist = str(details.get("league_id_hist") or "")
    is_different_league = bool(league_id and league_hist and league_hist != str(league_id))

    return {
        "date": details.get("date", "N/A"),
        "home_team": home,
        "away_team": away,
        "home_id": details.get("home_id"),
        "away_id": details.get("away_id"),
        "score": (details.get("score_raw") or details.get("score") or "N/A").replace("-", ":"),
        "handicap_line_raw": details.get("ahLine_raw") or details.get("ahLine") or "-",
        "ah_line": details.get("ahLine") or details.get("ahLine_raw") or "-",
        "ou_line": details.get("ouLine") or "N/A",
        "match_id": details.get("matchIndex"),
        "league_id_hist": league_hist,
        "is_different_league": is_different_league,
        "localia": "H" if subject_is_home else "A",
        "rival_name": rival_name,
        "rival_id": rival_id,
        "home_red": details.get("home_red"),
        "away_red": details.get("away_red"),
    }


def _general_matches(soup, table_id: str, team_name: str, league_id: str, odds_map, limit: int = 30) -> List[Dict]:
    is_home_table = table_id == "table_v1"
    rows = es.extract_recent_matches(
        soup,
        table_id,
        team_name,
        league_id,
        is_home_table,
        odds_map,
        limit=limit,
        is_neutral_venue=True,
    )
    out = []
    for row in rows:
        item = _row_to_match(row, team_name, league_id)
        if item:
            out.append(item)
    return out


def _pick_last_general(matches: List[Dict], league_id: str) -> Optional[Dict]:
    if not matches:
        return None
    same_league = [m for m in matches if not m.get("is_different_league")]
    return copy.deepcopy((same_league or matches)[0])


def _opponent_from_match(match: Optional[Dict], team_name: str) -> Tuple[Optional[str], Optional[str]]:
    if not match:
        return None, None
    return match.get("rival_name"), match.get("rival_id")


def _add_stats(match: Optional[Dict]) -> Optional[Dict]:
    if not match:
        return None
    enriched = dict(match)
    enriched["stats_rows"] = _get_stats_rows(enriched.get("match_id"))
    return enriched


def _comparison_payload(item: Optional[Dict], stats_rows: List[Dict], default_rival_name: Optional[str]) -> Dict:
    if not item:
        return {
            "rival_name": default_rival_name or "N/A",
            "stats_rows": None,
        }
    out = dict(item)
    out["stats_rows"] = stats_rows
    if not out.get("rival_name"):
        out["rival_name"] = default_rival_name or "N/A"
    return out


def _form_summary(matches: List[Dict], team_name: str, limit: int = 8) -> Dict:
    sample = [m for m in matches if not m.get("is_different_league")][:limit]
    wins = draws = losses = gf = ga = 0
    form = []
    compact_matches = []
    for item in sample:
        score = _score_parts(item.get("score"))
        if not score:
            continue
        home_goals, away_goals = score
        is_home = _same_team(item.get("home_team"), team_name)
        team_gf = home_goals if is_home else away_goals
        team_ga = away_goals if is_home else home_goals
        gf += team_gf
        ga += team_ga
        if team_gf > team_ga:
            wins += 1
            form.append("V")
            result = "V"
        elif team_gf == team_ga:
            draws += 1
            form.append("E")
            result = "E"
        else:
            losses += 1
            form.append("D")
            result = "D"
        compact_matches.append({
            "date": item.get("date"),
            "home_team": item.get("home_team"),
            "away_team": item.get("away_team"),
            "score": item.get("score"),
            "ah": item.get("handicap_line_raw") or item.get("ah_line"),
            "localia": item.get("localia"),
            "result": result,
        })

    played = wins + draws + losses
    return {
        "played": played,
        "wins": wins,
        "draws": draws,
        "losses": losses,
        "gf": gf,
        "ga": ga,
        "form": "-".join(form) if form else "-",
        "matches": compact_matches,
    }


def _extract_comparative(soup, table_id: str, main_team: str, opponent: Optional[str], league_id: str, is_home_table: bool, odds_map) -> Optional[Dict]:
    if not opponent:
        return None
    item = es.extract_comparative_match_of(soup, table_id, main_team, opponent, league_id, is_home_table, odds_map)
    if item and not item.get("rival_name"):
        item["rival_name"] = opponent
    return item


def _ids_match(row_id: Optional[str], target_id: Optional[str]) -> bool:
    return bool(row_id and target_id and str(row_id) == str(target_id))


def _team_match(row_name: str, row_id: Optional[str], target_name: Optional[str], target_id: Optional[str]) -> bool:
    return _ids_match(row_id, target_id) or _same_team(row_name, target_name or "")


def _col3_from_loaded_matches(matches: List[Dict], rival_a_name: Optional[str], rival_a_id: Optional[str], rival_b_name: Optional[str], rival_b_id: Optional[str]) -> Dict:
    if not (rival_a_name or rival_a_id) or not (rival_b_name or rival_b_id):
        return {"status": "error", "resultado": "N/A (Datos incompletos para H2H Col3 general)"}

    for item in matches or []:
        home_name = item.get("home_team") or ""
        away_name = item.get("away_team") or ""
        home_id = item.get("home_id")
        away_id = item.get("away_id")

        direct = (
            _team_match(home_name, home_id, rival_a_name, rival_a_id)
            and _team_match(away_name, away_id, rival_b_name, rival_b_id)
        )
        inverse = (
            _team_match(home_name, home_id, rival_b_name, rival_b_id)
            and _team_match(away_name, away_id, rival_a_name, rival_a_id)
        )
        if not (direct or inverse):
            continue

        score = _score_parts(item.get("score"))
        if not score:
            continue
        return {
            "status": "found",
            "goles_home": str(score[0]),
            "goles_away": str(score[1]),
            "handicap": item.get("handicap_line_raw") or item.get("ah_line") or "N/A",
            "match_id": item.get("match_id"),
            "h2h_home_team_name": home_name,
            "h2h_away_team_name": away_name,
            "date": item.get("date") or "N/A",
            "home_red": item.get("home_red"),
            "away_red": item.get("away_red"),
            "source": "loaded_general_history",
        }

    return {"status": "not_found", "resultado": f"H2H directo no encontrado para {rival_a_name or rival_a_id} vs {rival_b_name or rival_b_id}."}


def _lookup_col3_from_key_matches(key_match_ids: List[Optional[str]], rival_a_id: Optional[str], rival_b_id: Optional[str], rival_a_name: Optional[str], rival_b_name: Optional[str]) -> Dict:
    last_error = None
    seen = set()
    for key_match_id in key_match_ids:
        if not key_match_id or key_match_id in seen:
            continue
        seen.add(key_match_id)
        candidate = es.get_h2h_details_for_original_logic_of(
            key_match_id,
            rival_a_id,
            rival_b_id,
            rival_a_name or "Rival A",
            rival_b_name or "Rival B",
        )
        if candidate and candidate.get("status") == "found":
            candidate["source"] = f"h2h_key_{key_match_id}"
            return candidate
        last_error = candidate
    return last_error or {"status": "not_found", "resultado": f"H2H directo no encontrado para {rival_a_name or rival_a_id} vs {rival_b_name or rival_b_id}."}


def _col3_from_key_match_histories(key_match_ids: List[Optional[str]], rival_a_name: Optional[str], rival_a_id: Optional[str], rival_b_name: Optional[str], rival_b_id: Optional[str], league_id: str) -> Dict:
    collected = []
    seen_keys = set()
    for key_match_id in key_match_ids:
        if not key_match_id or key_match_id in seen_keys:
            continue
        seen_keys.add(key_match_id)
        try:
            key_soup = es._load_main_match_soup(str(key_match_id))
            _, _, key_league_id, key_home_name, key_away_name, _ = es.get_team_league_info_from_script_of(key_soup)
            key_odds_map = es.extract_vs_odds(key_soup)
            for table_id, team_name in (("table_v1", key_home_name), ("table_v2", key_away_name)):
                rows = _general_matches(
                    key_soup,
                    table_id,
                    team_name,
                    str(key_league_id or league_id or ""),
                    key_odds_map,
                    limit=50,
                )
                collected.extend(rows)
        except Exception:
            continue

    found = _col3_from_loaded_matches(collected, rival_a_name, rival_a_id, rival_b_name, rival_b_id)
    if found and found.get("status") == "found":
        found["source"] = "key_histories_both_tables"
    return found


def analyze(match_id: str) -> Dict:
    main_match_id = "".join(filter(str.isdigit, str(match_id)))
    if not main_match_id:
        return {"error": "ID de partido inválido."}

    soup = es._load_main_match_soup(main_match_id)
    home_id, away_id, league_id, home_name, away_name, league_name = es.get_team_league_info_from_script_of(soup)
    odds_map = es.extract_vs_odds(soup)

    home_general_matches = _general_matches(soup, "table_v1", home_name, league_id, odds_map, limit=30)
    away_general_matches = _general_matches(soup, "table_v2", away_name, league_id, odds_map, limit=30)
    last_home_general = _add_stats(_pick_last_general(home_general_matches, league_id))
    last_away_general = _add_stats(_pick_last_general(away_general_matches, league_id))

    home_rival_name, home_rival_id = _opponent_from_match(last_home_general, home_name)
    away_rival_name, away_rival_id = _opponent_from_match(last_away_general, away_name)

    col3 = _col3_from_key_match_histories(
        [
            (last_home_general or {}).get("match_id"),
            (last_away_general or {}).get("match_id"),
        ],
        home_rival_name,
        home_rival_id,
        away_rival_name,
        away_rival_id,
        str(league_id or ""),
    )
    if not col3 or col3.get("status") != "found":
        col3 = _lookup_col3_from_key_matches(
        [
            (last_home_general or {}).get("match_id"),
            (last_away_general or {}).get("match_id"),
        ],
            home_rival_id,
            away_rival_id,
            home_rival_name or "Rival A",
            away_rival_name or "Rival B",
        )
    if not col3 or col3.get("status") != "found":
        col3 = _col3_from_loaded_matches(
            home_general_matches + away_general_matches,
            home_rival_name,
            home_rival_id,
            away_rival_name,
            away_rival_id,
        )
    if col3 and col3.get("status") == "found":
        col3["stats_rows"] = _get_stats_rows(col3.get("match_id"))
        col3["is_different_league"] = bool(
            (last_home_general or {}).get("is_different_league")
            or (last_away_general or {}).get("is_different_league")
        )

    comp_left = _extract_comparative(
        soup,
        "table_v1",
        home_name,
        away_rival_name,
        league_id,
        True,
        odds_map,
    )
    comp_right = _extract_comparative(
        soup,
        "table_v2",
        away_name,
        home_rival_name,
        league_id,
        False,
        odds_map,
    )

    main_odds = es.extract_bet365_initial_odds_of(soup, main_match_id)
    final_score, _ = es.extract_final_score_of(soup)
    payload = {
        "match_id": main_match_id,
        "schema_version": CACHE_SCHEMA_VERSION,
        "home_name": home_name,
        "away_name": away_name,
        "league_id": str(league_id or ""),
        "league_name": league_name,
        "final_score": final_score,
        "match_date": es.extract_match_date_of(soup),
        "time": es.extract_match_time_of(soup),
        "main_match_odds": {
            "ah_linea": es.format_ah_as_decimal_string_of(main_odds.get("ah_linea_raw", "?")),
            "goals_linea": es.format_ah_as_decimal_string_of(main_odds.get("goals_linea_raw", "?")),
        },
        "last_general_home": last_home_general,
        "last_general_away": last_away_general,
        "h2h_col3_general": col3,
        "comparativas_indirectas_general": {
            "left": _comparison_payload(comp_left, _get_stats_rows((comp_left or {}).get("match_id")), away_rival_name),
            "right": _comparison_payload(comp_right, _get_stats_rows((comp_right or {}).get("match_id")), home_rival_name),
        },
        "recent_form_same_league": {
            "home": _form_summary(home_general_matches, home_name),
            "away": _form_summary(away_general_matches, away_name),
        },
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    normalize_red_card_stats_payload(payload)
    return payload


def analyze_col3_only(match_id: str) -> Dict:
    """Extrae Col3 sin estadisticas, comparativas ni analisis completo."""
    main_match_id = "".join(filter(str.isdigit, str(match_id)))
    if not main_match_id:
        return {"error": "ID de partido inválido."}

    started = time.time()
    try:
        soup = es._load_main_match_soup(main_match_id)
        _, _, league_id, home_name, away_name, _ = es.get_team_league_info_from_script_of(soup)
        odds_map = es.extract_vs_odds(soup)
        home_matches = _general_matches(soup, "table_v1", home_name, league_id, odds_map, limit=30)
        away_matches = _general_matches(soup, "table_v2", away_name, league_id, odds_map, limit=30)
        last_home = _pick_last_general(home_matches, league_id)
        last_away = _pick_last_general(away_matches, league_id)
        home_rival_name, home_rival_id = _opponent_from_match(last_home, home_name)
        away_rival_name, away_rival_id = _opponent_from_match(last_away, away_name)
        key_ids = [(last_home or {}).get("match_id"), (last_away or {}).get("match_id")]

        col3 = _lookup_col3_from_key_matches(
            key_ids, home_rival_id, away_rival_id,
            home_rival_name or "Rival A", away_rival_name or "Rival B",
        )
        if not col3 or col3.get("status") != "found":
            col3 = _col3_from_key_match_histories(
                key_ids, home_rival_name, home_rival_id,
                away_rival_name, away_rival_id, str(league_id or ""),
            )
        if not col3 or col3.get("status") != "found":
            col3 = _col3_from_loaded_matches(
                home_matches + away_matches, home_rival_name, home_rival_id,
                away_rival_name, away_rival_id,
            )

        return {
            "match_id": main_match_id,
            "schema_version": COL3_CACHE_SCHEMA_VERSION,
            "home_name": home_name,
            "away_name": away_name,
            "last_home_match": last_home,
            "last_away_match": last_away,
            "h2h_col3_general": col3 or {"status": "not_found"},
            "elapsed_seconds": round(time.time() - started, 2),
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
    except Exception as exc:
        return {"error": str(exc), "elapsed_seconds": round(time.time() - started, 2)}


def get_or_create_col3(match_id: str, force_refresh: bool = False) -> Tuple[Dict, bool]:
    """Cache pequena para los botones Col3 de las filas de contexto."""
    mid = "".join(filter(str.isdigit, str(match_id)))
    if not mid:
        return {"error": "ID de partido inválido."}, False

    with _col3_cache_lock:
        cache = _read_col3_cache()
        cached = cache.get(mid)
        if (
            not force_refresh and isinstance(cached, dict)
            and cached.get("schema_version") == COL3_CACHE_SCHEMA_VERSION
        ):
            return copy.deepcopy(cached), True

    result = analyze_col3_only(mid)
    if result and "error" not in result:
        with _col3_cache_lock:
            cache = _read_col3_cache()
            cache[mid] = result
            _write_col3_cache(cache)
    return copy.deepcopy(result), False


def get_or_create_rival_pair_col3(
    cache_key: str,
    key_match_ids: List[Optional[str]],
    rival_a_name: Optional[str],
    rival_a_id: Optional[str],
    rival_b_name: Optional[str],
    rival_b_id: Optional[str],
    league_id: str = "",
    force_refresh: bool = False,
) -> Tuple[Dict, bool]:
    """Busca el Col3 de dos rivales fijados por el partido principal."""
    safe_key = str(cache_key or "").strip()
    if not safe_key or not rival_a_name or not rival_b_name:
        return {"error": "No se pudieron fijar los dos rivales laterales."}, False

    with _col3_cache_lock:
        cache = _read_col3_cache()
        cached = cache.get(safe_key)
        if (
            not force_refresh and isinstance(cached, dict)
            and cached.get("schema_version") == COL3_CACHE_SCHEMA_VERSION
        ):
            return copy.deepcopy(cached), True

    started = time.time()
    col3 = None
    if rival_a_id and rival_b_id:
        col3 = _lookup_col3_from_key_matches(
            key_match_ids, rival_a_id, rival_b_id, rival_a_name, rival_b_name,
        )
    if not col3 or col3.get("status") != "found":
        col3 = _col3_from_key_match_histories(
            key_match_ids, rival_a_name, rival_a_id,
            rival_b_name, rival_b_id, str(league_id or ""),
        )

    result = {
        "schema_version": COL3_CACHE_SCHEMA_VERSION,
        "h2h_col3_general": col3 or {"status": "not_found"},
        "rival_a_name": rival_a_name,
        "rival_b_name": rival_b_name,
        "elapsed_seconds": round(time.time() - started, 2),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    with _col3_cache_lock:
        cache = _read_col3_cache()
        cache[safe_key] = result
        _write_col3_cache(cache)
    return copy.deepcopy(result), False


def get_or_create(match_id: str, force_refresh: bool = False) -> Tuple[Dict, bool]:
    mid = "".join(filter(str.isdigit, str(match_id)))
    if not mid:
        return {"error": "ID de partido inválido."}, False

    with _cache_lock:
        cache = _read_cache()
        if not force_refresh and mid in cache and cache[mid].get("schema_version") == CACHE_SCHEMA_VERSION:
            return copy.deepcopy(cache[mid]), True

    payload = analyze(mid)
    if payload and "error" not in payload:
        with _cache_lock:
            cache = _read_cache()
            cache[mid] = payload
            _write_cache(cache)
    return copy.deepcopy(payload), False


def process_match_ids(match_ids: Iterable[str], force_refresh: bool = False, max_items: Optional[int] = None) -> Dict:
    ids = []
    seen = set()
    for raw in match_ids or []:
        mid = "".join(filter(str.isdigit, str(raw)))
        if not mid or mid in seen:
            continue
        seen.add(mid)
        ids.append(mid)
        if max_items and len(ids) >= max_items:
            break

    ok = 0
    cached = 0
    failed = []
    for mid in ids:
        payload, was_cached = get_or_create(mid, force_refresh=force_refresh)
        if payload and "error" not in payload:
            ok += 1
            if was_cached:
                cached += 1
        else:
            failed.append({"match_id": mid, "error": (payload or {}).get("error", "error desconocido")})

    return {
        "total": len(ids),
        "ok": ok,
        "cached": cached,
        "scraped": ok - cached,
        "failed": failed,
        "cache_path": str(CACHE_PATH),
    }
