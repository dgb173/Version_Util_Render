"""Adaptador de datos NowGoal / Plataforma al contrato Snapshot del Motor v6.2.

Transforma los diccionarios cacheados en SQLite / Precacheo en un Snapshot estrictamente
validado por Pydantic para el motor dual entero-binario v6.2.
"""
from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from .contracts import (
    Snapshot, PastMatch, SimilarBlock, Similar,
    Reference, Bridge, Quote, LeagueRecord, Config
)
from .market import quarters

UTC = timezone.utc
SPAIN_TZ = ZoneInfo("Europe/Madrid")


def _norm_str(val: Any) -> str:
    if not val:
        return ""
    text = unicodedata.normalize("NFKD", str(val).strip().lower())
    return "".join(c for c in text if not unicodedata.combining(c))


def _parse_float(val: Any) -> Optional[float]:
    if val in (None, "", "N/A", "null", "None", "-"):
        return None
    try:
        clean = str(val).strip().replace("−", "-").replace(",", ".").replace("+", "")
        return float(clean)
    except (ValueError, TypeError):
        return None


def _parse_int(val: Any) -> Optional[int]:
    f = _parse_float(val)
    return int(round(f)) if f is not None else None


def _round_to_quarter(val: float) -> float:
    return round(val * 4) / 4.0


def _parse_dt(val: Any, default_tz=UTC) -> Optional[datetime]:
    if not val:
        return None
    text = str(val).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=default_tz)
        return dt.astimezone(UTC)
    except Exception:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=default_tz).astimezone(UTC)
        except Exception:
            continue
    return None


def _extract_score(score_str: Any) -> Tuple[Optional[int], Optional[int]]:
    if not score_str:
        return None, None
    m = re.search(r"(\d+)\s*[:\-]\s*(\d+)", str(score_str))
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def _extract_stats_from_rows(stats_data: Any, is_home: bool) -> Dict[str, Optional[int]]:
    out = {
        "shots_for": None, "shots_against": None,
        "sot_for": None, "sot_against": None,
        "attacks_for": None, "attacks_against": None,
        "ap_for": None, "ap_against": None,
        "corners_for": None, "corners_against": None,
    }
    if not stats_data:
        return out

    if isinstance(stats_data, dict):
        rows = []
        for k, v in stats_data.items():
            if isinstance(v, dict):
                rows.append({"label": k, **v})
            elif isinstance(v, (list, tuple)) and len(v) >= 2:
                rows.append({"label": k, "home": v[0], "away": v[1]})
    elif isinstance(stats_data, list):
        rows = stats_data
    else:
        return out

    for row in rows:
        if not isinstance(row, dict):
            continue
        label = _norm_str(row.get("label") or row.get("name") or "")

        h_raw = row.get("home") if row.get("home") is not None else row.get("h") if row.get("h") is not None else row.get("Home")
        a_raw = row.get("away") if row.get("away") is not None else row.get("a") if row.get("a") is not None else row.get("Away")

        h_val = _parse_int(h_raw)
        a_val = _parse_int(a_raw)
        if h_val is None or a_val is None or h_val < 0 or a_val < 0:
            continue

        val_for = h_val if is_home else a_val
        val_against = a_val if is_home else h_val

        # Ataques Peligrosos (AP / DA)
        if "peligros" in label or "dangerous" in label or label in ("ap", "da", "ataques_peligrosos", "dangerous_attacks"):
            out["ap_for"], out["ap_against"] = min(val_for, 200), min(val_against, 200)
        # Tiros a Puerta (SOT)
        elif "puerta" in label or "target" in label or label in ("sot", "tiros_a_puerta", "shots_on_target", "on_target"):
            out["sot_for"], out["sot_against"] = min(val_for, 50), min(val_against, 50)
        # Tiros Totales (Shots)
        elif "tiro" in label or "shot" in label or "remate" in label or "disparo" in label:
            out["shots_for"], out["shots_against"] = min(val_for, 80), min(val_against, 80)
        # Ataques Totales (Attacks)
        elif "ataque" in label or "attack" in label:
            out["attacks_for"], out["attacks_against"] = min(val_for, 300), min(val_against, 300)
        # Córners
        elif "corner" in label or "corners" in label or "saques de esquina" in label or "esquina" in label:
            out["corners_for"], out["corners_against"] = min(val_for, 30), min(val_against, 30)

    # Coherencia estricta: sot <= shots
    for side in ("for", "against"):
        shots, sot = out[f"shots_{side}"], out[f"sot_{side}"]
        if shots is not None and sot is not None and sot > shots:
            out[f"shots_{side}"] = sot

    return out


_STATS_RESOLVE_CACHE = {}

def _resolve_stats_for_match(m: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Resuelve telemetría detallada de ataques peligrosos y tiros bajo demanda."""
    existing = m.get("stats_rows") or m.get("stats") or m.get("statistics")
    if existing:
        return existing if isinstance(existing, list) else [existing]

    raw_mid = str(m.get("match_id") or m.get("matchIndex") or m.get("id") or "").strip()
    digits = "".join(filter(str.isdigit, raw_mid))
    if not digits:
        return []

    if digits in _STATS_RESOLVE_CACHE:
        return _STATS_RESOLVE_CACHE[digits]

    try:
        from ..last_general_context import _get_stats_rows
    except Exception:
        try:
            from modules.last_general_context import _get_stats_rows
        except Exception:
            _get_stats_rows = None

    if _get_stats_rows:
        try:
            rows = _get_stats_rows(digits) or []
            _STATS_RESOLVE_CACHE[digits] = rows
            return rows
        except Exception:
            pass

    return []


def build_snapshot_and_config(payload: Dict[str, Any], decision_at: Optional[datetime] = None) -> Tuple[Snapshot, Config]:
    """Convierte un payload de partido NowGoal en un par (Snapshot, Config) compatible y validado."""
    event_id = str(payload.get("match_id") or payload.get("id") or "0").strip()
    home_name = str(payload.get("home_name") or payload.get("home_team") or "Local").strip()
    away_name = str(payload.get("away_name") or payload.get("away_team") or "Visitante").strip()
    home_id = str(payload.get("home_id") or _norm_str(home_name) or "home").strip()
    away_id = str(payload.get("away_id") or _norm_str(away_name) or "away").strip()
    if home_id == away_id:
        home_id = f"{home_id}_H"
        away_id = f"{away_id}_A"

    # Identificador canónico de competición (usar ID si existe, o nombre)
    comp_id = str(payload.get("league_id") or payload.get("league_name") or "COMP").strip()

    # Fechas
    kickoff_dt = None
    if payload.get("start_time"):
        kickoff_dt = _parse_dt(payload["start_time"], default_tz=SPAIN_TZ)
    if not kickoff_dt and payload.get("time_obj"):
        kickoff_dt = _parse_dt(payload["time_obj"], default_tz=UTC)
    if not kickoff_dt:
        m_date = payload.get("match_date") or payload.get("date")
        m_time = payload.get("time") or "12:00"
        if m_date:
            kickoff_dt = _parse_dt(f"{m_date} {m_time}", default_tz=SPAIN_TZ)
    if not kickoff_dt:
        kickoff_dt = datetime.now(UTC) + timedelta(hours=2)

    now_utc = datetime.now(UTC)
    if decision_at is None:
        decision_at = now_utc

    # Regla estricta pre-partido: decision_at < kickoff
    if decision_at >= kickoff_dt:
        decision_at = kickoff_dt - timedelta(minutes=15)

    # Líneas principales
    odds_data = payload.get("main_match_odds") or {}
    raw_ah = _parse_float(payload.get("handicap") or odds_data.get("ah_linea"))
    if raw_ah is None:
        raw_ah = 0.0
    raw_home_ah = _round_to_quarter(raw_ah)

    raw_ou = _parse_float(payload.get("goal_line") or odds_data.get("goals_linea"))
    if raw_ou is None or raw_ou < 0.5:
        raw_ou = 2.5
    ou_line = _round_to_quarter(raw_ou)

    # Clasificación / Tabla
    home_st = payload.get("home_standings") or {}
    away_st = payload.get("away_standings") or {}
    rank_home = _parse_int(home_st.get("ranking"))
    if rank_home is not None and rank_home < 1:
        rank_home = None
    rank_away = _parse_int(away_st.get("ranking"))
    if rank_away is not None and rank_away < 1:
        rank_away = None

    record_home = None
    if _parse_int(home_st.get("total_v")) is not None:
        record_home = LeagueRecord(
            wins=max(0, _parse_int(home_st.get("total_v")) or 0),
            draws=max(0, _parse_int(home_st.get("total_e")) or 0),
            losses=max(0, _parse_int(home_st.get("total_d")) or 0),
        )

    record_away = None
    if _parse_int(away_st.get("total_v")) is not None:
        record_away = LeagueRecord(
            wins=max(0, _parse_int(away_st.get("total_v")) or 0),
            draws=max(0, _parse_int(away_st.get("total_e")) or 0),
            losses=max(0, _parse_int(away_st.get("total_d")) or 0),
        )

    # Historial de partidos
    history_matches: List[PastMatch] = []
    seen_history_keys = set()
    min_age_seen = float('inf')
    max_age_seen = 21.0
    max_role_diff_seen = 0.0
    current_home_strength = quarters(raw_home_ah)

    def process_match_list(matches: List[Dict[str, Any]], target_team_id: str, is_target_home: bool):
        nonlocal min_age_seen, max_age_seen, max_role_diff_seen
        if not isinstance(matches, list):
            return
        for idx, m in enumerate(matches):
            if not isinstance(m, dict):
                continue
            if m.get("is_different_league"):
                continue  # ignorar partidos de otra competición para mantener muestra homogénea
            raw_mid = str(m.get("match_id") or m.get("matchIndex") or m.get("id") or
                          f"{m.get('date')}_{m.get('home_team')}_{m.get('away_team')}").strip()
            if raw_mid == event_id:
                continue

            k_date = m.get("date") or m.get("match_date")
            m_dt = _parse_dt(k_date, default_tz=SPAIN_TZ)
            if not m_dt or m_dt >= decision_at:
                continue

            age_days = (decision_at - m_dt).total_seconds() / 86400.0
            if age_days > max_age_seen:
                max_age_seen = age_days
            if age_days < min_age_seen:
                min_age_seen = age_days

            avail_dt = min(decision_at, m_dt + timedelta(hours=2))
            if not m_dt < avail_dt <= decision_at:
                avail_dt = m_dt + timedelta(minutes=1)
                if avail_dt > decision_at:
                    continue

            # Determinación de equipos y goles
            m_home = str(m.get("home_team") or m.get("home") or "").strip()
            m_away = str(m.get("away_team") or m.get("away") or "").strip()
            score_gf, score_ga = _extract_score(m.get("score") or m.get("score_raw"))
            if score_gf is None or score_ga is None:
                continue

            target_name = home_name if target_team_id == home_id else away_name
            if _norm_str(target_name) == _norm_str(m_home):
                venue, opponent = "home", m_away
            elif _norm_str(target_name) == _norm_str(m_away):
                venue, opponent = "away", m_home
            else:
                continue
            opp_id = (home_id if _norm_str(opponent) == _norm_str(home_name) else
                      away_id if _norm_str(opponent) == _norm_str(away_name) else _norm_str(opponent))
            if venue == "home":
                gf, ga = score_gf, score_ga
            else:
                gf, ga = score_ga, score_gf

            hist_ah = _parse_float(m.get("ahLine") or m.get("ahLine_raw") or m.get("handicap_line_raw"))
            if hist_ah is None:
                # Si no tiene línea histórica, asignar la línea actual de su rol para no descartar
                hist_home_ah = raw_home_ah
            else:
                hist_home_ah = _round_to_quarter(hist_ah)

            role_diff = abs(quarters(hist_home_ah) - current_home_strength)
            if role_diff > max_role_diff_seen:
                max_role_diff_seen = role_diff

            hist_ou = _parse_float(m.get("ouLine") or m.get("ouLine_raw") or m.get("over_under_line_raw"))
            hist_ou_line = _round_to_quarter(hist_ou) if (hist_ou is not None and hist_ou >= 0.5) else ou_line

            # Telemetría de remates y ataques peligrosos
            raw_stats = m.get("stats_rows") or m.get("stats") or m.get("statistics")
            if not raw_stats and raw_mid and age_days <= 25:
                raw_stats = _resolve_stats_for_match(m)
            stats = _extract_stats_from_rows(raw_stats, is_home=(venue == "home"))

            # ID con espacio de nombres por equipo para evitar colisiones entre perspectivas
            scoped_mid = f"{target_team_id}_{raw_mid}"
            key = (target_team_id, scoped_mid)
            if key in seen_history_keys:
                continue

            pm = PastMatch(
                match_id=scoped_mid,
                team_id=target_team_id,
                opponent_id=opp_id,
                competition_id=comp_id,  # Homogéneo para que pase el filtro de competición
                venue=venue,
                opponent_squad="senior",
                kickoff=m_dt,
                available_at=avail_dt,
                source_ref=f"NowGoal:{scoped_mid}",
                raw_home_ah=hist_home_ah,
                ou_line=hist_ou_line,
                gf=gf,
                ga=ga,
                shots_for=stats["shots_for"],
                shots_against=stats["shots_against"],
                sot_for=stats["sot_for"],
                sot_against=stats["sot_against"],
                attacks_for=stats["attacks_for"],
                attacks_against=stats["attacks_against"],
                ap_for=stats["ap_for"],
                ap_against=stats["ap_against"],
                corners_for=stats["corners_for"],
                corners_against=stats["corners_against"],
                red_card_context=bool(m.get("home_red") or m.get("away_red")),
            )
            history_matches.append(pm)
            seen_history_keys.add(key)

    # Merge every real source before deduplication so a summary cannot hide telemetry.
    for side, team in (("home", home_id), ("away", away_id)):
        merged = {}
        last_m = payload.get(f"last_{side}_match")
        if isinstance(last_m, dict) and not (last_m.get("stats_rows") or last_m.get("stats")):
            resolved_last = _resolve_stats_for_match(last_m)
            if resolved_last:
                last_m["stats_rows"] = resolved_last
        sources = [last_m] + [payload.get(f"recent_{side}_matches{suffix}") for suffix in
                   ("", "_all", "_same_league_general", "_same_league_specific")]
        for source in sources:
            source_items = source if isinstance(source, list) else [source]
            for row in source_items:
                if not isinstance(row, dict):
                    continue

                key = (str(row.get("date") or row.get("match_date")),
                       _norm_str(row.get("home_team") or row.get("home")),
                       _norm_str(row.get("away_team") or row.get("away")))
                old = merged.get(key, {})
                stats = {_norm_str(x.get("label") or x.get("name")): x
                         for x in (old.get("stats_rows") or []) + (row.get("stats_rows") or [])
                         if isinstance(x, dict) and _parse_int(x.get("home")) is not None
                         and _parse_int(x.get("away")) is not None}
                merged[key] = {**old, **{k:v for k,v in row.items() if v is not None},
                               "stats_rows": list(stats.values())}
        process_match_list(list(merged.values()), team, is_target_home=(side == "home"))

    # Ajustar configuración adaptativa para abarcar el historial real sin violar las reglas
    history_days_needed = max(90, int(max_age_seen + 7))
    role_tol_needed = max(3, int(max_role_diff_seen + 2))

    # Si la competición tiene parón o el último partido fue hace > 18 días,
    # expandir active_days para abarcar la ventana activa del equipo
    if min_age_seen < float('inf') and min_age_seen > 18:
        active_days_needed = int(min_age_seen + 35)
    else:
        active_days_needed = 21

    cfg = Config(
        version="6.2.0-binary",
        active_days=max(21, min(history_days_needed, active_days_needed)),
        history_days=history_days_needed,
        active_n=5,
        role_tolerance_q=role_tol_needed,
        min_active_n=1,
        min_similar_n=1,
    )

    # Cuotas negociables
    quotes: List[Quote] = []
    avail_q = decision_at - timedelta(minutes=10)
    ah_h_odds = _parse_float(odds_data.get("ah_home_cuota"))
    ah_a_odds = _parse_float(odds_data.get("ah_away_cuota"))
    ou_o_odds = _parse_float(odds_data.get("goals_over_cuota"))
    ou_u_odds = _parse_float(odds_data.get("goals_under_cuota"))

    price_fmt = str(odds_data.get("price_format") or odds_data.get("ah_price_format") or "").lower()
    is_hk = "hongkong" in price_fmt or "hk" in price_fmt

    def adjust_odds(val: Optional[float]) -> Optional[float]:
        if val is None:
            return None
        if is_hk and val < 1.0:
            return round(val + 1.0, 3)
        return round(val, 3) if val > 1.0 else None

    ah_h_dec = adjust_odds(ah_h_odds)
    ah_a_dec = adjust_odds(ah_a_odds)
    ou_o_dec = adjust_odds(ou_o_odds)
    ou_u_dec = adjust_odds(ou_u_odds)

    h_q = quarters(raw_home_ah)
    t_q = quarters(ou_line)

    if ah_h_dec:
        quotes.append(Quote(market="AH_HOME", line_q=-h_q, odds=ah_h_dec, available_at=avail_q, bookmaker="Bet365", source_ref=f"Bet365:AH_HOME:{event_id}"))
    if ah_a_dec:
        quotes.append(Quote(market="AH_AWAY", line_q=h_q, odds=ah_a_dec, available_at=avail_q, bookmaker="Bet365", source_ref=f"Bet365:AH_AWAY:{event_id}"))
    if ou_o_dec:
        quotes.append(Quote(market="OVER", line_q=t_q, odds=ou_o_dec, available_at=avail_q, bookmaker="Bet365", source_ref=f"Bet365:OVER:{event_id}"))
    if ou_u_dec:
        quotes.append(Quote(market="UNDER", line_q=t_q, odds=ou_u_dec, available_at=avail_q, bookmaker="Bet365", source_ref=f"Bet365:UNDER:{event_id}"))

    # Referencias de goles (H2H previos)
    references: List[Reference] = []
    seen_refs = set()
    for pm in history_matches:
        if pm.match_id not in seen_refs:
            references.append(Reference(
                match_id=pm.match_id,
                total_goals=pm.gf + pm.ga,
                kickoff=pm.kickoff,
                available_at=pm.available_at,
                source_ref=pm.source_ref,
            ))
            seen_refs.add(pm.match_id)

    # Baseline de liga
    baseline_dt = decision_at - timedelta(days=1)

    snapshot = Snapshot(
        schema_version="6.2-binary",
        event_id=event_id,
        home_id=home_id,
        away_id=away_id,
        competition_id=comp_id,
        decision_at=decision_at,
        kickoff=kickoff_dt,
        raw_home_ah=raw_home_ah,
        ou_line=ou_line,
        rank_home=rank_home,
        rank_away=rank_away,
        record_home=record_home,
        record_away=record_away,
        senior_match=True,
        synthetic=False,
        provider="NowGoal",
        provider_definitions="90 min; tiros oficiales y córners de NowGoal",
        history=history_matches,
        similar_current=None,
        similar_h2h=None,
        references=references[:10],
        bridges=[],
        quotes=quotes,
        league_sot_per_team=4.5,
        league_conversion=0.30,
        baseline_available_at=baseline_dt,
        baseline_source_ref=f"NowGoal:Baseline:{comp_id}",
    )

    return snapshot, cfg


def build_snapshot_from_payload(payload: Dict[str, Any], decision_at: Optional[datetime] = None) -> Snapshot:
    snap, _ = build_snapshot_and_config(payload, decision_at)
    return snap
