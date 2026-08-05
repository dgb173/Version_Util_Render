"""Exporta contexto previo y prompt LLM en un TXT legible."""

from __future__ import annotations

import re
import unicodedata


def _rows(value):
    return value if isinstance(value, list) else []


def _normalize_team(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(char for char in text.casefold() if char.isalnum())


def _same_team(left, right):
    a = _normalize_team(left)
    b = _normalize_team(right)
    return bool(a and b and (a == b or a in b or b in a))


def _parse_score(value):
    match = re.search(r"(\d+)\s*[:-]\s*(\d+)", str(value or ""))
    return (int(match.group(1)), int(match.group(2))) if match else None


def _fallback_context(match):
    home = match.get("home_name") or match.get("home_team") or "Local N/D"
    away = match.get("away_name") or match.get("away_team") or "Visitante N/D"
    current = {
        "date": match.get("match_date") or match.get("date") or "fecha N/D",
        "home_name": home,
        "away_name": away,
        "league_name": match.get("league_name") or match.get("league") or "",
        "is_neutral_venue": bool(match.get("is_neutral_venue")),
        "home_matches": (
            match.get("recent_home_matches_same_league_specific")
            or match.get("recent_home_matches")
            or []
        ),
        "away_matches": (
            match.get("recent_away_matches_same_league_specific")
            or match.get("recent_away_matches")
            or []
        ),
    }

    raw_h2h = match.get("h2h_general") or match.get("h2h_stadium") or {}
    previous = None
    if isinstance(raw_h2h, dict) and raw_h2h.get("match6_id"):
        previous_home = raw_h2h.get("h2h_gen_home") or home
        previous_away = raw_h2h.get("h2h_gen_away") or away
        cutoff = str(raw_h2h.get("date6") or "")

        def history_for(team):
            if _same_team(team, home):
                return (
                    match.get("recent_home_matches_same_league_general")
                    or match.get("recent_home_matches_all")
                    or match.get("recent_home_matches")
                    or []
                )
            return (
                match.get("recent_away_matches_same_league_general")
                or match.get("recent_away_matches_all")
                or match.get("recent_away_matches")
                or []
            )

        def older_at_venue(team, role):
            selected = []
            for item in _rows(history_for(team)):
                venue_name = item.get("home") or item.get("home_team") if role == "home" else item.get("away") or item.get("away_team")
                if not _same_team(venue_name, team):
                    continue
                item_date = str(item.get("date") or "")
                if cutoff and item_date and item_date >= cutoff:
                    continue
                selected.append(item)
            return selected[:10]

        previous = {
            "date": raw_h2h.get("date6") or "fecha N/D",
            "score": raw_h2h.get("res6") or "-",
            "ah_line": raw_h2h.get("ah6") or "-",
            "home_name": previous_home,
            "away_name": previous_away,
            "home_matches": older_at_venue(previous_home, "home"),
            "away_matches": older_at_venue(previous_away, "away"),
        }
    return {"current": current, "previous": previous}


def _team_history_text(team_name, role, matches, all_venues=False):
    wins = draws = losses = 0
    scope = "TODAS LAS LOCALÍAS" if all_venues else ("CASA" if role == "home" else "FUERA")
    lines = [f"{scope} — {team_name}"]
    for item in _rows(matches):
        home = item.get("home") or item.get("home_team") or "-"
        away = item.get("away") or item.get("away_team") or "-"
        raw_score = item.get("score") or item.get("score_raw") or item.get("result") or "-"
        score = _parse_score(raw_score)
        subject_home = _same_team(home, team_name)
        if score:
            own, rival = score if subject_home else (score[1], score[0])
            if own > rival:
                wins += 1
            elif own < rival:
                losses += 1
            else:
                draws += 1
        ah = item.get("ahLine")
        if ah in (None, ""):
            ah = item.get("ahLine_raw")
        if ah in (None, ""):
            ah = item.get("ah_line")
        league_id = item.get("league_id_hist") or item.get("league_id") or "-"
        lines.append(
            f"{item.get('date') or '-'} | Liga ID {league_id} | "
            f"{home} {raw_score} {away} | AH {ah if ah not in (None, '') else '-'}"
        )
    if len(lines) == 1:
        lines.append("Sin historial previo en esta localía")
    lines.append(f"Resumen: V {wins} | E {draws} | D {losses}")
    return "\n".join(lines)


def _format_number(value):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "N/D"
    text = f"{parsed:g}"
    return f"+{text}" if parsed > 0 else text


def _similar_handicaps_text(moment):
    analysis = (moment or {}).get("similar_ah")
    if not isinstance(analysis, dict):
        return "HÁNDICAPS SIMILARES: sin datos"

    def row(label, summary):
        data = summary if isinstance(summary, dict) else {}
        cover = data.get("cover_pct")
        cover_text = f"{round(float(cover))}%" if cover is not None else "N/D"
        return (
            f"{label} AH {_format_number(data.get('target_line'))} (±{data.get('threshold', 'N/D')}): "
            f"{data.get('samples', 0)} partidos | V {data.get('wins', 0)} "
            f"E {data.get('draws', 0)} D {data.get('losses', 0)} | Cover {cover_text} | "
            f"GF {data.get('goals_for_avg', 'N/D')} GC {data.get('goals_against_avg', 'N/D')}"
        )

    correlation = analysis.get("correlation") if isinstance(analysis.get("correlation"), dict) else {}
    home_label = "EQUIPO 1" if (moment or {}).get("is_neutral_venue") else "CASA"
    away_label = "EQUIPO 2" if (moment or {}).get("is_neutral_venue") else "FUERA"
    return "\n".join([
        "HÁNDICAPS SIMILARES",
        row(home_label, analysis.get("home")),
        row(away_label, analysis.get("away")),
        f"CORRELACIÓN: {correlation.get('label', 'Muestra insuficiente')} · confianza {correlation.get('confidence', 'BAJA')}",
    ])


def format_pre_match_context(match):
    stored_context = match.get("pre_match_context")
    context = stored_context if isinstance(stored_context, dict) else _fallback_context(match)
    current = context.get("current") if isinstance(context.get("current"), dict) else _fallback_context(match)["current"]
    previous = context.get("previous") if isinstance(context.get("previous"), dict) else None

    home = current.get("home_name") or match.get("home_name") or match.get("home_team") or "Local N/D"
    away = current.get("away_name") or match.get("away_name") or match.get("away_team") or "Visitante N/D"
    current_neutral = bool(current.get("is_neutral_venue"))
    scope = (
        "TODAS LAS LOCALÍAS · TODAS LAS LIGAS"
        if current_neutral
        else "CASA VS FUERA · TODAS LAS LIGAS"
    )
    blocks = [
        f"CONTEXTO PREVIO ({scope})",
        "",
        f"PARTIDO ACTUAL — {home} vs {away} — {current.get('date') or 'fecha N/D'}",
        _team_history_text(home, "home", current.get("home_matches"), current_neutral),
        "",
        _team_history_text(away, "away", current.get("away_matches"), current_neutral),
        "",
        _similar_handicaps_text(current),
    ]

    blocks.extend(["", ""])
    if previous:
        previous_home = previous.get("home_name") or home
        previous_away = previous.get("away_name") or away
        previous_neutral = bool(previous.get("is_neutral_venue"))
        venue = "LOCALÍAS INVERTIDAS" if _same_team(previous_home, away) else "MISMAS LOCALÍAS"
        blocks.extend([
            f"ÚLTIMO ENFRENTAMIENTO ENTRE ELLOS — {previous_home} {previous.get('score') or '-'} {previous_away} — {previous.get('date') or 'fecha N/D'} — {venue}",
            "Cómo llegaban antes de ese partido:",
            _team_history_text(previous_home, "home", previous.get("home_matches"), previous_neutral),
            "",
            _team_history_text(previous_away, "away", previous.get("away_matches"), previous_neutral),
            "",
            _similar_handicaps_text(previous),
        ])
    else:
        blocks.append("ÚLTIMO ENFRENTAMIENTO ENTRE ELLOS — No disponible")
    return "\n".join(blocks).strip()


def format_match_bundle(match, prompt, index, total):
    match_id = str(match.get("match_id") or match.get("id") or "N/D")
    home = match.get("home_name") or match.get("home_team") or "Local N/D"
    away = match.get("away_name") or match.get("away_team") or "Visitante N/D"
    divider = "=" * 88
    return "\n".join([
        divider,
        f"PARTIDO {index}/{total} · ID {match_id} · {home} vs {away}",
        divider,
        "",
        format_pre_match_context(match),
        "",
        "=" * 72,
        "PROMPT LLM COMPLETO",
        "=" * 72,
        "",
        str(prompt or "PROMPT NO DISPONIBLE"),
    ]).strip()
