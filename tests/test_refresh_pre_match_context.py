from scripts.refresh_pre_match_context import (
    _context_is_reusable,
    _select_context_jobs,
)
from pathlib import Path


def _cached(match_id, generated_at, with_context=True):
    row = {"match_id": match_id, "main_match_odds": {"ah_linea": "-0.5"}}
    if with_context:
        row["pre_match_context"] = {
            "generated_at_epoch": generated_at,
            "current": {"home_matches": [], "away_matches": []},
        }
    return row


def test_context_cache_is_reusable_for_eight_hours():
    row = _cached("1", generated_at=10_000)

    assert _context_is_reusable(row, now_epoch=10_000 + 7 * 3600, ttl_hours=8)
    assert not _context_is_reusable(row, now_epoch=10_000 + 8 * 3600, ttl_hours=8)


def test_selects_only_cached_upcoming_with_missing_or_expired_context():
    snapshot = {
        "upcoming_matches": [
            {"id": "1"},
            {"id": "2"},
            {"id": "3"},
            {"id": "4"},
        ]
    }
    cached = {
        "1": _cached("1", generated_at=39_000),
        "2": _cached("2", generated_at=1_000),
        "3": _cached("3", generated_at=0, with_context=False),
    }

    jobs = _select_context_jobs(snapshot, cached, now_epoch=40_000, ttl_hours=8)

    assert [row["match_id"] for row in jobs] == ["2", "3"]


def test_force_selects_all_analyzed_upcoming_matches():
    snapshot = {"upcoming_matches": [{"id": "1"}, {"id": "2"}]}
    cached = {"1": _cached("1", 20_000), "2": _cached("2", 20_000)}

    jobs = _select_context_jobs(snapshot, cached, force=True, now_epoch=21_000)

    assert [row["match_id"] for row in jobs] == ["1", "2"]


def test_precacheo_combines_context_and_prompt_and_removes_league_button():
    template = (Path(__file__).parents[1] / "src" / "templates" / "precacheo.html").read_text(encoding="utf-8")

    assert "PROMPT LLM COMPLETO" in template
    assert "deferredClipboard.finish(combined)" in template
    assert "Copiar contexto + prompt" in template
    assert "league-table-trigger" not in template
    assert "Lectura de liga, hándicap, localía y Over/Under" not in template
