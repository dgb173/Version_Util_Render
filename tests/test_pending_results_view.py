import datetime
import inspect
from pathlib import Path

from src.modules import data_manager, pending_results_query


def test_pending_cleanup_keeps_two_days_and_cleans_both_buckets(monkeypatch):
    today = datetime.datetime.now().date()
    recent = (today - datetime.timedelta(days=1)).isoformat()
    obsolete = (today - datetime.timedelta(days=3)).isoformat()
    deleted = []

    monkeypatch.setattr(data_manager, "load_precacheo_matches", lambda: [
        {"match_id": "old-precache", "match_date": obsolete, "score": "?:?"},
        {"match_id": "recent-precache", "match_date": recent, "score": "?:?"},
    ])
    monkeypatch.setattr(data_manager, "load_pending_results_matches", lambda: [
        {"match_id": "old-pending", "match_date": obsolete, "score": "??"},
    ])
    monkeypatch.setattr(
        data_manager.sql_store,
        "delete_match",
        lambda match_id, bucket=None: deleted.append((bucket, match_id)) or True,
    )
    monkeypatch.setattr(data_manager, "_sync_legacy_buckets", lambda buckets: None)

    removed = data_manager.clean_old_precacheo_matches()

    assert removed == 2
    assert set(deleted) == {
        (data_manager.PRECACHEO_BUCKET, "old-precache"),
        (data_manager.PENDING_RESULTS_BUCKET, "old-pending"),
    }


def test_pending_view_separates_exact_upcoming_cutoff_from_pending_threshold():
    root = Path(__file__).resolve().parents[1]
    html = (root / "src" / "templates" / "precacheo.html").read_text(encoding="utf-8")

    assert "return nowMinutes < matchMinutes" in html
    assert "return startTime.getTime() > Date.now()" in html
    assert "[...pendingFromMain, ...pendingFromPrecache]" in html
    assert "function isWithinPendingWindow(match, maxHours = 48)" in html
    assert "ageHours >= 0.5 && ageHours <= maxHours" in html
    assert "const sortAsc = currentViewMode !== 'pendientes'" in html
    assert "currentViewMode === 'proximos' && (!matchesToShow" in html


def test_pending_retention_default_is_two_days():
    default = inspect.signature(data_manager.clean_old_precacheo_matches).parameters[
        "pending_days_threshold"
    ].default
    assert default == 2


def test_pending_page_is_limited_to_100_and_ordered_recent_first(monkeypatch):
    now = datetime.datetime(2026, 7, 20, 12, 0, tzinfo=datetime.timezone.utc)
    candidates = []
    for index in range(205):
        scheduled = now - datetime.timedelta(minutes=30 + index)
        candidates.append({
            "match_id": str(index),
            "start_time": scheduled.isoformat(),
            "score": "?:?",
            "handicap": 1.5,
        })

    received_filters = []

    def fake_candidates(filters):
        received_filters.extend(filters or [])
        return candidates

    monkeypatch.setattr(pending_results_query, "_fetch_candidates", fake_candidates)
    monkeypatch.setattr(
        pending_results_query,
        "_fetch_payloads_by_ids",
        lambda match_ids: {
            str(row["match_id"]): row
            for row in candidates
            if str(row["match_id"]) in set(match_ids)
        },
    )
    result = pending_results_query.fetch_pending_page(
        page=2,
        per_page=500,
        handicap_buckets=["1.5"],
        now=now,
    )

    assert received_filters == ["1.5"]
    assert result["total"] == 205
    assert result["per_page"] == 100
    assert result["total_pages"] == 3
    assert len(result["matches"]) == 100
    assert result["matches"][0]["match_id"] == "100"


def test_pending_handicap_sql_uses_ui_buckets():
    sql, params = pending_results_query._handicap_bucket_sql(["0.5", "-1.5", "2.5"])

    assert sql.count(" OR ") == 2
    assert "handicap >= ?" in sql
    assert params == [0.24, 0.76, -1.76, -1.24, 2.24]


def test_upcoming_page_is_limited_to_100_and_ordered_chronologically(monkeypatch):
    now = datetime.datetime(2026, 7, 20, 12, 0, tzinfo=datetime.timezone.utc)
    candidates = [
        {
            "match_id": str(index),
            "start_time": (now + datetime.timedelta(minutes=index + 1)).isoformat(),
            "score": "?:?",
            "handicap": 0.5,
        }
        for index in range(205)
    ]
    received_filters = []

    def fake_candidates(filters):
        received_filters.extend(filters or [])
        return candidates

    monkeypatch.setattr(pending_results_query, "_fetch_candidates", fake_candidates)
    monkeypatch.setattr(
        pending_results_query,
        "_fetch_payloads_by_ids",
        lambda match_ids: {
            str(row["match_id"]): row
            for row in candidates
            if str(row["match_id"]) in set(match_ids)
        },
    )

    result = pending_results_query.fetch_upcoming_page(
        page=2,
        per_page=500,
        handicap_buckets=["0.5"],
        now=now,
    )

    assert received_filters == ["0.5"]
    assert result["total"] == 205
    assert result["per_page"] == 100
    assert result["total_pages"] == 3
    assert len(result["matches"]) == 100
    assert result["matches"][0]["match_id"] == "100"


def test_upcoming_page_removes_match_at_kickoff(monkeypatch):
    now = datetime.datetime(2026, 7, 21, 11, 11, tzinfo=datetime.timezone.utc)
    candidates = [
        {"match_id": "started", "start_time": now.isoformat(), "score": "?:?"},
        {"match_id": "future", "start_time": (now + datetime.timedelta(minutes=1)).isoformat(), "score": "?:?"},
    ]
    monkeypatch.setattr(pending_results_query, "_fetch_candidates", lambda filters: candidates)
    monkeypatch.setattr(
        pending_results_query,
        "_fetch_payloads_by_ids",
        lambda match_ids: {row["match_id"]: row for row in candidates if row["match_id"] in match_ids},
    )

    result = pending_results_query.fetch_upcoming_page(now=now)

    assert [match["match_id"] for match in result["matches"]] == ["future"]


def test_pending_frontend_uses_server_pagination_and_requeries_handicap():
    root = Path(__file__).resolve().parents[1]
    html = (root / "src" / "templates" / "precacheo.html").read_text(encoding="utf-8")
    app_source = (root / "src" / "app.py").read_text(encoding="utf-8")

    assert "const pendingItemsPerPage = 100" in html
    assert "/api/precacheo_pending_list" in html
    assert "handleMainHandicapFilterChange" in html
    assert "params.append('handicap', value)" in html
    assert "currentPage = 1" in html
    assert "@app.route('/api/precacheo_pending_list')" in app_source
    assert "const itemsPerPage = 100" in html
    assert "/api/precacheo_upcoming_list" in html
    assert "upcomingTotalPages" in html
    assert "@app.route('/api/precacheo_upcoming_list')" in app_source
