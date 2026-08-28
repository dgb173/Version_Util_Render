import datetime

from src.app import _precache_row_visible_now


TODAY = datetime.date(2026, 8, 26)


def test_current_and_future_precache_rows_are_visible():
    assert _precache_row_visible_now({'match_date': '2026-08-26'}, today=TODAY)
    assert _precache_row_visible_now({'time_obj': '2026-08-27T10:00:00'}, today=TODAY)


def test_old_and_undated_precache_rows_are_not_visible():
    assert not _precache_row_visible_now({'match_date': '2026-08-24'}, today=TODAY)
    assert not _precache_row_visible_now({'home_name': 'Old match'}, today=TODAY)


def test_only_unresolved_yesterday_row_is_kept_for_pending_view():
    assert _precache_row_visible_now(
        {'match_date': '2026-08-25', 'score': '??'}, today=TODAY
    )
    assert not _precache_row_visible_now(
        {'match_date': '2026-08-25', 'score': '2-0'}, today=TODAY
    )
