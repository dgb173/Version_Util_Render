import datetime as dt
import json
from zoneinfo import ZoneInfo

from scripts.cloud_refresh_precacheo import _merge_jobs, _stale_history_jobs


def test_stale_history_jobs_recovers_started_match_from_today(tmp_path):
    today_value = dt.datetime.now(ZoneInfo("Europe/Madrid")).date()
    today = f"{today_value.month}/{today_value.day}/{today_value.year}"
    cache = tmp_path / "data_precacheo.json"
    cache.write_text(
        json.dumps(
            [
                {
                    "match_id": "3045870",
                    "match_date": today,
                    "handicap": "-1.5",
                    "history_data_version": None,
                },
                {
                    "match_id": "already-new",
                    "match_date": today,
                    "history_data_version": 2,
                },
            ]
        ),
        encoding="utf-8",
    )

    assert _stale_history_jobs(cache, 1) == [
        {
            "id": "3045870",
            "ah": "-1.5",
            "season": "stale_history_upgrade",
            "league_id": "stale_history_upgrade",
        }
    ]


def test_merge_jobs_keeps_primary_and_deduplicates_extra():
    primary = [{"id": "1", "ah": "-1"}]
    extra = [{"id": "1", "ah": "N/A"}, {"id": "2", "ah": "+0.5"}]

    assert _merge_jobs(primary, extra) == [primary[0], extra[1]]
