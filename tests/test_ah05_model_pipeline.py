from datetime import date

from scripts.ah05_model.pipeline import (
    MatchRecord,
    dated_events,
    event_team_view,
    favourite_is_home,
    raw_ah_to_home_handicap,
)


def test_database_ah_sign_convention_is_not_inverted():
    assert favourite_is_home(0.5)
    assert not favourite_is_home(-0.5)
    assert raw_ah_to_home_handicap(0.5) == -0.5
    assert raw_ah_to_home_handicap(-0.5) == 0.5


def test_event_is_strictly_before_target():
    record = MatchRecord("1", date(2026, 5, 10), "A", "B", "L", "2026", .5, 2.5, 1, 0, {
        "history": [{"date": "2026-05-09"}, {"date": "2026-05-10"}, {"date": "2026-05-11"}]
    })
    assert [row["date"] for row in dated_events(record, "history")] == ["2026-05-09"]


def test_residual_uses_team_perspective():
    home_favourite = event_team_view({"home": "A", "away": "B", "score": "1:0", "ahLine": .5}, "A")
    away_underdog = event_team_view({"home": "A", "away": "B", "score": "1:0", "ahLine": .5}, "B")
    assert home_favourite["team_handicap"] == -.5
    assert home_favourite["cover"] == 1
    assert away_underdog["team_handicap"] == .5
    assert away_underdog["cover"] == 0

