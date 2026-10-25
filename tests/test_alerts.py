"""The Manager alerts must fire when the platform degrades and stay quiet on healthy data.

`decline_dir`: of 10 users who watched in the previous 28 days, only 5 (and 2 of 5 titles) keep watching in the recent
28 days. `healthy_dir`: all 10 users and all 5 titles are still active. Both end on 2026-03-01.
"""
from __future__ import annotations

import pandas as pd
import pytest

from src import business_rules as rules
from src.alerts import activity_alerts, genre_alert, genre_week_changes, window_change
from tests.helpers import pandas_tables, records, spark_tables

WINDOWS = {
    # previous 2026-01-05..02-01: 10 views, 10 users, 5 titles
    "decline_dir": [
        ("previous", "2026-01-05", "2026-02-01", 10, 10, 5),
        ("recent", "2026-02-02", "2026-03-01", 6, 5, 2),
    ],
    "healthy_dir": [
        ("previous", "2026-01-05", "2026-02-01", 10, 10, 5),
        ("recent", "2026-02-02", "2026-03-01", 11, 10, 5),
    ],
}


@pytest.fixture(params=["pandas", pytest.param("spark", marks=pytest.mark.spark)])
def engine(request):
    return request.param


def activity_table(engine, request, directory: str) -> pd.DataFrame:
    raw = request.getfixturevalue(directory)
    if engine == "pandas":
        return pandas_tables(raw)[0]["activity_windows"]
    return spark_tables(request.getfixturevalue("spark"), raw)[0]["activity_windows"]


@pytest.mark.parametrize("directory", list(WINDOWS))
def test_activity_windows_match_hand_computed_values(engine, request, directory):
    assert records(activity_table(engine, request, directory)) == WINDOWS[directory]


def test_alerts_fire_when_users_and_titles_stop_watching(engine, request):
    alerts = {alert.key: alert for alert in activity_alerts(activity_table(engine, request, "decline_dir"))}
    # users 10 -> 5 = -50%; titles 5 -> 2 = -60%
    assert alerts["active_users"].firing and alerts["active_users"].change == pytest.approx(-0.5)
    assert alerts["active_titles"].firing and alerts["active_titles"].change == pytest.approx(-0.6)


def test_alerts_stay_quiet_on_healthy_data(engine, request):
    alerts = activity_alerts(activity_table(engine, request, "healthy_dir"))
    assert [alert.change for alert in alerts] == [0.0, 0.0] and not any(alert.firing for alert in alerts)


def test_no_alert_without_a_previous_window(request):
    # the normal hand-made fixture holds four days of views: no previous window to compare with
    windows = pandas_tables(request.getfixturevalue("fixture_dir"))[0]["activity_windows"]
    assert window_change(windows, "active_users") is None
    assert not any(alert.firing for alert in activity_alerts(windows))


def test_limits_are_the_named_constants():
    assert rules.ACTIVE_USERS_DROP_ALERT == rules.ACTIVE_TITLES_DROP_ALERT == rules.GENRE_DROP_ALERT == 0.10


def grid(last_week_views: dict[str, int], previous_week_views: dict[str, int]) -> pd.DataFrame:
    days = pd.date_range("2026-03-01", periods=14, freq="D")
    rows = []
    for genre in previous_week_views:
        for index, day in enumerate(days):
            total = previous_week_views[genre] if index < 7 else last_week_views[genre]
            rows.append({"day": day.date(), "genre": genre, "views": total // 7, "views_ma7": None})
    return pd.DataFrame(rows)


def test_genre_alert_fires_when_one_genre_loses_a_third_of_its_views():
    frame = grid({"Action": 70, "Drama": 42}, {"Action": 70, "Drama": 63})
    alert, changes = genre_alert(frame)
    assert changes["Action"] == 0.0 and changes["Drama"] == pytest.approx(-1 / 3)
    assert alert.firing and alert.change == pytest.approx(-1 / 3)


def test_genre_alert_stays_quiet_on_normal_variation():
    frame = grid({"Action": 70, "Drama": 63}, {"Action": 70, "Drama": 70})  # Drama -10% is the limit, not below it
    alert, _ = genre_alert(frame)
    assert not alert.firing and genre_week_changes(frame)["Action"] == 0.0
