"""Hand-computed checks of the BR2 growth window, BR8 peer thresholds and BR10 moving average.

Dataset: `PEER_VIEWS` in conftest.py. One viewer, 13 Comedy movies and 2 never-watched Drama movies, all of
duration 100 minutes (so completion rate = minutes per view). The latest watch date (reference date) is
2026-03-15: recent window = 03-09..03-15, previous window = 03-02..03-08, older views are on or before 03-01.
"""
from __future__ import annotations

import pytest

from tests.helpers import pandas_tables, records, spark_tables

EXPECTED = {
    # growth = (recent - previous) / (previous + 10), recent >= 10, best first, ties by movie_id.
    #   C12 40/0 -> 40/10 = 4.0; C05 10/0 -> 1.0 (exactly the volume floor); C11 30/10 -> 20/20 = 1.0 (tie, later id);
    #   C10 20/10 -> 0.5; C09 12/12 -> 0.0 (its 25th view on 03-01 is outside both windows); C07 10/15 -> -5/25 = -0.2.
    # Left out: C06 (9 recent, growth would be 0.46), C13 (5 recent, all new) and C03/C04 (3-4 recent).
    "trending_growth": [
        ("C12", "Comedy 12", "Comedy", 40, 0, 4.0, "2026-03-15"),
        ("C05", "Comedy 05", "Comedy", 10, 0, 1.0, "2026-03-15"),
        ("C11", "Comedy 11", "Comedy", 30, 10, 1.0, "2026-03-15"),
        ("C10", "Comedy 10", "Comedy", 20, 10, 0.5, "2026-03-15"),
        ("C09", "Comedy 09", "Comedy", 12, 12, 0.0, "2026-03-15"),
        ("C07", "Comedy 07", "Comedy", 10, 15, -0.2, "2026-03-15"),
    ],
    # Comedy (13 titles): views 0,5,5,6,8,10,12,20,25,25,30,40,60 -> p10 = ceil(1.3) = 2nd value = 5.
    #   Titles with >= 5 views (12) sorted by completion: 20,40,50,55,60,65,70,75,80,85,90,95 -> p10 = 2nd = 40.0.
    #   C02 (5 views, 40.0) is at both thresholds -> both; C01 (never watched) and C13 (5 views, 90.0) -> low_views;
    #   C03 (6 views, 20.0) -> low_completion; C08 (50.0) is above the threshold.
    # Drama (2 titles, no views): p10 views = 0 -> both low_views; nobody has >= 5 views -> completion p10 is empty.
    "low_engagement_movies": [
        ("C02", "Comedy 02", "Comedy", 5, 40.0, 5, 40.0, "both", 1),
        ("C01", "Comedy 01", "Comedy", 0, None, 5, 40.0, "low_views", 2),
        ("D1", "Drama 1", "Drama", 0, None, 0, None, "low_views", 2),
        ("D2", "Drama 2", "Drama", 0, None, 0, None, "low_views", 2),
        ("C13", "Comedy 13", "Comedy", 5, 90.0, 5, 40.0, "low_views", 2),
        ("C03", "Comedy 03", "Comedy", 6, 20.0, 5, 40.0, "low_completion", 3),
    ],
    "genre_engagement_peers": [("Comedy", 13, 5, 40.0, 2, 1, 1, 4), ("Drama", 2, 0, None, 2, 0, 0, 2)],
    # C05: 10 recent views on days i % 7 -> 03-09..03-11 twice, 03-12..03-15 once.
    "movie_daily": [
        ("C05", "2026-03-09", 2), ("C05", "2026-03-10", 2), ("C05", "2026-03-11", 2), ("C05", "2026-03-12", 1),
        ("C05", "2026-03-13", 1), ("C05", "2026-03-14", 1), ("C05", "2026-03-15", 1),
    ],
}


@pytest.fixture(scope="module", params=["pandas", pytest.param("spark", marks=pytest.mark.spark)])
def tables(request, peer_dir):
    if request.param == "pandas":
        return pandas_tables(peer_dir)[0]
    return spark_tables(request.getfixturevalue("spark"), peer_dir)[0]


@pytest.mark.parametrize("name", ["trending_growth", "low_engagement_movies", "genre_engagement_peers"])
def test_table_matches_hand_computed_values(tables, name):
    assert records(tables[name]) == EXPECTED[name]


def test_movie_daily_series_of_one_movie(tables):
    series = tables["movie_daily_views"]
    assert records(series[series["movie_id"] == "C05"]) == EXPECTED["movie_daily"]


def test_genre_calendar_is_complete_and_moving_average_needs_seven_days(tables):
    daily = tables["genre_daily_views"]
    comedy = daily[daily["genre"] == "Comedy"].reset_index(drop=True)
    days = [str(day) for day in comedy["day"]]
    # 02-25 (oldest view) .. 03-15, every calendar day once, for both genres
    assert days[0] == "2026-02-25" and days[-1] == "2026-03-15" and len(days) == 19
    assert len(daily) == 2 * 19
    # the first six days have no full window; 03-03 is the first average
    assert comedy["views_ma7"][:6].isna().all() and comedy["views_ma7"][6:].notna().all()
    by_day = dict(zip(days, comedy["views_ma7"]))
    # 03-08: previous window 03-02..03-08 holds 3+4+3+15+12+10+10 = 57 views -> 57/7 = 8.142... -> 8.14
    assert by_day["2026-03-08"] == 8.14
    # 03-15: recent window holds 3+4+10+9+10+12+20+30+40+5 = 143 views -> 20.428... -> 20.43
    assert by_day["2026-03-15"] == 20.43


def test_moving_average_equals_plain_python_rolling_mean(tables):
    daily = tables["genre_daily_views"]
    for _, series in daily.groupby("genre"):
        views = series["views"].tolist()
        for index, average in enumerate(series["views_ma7"].tolist()):
            if index < 6:
                assert average != average  # NaN / None: not enough history
            else:
                assert average == pytest.approx(round(sum(views[index - 6: index + 1]) / 7, 2), abs=1e-9)
