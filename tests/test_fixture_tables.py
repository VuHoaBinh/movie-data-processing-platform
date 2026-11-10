"""Every dashboard table checked against values derived by hand from the dataset in conftest.py."""
from __future__ import annotations

import pytest

from tests.helpers import pandas_tables, records, spark_tables

# Hand derivation (C = global mean rating = 23/6, m = median rating count = 2):
#   M1: 4 views (2 viewers), 220 min, 3 ratings avg 4.0  -> weighted (3*4.0 + 2*C)/5 = 3.93
#   M2: 34 views (2 viewers), 470 min, 1 rating 2.0      -> weighted (1*2.0 + 2*C)/3 = 3.22
#   M3: 12 views (2 viewers), 175 min, 2 ratings avg 4.5 -> weighted (2*4.5 + 2*C)/4 = 4.17
#   M4: no views, no ratings
# Completion = mean over views of min(1, watch_duration / duration), as a ratio:
#   M1: (.6 + 1 + .4 + .2) / 4 = .55;  M2: (.5 + 1 + 32 * .1) / 34 = .138;  M3: (.5 + 1 + .2 + 9 * .2) / 12 = .292
# BR8 (min_views 20, threshold .3): only M2 is flagged; M3 has a low rate but too few views.
EXPECTED = {
    "movie_performance": [
        ("M1", "Alpha", "Action", 2020, 100, "Movie", "English", "P", 4, 2, 4.0, 3, 3.93, 3.7, 0.55),
        ("M2", "Beta", "Action", 2021, 100, "Movie", "English", "T13", 34, 2, 2.0, 1, 3.22, 7.8, 0.138),
        ("M3", "Gamma", "Drama", 2019, 50, "Series", "English", "T16", 12, 2, 4.5, 2, 4.17, 2.9, 0.292),
        ("M4", "Delta", "Drama", 2022, 80, "Movie", "Korean", "T18", 0, 0, None, None, None, 0.0, None),
    ],
    "low_engagement_movies": [
        ("M1", "Alpha", "Action", 4, 220, 2.2, 0.55, False),
        ("M2", "Beta", "Action", 34, 470, 4.7, 0.138, True),
        ("M3", "Gamma", "Drama", 12, 175, 3.5, 0.292, False),
        ("M4", "Delta", "Drama", 0, 0, 0.0, None, False),
    ],
    # Action: 690 min = 11.5 h, ratings 5,4,3,2 -> 3.5; Drama: 175 min = 2.9 h, ratings 5,4 -> 4.5
    "genre_popularity": [("Action", 38, 2, 11.5, 3.5), ("Drama", 12, 2, 2.9, 4.5)],
    "subscription_views": [("Basic", 36, 1), ("Premium", 3, 1), ("Standard", 11, 1)],
    # U1: 36 views (Active); U2: 3 (Inactive); U3: 11 (Casual); U4: none (Inactive)
    "user_engagement": [
        ("U1", "Basic", "Vietnam", 36, 540, 4, 5.5, 0.153, "Active"),
        ("U2", "Premium", "Japan", 3, 160, 2, 1.6, 0.533, "Inactive"),
        ("U3", "Standard", "Korea", 11, 165, 3, 3.3, 0.3, "Casual"),
        ("U4", "Basic", "Thailand", 0, 0, 0, 0.0, None, "Inactive"),
    ],
    "user_segments": [("Inactive", 2, 3), ("Casual", 1, 11), ("Active", 1, 36)],
    "user_preferences": [("U1", "Action|Drama"), ("U2", "Action"), ("U3", "Drama")],
    # W9 is 12:00+07:00 = 05:00 UTC on 2026-01-03
    "daily_views": [("2026-01-01", 2, 1), ("2026-01-02", 3, 3), ("2026-01-03", 4, 3), ("2026-01-04", 41, 2)],
    "hourly_views": [(5, 1), (8, 32), (9, 9), (10, 3), (11, 2), (12, 1), (22, 1), (23, 1)],
    "summary_metrics": [(4, 4, 50, 6, 3.83)],
    # Every user already watched every rated movie of their favourite genres (M4 has no rating).
    "user_recommendations": [],
    "rating_distribution": [
        ("M1", "Alpha", "Action", 3, 4.0, 0, 0, 1, 1, 1),
        ("M2", "Beta", "Action", 1, 2.0, 0, 1, 0, 0, 0),
        ("M3", "Gamma", "Drama", 2, 4.5, 0, 0, 0, 1, 1),
    ],
    "genre_monthly": [("Action", "2026-01-01", 38, 2, 11.5, 3.5), ("Drama", "2026-01-01", 12, 2, 2.9, 4.5)],
    # 2026-01-01 is a Thursday: the four days share the ISO week starting Monday 2025-12-29.
    "view_trends": [
        ("all", "all", "day", "2026-01-01", 2, 0, None),
        ("all", "all", "day", "2026-01-02", 3, 2, 0.5),
        ("all", "all", "day", "2026-01-03", 4, 3, 0.333),
        ("all", "all", "day", "2026-01-04", 41, 4, 9.25),
        ("all", "all", "month", "2026-01-01", 50, 0, None),
        ("all", "all", "week", "2025-12-29", 50, 0, None),
        ("genre", "Action", "day", "2026-01-01", 2, 0, None),
        ("genre", "Action", "day", "2026-01-02", 2, 2, 0.0),
        ("genre", "Action", "day", "2026-01-03", 2, 2, 0.0),
        ("genre", "Action", "day", "2026-01-04", 32, 2, 15.0),
        ("genre", "Action", "month", "2026-01-01", 38, 0, None),
        ("genre", "Action", "week", "2025-12-29", 38, 0, None),
        ("genre", "Drama", "day", "2026-01-02", 1, 0, None),  # no Drama view on Jan 1: no growth
        ("genre", "Drama", "day", "2026-01-03", 2, 1, 1.0),
        ("genre", "Drama", "day", "2026-01-04", 9, 2, 3.5),
        ("genre", "Drama", "month", "2026-01-01", 12, 0, None),
        ("genre", "Drama", "week", "2025-12-29", 12, 0, None),
        ("movie", "M1", "month", "2026-01-01", 4, 0, None),
        ("movie", "M1", "week", "2025-12-29", 4, 0, None),
        ("movie", "M2", "month", "2026-01-01", 34, 0, None),
        ("movie", "M2", "week", "2025-12-29", 34, 0, None),
        ("movie", "M3", "month", "2026-01-01", 12, 0, None),
        ("movie", "M3", "week", "2025-12-29", 12, 0, None),
    ],
}
TABLE_COUNT = 14


@pytest.fixture(scope="module", params=["pandas", pytest.param("spark", marks=pytest.mark.spark)])
def tables(request, fixture_dir):
    if request.param == "pandas":
        return pandas_tables(fixture_dir)[0]
    return spark_tables(request.getfixturevalue("spark"), fixture_dir)[0]


@pytest.mark.parametrize("name", list(EXPECTED))
def test_table_matches_hand_computed_values(tables, name):
    assert records(tables[name]) == EXPECTED[name]


def test_all_tables_are_covered(tables):
    assert set(tables) == set(EXPECTED) and len(EXPECTED) == TABLE_COUNT


def test_ratings_in_range_and_bayesian_between_mean_and_raw(tables):
    performance = tables["movie_performance"].dropna(subset=["weighted_rating"])
    global_mean = 23 / 6
    for row in performance.itertuples():
        low, high = sorted((row.avg_rating, global_mean))
        assert low - 0.01 <= row.weighted_rating <= high + 0.01


@pytest.fixture(scope="module", params=["pandas", pytest.param("spark", marks=pytest.mark.spark)])
def capped_tables(request, fixture_dir, tmp_path_factory):
    """The fixture plus one view of M4 (80 min) watched for 160 min: its completion counts as 1, not 2."""
    import shutil

    directory = tmp_path_factory.mktemp("capped")
    for path in fixture_dir.iterdir():
        shutil.copy(path, directory / path.name)
    with (directory / "watch_history.csv").open("a", encoding="utf-8") as file:
        file.write("WCAP,U4,M4,2026-01-04 10:00:00,160,TV\n")
    if request.param == "pandas":
        return pandas_tables(directory)[0]
    return spark_tables(request.getfixturevalue("spark"), directory)[0]


def test_completion_is_capped_per_view(capped_tables):
    performance = capped_tables["movie_performance"].set_index("movie_id")
    assert performance.loc["M4", "completion_rate"] == 1.0
    user = capped_tables["user_engagement"].set_index("user_id").loc["U4"]
    assert (user["views"], user["completion_sum"], user["avg_completion"]) == (1, 1.0, 1.0)


@pytest.fixture(scope="module", params=["pandas", pytest.param("spark", marks=pytest.mark.spark)])
def recommendation_tables(request, fixture_dir, tmp_path_factory):
    """The fixture plus an unwatched Action movie M5 and ratings for M4 and M5.

    Ratings: M1 5,4,3; M2 2; M3 5,4; M4 2; M5 4 -> C = 29/8 = 3.625, m = median(3,1,2,1,1) = 1.
    Weighted: M1 3.91, M2 2.81, M3 4.21, M4 2.81, M5 3.81.
    U1 (Action|Drama, watched M1-M3) -> M5, M4; U2 (Action, watched M1, M2) -> M5;
    U3 (Drama, watched M3) -> M4; U4 has no history -> nothing.
    """
    import shutil

    directory = tmp_path_factory.mktemp("recommend")
    for path in fixture_dir.iterdir():
        shutil.copy(path, directory / path.name)
    with (directory / "movies.csv").open("a", encoding="utf-8") as file:
        file.write("M5,Epsilon,Action,2023,90,Movie,English,P\n")
    with (directory / "ratings.csv").open("a", encoding="utf-8") as file:
        file.write("R7,U2,M4,2,2026-01-03 12:00:00\nR8,U4,M5,4,2026-01-03 12:00:00\n")
    if request.param == "pandas":
        return pandas_tables(directory)[0]
    return spark_tables(request.getfixturevalue("spark"), directory)[0]


def test_recommendations_skip_watched_movies_and_rank_by_weighted_rating(recommendation_tables):
    assert records(recommendation_tables["user_recommendations"]) == [
        ("U1", 1, "M5", "Epsilon", "Action", 3.81, 1),
        ("U1", 2, "M4", "Delta", "Drama", 2.81, 1),
        ("U2", 1, "M5", "Epsilon", "Action", 3.81, 1),
        ("U3", 1, "M4", "Delta", "Drama", 2.81, 1),
    ]
