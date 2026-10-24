"""Every dashboard table checked against values derived by hand from the dataset in conftest.py."""
from __future__ import annotations

import pytest

from tests.helpers import pandas_tables, records, spark_tables

# Hand derivation (C = global mean rating = 23/6, m = median rating count = 2):
#   M1: 4 views (2 viewers), 220 min, 3 ratings avg 4.0  -> weighted (3*4.0 + 2*C)/5 = 3.93
#   M2: 34 views (2 viewers), 470 min, 1 rating 2.0      -> weighted (1*2.0 + 2*C)/3 = 3.22
#   M3: 12 views (2 viewers), 175 min, 2 ratings avg 4.5 -> weighted (2*4.5 + 2*C)/4 = 4.17
#   M4: no views, no ratings
EXPECTED = {
    "movie_performance": [
        ("M1", "Alpha", "Action", 2020, 100, 4, 2, 4.0, 3, 3.93, 3.7, 55.0),
        ("M2", "Beta", "Action", 2021, 100, 34, 2, 2.0, 1, 3.22, 7.8, 13.8),
        ("M3", "Gamma", "Drama", 2019, 50, 12, 2, 4.5, 2, 4.17, 2.9, 29.2),
        ("M4", "Delta", "Drama", 2022, 80, 0, 0, None, None, None, 0.0, None),
    ],
    "low_engagement_movies": [
        ("M4", "Delta", "Drama", 0, None),
        ("M1", "Alpha", "Action", 4, 55.0),
        ("M3", "Gamma", "Drama", 12, 29.2),
        ("M2", "Beta", "Action", 34, 13.8),
    ],
    "genre_popularity": [("Action", 38, 2), ("Drama", 12, 2)],
    "subscription_views": [("Basic", 36, 1), ("Premium", 3, 1), ("Standard", 11, 1)],
    # U1: 36 views (Active); U2: 3 (Inactive); U3: 11 (Casual); U4: none (Inactive)
    "user_engagement": [
        ("U1", "Basic", "Vietnam", 36, 540, 4, "Active"),
        ("U2", "Premium", "Japan", 3, 160, 2, "Inactive"),
        ("U3", "Standard", "Korea", 11, 165, 3, "Casual"),
        ("U4", "Basic", "Thailand", 0, 0, 0, "Inactive"),
    ],
    "user_segments": [("Inactive", 2, 3), ("Casual", 1, 11), ("Active", 1, 36)],
    "user_preferences": [("U1", "Action|Drama"), ("U2", "Action"), ("U3", "Drama")],
    # BR3: every title in a user's favourite genres has been watched by that user (the only unwatched title,
    # M4, has no rating), so all lists are the popular filler: most views first, watched titles skipped.
    #   U1 watched M1-M3 -> M4;  U2 watched M1, M2 -> M3, M4;  U3 watched M3 -> M2, M1, M4;  U4 nothing -> all four
    "user_recommendations": [
        ("U1", 1, "M4", "Delta", "Drama", "popular", None, None, 0),
        ("U2", 1, "M3", "Gamma", "Drama", "popular", 4.17, 4.5, 12),
        ("U2", 2, "M4", "Delta", "Drama", "popular", None, None, 0),
        ("U3", 1, "M2", "Beta", "Action", "popular", 3.22, 2.0, 34),
        ("U3", 2, "M1", "Alpha", "Action", "popular", 3.93, 4.0, 4),
        ("U3", 3, "M4", "Delta", "Drama", "popular", None, None, 0),
        ("U4", 1, "M2", "Beta", "Action", "popular", 3.22, 2.0, 34),
        ("U4", 2, "M3", "Gamma", "Drama", "popular", 4.17, 4.5, 12),
        ("U4", 3, "M1", "Alpha", "Action", "popular", 3.93, 4.0, 4),
        ("U4", 4, "M4", "Delta", "Drama", "popular", None, None, 0),
    ],
    # W9 is 12:00+07:00 = 05:00 UTC on 2026-01-03
    "daily_views": [("2026-01-01", 2, 1), ("2026-01-02", 3, 3), ("2026-01-03", 4, 3), ("2026-01-04", 41, 2)],
    "hourly_views": [(5, 1), (8, 32), (9, 9), (10, 3), (11, 2), (12, 1), (22, 1), (23, 1)],
    "summary_metrics": [(4, 4, 50, 6, 3.83)],
}


@pytest.fixture(scope="module", params=["pandas", pytest.param("spark", marks=pytest.mark.spark)])
def tables(request, fixture_dir):
    if request.param == "pandas":
        return pandas_tables(fixture_dir)[0]
    return spark_tables(request.getfixturevalue("spark"), fixture_dir)[0]


@pytest.mark.parametrize("name", list(EXPECTED))
def test_table_matches_hand_computed_values(tables, name):
    assert records(tables[name]) == EXPECTED[name]


def test_all_eleven_tables_are_covered(tables):
    assert set(tables) == set(EXPECTED) and len(EXPECTED) == 11


def test_ratings_in_range_and_bayesian_between_mean_and_raw(tables):
    performance = tables["movie_performance"].dropna(subset=["weighted_rating"])
    global_mean = 23 / 6
    for row in performance.itertuples():
        low, high = sorted((row.avg_rating, global_mean))
        assert low - 0.01 <= row.weighted_rating <= high + 0.01
