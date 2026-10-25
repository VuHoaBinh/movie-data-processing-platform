"""The profiler on the hand-made fixture: every asserted value is derived by hand from conftest.py."""
from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from src import profile_data


def metric(rows, dataset: str, section: str, name: str):
    matches = [value for d, s, m, value in rows if (d, s, m) == (dataset, section, name)]
    assert len(matches) == 1, f"{dataset}/{section}/{name}: {matches}"
    return matches[0]


@pytest.fixture(scope="module")
def rows(fixture_dir: Path):
    return profile_data.profile(fixture_dir)


def test_row_counts_and_keys(rows):
    assert [metric(rows, t, "overview", "rows") for t in profile_data.TABLES] == [4, 4, 6, 50]
    assert metric(rows, "watch_history", "overview", "columns") == 6
    assert metric(rows, "watch_history", "keys", "watch_id_unique") is True
    assert metric(rows, "users", "null_pct", "region") == 0.0
    assert metric(rows, "movies", "distinct", "genre") == 2


def test_clean_fixture_has_no_orphans(rows):
    for table, column, parent in profile_data.FOREIGN_KEYS:
        assert metric(rows, table, "orphans", f"{column}_not_in_{parent}") == 0


def test_skew_views_per_movie(rows):
    # views per movie: M1 = 4, M2 = 34, M3 = 12, M4 = 0 (total 50); top 1% and 10% of 4 movies = 1 movie
    assert metric(rows, "movies", "skew", "top_1pct_movies_view_share_pct") == 68.0
    assert metric(rows, "movies", "skew", "top_10pct_movies_view_share_pct") == 68.0
    assert metric(rows, "movies", "skew", "movies_without_views") == 1
    # Gini of [0, 4, 12, 34]: 2 * (1*0 + 2*4 + 3*12 + 4*34) / (4 * 50) - 5/4 = 0.55
    assert metric(rows, "movies", "skew", "gini_views_per_movie") == 0.55
    assert metric(rows, "users", "skew", "users_without_views") == 1  # U4
    assert metric(rows, "users", "skew", "users_without_views_pct") == 25.0


def test_time_pattern(rows):
    # 2 + 3 + 4 + 41 events on Thu 01-01 .. Sun 01-04 (W9 is 05:00 UTC on Sat 01-03); weekend = 4 + 41
    assert metric(rows, "watch_history", "time", "days_with_events") == 4
    assert metric(rows, "watch_history", "events_per_day", "min") == 2
    assert metric(rows, "watch_history", "events_per_day", "median") == 3.5
    assert metric(rows, "watch_history", "events_per_day", "max") == 41
    assert metric(rows, "watch_history", "weekday_vs_weekend", "weekend_share_pct") == 90.0
    assert metric(rows, "watch_history", "time", "min_timestamp") == "2026-01-01 10:00:00"


def test_ratings_and_coverage(rows):
    assert metric(rows, "ratings", "rating_histogram", "rating=5") == 2
    assert metric(rows, "ratings", "rating_histogram_pct", "rating=3") == 16.67
    assert metric(rows, "ratings", "coverage", "ratings_per_watch_pct") == 12.0
    # every watch except W8 (U2, M2) has a rating from the same user for the same movie
    assert metric(rows, "ratings", "coverage", "watches_with_rated_user_movie_pair_pct") == 98.0
    assert metric(rows, "ratings", "coverage", "movies_without_ratings") == 1
    assert metric(rows, "watch_history", "completion_ratio", "completion_max") == 1.0


def test_nulls_and_orphans_are_counted(fixture_dir: Path, tmp_path: Path):
    shutil.copytree(fixture_dir, tmp_path / "dirty")
    dirty = tmp_path / "dirty"
    users = (dirty / "users.csv").read_text(encoding="utf-8").replace("U4,19,Male,Thailand", "U4,19,Male,")
    (dirty / "users.csv").write_text(users, encoding="utf-8")
    with (dirty / "ratings.csv").open("a", encoding="utf-8") as handle:
        handle.write("R7,U9,M1,3,2026-01-04 12:00:00\n")
    rows = profile_data.profile(dirty)
    assert metric(rows, "users", "null_pct", "region") == 25.0
    assert metric(rows, "ratings", "orphans", "user_id_not_in_users") == 1
    assert metric(rows, "ratings", "orphans", "movie_id_not_in_movies") == 0


def test_outputs_are_written(rows, tmp_path: Path):
    profile_data.write_outputs(rows, tmp_path)
    table = pd.read_csv(tmp_path / "data_profile.csv")
    assert list(table.columns) == ["dataset", "section", "metric", "value"]
    assert len(table) == len(rows)
    assert "## watch_history" in (tmp_path / "data_profile.md").read_text(encoding="utf-8")
