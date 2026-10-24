"""Cassandra serving tables must hold exactly the rows the batch computed (needs the Docker stack).

Run after `docker compose up` (or `python -m src.spark_batch_job --cassandra-host localhost`) with
CASSANDRA_HOST=localhost; RAW_DIR points at the CSVs the batch read (default: data).
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.cassandra_store import CassandraUnavailable, connect, table_counts
from tests.helpers import pandas_tables

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def session():
    try:
        return connect(timeout=5)
    except CassandraUnavailable as error:
        pytest.skip(str(error))


def expected_counts(tables: dict) -> dict[str, int]:
    """Row counts per Cassandra table, derived independently with Pandas from the raw CSVs."""
    performance = tables["movie_performance"]
    rated = int(performance["weighted_rating"].notna().sum())
    return {
        "movie_performance": len(performance),
        "movies_by_genre_rating": rated,
        "top_rated_movies": rated,
        "top_viewed_movies": len(performance),
        "low_engagement_movies": len(tables["low_engagement_movies"]),
        "genre_popularity": len(tables["genre_popularity"]),
        "subscription_views": len(tables["subscription_views"]),
        "user_engagement": len(tables["user_engagement"]),
        "hourly_views": len(tables["hourly_views"]),
        "user_segments": len(tables["user_segments"]),
        "user_preferences": len(tables["user_preferences"]),
        "daily_views": len(tables["daily_views"]),
        "summary_metrics": 1,
    }


def test_cassandra_row_counts_match_batch_output(session):
    tables, _ = pandas_tables(Path(os.getenv("RAW_DIR", "data")))
    expected = expected_counts(tables)
    assert table_counts(session, list(expected)) == expected


def test_summary_metrics_match_raw_data(session):
    tables, _ = pandas_tables(Path(os.getenv("RAW_DIR", "data")))
    row = session.execute("SELECT * FROM summary_metrics WHERE bucket = 'all'").one()
    expected = tables["summary_metrics"].iloc[0]
    assert (row["users"], row["movies"], row["views"], row["ratings"]) == tuple(
        int(expected[column]) for column in ("users", "movies", "views", "ratings")
    )
    assert row["average_rating"] == expected["average_rating"]


def test_live_trending_tables_keep_ranks_one_to_ten(session):
    for table in ("live_trending", "live_trending_spark"):
        windows = {}
        for row in session.execute(f"SELECT day, window_start, rank FROM {table} LIMIT 2000"):
            windows.setdefault(row["window_start"], []).append(row["rank"])
        assert all(sorted(ranks) == list(range(1, len(ranks) + 1)) and len(ranks) <= 10 for ranks in windows.values())
