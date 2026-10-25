"""Cassandra serving tables must hold exactly the rows the batch computed (needs the Docker stack).

Run after `docker compose up` (or `python -m src.spark_batch_job --cassandra-host localhost`) with
CASSANDRA_HOST=localhost; RAW_DIR points at the CSVs the batch read (default: data).
"""
from __future__ import annotations

import os
import random
from pathlib import Path

import pytest

from src import batch_processing
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
        "trending_growth": len(tables["trending_growth"]),
        "low_engagement_movies": len(tables["low_engagement_movies"]),
        "low_engagement_by_genre": len(tables["low_engagement_movies"]),
        "genre_engagement_peers": len(tables["genre_engagement_peers"]),
        "lowest_viewed_movies": len(tables["lowest_viewed_movies"]),
        "genre_popularity": len(tables["genre_popularity"]),
        "subscription_views": len(tables["subscription_views"]),
        "user_engagement": len(tables["user_engagement"]),
        "hourly_views": len(tables["hourly_views"]),
        "user_segments": len(tables["user_segments"]),
        "segment_thresholds": len(tables["segment_thresholds"]),
        "segment_agreement": len(tables["segment_agreement"]),
        "user_preferences": len(tables["user_preferences"]),
        "user_recommendations": len(tables["user_recommendations"]),
        "daily_views": len(tables["daily_views"]),
        "genre_daily_views": len(tables["genre_daily_views"]),
        "movie_daily_views": len(tables["movie_daily_views"]),
        "weekday_weekend_views": len(tables["weekday_weekend_views"]),
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


def watched_pairs() -> set[tuple[str, str]]:
    curated, _ = batch_processing.curate(*batch_processing.read_raw(Path(os.getenv("RAW_DIR", "data"))))
    return set(zip(curated[3]["user_id"], curated[3]["movie_id"]))


@pytest.mark.parametrize("table", ["user_recommendations", "user_recommendations_als"])
def test_no_recommendation_is_a_watched_title(session, table):
    """200 random users: ranks run 1..n (n <= 10) and no listed movie is in the user's watch history."""
    tables, _ = pandas_tables(Path(os.getenv("RAW_DIR", "data")))
    watched = watched_pairs()
    users = random.Random(3).sample(sorted(tables["user_engagement"]["user_id"]), 200)
    served = 0
    for user in users:
        rows = list(session.execute(f"SELECT rank, movie_id FROM {table} WHERE user_id = %s", [user]))
        served += bool(rows)
        assert [row["rank"] for row in rows] == list(range(1, len(rows) + 1)) and len(rows) <= 10
        assert not [row["movie_id"] for row in rows if (user, row["movie_id"]) in watched], user
    assert served > 0


def test_genre_recommendations_equal_the_pandas_twin(session):
    tables, _ = pandas_tables(Path(os.getenv("RAW_DIR", "data")))
    expected = tables["user_recommendations"].set_index("user_id")
    for user in random.Random(5).sample(sorted(expected.index.unique()), 50):
        rows = list(session.execute("SELECT rank, movie_id, source FROM user_recommendations WHERE user_id = %s", [user]))
        want = expected.loc[[user]]
        assert [(r["rank"], r["movie_id"], r["source"]) for r in rows] == list(
            zip(want["rank"], want["movie_id"], want["source"])
        ), user


def test_pipeline_metadata_matches_the_tables(session):
    _, report = pandas_tables(Path(os.getenv("RAW_DIR", "data")))
    stored = {}
    for row in session.execute("SELECT table_name, reason, rows FROM quality_report"):
        stored.setdefault(row["table_name"], {})[row["reason"]] = row["rows"]
    for table, counts in report.items():
        assert stored[table].get("accepted", 0) == counts["output"], table
        assert sum(v for k, v in stored[table].items() if k != "accepted") == counts["input"] - counts["output"], table
    written = {row["table_name"]: row["rows"] for row in session.execute("SELECT table_name, rows FROM batch_table_rows")}
    assert written and table_counts(session, list(written)) == written
    run = session.execute("SELECT status, seconds FROM batch_runs WHERE bucket = 'all' LIMIT 1").one()
    assert run["status"] == "succeeded" and run["seconds"] > 0


def test_live_trending_tables_keep_ranks_one_to_ten(session):
    for table in ("live_trending", "live_trending_spark"):
        windows = {}
        for row in session.execute(f"SELECT day, window_start, rank FROM {table} LIMIT 2000"):
            windows.setdefault(row["window_start"], []).append(row["rank"])
        assert all(sorted(ranks) == list(range(1, len(ranks) + 1)) and len(ranks) <= 10 for ranks in windows.values())
