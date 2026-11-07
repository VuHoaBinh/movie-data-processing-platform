"""An older Cassandra volume must upgrade cleanly: changed serving tables are rebuilt, kept tables are not (needs the stack)."""
from __future__ import annotations

import pytest

from src.cassandra_store import CassandraUnavailable, SchemaConflict, apply_schema, changed_tables, connect

pytestmark = pytest.mark.integration

KEYSPACE = "movie_analytics_migration_test"

# The definition shipped before the peer-relative BR8 upgrade (PR #2): primary key (bucket, views, title, movie_id).
OLD_LOW_ENGAGEMENT = """
CREATE TABLE {ks}.low_engagement_movies (
  bucket text, views bigint, title text, movie_id text, genre text, completion_rate double,
  PRIMARY KEY ((bucket), views, title, movie_id)
) WITH CLUSTERING ORDER BY (views ASC, title ASC, movie_id ASC)
"""
OLD_LIVE = """
CREATE TABLE {ks}.live_trending (
  day date, window_start timestamp, rank int, movie_id text,
  PRIMARY KEY ((day), window_start, rank)
) WITH CLUSTERING ORDER BY (window_start DESC, rank ASC)
"""


@pytest.fixture()
def session():
    try:
        session = connect(keyspace=None, timeout=5)
    except CassandraUnavailable as error:
        pytest.skip(str(error))
    session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")
    yield session
    session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")


def old_volume(session) -> None:
    session.execute(f"CREATE KEYSPACE {KEYSPACE} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    session.execute(OLD_LOW_ENGAGEMENT.format(ks=KEYSPACE))
    session.execute(
        f"INSERT INTO {KEYSPACE}.low_engagement_movies (bucket, views, title, movie_id, genre, completion_rate) "
        "VALUES ('all', 1, 'Old', 'M1', 'Action', 50.0)"
    )


def column_names(session, table: str) -> set[str]:
    rows = session.execute(
        "SELECT column_name FROM system_schema.columns WHERE keyspace_name = %s AND table_name = %s", [KEYSPACE, table]
    )
    return {row["column_name"] for row in rows}


def test_old_low_engagement_table_is_rebuilt_with_the_new_columns(session):
    old_volume(session)
    assert "priority" not in column_names(session, "low_engagement_movies")
    assert changed_tables(session, keyspace=KEYSPACE) == ["low_engagement_movies"]

    rebuilt = apply_schema(session, keyspace=KEYSPACE)

    assert rebuilt == ["low_engagement_movies"]
    assert {"priority", "reason", "genre_views_p10", "genre_completion_p10"} <= column_names(session, "low_engagement_movies")
    assert session.execute(f"SELECT COUNT(*) AS n FROM {KEYSPACE}.low_engagement_movies").one()["n"] == 0
    assert "top_viewed_movies" in {
        row["table_name"] for row in session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s", [KEYSPACE])
    }


def test_second_run_changes_nothing(session):
    old_volume(session)
    apply_schema(session, keyspace=KEYSPACE)
    assert changed_tables(session, keyspace=KEYSPACE) == []
    assert apply_schema(session, keyspace=KEYSPACE) == []


def test_tables_that_hold_unrebuildable_data_are_never_dropped(session):
    old_volume(session)
    session.execute(OLD_LIVE.format(ks=KEYSPACE))
    session.execute(f"INSERT INTO {KEYSPACE}.live_trending (day, window_start, rank, movie_id) VALUES ('2026-01-01', '2026-01-01 00:00:00', 1, 'M1')")
    with pytest.raises(SchemaConflict):
        apply_schema(session, keyspace=KEYSPACE)
    assert session.execute(f"SELECT COUNT(*) AS n FROM {KEYSPACE}.live_trending").one()["n"] == 1
    assert "priority" not in column_names(session, "low_engagement_movies")  # nothing was dropped before the check failed
