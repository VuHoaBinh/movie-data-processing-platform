"""Thin Cassandra access layer shared by the dashboard, materializer and tests."""
from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

from cassandra.cluster import Cluster, Session
from cassandra.query import dict_factory

if TYPE_CHECKING:
    import pandas as pd

KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "movie_analytics")
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "cassandra_schema.cql"
SERVING_STATE_ID = "batch"

# CQL bind markers cannot represent identifiers. Keep queries static so that
# callers cannot inject CQL through a table name.
COUNT_QUERIES = {
    name: f"SELECT COUNT(*) AS n FROM {name}"
    for name in (
        "genre_popularity", "genre_monthly", "subscription_views", "user_engagement", "hourly_views",
        "user_segments", "user_preferences", "rating_distribution",
        "live_trending", "live_trending_spark", "live_user_engagement", "live_user_engagement_spark",
        "live_movie_engagement", "live_movie_engagement_spark",
    )
}
# Versioned tables: count only the published version.
COUNT_QUERIES.update({
    name: f"SELECT COUNT(*) AS n FROM {name} WHERE bucket = %s"
    for name in ("top_rated_movies", "top_viewed_movies", "low_engagement_movies", "daily_views", "summary_metrics")
})
COUNT_QUERIES.update({
    name: f"SELECT COUNT(*) AS n FROM {name} WHERE bucket = %s ALLOW FILTERING"
    for name in ("user_recommendations", "view_trends")
})
GENRE_COUNT_QUERIES = {
    name: f"SELECT COUNT(*) AS n FROM {name} WHERE genre = %s AND bucket = %s"
    for name in ("movie_performance", "movies_by_genre_rating")
}


class CassandraUnavailable(RuntimeError):
    """Raised when the cluster cannot be reached, with a message fit for the UI."""


def contact_points() -> list[str]:
    return os.getenv("CASSANDRA_HOST", "localhost").split(",")


def connect(keyspace: str | None = KEYSPACE, timeout: float = 30.0) -> Session:
    # The default reactor works on Python 3.12 with cassandra-driver 3.29.2 (AsyncioConnection times out there).
    cluster = Cluster(
        contact_points(),
        port=int(os.getenv("CASSANDRA_PORT", "9042")),
        connect_timeout=timeout,
        protocol_version=5,
    )
    try:
        session = cluster.connect(keyspace) if keyspace else cluster.connect()
    except Exception as error:
        cluster.shutdown()
        raise CassandraUnavailable(
            f"Cannot reach Cassandra at {','.join(contact_points())}: {error}"
        ) from error
    session.row_factory = dict_factory
    session.default_fetch_size = 5000
    session.default_timeout = 60.0
    return session


def wait_for_cassandra(retries: int = 60, delay: float = 3.0) -> Session:
    for attempt in range(retries):
        try:
            return connect(keyspace=None)
        except CassandraUnavailable:
            if attempt == retries - 1:
                raise
            time.sleep(delay)
    raise CassandraUnavailable("unreachable")


def apply_schema(session: Session, path: Path = SCHEMA_PATH, keyspace: str = "movie_analytics") -> None:
    """Run every statement of the CQL file (statements are separated by ';') into `keyspace`."""
    import re

    text = re.sub(r"\bmovie_analytics\b", keyspace, path.read_text(encoding="utf-8"))
    lines = [line for line in text.splitlines() if not line.strip().startswith("--")]
    for statement in "\n".join(lines).split(";"):
        statement = statement.strip()
        if not statement:
            continue
        if statement.upper().startswith("USE "):
            session.set_keyspace(statement.split()[1])
        else:
            session.execute(statement)


def query_frame(session: Session, cql: str, params: Sequence[Any] | None = None) -> "pd.DataFrame":
    import pandas as pd  # not installed in the stream image

    return pd.DataFrame(list(session.execute(cql, params or [])))


def serving_state(session: Session) -> dict | None:
    """The published batch version and the batch/speed boundary, or None before the first batch run."""
    return session.execute("SELECT * FROM serving_state WHERE id = %s", [SERVING_STATE_ID]).one()


def publish_serving_state(
    session: Session, version: str, replay_day: Any = None, batch_cutoff: Any = None
) -> str | None:
    """Switch readers to `version`; returns the version that is no longer referenced (to delete)."""
    from datetime import datetime, timezone

    current = serving_state(session)
    session.execute(
        "INSERT INTO serving_state (id, version, previous_version, replay_day, batch_cutoff, published_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        [SERVING_STATE_ID, version, current["version"] if current else None, replay_day, batch_cutoff,
         datetime.now(timezone.utc)],
    )
    return current["previous_version"] if current else None


def publish_batch_version(
    session: Session, version: str, genres: Sequence[str], replay_day: Any = None, batch_cutoff: Any = None
) -> None:
    """Point the dashboard at `version` (in the session's keyspace), then drop the partitions of the
    version before the previous one (the previous one stays for readers that loaded it a moment ago)."""
    from src.serving_spec import GENRE_VERSIONED_TABLES, VERSIONED_TABLES

    stale = publish_serving_state(session, version, replay_day, batch_cutoff)
    if stale:
        for table in VERSIONED_TABLES:
            session.execute(f"DELETE FROM {table} WHERE bucket = %s", [stale])
        for table in GENRE_VERSIONED_TABLES:
            for genre in genres:
                session.execute(f"DELETE FROM {table} WHERE genre = %s AND bucket = %s", [genre, stale])
    print(f"Serving version {version} published in {session.keyspace}" + (f"; removed {stale}" if stale else ""))


def table_counts(session: Session, tables: Sequence[str], version: str | None = None) -> dict[str, int]:
    """Row counts; versioned tables count only `version` (default: the published one)."""
    unknown = set(tables) - COUNT_QUERIES.keys() - GENRE_COUNT_QUERIES.keys()
    if unknown:
        raise ValueError(f"Unknown Cassandra tables: {sorted(unknown)}")
    if version is None:
        state = serving_state(session)
        version = state["version"] if state else None
    counts = {}
    for table in tables:
        if table in GENRE_COUNT_QUERIES:
            genres = [row["genre"] for row in session.execute("SELECT genre FROM genre_popularity")]
            counts[table] = sum(
                session.execute(GENRE_COUNT_QUERIES[table], [genre, version]).one()["n"] for genre in genres
            )
        else:
            query = COUNT_QUERIES[table]
            counts[table] = session.execute(query, [version] if "%s" in query else []).one()["n"]
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Cassandra schema helper.")
    parser.add_argument("command", choices=["init"], help="init: wait for Cassandra and apply the schema")
    parser.parse_args()
    from src import settings

    session = wait_for_cassandra()
    for keyspace in (settings.SPARK_KEYSPACE, settings.FLINK_KEYSPACE):
        apply_schema(session, keyspace=keyspace)
        print(f"Schema applied to keyspace {keyspace}")


if __name__ == "__main__":
    main()
