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
BATCH_BUCKET = "all"

# CQL bind markers cannot represent identifiers. Keep queries static so that
# callers cannot inject CQL through a table name.
COUNT_QUERIES = {
    "movie_performance": "SELECT COUNT(*) AS n FROM movie_performance",
    "movies_by_genre_rating": "SELECT COUNT(*) AS n FROM movies_by_genre_rating",
    "top_rated_movies": "SELECT COUNT(*) AS n FROM top_rated_movies",
    "top_viewed_movies": "SELECT COUNT(*) AS n FROM top_viewed_movies",
    "low_engagement_movies": "SELECT COUNT(*) AS n FROM low_engagement_movies",
    "genre_popularity": "SELECT COUNT(*) AS n FROM genre_popularity",
    "subscription_views": "SELECT COUNT(*) AS n FROM subscription_views",
    "user_engagement": "SELECT COUNT(*) AS n FROM user_engagement",
    "hourly_views": "SELECT COUNT(*) AS n FROM hourly_views",
    "user_segments": "SELECT COUNT(*) AS n FROM user_segments",
    "user_preferences": "SELECT COUNT(*) AS n FROM user_preferences",
    "daily_views": "SELECT COUNT(*) AS n FROM daily_views",
    "summary_metrics": "SELECT COUNT(*) AS n FROM summary_metrics",
    "live_trending": "SELECT COUNT(*) AS n FROM live_trending",
    "live_trending_spark": "SELECT COUNT(*) AS n FROM live_trending_spark",
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


def apply_schema(session: Session, path: Path = SCHEMA_PATH) -> None:
    """Run every statement of the CQL file (statements are separated by ';')."""
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if not line.strip().startswith("--")]
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


def table_counts(session: Session, tables: Sequence[str]) -> dict[str, int]:
    unknown = set(tables) - COUNT_QUERIES.keys()
    if unknown:
        raise ValueError(f"Unknown Cassandra tables: {sorted(unknown)}")
    return {table: session.execute(COUNT_QUERIES[table]).one()["n"] for table in tables}


def main() -> None:
    parser = argparse.ArgumentParser(description="Cassandra schema helper.")
    parser.add_argument("command", choices=["init"], help="init: wait for Cassandra and apply the schema")
    parser.parse_args()
    session = wait_for_cassandra()
    apply_schema(session)
    print(f"Schema applied to keyspace {KEYSPACE}")


if __name__ == "__main__":
    main()
