"""The dashboard's read queries, expressed for both storage options so they can be compared.

* `cql`: served from Cassandra (query-first tables, `sql/cassandra_schema.cql`).
* `sql`: served from the Hive/Parquet `analytics` tables with Spark SQL.

Each pair returns the same rows in the same order; `tests/test_storage_equivalence.py` checks this.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd
    from cassandra.cluster import Session
    from pyspark.sql import SparkSession

GENRE = "Action"
USER = "U00001"
TOP_SHARE_ROWS = 6000


@dataclass(frozen=True)
class DashboardQuery:
    name: str
    br: str
    kind: str  # point | range | scan: how many rows the query touches
    cql: str
    sql: str
    params: tuple[Any, ...] = ()


DASHBOARD_QUERIES: tuple[DashboardQuery, ...] = (
    DashboardQuery(
        "genre_catalog", "BR1", "range",
        "SELECT movie_id, title, genre, release_year, duration, views, avg_rating, completion_rate "
        "FROM movie_performance WHERE genre = %s",
        f"SELECT movie_id, title, genre, release_year, duration, views, avg_rating, completion_rate "
        f"FROM analytics.movie_performance WHERE genre = '{GENRE}' ORDER BY views DESC, movie_id",
        (GENRE,),
    ),
    DashboardQuery(
        "top_viewed", "BR2", "range",
        "SELECT title, genre, views, avg_rating FROM top_viewed_movies WHERE bucket = 'all' LIMIT 10",
        "SELECT title, genre, views, avg_rating FROM analytics.movie_performance "
        "ORDER BY views DESC, movie_id LIMIT 10",
    ),
    DashboardQuery(
        "user_favorite_genres", "BR3", "point",
        "SELECT favorite_genres FROM user_preferences WHERE user_id = %s",
        f"SELECT favorite_genres FROM analytics.user_preferences WHERE user_id = '{USER}'",
        (USER,),
    ),
    DashboardQuery(
        "genre_top_rated", "BR3", "range",
        "SELECT title, genre, weighted_rating, avg_rating, rating_count, views "
        "FROM movies_by_genre_rating WHERE genre = %s LIMIT 8",
        f"SELECT title, genre, weighted_rating, avg_rating, rating_count, views "
        f"FROM analytics.movie_performance WHERE genre = '{GENRE}' AND weighted_rating IS NOT NULL "
        f"ORDER BY weighted_rating DESC, movie_id LIMIT 8",
        (GENRE,),
    ),
    DashboardQuery(
        "top_rated", "BR4", "range",
        "SELECT title, genre, weighted_rating, avg_rating, rating_count "
        "FROM top_rated_movies WHERE bucket = 'all' LIMIT 10",
        "SELECT title, genre, weighted_rating, avg_rating, rating_count FROM analytics.movie_performance "
        "WHERE weighted_rating IS NOT NULL ORDER BY weighted_rating DESC, rating_count DESC, movie_id LIMIT 10",
    ),
    DashboardQuery(
        "genre_popularity", "BR5", "range",
        "SELECT genre, views, unique_viewers FROM genre_popularity",
        "SELECT genre, views, unique_viewers FROM analytics.genre_popularity ORDER BY genre",
    ),
    DashboardQuery(
        "top_share_performance", "BR7", "range",
        "SELECT title, genre, views, avg_rating, completion_rate, watch_hours "
        "FROM top_viewed_movies WHERE bucket = 'all' LIMIT %s",
        "SELECT title, genre, views, avg_rating, completion_rate, watch_hours "
        f"FROM analytics.movie_performance ORDER BY views DESC, movie_id LIMIT {TOP_SHARE_ROWS}",
        (TOP_SHARE_ROWS,),
    ),
    DashboardQuery(
        "user_views_scan", "BR6", "scan",
        "SELECT views FROM user_engagement",
        "SELECT views FROM analytics.user_engagement",
    ),
    DashboardQuery(
        "hourly_views", "BR6", "range",
        "SELECT hour, views FROM hourly_views",
        "SELECT hour, views FROM analytics.hourly_views ORDER BY hour",
    ),
    DashboardQuery(
        "low_engagement", "BR8", "range",
        "SELECT movie_id, title, genre, views, completion_rate FROM low_engagement_movies "
        "WHERE bucket = 'all' LIMIT 10",
        "SELECT movie_id, title, genre, views, completion_rate FROM analytics.low_engagement_movies "
        "ORDER BY views ASC, title ASC, movie_id ASC LIMIT 10",
    ),
    DashboardQuery(
        "user_segments", "BR9", "range",
        "SELECT segment, users, views FROM user_segments",
        "SELECT segment, users, views FROM analytics.user_segments ORDER BY segment",
    ),
    DashboardQuery(
        "daily_views", "BR10", "range",
        "SELECT day, views, unique_viewers FROM daily_views WHERE bucket = 'all'",
        "SELECT day, views, unique_viewers FROM analytics.daily_views ORDER BY day",
    ),
)

# An ad-hoc question the lake answers directly from curated events (partition pruning by watch_date);
# Cassandra has no table for it, so it is benchmarked on Hive only.
ADHOC_SQL = (
    "SELECT watch_date, COUNT(*) AS views, COUNT(DISTINCT user_id) AS unique_viewers "
    "FROM curated.watch_events WHERE watch_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-07' "
    "GROUP BY watch_date ORDER BY watch_date"
)


def run_cql(session: "Session", query: DashboardQuery) -> "pd.DataFrame":
    from src.cassandra_store import query_frame

    return query_frame(session, query.cql, list(query.params))


def run_sql(spark: "SparkSession", query: DashboardQuery) -> "pd.DataFrame":
    import pandas as pd

    # collect() instead of toPandas(): PySpark 3.5 toPandas needs distutils, removed in Python 3.12.
    frame = spark.sql(query.sql)
    return pd.DataFrame([row.asDict() for row in frame.collect()], columns=frame.columns)
