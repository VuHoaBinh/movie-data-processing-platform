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


VERSION = object()  # placeholder for the published batch version (bound at run time)

DASHBOARD_QUERIES: tuple[DashboardQuery, ...] = (
    DashboardQuery(
        "genre_catalog", "BR1", "range",
        "SELECT movie_id, title, genre, release_year, duration, content_type, language, maturity_rating, "
        "views, avg_rating, completion_rate FROM movie_performance WHERE genre = %s AND bucket = %s",
        f"SELECT movie_id, title, genre, release_year, duration, content_type, language, maturity_rating, "
        f"views, avg_rating, completion_rate FROM analytics.movie_performance WHERE genre = '{GENRE}' "
        f"ORDER BY views DESC, movie_id",
        (GENRE, VERSION),
    ),
    DashboardQuery(
        "top_viewed", "BR2", "range",
        "SELECT title, genre, views, avg_rating FROM top_viewed_movies WHERE bucket = %s LIMIT 10",
        "SELECT title, genre, views, avg_rating FROM analytics.movie_performance "
        "ORDER BY views DESC, movie_id LIMIT 10",
        (VERSION,),
    ),
    DashboardQuery(
        "user_recommendations", "BR3", "point",
        "SELECT rank, movie_id, title, genre, weighted_rating, rating_count FROM user_recommendations "
        "WHERE user_id = %s",
        f"SELECT rank, movie_id, title, genre, weighted_rating, rating_count FROM analytics.user_recommendations "
        f"WHERE user_id = '{USER}' ORDER BY rank",
        (USER,),
    ),
    DashboardQuery(
        "top_rated", "BR4", "range",
        "SELECT title, genre, weighted_rating, avg_rating, rating_count "
        "FROM top_rated_movies WHERE bucket = %s LIMIT 10",
        "SELECT title, genre, weighted_rating, avg_rating, rating_count FROM analytics.movie_performance "
        "WHERE weighted_rating IS NOT NULL ORDER BY weighted_rating DESC, rating_count DESC, movie_id LIMIT 10",
        (VERSION,),
    ),
    DashboardQuery(
        "rating_distribution", "BR4", "point",
        "SELECT movie_id, rating_count, avg_rating, r1, r2, r3, r4, r5 FROM rating_distribution WHERE movie_id = %s",
        "SELECT movie_id, rating_count, avg_rating, r1, r2, r3, r4, r5 FROM analytics.rating_distribution "
        "WHERE movie_id = 'M00001'",
        ("M00001",),
    ),
    DashboardQuery(
        "genre_popularity", "BR5", "range",
        "SELECT genre, views, unique_viewers, watch_hours, avg_rating FROM genre_popularity",
        "SELECT genre, views, unique_viewers, watch_hours, avg_rating FROM analytics.genre_popularity ORDER BY genre",
    ),
    DashboardQuery(
        "genre_monthly", "BR5", "range",
        "SELECT genre, month, views, unique_viewers, watch_hours, avg_rating FROM genre_monthly WHERE genre = %s",
        f"SELECT genre, month, views, unique_viewers, watch_hours, avg_rating FROM analytics.genre_monthly "
        f"WHERE genre = '{GENRE}' ORDER BY month",
        (GENRE,),
    ),
    DashboardQuery(
        "top_share_performance", "BR7", "range",
        "SELECT title, genre, views, avg_rating, completion_rate, watch_hours "
        "FROM top_viewed_movies WHERE bucket = %s LIMIT %s",
        "SELECT title, genre, views, avg_rating, completion_rate, watch_hours "
        f"FROM analytics.movie_performance ORDER BY views DESC, movie_id LIMIT {TOP_SHARE_ROWS}",
        (VERSION, TOP_SHARE_ROWS),
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
        "low_engagement", "BR8", "scan",
        "SELECT movie_id, title, genre, views, completion_sum, completion_rate, is_low_engagement "
        "FROM low_engagement_movies WHERE bucket = %s",
        "SELECT movie_id, title, genre, views, completion_sum, completion_rate, is_low_engagement "
        "FROM analytics.low_engagement_movies ORDER BY movie_id",
        (VERSION,),
    ),
    DashboardQuery(
        "user_segments", "BR9", "range",
        "SELECT segment, users, views FROM user_segments",
        "SELECT segment, users, views FROM analytics.user_segments ORDER BY segment",
    ),
    DashboardQuery(
        "daily_views", "BR10", "range",
        "SELECT day, views, unique_viewers FROM daily_views WHERE bucket = %s",
        "SELECT day, views, unique_viewers FROM analytics.daily_views ORDER BY day",
        (VERSION,),
    ),
    DashboardQuery(
        "genre_weekly_trend", "BR10", "range",
        "SELECT period_start, views, prev_views, growth FROM view_trends "
        "WHERE scope = 'genre' AND key = %s AND period = 'week'",
        f"SELECT period_start, views, prev_views, growth FROM analytics.view_trends "
        f"WHERE scope = 'genre' AND key = '{GENRE}' AND period = 'week' ORDER BY period_start",
        (GENRE,),
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
    from src.cassandra_store import query_frame, serving_state

    version = serving_state(session)["version"] if VERSION in query.params else None
    params = [version if param is VERSION else param for param in query.params]
    return query_frame(session, query.cql, params)


def run_sql(spark: "SparkSession", query: DashboardQuery) -> "pd.DataFrame":
    import pandas as pd

    # collect() instead of toPandas(): PySpark 3.5 toPandas needs distutils, removed in Python 3.12.
    frame = spark.sql(query.sql)
    return pd.DataFrame([row.asDict() for row in frame.collect()], columns=frame.columns)
