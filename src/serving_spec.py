"""How the analytics tables are reshaped into the Cassandra serving tables.

Shared by both batch branches so they serve identical tables: Spark (`spark_batch_job.serving_frames`,
keyspace movie_analytics) and Flink (`load_serving`, keyspace movie_analytics_flink).

Every entry: Cassandra table -> source analytics table, whether only rated movies are kept, and the
column list ("bucket" = the batch version written by this run). `None` columns = every source column,
plus `bucket` when `bucket_column` is set.
"""
from __future__ import annotations

PERFORMANCE_COLUMNS = [
    "movie_id", "title", "genre", "release_year", "duration", "content_type", "language", "maturity_rating",
    "views", "unique_viewers", "avg_rating", "rating_count", "weighted_rating", "watch_hours", "completion_rate",
]

SERVING_TABLES: dict[str, dict] = {
    "movie_performance": {"source": "movie_performance", "columns": ["bucket", *PERFORMANCE_COLUMNS]},
    "movies_by_genre_rating": {
        "source": "movie_performance", "rated": True,
        "columns": ["genre", "bucket", "weighted_rating", "movie_id", "title", "avg_rating", "rating_count", "views"],
    },
    "top_rated_movies": {
        "source": "movie_performance", "rated": True,
        "columns": ["bucket", "weighted_rating", "rating_count", "movie_id", "title", "genre", "avg_rating", "views"],
    },
    "top_viewed_movies": {
        "source": "movie_performance",
        "columns": ["bucket", "views", "movie_id", "title", "genre", "unique_viewers", "avg_rating",
                    "completion_rate", "watch_hours"],
    },
    "low_engagement_movies": {
        "source": "low_engagement_movies",
        "columns": ["bucket", "movie_id", "title", "genre", "views", "watch_minutes", "completion_sum",
                    "completion_rate", "is_low_engagement"],
    },
    "genre_popularity": {"source": "genre_popularity"},
    "genre_monthly": {"source": "genre_monthly"},
    "subscription_views": {"source": "subscription_views"},
    "user_engagement": {"source": "user_engagement"},
    "hourly_views": {"source": "hourly_views"},
    "user_segments": {"source": "user_segments"},
    "user_preferences": {"source": "user_preferences"},
    "user_recommendations": {"source": "user_recommendations", "bucket_column": True},
    "rating_distribution": {"source": "rating_distribution"},
    "view_trends": {"source": "view_trends", "bucket_column": True},
    "daily_views": {"source": "daily_views", "columns": ["bucket", "day", "views", "unique_viewers"]},
    "summary_metrics": {
        "source": "summary_metrics", "columns": ["bucket", "users", "movies", "views", "ratings", "average_rating"],
    },
}

# Tables with the version in the partition key (old versions are deleted after a newer one is published);
# the genre ones have one partition per (genre, version).
VERSIONED_TABLES = ("top_rated_movies", "top_viewed_movies", "low_engagement_movies", "daily_views", "summary_metrics")
GENRE_VERSIONED_TABLES = ("movie_performance", "movies_by_genre_rating")


def serving_columns(table: str, source_columns: list[str]) -> list[str]:
    spec = SERVING_TABLES[table]
    if spec.get("columns"):
        return list(spec["columns"])
    return [*source_columns, "bucket"] if spec.get("bucket_column") else list(source_columns)
