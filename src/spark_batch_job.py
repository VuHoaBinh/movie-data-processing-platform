from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, Window

RAW_FILES = ("users.csv", "movies.csv", "ratings.csv", "watch_history.csv")


def _read_raw(spark: SparkSession, raw_dir: Path) -> tuple[DataFrame, ...]:
    missing = [name for name in RAW_FILES if not (raw_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing raw files in {raw_dir}: {', '.join(missing)}")

    options = {"header": True, "inferSchema": True}
    users = spark.read.options(**options).csv(str(raw_dir / "users.csv"))
    movies = spark.read.options(**options).csv(str(raw_dir / "movies.csv"))
    ratings = spark.read.options(**options).csv(str(raw_dir / "ratings.csv"))
    watches = spark.read.options(**options).csv(str(raw_dir / "watch_history.csv"))
    ratings = ratings.withColumn("timestamp", F.to_timestamp("timestamp"))
    watches = watches.withColumn("watch_timestamp", F.to_timestamp("watch_timestamp"))
    return users, movies, ratings, watches


def _write_csv_file(frame: DataFrame, output_dir: Path, name: str) -> None:
    """Write one dashboard-compatible CSV using Spark's distributed CSV writer."""
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(tempfile.mkdtemp(prefix=f".{name}_", dir=output_dir))
    try:
        frame.coalesce(1).write.mode("overwrite").option("header", True).csv(str(temp_dir))
        part_files = list(temp_dir.glob("part-*.csv"))
        if len(part_files) != 1:
            raise RuntimeError(f"Expected one Spark CSV part for {name}, found {len(part_files)}")
        destination = output_dir / f"{name}.csv"
        part_files[0].replace(destination)
        print(f"Wrote {destination}")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def build_tables(
    users: DataFrame, movies: DataFrame, ratings: DataFrame, watches: DataFrame
) -> dict[str, DataFrame]:
    """Build the batch analytics tables consumed by the Streamlit dashboard."""
    watches = watches.dropDuplicates(["watch_id"]).dropna()
    ratings = ratings.dropDuplicates(["rating_id"]).dropna()

    joined = watches.join(movies, "movie_id").join(users, "user_id")
    rating_stats = ratings.groupBy("movie_id").agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("rating_id").alias("rating_count"),
    )

    watch_stats = joined.groupBy("movie_id").agg(
        F.count("watch_id").alias("views"),
        F.sum("watch_duration").alias("watch_minutes_raw"),
        F.avg("watch_duration").alias("avg_watch_minutes"),
        F.countDistinct("user_id").alias("unique_viewers"),
    )
    movie_performance = (
        movies.select("movie_id", "title", "genre", "release_year", "duration")
        .join(watch_stats, "movie_id", "left")
        .join(rating_stats, "movie_id", "left")
        .withColumn("views", F.coalesce(F.col("views"), F.lit(0)))
        .withColumn("watch_minutes_raw", F.coalesce(F.col("watch_minutes_raw"), F.lit(0)))
        .withColumn("unique_viewers", F.coalesce(F.col("unique_viewers"), F.lit(0)))
        .withColumn("watch_hours", F.round(F.col("watch_minutes_raw") / 60, 1))
        .withColumn(
            "completion_rate",
            F.round(F.least(F.lit(100.0), F.col("avg_watch_minutes") / F.col("duration") * 100), 1),
        )
        .drop("watch_minutes_raw", "avg_watch_minutes")
        .select(
            "movie_id", "title", "genre", "release_year", "duration", "views",
            "unique_viewers", "avg_rating", "rating_count", "watch_hours", "completion_rate",
        )
    )
    low_engagement_movies = movie_performance.select(
        "movie_id", "title", "genre", "views", "completion_rate"
    ).orderBy(F.asc("views"), F.asc("title"))

    genre_popularity = joined.groupBy("genre").agg(
        F.count("watch_id").alias("views"), F.countDistinct("user_id").alias("unique_viewers")
    )
    subscription_views = joined.groupBy("subscription_plan").agg(
        F.count("watch_id").alias("views"), F.countDistinct("user_id").alias("unique_viewers")
    )

    engagement_stats = joined.groupBy("user_id").agg(
        F.count("watch_id").alias("views"),
        F.sum("watch_duration").alias("watch_minutes"),
        F.countDistinct(F.to_date("watch_timestamp")).alias("active_days"),
    )
    user_engagement = (
        users.select("user_id", "subscription_plan", "region")
        .join(engagement_stats, "user_id", "left")
        .withColumn("views", F.coalesce(F.col("views"), F.lit(0)))
        .withColumn("watch_minutes", F.coalesce(F.col("watch_minutes"), F.lit(0)))
        .withColumn("active_days", F.coalesce(F.col("active_days"), F.lit(0)))
        .withColumn(
            "segment",
            F.when(F.col("views") <= 10, "Inactive")
            .when(F.col("views") <= 35, "Casual")
            .otherwise("Active"),
        )
        .select("user_id", "views", "watch_minutes", "active_days", "subscription_plan", "region", "segment")
    )
    user_segments = user_engagement.groupBy("segment").agg(
        F.count("user_id").alias("users"), F.sum("views").alias("views")
    )

    genre_by_user = joined.groupBy("user_id", "genre").agg(F.count(F.lit(1)).alias("genre_views"))
    preference_window = Window.partitionBy("user_id").orderBy(F.desc("genre_views"), F.asc("genre"))
    user_preferences = (
        genre_by_user.withColumn("genre_rank", F.row_number().over(preference_window))
        .where(F.col("genre_rank") <= 2)
        .groupBy("user_id")
        .agg(F.sort_array(F.collect_list(F.struct("genre_rank", "genre"))).alias("_ranked_genres"))
        .withColumn("favorite_genres", F.expr("transform(_ranked_genres, x -> x.genre)"))
        .withColumn("favorite_genres", F.concat_ws("|", "favorite_genres"))
        .select("user_id", "favorite_genres")
    )

    daily_views = joined.groupBy(F.to_date("watch_timestamp").alias("day")).agg(
        F.count("watch_id").alias("views"), F.countDistinct("user_id").alias("unique_viewers")
    )
    hourly_views = joined.groupBy(F.hour("watch_timestamp").alias("hour")).agg(
        F.count("watch_id").alias("views")
    )
    summary_metrics = spark_create_summary(users, movies, watches, ratings)

    return {
        "movie_performance": movie_performance,
        "low_engagement_movies": low_engagement_movies,
        "genre_popularity": genre_popularity,
        "subscription_views": subscription_views,
        "user_engagement": user_engagement,
        "user_segments": user_segments,
        "user_preferences": user_preferences,
        "daily_views": daily_views,
        "hourly_views": hourly_views,
        "summary_metrics": summary_metrics,
    }


def spark_create_summary(users: DataFrame, movies: DataFrame, watches: DataFrame, ratings: DataFrame) -> DataFrame:
    stats = ratings.agg(F.round(F.avg("rating"), 2).alias("average_rating"))
    return stats.select(
        F.lit(users.count()).alias("users"),
        F.lit(movies.count()).alias("movies"),
        F.lit(watches.count()).alias("views"),
        F.lit(ratings.count()).alias("ratings"),
        "average_rating",
    )


def run(raw_dir: Path, output_dir: Path) -> None:
    spark = SparkSession.builder.appName("movie-analytics-batch").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        tables = build_tables(*_read_raw(spark, raw_dir))
        for name, table in tables.items():
            _write_csv_file(table, output_dir, name)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build dashboard analytics tables with Apache Spark.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/processed"))
    args = parser.parse_args()
    run(args.raw_dir, args.output_dir)


if __name__ == "__main__":
    main()
