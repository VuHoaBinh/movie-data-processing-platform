from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

CASSANDRA_PACKAGE = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
RAW_FILES = ("users.csv", "movies.csv", "ratings.csv", "watch_history.csv")
BATCH_BUCKET = "all"

# Explicit schemas: no inferSchema pass over the files, and drift in the raw files shows up as nulls.
# Timestamps stay strings in the raw zone (mixed naive / ISO-offset formats) and are parsed in curation.
RAW_SCHEMAS = {
    "users": StructType([
        StructField("user_id", StringType()), StructField("age", IntegerType()),
        StructField("gender", StringType()), StructField("region", StringType()),
        StructField("registration_date", DateType()), StructField("subscription_plan", StringType()),
        StructField("activity_profile", StringType()),
    ]),
    "movies": StructType([
        StructField("movie_id", StringType()), StructField("title", StringType()),
        StructField("genre", StringType()), StructField("release_year", IntegerType()),
        StructField("duration", IntegerType()), StructField("content_type", StringType()),
        StructField("language", StringType()), StructField("maturity_rating", StringType()),
    ]),
    "ratings": StructType([
        StructField("rating_id", StringType()), StructField("user_id", StringType()),
        StructField("movie_id", StringType()), StructField("rating", IntegerType()),
        StructField("timestamp", StringType()),
    ]),
    "watch_history": StructType([
        StructField("watch_id", StringType()), StructField("user_id", StringType()),
        StructField("movie_id", StringType()), StructField("watch_timestamp", StringType()),
        StructField("watch_duration", IntegerType()), StructField("device", StringType()),
    ]),
}


def create_spark(
    warehouse_dir: Path | None = None, cassandra_host: str | None = None, app_name: str = "movie-analytics-batch"
) -> SparkSession:
    """Build a session. With a warehouse dir it uses Hive support (embedded Derby metastore)."""
    # UTC session time zone: naive raw timestamps are UTC, and ISO values with offsets convert to UTC.
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")  # the default 200 is far too many for this data size
    )
    if warehouse_dir is not None:
        warehouse_dir = warehouse_dir.resolve()
        builder = (
            builder.config("spark.sql.warehouse.dir", (warehouse_dir / "spark-warehouse").as_posix())
            .config(
                "spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={(warehouse_dir / 'metastore_db').as_posix()};create=true",
            )
            .enableHiveSupport()
        )
    if cassandra_host:
        builder = builder.config("spark.jars.packages", CASSANDRA_PACKAGE).config(
            "spark.cassandra.connection.host", cassandra_host
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_raw(spark: SparkSession, raw_dir: Path) -> tuple[DataFrame, ...]:
    """Read the raw CSVs with explicit schemas (raw zone: nothing is dropped or parsed yet)."""
    missing = [name for name in RAW_FILES if not (raw_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing raw files in {raw_dir}: {', '.join(missing)}")
    return tuple(
        spark.read.option("header", True).schema(RAW_SCHEMAS[Path(name).stem]).csv(str(raw_dir / name))
        for name in RAW_FILES
    )


def curate(
    users: DataFrame, movies: DataFrame, ratings: DataFrame, watches: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Raw -> curated: parse timestamps (UTC), drop duplicate ids and rows with missing values."""
    ratings = ratings.withColumn("timestamp", F.to_timestamp("timestamp"))
    watches = watches.withColumn("watch_timestamp", F.to_timestamp("watch_timestamp"))
    watches = watches.dropDuplicates(["watch_id"]).dropna()
    ratings = ratings.dropDuplicates(["rating_id"]).dropna()
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
    """Build the batch analytics tables from curated frames (see `curate`)."""
    joined = watches.join(movies, "movie_id").join(users, "user_id")
    rating_stats = ratings.groupBy("movie_id").agg(
        F.avg("rating").alias("avg_rating_raw"),
        F.count("rating_id").alias("rating_count"),
    )
    # Bayesian average: shrink each movie's mean toward the global mean C, with
    # prior weight m = median rating count, so titles with few ratings cannot top BR3/BR4.
    global_mean = ratings.agg(F.avg("rating")).first()[0]
    min_votes = rating_stats.agg(F.median("rating_count")).first()[0]
    rating_stats = rating_stats.select(
        "movie_id",
        "rating_count",
        F.round("avg_rating_raw", 2).alias("avg_rating"),
        F.round(
            (F.col("rating_count") * F.col("avg_rating_raw") + F.lit(min_votes) * F.lit(global_mean))
            / (F.col("rating_count") + F.lit(min_votes)),
            2,
        ).alias("weighted_rating"),
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
            "unique_viewers", "avg_rating", "rating_count", "weighted_rating", "watch_hours",
            "completion_rate",
        )
        .orderBy("movie_id")
    )
    low_engagement_movies = movie_performance.select(
        "movie_id", "title", "genre", "views", "completion_rate"
    ).orderBy(F.asc("views"), F.asc("title"))

    # Spark groupBy output is unordered; every table below is sorted like the Pandas pipeline.
    genre_popularity = joined.groupBy("genre").agg(
        F.count("watch_id").alias("views"), F.countDistinct("user_id").alias("unique_viewers")
    ).orderBy(F.desc("views"))
    subscription_views = joined.groupBy("subscription_plan").agg(
        F.count("watch_id").alias("views"), F.countDistinct("user_id").alias("unique_viewers")
    ).orderBy("subscription_plan")

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
        .select("user_id", "subscription_plan", "region", "views", "watch_minutes", "active_days", "segment")
        .orderBy("user_id")
    )
    segment_order = F.when(F.col("segment") == "Inactive", 0).when(F.col("segment") == "Casual", 1).otherwise(2)
    user_segments = (
        user_engagement.groupBy("segment")
        .agg(F.count("user_id").alias("users"), F.sum("views").alias("views"))
        .orderBy(segment_order)
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
        .orderBy("user_id")
    )

    daily_views = joined.groupBy(F.to_date("watch_timestamp").alias("day")).agg(
        F.count("watch_id").alias("views"), F.countDistinct("user_id").alias("unique_viewers")
    ).orderBy("day")
    hourly_views = joined.groupBy(F.hour("watch_timestamp").alias("hour")).agg(
        F.count("watch_id").alias("views")
    ).orderBy("hour")
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


def serving_frames(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Reshape the dashboard tables into the Cassandra tables (one per dashboard query)."""
    performance = tables["movie_performance"]
    bucket = F.lit(BATCH_BUCKET).alias("bucket")
    rated = performance.where(F.col("weighted_rating").isNotNull())
    return {
        "movie_performance": performance,
        "movies_by_genre_rating": rated.select(
            "genre", "weighted_rating", "movie_id", "title", "avg_rating", "rating_count", "views"
        ),
        "top_rated_movies": rated.select(
            bucket, "weighted_rating", "rating_count", "movie_id", "title", "genre", "avg_rating", "views"
        ),
        "top_viewed_movies": performance.select(
            bucket, "views", "movie_id", "title", "genre", "unique_viewers", "avg_rating",
            "completion_rate", "watch_hours",
        ),
        "low_engagement_movies": tables["low_engagement_movies"].select(
            bucket, "views", "title", "movie_id", "genre", "completion_rate"
        ),
        "genre_popularity": tables["genre_popularity"],
        "subscription_views": tables["subscription_views"],
        "user_engagement": tables["user_engagement"],
        "hourly_views": tables["hourly_views"],
        "user_segments": tables["user_segments"],
        "user_preferences": tables["user_preferences"],
        "daily_views": tables["daily_views"].select(bucket, "day", "views", "unique_viewers"),
        "summary_metrics": tables["summary_metrics"].select(
            bucket, "users", "movies", "views", "ratings", "average_rating"
        ),
    }


def write_cassandra(frames: dict[str, DataFrame], keyspace: str = "movie_analytics") -> dict[str, int]:
    """Replace the serving tables (truncate + insert) and return the row counts written."""
    counts: dict[str, int] = {}
    for name, frame in frames.items():
        frame = frame.cache()
        counts[name] = frame.count()
        (
            frame.write.format("org.apache.spark.sql.cassandra")
            .options(table=name, keyspace=keyspace)
            .option("confirm.truncate", "true")
            .mode("overwrite")
            .save()
        )
        frame.unpersist()
        print(f"Cassandra {keyspace}.{name}: {counts[name]:,} rows")
    return counts


def _save_hive(frame: DataFrame, table: str, partition_by: str | None = None) -> None:
    spark = frame.sparkSession
    if not spark.catalog.tableExists(table):
        # A crashed run can leave table files without a metastore entry, which Spark refuses to overwrite.
        database, name = table.split(".")
        warehouse = spark.conf.get("spark.sql.warehouse.dir").removeprefix("file:")
        shutil.rmtree(Path(warehouse) / f"{database}.db" / name, ignore_errors=True)
    writer = frame.write.mode("overwrite").format("parquet")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.saveAsTable(table)
    print(f"Hive table {table} written")


def run(
    raw_dir: Path,
    warehouse_dir: Path | None = Path("data/warehouse"),
    cassandra_host: str | None = None,
    csv_output_dir: Path | None = None,
) -> None:
    """raw CSV -> raw Parquet -> curated Parquet -> analytics tables -> Hive / Cassandra / optional CSV."""
    spark = create_spark(warehouse_dir, cassandra_host)
    try:
        raw = read_raw(spark, raw_dir)
        if warehouse_dir is not None:
            for database in ("raw", "curated", "analytics"):
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
            for name, frame in zip(RAW_SCHEMAS, raw):
                _save_hive(frame, f"raw.{name}")
            # The curated zone is rebuilt from the raw Parquet tables, not from the CSV files.
            users, movies, ratings, watches = curate(*(spark.table(f"raw.{name}") for name in RAW_SCHEMAS))
            watches = watches.withColumn("watch_date", F.to_date("watch_timestamp"))
            _save_hive(users, "curated.users")
            _save_hive(movies, "curated.movies")
            _save_hive(ratings, "curated.ratings")
            _save_hive(watches.repartition("watch_date"), "curated.watch_events", partition_by="watch_date")
            curated = tuple(
                spark.table(f"curated.{name}") for name in ("users", "movies", "ratings", "watch_events")
            )
            tables = build_tables(*curated)
            for name, table in tables.items():
                _save_hive(table, f"analytics.{name}")
            tables = {name: spark.table(f"analytics.{name}") for name in tables}
        else:
            tables = build_tables(*curate(*raw))
        if cassandra_host:
            write_cassandra(serving_frames(tables))
        if csv_output_dir is not None:
            for name, table in tables.items():
                _write_csv_file(table, csv_output_dir, name)
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build dashboard analytics tables with Apache Spark.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    parser.add_argument(
        "--warehouse-dir", type=Path, default=Path(os.getenv("WAREHOUSE_DIR", "data/warehouse")),
        help="Hive warehouse + metastore location",
    )
    parser.add_argument("--no-hive", action="store_true", help="skip the Hive/Parquet lake tables")
    parser.add_argument(
        "--cassandra-host", default=os.getenv("CASSANDRA_HOST"),
        help="write the serving tables to Cassandra (default: $CASSANDRA_HOST; unset = skip)",
    )
    parser.add_argument("--csv-output-dir", type=Path, default=None, help="also write the 10 tables as CSV")
    args = parser.parse_args()
    run(args.raw_dir, None if args.no_hive else args.warehouse_dir, args.cassandra_host, args.csv_output_dir)


if __name__ == "__main__":
    main()
