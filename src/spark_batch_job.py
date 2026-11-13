from __future__ import annotations

import argparse
import os
import shutil
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from src import settings
from src.data_quality import clean_spark, format_report, report_from_rows, utc_now
from src.replay_clock import batch_cutoff as compute_batch_cutoff
from src.serving_spec import SERVING_TABLES, serving_columns

CASSANDRA_PACKAGE = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
RAW_FILES = ("users.csv", "movies.csv", "ratings.csv", "watch_history.csv")

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
        StructField("genre", StringType()), StructField("release_year", DoubleType()),
        StructField("duration", DoubleType()), StructField("content_type", StringType()),
        StructField("language", StringType()), StructField("maturity_rating", StringType()),
    ]),
    "ratings": StructType([
        StructField("rating_id", StringType()), StructField("user_id", StringType()),
        StructField("movie_id", StringType()), StructField("rating", DoubleType()),
        StructField("timestamp", StringType()),
    ]),
    "watch_history": StructType([
        StructField("watch_id", StringType()), StructField("user_id", StringType()),
        StructField("movie_id", StringType()), StructField("watch_timestamp", StringType()),
        StructField("watch_duration", DoubleType()), StructField("device", StringType()),
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
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
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
    """Read the raw CSVs with explicit schemas (raw zone: nothing is dropped or parsed yet).

    `_seq` numbers the rows in file order at ingest (increasing, not consecutive). It is stored in the raw
    zone so that every engine keeps the same row of a duplicated id.
    """
    missing = [name for name in RAW_FILES if not (raw_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing raw files in {raw_dir}: {', '.join(missing)}")
    return tuple(
        spark.read.option("header", True).option("nullValue", "").schema(
            RAW_SCHEMAS[Path(name).stem]
        ).csv(str(raw_dir / name)).withColumn("_seq", F.monotonically_increasing_id())
        for name in RAW_FILES
    )


def read_archive(spark: SparkSession, archive_dir: Path | None) -> DataFrame | None:
    """Live events archived from Kafka by `src.stream_archiver`. Reading through the file-sink
    metadata log returns only committed files, so a run concurrent with the archiver is consistent."""
    if archive_dir is None or not (archive_dir / "_spark_metadata").is_dir():
        return None
    schema = RAW_SCHEMAS["watch_history"]
    archived = spark.read.schema(schema).option("header", False).option("nullValue", "").csv(str(archive_dir))
    # Drop the watch_date partition column; number the rows like `read_raw`.
    return archived.select(*[field.name for field in schema.fields]).withColumn(
        "_seq", F.monotonically_increasing_id()
    )


def _event_day(column: str = "watch_timestamp"):
    return F.to_date(F.to_timestamp(column))


def select_watch_events(
    watches: DataFrame, archived: DataFrame | None, replay_day: date | None, batch_cutoff: date | None
) -> DataFrame:
    """Spark twin of `batch_processing.select_watch_events`: CSV history without the replay day plus
    archived live events of closed days (re-sent events share a watch_id and are de-duplicated).
    `_src` (0 = CSV, 1 = archive) orders the CSV row first when an id is in both."""
    watches = watches.withColumn("_src", F.lit(0))
    if replay_day is not None:
        day = _event_day()
        watches = watches.where(day.isNull() | (day != F.lit(replay_day)))
    if archived is not None:
        cutoff = batch_cutoff or replay_day
        if cutoff is not None:
            archived = archived.where(_event_day() < F.lit(cutoff))
        watches = watches.unionByName(archived.withColumn("_src", F.lit(1)))
    return watches


def detect_replay_day(watches: DataFrame, now: datetime | None = None) -> date | None:
    """Last day of the raw watch history (timestamps in the future are ignored)."""
    stamp = F.to_timestamp("watch_timestamp")
    limit = F.lit((now or utc_now()).strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    return watches.where(stamp <= limit).agg(F.max(F.to_date(stamp))).first()[0]


def resolve_replay_day(value: str, watches: DataFrame) -> date | None:
    """'auto' = last day of watch_history, 'none' = no live day (batch reads every CSV row)."""
    if value.lower() == "none":
        return None
    return detect_replay_day(watches) if value.lower() == "auto" else date.fromisoformat(value)


def simulated_day(cassandra_host: str | None, archive_lag_seconds: float = 30.0) -> date | None:
    """Current day of the replay clock (minus the archiver's lag), or None without a clock."""
    if not cassandra_host:
        return None
    from src.cassandra_store import CassandraUnavailable, connect
    from src.replay_clock import load_clock

    os.environ.setdefault("CASSANDRA_HOST", cassandra_host)
    try:
        session = connect(timeout=10)
    except CassandraUnavailable:
        return None
    try:
        clock = load_clock(session)
    finally:
        session.cluster.shutdown()
    if clock is None:
        return None
    return clock.sim_now(datetime.now(timezone.utc) - timedelta(seconds=archive_lag_seconds)).date()


def write_boundary(
    warehouse_dir: Path, replay_day: date | None, batch_cutoff: date | None, now: datetime, has_archive: bool
) -> None:
    """Record the ingest boundary next to the raw zone; both batch branches clean the same rows with it."""
    import json

    warehouse_dir.mkdir(parents=True, exist_ok=True)
    (warehouse_dir / "batch_boundary.json").write_text(json.dumps({
        "replay_day": replay_day and replay_day.isoformat(),
        "batch_cutoff": batch_cutoff and batch_cutoff.isoformat(),
        "now": now.strftime("%Y-%m-%d %H:%M:%S"),  # reference time of the future_timestamp rule
        "has_archive": has_archive,
    }))


def curate(
    users: DataFrame, movies: DataFrame, ratings: DataFrame, watches: DataFrame, now: datetime | None = None
) -> tuple[tuple[DataFrame, ...], DataFrame]:
    """Raw -> curated: parse timestamps (UTC) and apply the data-quality rules.

    Returns the clean frames and a lazy (table_name, reason, rows) report of rejected rows.
    """
    # `_bad_ts` marks text that is present but cannot be parsed, so it is counted as invalid_timestamp.
    ratings = ratings.withColumn("_bad_ts", F.col("timestamp").isNotNull() & F.to_timestamp("timestamp").isNull())
    ratings = ratings.withColumn("timestamp", F.to_timestamp("timestamp"))
    watches = watches.withColumn(
        "_bad_ts", F.col("watch_timestamp").isNotNull() & F.to_timestamp("watch_timestamp").isNull()
    )
    watches = watches.withColumn("watch_timestamp", F.to_timestamp("watch_timestamp"))
    return clean_spark(users, movies, ratings, watches, now)


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
    joined = watches.join(movies, "movie_id").join(users, "user_id").withColumn(
        # Completion of one view, capped at 1 (watch_duration and duration are both minutes).
        "completion", F.least(F.lit(1.0), F.col("watch_duration") / F.col("duration"))
    )
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
        F.sum(F.least("watch_duration", "duration")).alias("capped_minutes"),
        F.first("duration").alias("movie_duration"),
        F.countDistinct("user_id").alias("unique_viewers"),
    )
    # A movie has one duration: sum capped integer minutes before dividing. Summing
    # per-view floating ratios can move exact rounding ties to either side.
    watch_stats = (
        watch_stats.withColumn("completion_sum", F.col("capped_minutes").cast("double") / F.col("movie_duration"))
        .withColumn("avg_completion", F.col("capped_minutes").cast("double") /
                    (F.col("movie_duration").cast("long") * F.col("views")))
        .drop("capped_minutes", "movie_duration")
    )
    movie_stats = (
        movies.select(
            "movie_id", "title", "genre", "release_year", "duration", "content_type", "language", "maturity_rating"
        )
        .join(watch_stats, "movie_id", "left")
        .join(rating_stats, "movie_id", "left")
        .withColumn("views", F.coalesce(F.col("views"), F.lit(0)))
        .withColumn("watch_minutes_raw", F.coalesce(F.col("watch_minutes_raw"), F.lit(0)))
        .withColumn("unique_viewers", F.coalesce(F.col("unique_viewers"), F.lit(0)))
        .withColumn("watch_hours", F.round(F.col("watch_minutes_raw") / 60, 1))
        .withColumn("completion_rate", F.round("avg_completion", 3))
    )
    # BR8: additive sums so the serving layer can merge batch history with the speed layer's live days.
    low_engagement_movies = movie_stats.select(
        "movie_id", "title", "genre", "views",
        F.col("watch_minutes_raw").cast("long").alias("watch_minutes"),
        F.round(F.coalesce(F.col("completion_sum"), F.lit(0.0)), 4).alias("completion_sum"),
        "completion_rate",
        (
            (F.col("views") >= settings.MIN_VIEWS)
            & F.coalesce(F.col("avg_completion") < settings.LOW_COMPLETION_THRESHOLD, F.lit(False))
        ).alias("is_low_engagement"),
    ).orderBy("movie_id")
    movie_performance = movie_stats.select(
        "movie_id", "title", "genre", "release_year", "duration", "content_type", "language",
        "maturity_rating", "views", "unique_viewers", "avg_rating", "rating_count", "weighted_rating",
        "watch_hours", "completion_rate",
    ).orderBy("movie_id")

    # Spark groupBy output is unordered; every table below is sorted like the Pandas pipeline.
    genre_popularity, genre_monthly = build_genre_tables(joined, ratings, movies)
    subscription_views = joined.groupBy("subscription_plan").agg(
        F.count("watch_id").alias("views"), F.countDistinct("user_id").alias("unique_viewers")
    ).orderBy("subscription_plan")

    engagement_stats = joined.groupBy("user_id").agg(
        F.count("watch_id").alias("views"),
        F.sum("watch_duration").alias("watch_minutes"),
        F.countDistinct(F.to_date("watch_timestamp")).alias("active_days"),
        F.sum("completion").alias("completion_sum"),
        F.avg("completion").alias("avg_completion"),
    )
    user_engagement = (
        users.select("user_id", "subscription_plan", "region")
        .join(engagement_stats, "user_id", "left")
        .withColumn("views", F.coalesce(F.col("views"), F.lit(0)))
        .withColumn("watch_minutes", F.coalesce(F.col("watch_minutes"), F.lit(0)))
        .withColumn("active_days", F.coalesce(F.col("active_days"), F.lit(0)))
        .withColumn("completion_sum", F.round(F.coalesce(F.col("completion_sum"), F.lit(0.0)), 4))
        .withColumn("avg_completion", F.round("avg_completion", 3))
        .withColumn(
            "segment",
            F.when(F.col("views") <= 10, "Inactive")
            .when(F.col("views") <= 35, "Casual")
            .otherwise("Active"),
        )
        .select(
            "user_id", "subscription_plan", "region", "views", "watch_minutes", "active_days",
            "completion_sum", "avg_completion", "segment",
        )
        .orderBy("user_id")
    )
    # Every segment is always present (with 0 users when empty), like the Pandas categorical groupby.
    segment_names = users.sparkSession.sql(
        "SELECT * FROM VALUES ('Inactive', 0), ('Casual', 1), ('Active', 2) AS names(segment, segment_order)"
    )
    user_segments = (
        segment_names.join(
            user_engagement.groupBy("segment").agg(
                F.count("user_id").alias("users"), F.sum("views").alias("views")
            ),
            "segment",
            "left",
        )
        .withColumn("users", F.coalesce(F.col("users"), F.lit(0)))
        .withColumn("views", F.coalesce(F.col("views"), F.lit(0)))
        .orderBy("segment_order")
        .select("segment", "users", "views")
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
        "user_recommendations": build_recommendations(joined, user_preferences, movie_performance),
        "rating_distribution": build_rating_distribution(ratings, movie_performance),
        "genre_monthly": genre_monthly,
        "view_trends": build_view_trends(joined),
    }


def build_genre_tables(joined: DataFrame, ratings: DataFrame, movies: DataFrame) -> tuple[DataFrame, DataFrame]:
    """BR5 (Spark twin of batch_processing.build_genre_tables)."""
    rated = ratings.join(movies.select("movie_id", "genre"), "movie_id")

    def summarise(views: DataFrame, rated_part: DataFrame, keys: list[str]) -> DataFrame:
        frame = views.groupBy(*keys).agg(
            F.count("watch_id").alias("views"), F.countDistinct("user_id").alias("unique_viewers"),
            F.round(F.sum("watch_duration") / 60, 1).alias("watch_hours"),
        )
        rating = rated_part.groupBy(*keys).agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
        return frame.join(rating, keys, "left").select(*keys, "views", "unique_viewers", "watch_hours", "avg_rating")

    overall = summarise(joined, rated, ["genre"]).orderBy(F.desc("views"))
    monthly = summarise(
        joined.withColumn("month", F.trunc(F.to_date("watch_timestamp"), "month")),
        rated.withColumn("month", F.trunc(F.to_date("timestamp"), "month")),
        ["genre", "month"],
    ).orderBy("genre", "month")
    return overall, monthly


def build_recommendations(
    joined: DataFrame, user_preferences: DataFrame, movie_performance: DataFrame,
    top_n: int = settings.RECOMMENDATIONS_TOP_N,
) -> DataFrame:
    """BR3 (Spark twin of batch_processing.build_recommendations): top N rated, unwatched movies of
    the favourite genres; only each genre's top N + (movies the user watched in it) are candidates."""
    order = [F.desc("weighted_rating"), F.desc("rating_count"), F.asc("movie_id")]
    rated = movie_performance.where(F.col("weighted_rating").isNotNull()).withColumn(
        "genre_rank", F.row_number().over(Window.partitionBy("genre").orderBy(*order))
    ).select("genre", "genre_rank", "movie_id", "title", "weighted_rating", "rating_count")
    favorites = user_preferences.select("user_id", F.explode(F.split("favorite_genres", r"\|")).alias("genre"))
    watched = joined.select("user_id", "movie_id", "genre").distinct()
    watched_in_genre = watched.groupBy("user_id", "genre").agg(F.count(F.lit(1)).alias("watched"))
    candidates = (
        favorites.join(watched_in_genre, ["user_id", "genre"], "left")
        .withColumn("watched", F.coalesce(F.col("watched"), F.lit(0)))
        .join(rated, "genre")
        .where(F.col("genre_rank") <= F.lit(top_n) + F.col("watched"))
        .join(watched.select("user_id", "movie_id"), ["user_id", "movie_id"], "left_anti")
        .withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(*order)))
        .where(F.col("rank") <= top_n)
    )
    return candidates.select(
        "user_id", "rank", "movie_id", "title", "genre", "weighted_rating", "rating_count"
    ).orderBy("user_id", "rank")


def build_rating_distribution(ratings: DataFrame, movie_performance: DataFrame) -> DataFrame:
    """BR4 (Spark twin of batch_processing.build_rating_distribution)."""
    counts = ratings.groupBy("movie_id").agg(
        *[F.count(F.when(F.col("rating") == star, 1)).alias(f"r{star}") for star in range(1, 6)]
    )
    return (
        movie_performance.where(F.col("rating_count").isNotNull())
        .select("movie_id", "title", "genre", "rating_count", "avg_rating")
        .join(counts, "movie_id")
        .orderBy("movie_id")
    )


TREND_PERIODS = {"all": ("day", "week", "month"), "genre": ("day", "week", "month"), "movie": ("week", "month")}


def _period_start(period: str):
    day = F.to_date("watch_timestamp")
    if period == "day":
        return day
    if period == "week":  # ISO weeks start on Monday
        return F.to_date(F.date_trunc("week", "watch_timestamp"))
    return F.trunc(day, "month")


def _previous_start(period: str):
    start = F.col("period_start")
    return {"day": F.date_sub(start, 1), "week": F.date_sub(start, 7), "month": F.add_months(start, -1)}[period]


def build_view_trends(joined: DataFrame) -> DataFrame:
    """BR10 (Spark twin of batch_processing.build_view_trends)."""
    key = {"all": F.lit("all"), "genre": F.col("genre"), "movie": F.col("movie_id")}
    frames = []
    for scope, periods in TREND_PERIODS.items():
        for period in periods:
            counts = (
                joined.groupBy(key[scope].alias("key"), _period_start(period).alias("period_start"))
                .agg(F.count("watch_id").alias("views"))
            )
            previous = counts.select("key", F.col("period_start").alias("prev_start"), F.col("views").alias("prev_views"))
            frames.append(
                counts.withColumn("prev_start", _previous_start(period))
                .join(previous, ["key", "prev_start"], "left")
                .select(F.lit(scope).alias("scope"), "key", F.lit(period).alias("period"), "period_start",
                        "views", F.coalesce(F.col("prev_views"), F.lit(0)).alias("prev_views"))
            )
    trends = frames[0]
    for frame in frames[1:]:
        trends = trends.unionByName(frame)
    return trends.withColumn(
        "growth",
        F.when(F.col("prev_views") > 0, F.round((F.col("views") - F.col("prev_views")) / F.col("prev_views"), 3)),
    ).orderBy("scope", "key", "period", "period_start")


def spark_create_summary(users: DataFrame, movies: DataFrame, watches: DataFrame, ratings: DataFrame) -> DataFrame:
    stats = ratings.agg(F.round(F.avg("rating"), 2).alias("average_rating"))
    return stats.select(
        F.lit(users.count()).alias("users"),
        F.lit(movies.count()).alias("movies"),
        F.lit(watches.count()).alias("views"),
        F.lit(ratings.count()).alias("ratings"),
        "average_rating",
    )


def serving_frames(tables: dict[str, DataFrame], version: str) -> dict[str, DataFrame]:
    """Reshape the analytics tables into the Cassandra tables (src/serving_spec.py, shared with the
    Flink branch). Rows carry the run's `version` (`bucket`): a run writes next to the version the
    dashboard is reading, then `publish_batch_version` switches the dashboard over."""
    frames = {}
    for table, spec in SERVING_TABLES.items():
        frame = tables[spec["source"]]
        if spec.get("rated"):
            frame = frame.where(F.col("weighted_rating").isNotNull())
        frame = frame.withColumn("bucket", F.lit(version))
        frames[table] = frame.select(*serving_columns(table, [c for c in frame.columns if c != "bucket"]))
    return frames


def write_cassandra(frames: dict[str, DataFrame], keyspace: str = settings.SPARK_KEYSPACE) -> dict[str, int]:
    """Upsert the serving tables (no truncate: the dashboard keeps reading while a run writes)."""
    counts: dict[str, int] = {}
    for name, frame in frames.items():
        frame = frame.cache()
        counts[name] = frame.count()
        frame.write.format("org.apache.spark.sql.cassandra").options(table=name, keyspace=keyspace).mode("append").save()
        frame.unpersist()
        print(f"Cassandra {keyspace}.{name}: {counts[name]:,} rows")
    return counts


def publish_version(
    cassandra_host: str, version: str, genres: list[str], replay_day: date | None, batch_cutoff: date | None,
    keyspace: str = settings.SPARK_KEYSPACE,
) -> None:
    from src.cassandra_store import connect, publish_batch_version

    os.environ.setdefault("CASSANDRA_HOST", cassandra_host)
    session = connect(keyspace=keyspace, timeout=30)
    try:
        publish_batch_version(session, version, genres, replay_day, batch_cutoff)
    finally:
        session.cluster.shutdown()


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


def _timed(stage: str, started: float) -> float:
    """Print the seconds since `started` as a parseable TIMING line and return a fresh start time."""
    now = time.perf_counter()
    print(f"TIMING {stage} {now - started:.1f}", flush=True)
    return now


def run(
    raw_dir: Path,
    warehouse_dir: Path | None = Path("data/warehouse"),
    cassandra_host: str | None = None,
    csv_output_dir: Path | None = None,
    quality_report: bool = False,
    replay_day: str = "none",
    archive_dir: Path | None = None,
    ingest_only: bool = False,
) -> None:
    """raw CSV + archived live events -> raw Parquet -> curated Parquet -> analytics tables
    -> Hive / Cassandra / optional CSV."""
    spark = create_spark(warehouse_dir, cassandra_host)
    try:
        raw = read_raw(spark, raw_dir)
        clock = time.perf_counter()
        live_day = resolve_replay_day(replay_day, raw[3])
        archived = read_archive(spark, archive_dir)
        archived_max_day = None if archived is None else archived.agg(F.max(_event_day())).first()[0]
        cutoff = compute_batch_cutoff(live_day, archived_max_day, simulated_day(cassandra_host))
        print(f"replay_day={live_day} batch_cutoff={cutoff} archived_events={archived is not None}", flush=True)
        run_now = utc_now()
        if warehouse_dir is not None:
            # Ingest (shared by both batch branches): CSV + Kafka archive -> raw zone with `_seq`, plus the
            # boundary file. The Flink batch job reads these raw tables and this boundary.
            for database in ("raw", "curated", "analytics"):
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
            for name, frame in zip(RAW_SCHEMAS, raw):
                _save_hive(frame, f"raw.{name}")
            if archived is not None:
                _save_hive(archived, "raw.watch_events_stream")  # raw-zone copy of the Kafka archive
            elif spark.catalog.tableExists("raw.watch_events_stream"):
                spark.sql("DROP TABLE raw.watch_events_stream")  # no archive now: do not let Flink read an old one
            write_boundary(warehouse_dir, live_day, cutoff, run_now, archived is not None)
            clock = _timed("ingest", clock)
            if ingest_only:
                return
            # The curated zone is rebuilt from the raw Parquet tables, not from the CSV files.
            raw_tables = [spark.table(f"raw.{name}") for name in RAW_SCHEMAS]
            raw_tables[3] = select_watch_events(
                raw_tables[3], spark.table("raw.watch_events_stream") if archived is not None else None,
                live_day, cutoff,
            )
            (users, movies, ratings, watches), quality = curate(*raw_tables, now=run_now)
            quality = quality.cache()
            print(format_report(report_from_rows([tuple(row) for row in quality.collect()])))
            watches = watches.withColumn("watch_date", F.to_date("watch_timestamp"))
            _save_hive(users, "curated.users")
            _save_hive(movies, "curated.movies")
            _save_hive(ratings, "curated.ratings")
            _save_hive(watches.repartition("watch_date"), "curated.watch_events", partition_by="watch_date")
            # Publish the report only after all curated-table writes have succeeded.
            _save_hive(quality, "curated.quality_report")
            clock = _timed("hive_curated", clock)
            curated = tuple(
                spark.table(f"curated.{name}") for name in ("users", "movies", "ratings", "watch_events")
            )
            tables = build_tables(*curated)
            for name, table in tables.items():
                _save_hive(table, f"analytics.{name}")
            tables = {name: spark.table(f"analytics.{name}") for name in tables}
            clock = _timed("hive_analytics", clock)
        else:
            # Cache the curated frames: each output table would otherwise recompute them from the CSVs.
            frames, quality = curate(*raw[:3], select_watch_events(raw[3], archived, live_day, cutoff), now=run_now)
            if quality_report:  # costs an extra pass over the raw data
                print(format_report(report_from_rows([tuple(row) for row in quality.collect()])))
            tables = build_tables(*(frame.persist() for frame in frames))
        if cassandra_host:
            version = datetime.now(timezone.utc).strftime("v%Y%m%dT%H%M%S")
            write_cassandra(serving_frames(tables, version))
            genres = [row[0] for row in tables["genre_popularity"].select("genre").collect()]
            publish_version(cassandra_host, version, genres, live_day, cutoff)
            clock = _timed("cassandra_load", clock)
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
    parser.add_argument(
        "--quality-report", action="store_true",
        help="print the rejected-row report even without Hive (always printed and saved with Hive)",
    )
    parser.add_argument(
        "--replay-day", default=os.getenv("REPLAY_DAY", "auto"),
        help="day owned by the speed layer: 'auto' (last day of watch_history), 'none' or YYYY-MM-DD",
    )
    parser.add_argument(
        "--archive-dir", type=Path, default=settings.ARCHIVE_DIR,
        help="CSV archive of the watch-events topic written by src.stream_archiver",
    )
    parser.add_argument(
        "--ingest-only", action="store_true",
        help="only refresh the shared raw zone + boundary (e.g. before a Flink-only batch run)",
    )
    args = parser.parse_args()
    if args.ingest_only and args.no_hive:
        parser.error("--ingest-only writes the raw zone and needs the Hive warehouse")
    run(
        args.raw_dir, None if args.no_hive else args.warehouse_dir, args.cassandra_host, args.csv_output_dir,
        args.quality_report, args.replay_day, args.archive_dir, args.ingest_only,
    )


if __name__ == "__main__":
    main()
