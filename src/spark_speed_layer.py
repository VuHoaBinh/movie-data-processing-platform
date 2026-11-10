"""Speed layer, main solution: Spark Structured Streaming over Kafka `watch-events` (BR2, BR6, BR8).

Event time = `watch_timestamp` (`event_time_ms` in the message), watermark WATERMARK_SECONDS.
Three queries share one Spark application, each with its own checkpoint:

* trending  (BR2): 5-minute tumbling window per movie, top 10 per closed window (append mode)
                    -> live_trending_spark
* users     (BR6): per (day, user) views, watch minutes, completion sum, updated every micro-batch
                    (update mode) -> live_user_engagement_spark
* movies    (BR8): per (day, movie) views, watch minutes, completion sum (update mode)
                    -> live_movie_engagement_spark

Completion of one view = min(1, watch_duration / movie_duration), the same rule as the batch layer.
Heartbeat messages (no ids) are not filtered before the aggregation: a filter on `event_type` would be
pushed below the watermark operator and the heartbeats would no longer advance it. They land in the
null-key group, which is dropped before writing. The Flink twin is flink/flink_speed_sql_job.py.
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import Column, DataFrame, SparkSession, functions as F, Window
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from src import settings

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
CASSANDRA_PACKAGE = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
EVENT_SCHEMA = StructType([
    StructField("event_type", StringType()), StructField("watch_id", StringType()),
    StructField("user_id", StringType()), StructField("movie_id", StringType()),
    StructField("title", StringType()), StructField("genre", StringType()),
    StructField("subscription_plan", StringType()), StructField("device", StringType()),
    StructField("watch_duration", IntegerType()), StructField("movie_duration", IntegerType()),
    StructField("watch_timestamp", StringType()),
    StructField("event_time_ms", LongType()), StructField("sent_at_ms", LongType()),
])
TOP_N = 10
QUERIES = ("trending", "users", "movies")


def read_events(spark: SparkSession, brokers: str, topic: str, starting_offsets: str = "earliest") -> DataFrame:
    """Parsed watch-events stream with an `event_time` column (heartbeats included)."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")  # the topic is recreated by `docker compose down`
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .where(F.col("event_time_ms").isNotNull())
        .withColumn("event_time", F.expr("timestamp_millis(event_time_ms)"))
    )


def completion() -> Column:
    return F.when(
        F.col("movie_duration") > 0,
        F.least(F.lit(1.0), F.col("watch_duration").cast("double") / F.col("movie_duration")),
    )


def trending(events: DataFrame, window_seconds: int) -> DataFrame:
    # size(collect_set) is the exact distinct count; count(distinct) is unsupported on streams.
    return (
        events.groupBy(F.window("event_time", f"{window_seconds} seconds"), "movie_id")
        .agg(
            F.first("title", ignorenulls=True).alias("title"), F.first("genre", ignorenulls=True).alias("genre"),
            F.count("watch_id").alias("live_views"),
            F.size(F.collect_set("user_id")).alias("unique_viewers"),
            F.max("sent_at_ms").alias("last_sent_at_ms"),
        )
        .select(F.col("window.start").alias("window_start"), "movie_id", "title", "genre",
                "live_views", "unique_viewers", "last_sent_at_ms")
    )


def user_engagement(events: DataFrame) -> DataFrame:
    return (
        events.groupBy(F.window("event_time", "1 day"), "user_id")
        .agg(
            F.first("subscription_plan", ignorenulls=True).alias("subscription_plan"),
            F.count("watch_id").alias("views"),
            F.coalesce(F.sum("watch_duration"), F.lit(0)).cast("long").alias("watch_minutes"),
            F.coalesce(F.sum(completion()), F.lit(0.0)).alias("completion_sum"),
            F.max("sent_at_ms").alias("last_sent_at_ms"),
        )
        .select(F.to_date(F.col("window.start")).alias("day"), "user_id", "subscription_plan", "views",
                "watch_minutes", "completion_sum", "last_sent_at_ms")
    )


def movie_engagement(events: DataFrame) -> DataFrame:
    return (
        events.groupBy(F.window("event_time", "1 day"), "movie_id")
        .agg(
            F.first("title", ignorenulls=True).alias("title"), F.first("genre", ignorenulls=True).alias("genre"),
            F.count("watch_id").alias("views"),
            F.coalesce(F.sum("watch_duration"), F.lit(0)).cast("long").alias("watch_minutes"),
            F.coalesce(F.sum(completion()), F.lit(0.0)).alias("completion_sum"),
            F.max("sent_at_ms").alias("last_sent_at_ms"),
        )
        .select(F.to_date(F.col("window.start")).alias("day"), "movie_id", "title", "genre", "views",
                "watch_minutes", "completion_sum", "last_sent_at_ms")
    )


def window_top_n(batch: DataFrame) -> DataFrame:
    """Rank movies inside each window (views, then unique viewers, then title) and keep the top N."""
    ranking = Window.partitionBy("window_start").orderBy(
        F.desc("live_views"), F.desc("unique_viewers"), F.asc("title")
    )
    return (
        batch.withColumn("rank", F.row_number().over(ranking))
        .where(F.col("rank") <= TOP_N)
        .select(
            F.to_date("window_start").alias("day"), "window_start", "rank", "movie_id", "title", "genre",
            F.col("live_views").cast("long"), F.col("unique_viewers").cast("long"), "last_sent_at_ms",
        )
    )


def cassandra_writer(table: str, keyspace: str, key: str, transform=None):
    """foreachBatch sink: drop the heartbeat (null-key) group and upsert the rows by primary key."""
    def write(batch: DataFrame, batch_id: int) -> None:
        rows = batch.where(F.col(key).isNotNull())
        if transform is not None:
            rows = transform(rows)
        rows = (
            rows.withColumn("last_sent_at", F.expr("timestamp_millis(last_sent_at_ms)"))
            .withColumn("updated_at", F.current_timestamp())
            .drop("last_sent_at_ms")
        )
        rows.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).mode("append").save()
    return write


def run(
    brokers: str, topic: str, window_seconds: int, keyspace: str, cassandra_host: str,
    starting_offsets: str, trigger_seconds: int, checkpoint: str, queries: tuple[str, ...],
) -> None:
    spark = (
        SparkSession.builder.appName("movie-speed-layer-spark")
        .config("spark.jars.packages", f"{KAFKA_PACKAGE},{CASSANDRA_PACKAGE}")
        .config("spark.cassandra.connection.host", cassandra_host)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    events = read_events(spark, brokers, topic, starting_offsets).withWatermark(
        "event_time", f"{settings.WATERMARK_SECONDS} seconds"
    )
    plans = {
        # A window is final once the watermark passes its end, so it is written exactly once.
        "trending": (trending(events, window_seconds), "append",
                     cassandra_writer("live_trending_spark", keyspace, "movie_id", window_top_n)),
        # Running per-day totals: every micro-batch upserts the keys that changed.
        "users": (user_engagement(events), "update",
                  cassandra_writer("live_user_engagement_spark", keyspace, "user_id")),
        "movies": (movie_engagement(events), "update",
                   cassandra_writer("live_movie_engagement_spark", keyspace, "movie_id")),
    }
    for name in queries:
        frame, mode, writer = plans[name]
        (
            frame.writeStream.queryName(f"speed-{name}").outputMode(mode)
            .foreachBatch(writer)
            .option("checkpointLocation", f"{checkpoint}/{name}")
            .trigger(processingTime=f"{trigger_seconds} seconds")
            .start()
        )
    spark.streams.awaitAnyTermination()


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming speed layer (BR2, BR6, BR8).")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    parser.add_argument("--topic", default=settings.WATCH_TOPIC)
    parser.add_argument("--window-seconds", type=int, default=settings.TREND_WINDOW_SECONDS)
    parser.add_argument("--keyspace", default=os.getenv("CASSANDRA_KEYSPACE", "movie_analytics"))
    parser.add_argument("--cassandra-host", default=os.getenv("CASSANDRA_HOST", "cassandra"))
    parser.add_argument("--starting-offsets", default=os.getenv("STARTING_OFFSETS", "earliest"))
    parser.add_argument("--trigger-seconds", type=int, default=settings.SPARK_TRIGGER_SECONDS)
    parser.add_argument("--checkpoint", default=os.getenv("CHECKPOINT_DIR", "/tmp/spark-speed-checkpoint"))
    parser.add_argument("--queries", default=",".join(QUERIES), help=f"comma-separated subset of {QUERIES}")
    args = parser.parse_args()
    queries = tuple(name.strip() for name in args.queries.split(",") if name.strip())
    unknown = set(queries) - set(QUERIES)
    if unknown:
        parser.error(f"unknown queries: {sorted(unknown)}")
    run(
        args.brokers, args.topic, args.window_seconds, args.keyspace, args.cassandra_host,
        args.starting_offsets, args.trigger_seconds, args.checkpoint, queries,
    )


if __name__ == "__main__":
    main()
