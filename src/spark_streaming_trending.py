"""Alternative to the Flink job: Spark Structured Streaming 5-minute top-10 trending from Kafka.

Reads `watch-events`, aggregates tumbling event-time windows per movie (event time = Kafka record
timestamp, like the Flink job) and upserts each closed window's top 10 into a Cassandra table, so
the two engines can be compared on the same input.
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from pyspark.sql.types import StringType, StructField, StructType

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
CASSANDRA_PACKAGE = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
EVENT_SCHEMA = StructType([
    StructField("movie_id", StringType()), StructField("title", StringType()),
    StructField("genre", StringType()), StructField("user_id", StringType()),
])
TOP_N = 10


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
            F.col("live_views").cast("long"), F.col("unique_viewers").cast("long"),
        )
    )


def write_batch(table: str, keyspace: str):
    def write(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        (
            window_top_n(batch).write.format("org.apache.spark.sql.cassandra")
            .options(table=table, keyspace=keyspace).mode("append").save()
        )
    return write


def run(
    brokers: str, topic: str, window_seconds: int, table: str, keyspace: str, cassandra_host: str,
    starting_offsets: str, trigger_seconds: int, checkpoint: str,
) -> None:
    spark = (
        SparkSession.builder.appName("movie-trending-spark-streaming")
        .config("spark.jars.packages", f"{KAFKA_PACKAGE},{CASSANDRA_PACKAGE}")
        .config("spark.cassandra.connection.host", cassandra_host)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
        .select(
            F.col("timestamp").alias("event_time"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"),
        )
        .select("event_time", "event.*")
    )
    # size(collect_set) is the exact distinct count; count(distinct) is unsupported on streams.
    windows = (
        events.withWatermark("event_time", "0 seconds")
        .groupBy(F.window("event_time", f"{window_seconds} seconds"), "movie_id")
        .agg(
            F.first("title").alias("title"), F.first("genre").alias("genre"),
            F.count(F.lit(1)).alias("live_views"),
            F.size(F.collect_set("user_id")).alias("unique_viewers"),
        )
        .select(F.col("window.start").alias("window_start"), "movie_id", "title", "genre", "live_views", "unique_viewers")
    )
    query = (
        windows.writeStream.outputMode("append")
        .foreachBatch(write_batch(table, keyspace))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )
    query.awaitTermination()


def main() -> None:
    window_minutes = int(os.getenv("TREND_WINDOW_MINUTES", "5"))
    parser = argparse.ArgumentParser(description="Spark Structured Streaming trending job (Flink alternative).")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_WATCH_TOPIC", "watch-events"))
    parser.add_argument("--window-seconds", type=int, default=int(os.getenv("TREND_WINDOW_SECONDS", window_minutes * 60)))
    parser.add_argument("--table", default=os.getenv("TRENDING_TABLE", "live_trending_spark"))
    parser.add_argument("--keyspace", default=os.getenv("CASSANDRA_KEYSPACE", "movie_analytics"))
    parser.add_argument("--cassandra-host", default=os.getenv("CASSANDRA_HOST", "cassandra"))
    parser.add_argument("--starting-offsets", default=os.getenv("STARTING_OFFSETS", "earliest"))
    parser.add_argument("--trigger-seconds", type=int, default=int(os.getenv("TRIGGER_SECONDS", "5")))
    parser.add_argument("--checkpoint", default=os.getenv("CHECKPOINT_DIR", "/tmp/spark-trending-checkpoint"))
    args = parser.parse_args()
    run(
        args.brokers, args.topic, args.window_seconds, args.table, args.keyspace, args.cassandra_host,
        args.starting_offsets, args.trigger_seconds, args.checkpoint,
    )


if __name__ == "__main__":
    main()
