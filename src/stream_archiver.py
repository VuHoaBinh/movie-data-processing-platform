"""Archive Kafka `watch-events` into the raw zone so the next batch run includes the live events.

Spark Structured Streaming file sink (exactly-once through its checkpoint and `_spark_metadata` log):
watch events only (heartbeats dropped), written as headerless CSV in the raw `watch_history` column
layout (the source format, readable by Spark, Pandas and the Flink batch job without Hadoop),
partitioned by `watch_date`. The batch job reads this directory (see `spark_batch_job.select_watch_events`).
The archive is engine-neutral: it runs whichever stream engine is used for BR2/BR6/BR8.
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, functions as F

from src import settings
from src.spark_speed_layer import KAFKA_PACKAGE, read_events


def run(brokers: str, topic: str, archive_dir: str, checkpoint: str, trigger_seconds: int) -> None:
    spark = (
        SparkSession.builder.appName("movie-watch-events-archiver")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    watches = (
        read_events(spark, brokers, topic)
        .where((F.col("event_type") == "watch") & F.col("watch_id").isNotNull())
        .select(
            "watch_id", "user_id", "movie_id", "watch_timestamp", "watch_duration", "device",
            F.to_date("event_time").alias("watch_date"),
        )
    )
    (
        watches.writeStream.format("csv")
        .option("header", "false")
        .option("path", archive_dir)
        .option("checkpointLocation", checkpoint)
        .partitionBy("watch_date")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
        .awaitTermination()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive watch-events into the raw zone (CSV).")
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    parser.add_argument("--topic", default=settings.WATCH_TOPIC)
    parser.add_argument("--archive-dir", default=str(settings.ARCHIVE_DIR))
    parser.add_argument("--checkpoint", default=str(settings.ARCHIVE_CHECKPOINT_DIR))
    parser.add_argument("--trigger-seconds", type=int, default=10)
    args = parser.parse_args()
    run(args.brokers, args.topic, args.archive_dir, args.checkpoint, args.trigger_seconds)


if __name__ == "__main__":
    main()
