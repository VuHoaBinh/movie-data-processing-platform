"""Parameters shared by the batch and speed layers (Spark, Flink, Pandas twin, dashboard).

Every value can be overridden with an environment variable so all engines run with the same settings.
"""
from __future__ import annotations

import os
from pathlib import Path

# Batch serving keyspaces; live tables use the Spark keyspace.
SPARK_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "movie_analytics")
FLINK_KEYSPACE = os.getenv("FLINK_CASSANDRA_KEYSPACE", "movie_analytics_flink")
BATCH_KEYSPACES = {"Spark": SPARK_KEYSPACE, "Flink": FLINK_KEYSPACE}

# BR8: a movie is low-engagement when views >= MIN_VIEWS and avg completion < LOW_COMPLETION_THRESHOLD.
# With ~50 views per movie on average over the history, 20 views keeps the flag away from noise.
MIN_VIEWS = int(os.getenv("BR8_MIN_VIEWS", "20"))
LOW_COMPLETION_THRESHOLD = float(os.getenv("BR8_COMPLETION_THRESHOLD", "0.3"))

# BR3: recommendations per user (favourite genres = top 2 genres by views).
RECOMMENDATIONS_TOP_N = int(os.getenv("BR3_TOP_N", "8"))

# Speed layer: event time = watch_timestamp; events up to this late are still counted.
WATERMARK_SECONDS = int(os.getenv("WATERMARK_SECONDS", "60"))
TREND_WINDOW_SECONDS = int(os.getenv("TREND_WINDOW_SECONDS", "300"))
SPARK_TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "5"))

# Kafka topics.
WATCH_TOPIC = os.getenv("KAFKA_WATCH_TOPIC", "watch-events")
TREND_TOPIC = os.getenv("KAFKA_TREND_TOPIC", "trending-results")
USER_ENGAGEMENT_TOPIC = os.getenv("KAFKA_USER_ENGAGEMENT_TOPIC", "live-user-engagement")
MOVIE_ENGAGEMENT_TOPIC = os.getenv("KAFKA_MOVIE_ENGAGEMENT_TOPIC", "live-movie-engagement")

# Raw-zone archive of the watch-events topic (Spark file sink, headerless CSV, read by the next batch run).
ARCHIVE_DIR = Path(os.getenv("ARCHIVE_DIR", "data/warehouse/landing/watch_events"))
ARCHIVE_CHECKPOINT_DIR = Path(os.getenv("ARCHIVE_CHECKPOINT_DIR", "data/warehouse/landing/_checkpoints/watch_events"))
