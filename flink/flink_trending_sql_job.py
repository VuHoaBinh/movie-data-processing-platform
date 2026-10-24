"""Flink SQL variant of the trending job: same Kafka input/output, windowing runs inside the JVM.

The DataStream job (flink_trending_job.py) keeps window state in Python, which caps it at a few hundred
events per second; this variant uses the SQL tumbling-window TVF and reaches a much higher rate with
the same results. Submit it instead of the DataStream job:
    flink run -m flink-jobmanager:8081 -py /opt/flink/usrlib/flink_trending_sql_job.py
"""
from __future__ import annotations

import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def main() -> None:
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    source_topic = os.getenv("KAFKA_WATCH_TOPIC", "watch-events")
    sink_topic = os.getenv("KAFKA_TREND_TOPIC", "trending-results")
    window_minutes = int(os.getenv("TREND_WINDOW_MINUTES", "5"))
    window_seconds = int(os.getenv("TREND_WINDOW_SECONDS", str(window_minutes * 60)))

    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    config = table_env.get_config()
    config.set("parallelism.default", "1")
    config.set("table.local-time-zone", "UTC")
    config.set("execution.checkpointing.interval", "10 s")

    # Event time = Kafka record timestamp, with monotonous watermarks, like the DataStream job.
    table_env.execute_sql(f"""
        CREATE TABLE watch_events (
            movie_id STRING, title STRING, genre STRING, user_id STRING,
            event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
            WATERMARK FOR event_time AS event_time
        ) WITH (
            'connector' = 'kafka', 'topic' = '{source_topic}',
            'properties.bootstrap.servers' = '{brokers}', 'properties.group.id' = 'movie-trending-flink-sql',
            'scan.startup.mode' = 'earliest-offset', 'format' = 'json', 'json.ignore-parse-errors' = 'true'
        )""")
    table_env.execute_sql(f"""
        CREATE TABLE trending_results (
            movie_id STRING, title STRING, genre STRING,
            window_start_ms BIGINT, live_views BIGINT, unique_viewers BIGINT
        ) WITH (
            'connector' = 'kafka', 'topic' = '{sink_topic}',
            'properties.bootstrap.servers' = '{brokers}', 'format' = 'json',
            'json.encode.decimal-as-plain-number' = 'true'
        )""")
    result = table_env.execute_sql(f"""
        INSERT INTO trending_results
        SELECT movie_id, MAX(title), MAX(genre),
               UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000,
               COUNT(*), COUNT(DISTINCT user_id)
        FROM TABLE(TUMBLE(TABLE watch_events, DESCRIPTOR(event_time), INTERVAL '{window_seconds}' SECOND(6)))
        GROUP BY movie_id, window_start, window_end""")
    result.wait()


if __name__ == "__main__":
    main()
