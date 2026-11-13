"""Speed layer, alternative solution: Flink SQL (STREAMING mode) over Kafka `watch-events`.

Same input, logic and parameters as the Spark speed layer (src/spark_speed_layer.py):

* BR2 trending: TUMBLE window of TREND_WINDOW_SECONDS per movie on event time -> Kafka trending-results
  (append-only; the materializer ranks the top 10 per window -> Cassandra live_trending).
* BR6 per (day, user) and BR8 per (day, movie): running totals (views, watch minutes, completion sum)
  -> upsert-kafka topics live-user-engagement / live-movie-engagement
  -> materializer -> Cassandra live_user_engagement / live_movie_engagement.

Event time = `event_time_ms` (the original watch_timestamp), watermark WATERMARK_SECONDS. Heartbeat
messages advance the watermark at the source and are filtered out before every aggregation.
Flink has no Cassandra SQL sink, so results go through Kafka and src/speed_materializer.py.

    flink run -m flink-jobmanager:8081 -py /opt/flink/usrlib/flink_speed_sql_job.py
"""
from __future__ import annotations

import os

from pyflink.table import EnvironmentSettings, TableEnvironment

# Same expression as spark_speed_layer.completion(): min(1, watch_duration / movie_duration).
COMPLETION = (
    "CASE WHEN movie_duration > 0 "
    "THEN LEAST(CAST(1.0 AS DOUBLE), CAST(watch_duration AS DOUBLE) / movie_duration) END"
)


def main() -> None:
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    source_topic = os.getenv("KAFKA_WATCH_TOPIC", "watch-events")
    trend_topic = os.getenv("KAFKA_TREND_TOPIC", "trending-results")
    user_topic = os.getenv("KAFKA_USER_ENGAGEMENT_TOPIC", "live-user-engagement")
    movie_topic = os.getenv("KAFKA_MOVIE_ENGAGEMENT_TOPIC", "live-movie-engagement")
    window_seconds = int(os.getenv("TREND_WINDOW_SECONDS", "300"))
    watermark_seconds = int(os.getenv("WATERMARK_SECONDS", "60"))

    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    config = table_env.get_config()
    config.set("parallelism.default", os.getenv("FLINK_STREAM_PARALLELISM", "1"))
    config.set("table.local-time-zone", "UTC")
    config.set("execution.checkpointing.interval", "10 s")
    # Per-day running totals: a day's keys get no update after the day ends (late events aside),
    # so their state can expire. Keep it above one real day for a replay at speed 1.
    config.set("table.exec.state.ttl", "36 h")

    table_env.execute_sql(f"""
        CREATE TABLE watch_events (
            event_type STRING, watch_id STRING, user_id STRING, movie_id STRING,
            title STRING, genre STRING, subscription_plan STRING,
            watch_duration INT, movie_duration INT,
            event_time_ms BIGINT, sent_at_ms BIGINT,
            event_time AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '{watermark_seconds}' SECOND
        ) WITH (
            'connector' = 'kafka', 'topic' = '{source_topic}',
            'properties.bootstrap.servers' = '{brokers}', 'properties.group.id' = 'movie-speed-flink-sql',
            'scan.startup.mode' = 'earliest-offset', 'format' = 'json',
            'json.ignore-parse-errors' = 'true', 'json.fail-on-missing-field' = 'false'
        )""")
    table_env.execute_sql(f"""
        CREATE TABLE trending_results (
            movie_id STRING, title STRING, genre STRING,
            window_start_ms BIGINT, live_views BIGINT, unique_viewers BIGINT, last_sent_at_ms BIGINT
        ) WITH (
            'connector' = 'kafka', 'topic' = '{trend_topic}',
            'properties.bootstrap.servers' = '{brokers}', 'format' = 'json'
        )""")
    table_env.execute_sql(f"""
        CREATE TABLE user_engagement_results (
            `day` STRING, user_id STRING, subscription_plan STRING,
            `views` BIGINT, watch_minutes BIGINT, completion_sum DOUBLE, last_sent_at_ms BIGINT,
            PRIMARY KEY (`day`, user_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka', 'topic' = '{user_topic}',
            'properties.bootstrap.servers' = '{brokers}', 'key.format' = 'json', 'value.format' = 'json'
        )""")
    table_env.execute_sql(f"""
        CREATE TABLE movie_engagement_results (
            `day` STRING, movie_id STRING, title STRING, genre STRING,
            `views` BIGINT, watch_minutes BIGINT, completion_sum DOUBLE, last_sent_at_ms BIGINT,
            PRIMARY KEY (`day`, movie_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka', 'topic' = '{movie_topic}',
            'properties.bootstrap.servers' = '{brokers}', 'key.format' = 'json', 'value.format' = 'json'
        )""")

    statements = table_env.create_statement_set()
    # BR2: one row per (window, movie) once the watermark passes the window end.
    statements.add_insert_sql(f"""
        INSERT INTO trending_results
        SELECT movie_id, MAX(title), MAX(genre),
               UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000,
               COUNT(*), COUNT(DISTINCT user_id), MAX(sent_at_ms)
        FROM TABLE(TUMBLE(TABLE watch_events, DESCRIPTOR(event_time), INTERVAL '{window_seconds}' SECOND(6)))
        WHERE event_type = 'watch' AND movie_id IS NOT NULL
        GROUP BY movie_id, window_start, window_end""")
    # BR6: running totals per (UTC day, user); each event updates one key (upsert by primary key).
    statements.add_insert_sql(f"""
        INSERT INTO user_engagement_results
        SELECT DATE_FORMAT(event_time, 'yyyy-MM-dd'), user_id, MAX(subscription_plan),
               COUNT(*), CAST(COALESCE(SUM(watch_duration), 0) AS BIGINT),
               COALESCE(SUM({COMPLETION}), CAST(0.0 AS DOUBLE)), MAX(sent_at_ms)
        FROM watch_events
        WHERE event_type = 'watch' AND user_id IS NOT NULL
        GROUP BY DATE_FORMAT(event_time, 'yyyy-MM-dd'), user_id""")
    # BR8: running totals per (UTC day, movie).
    statements.add_insert_sql(f"""
        INSERT INTO movie_engagement_results
        SELECT DATE_FORMAT(event_time, 'yyyy-MM-dd'), movie_id, MAX(title), MAX(genre),
               COUNT(*), CAST(COALESCE(SUM(watch_duration), 0) AS BIGINT),
               COALESCE(SUM({COMPLETION}), CAST(0.0 AS DOUBLE)), MAX(sent_at_ms)
        FROM watch_events
        WHERE event_type = 'watch' AND movie_id IS NOT NULL
        GROUP BY DATE_FORMAT(event_time, 'yyyy-MM-dd'), movie_id""")
    statements.execute().wait()


if __name__ == "__main__":
    main()
