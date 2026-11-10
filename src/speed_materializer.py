"""Materialize the Flink speed-layer results from Kafka into Cassandra.

Flink SQL has no Cassandra sink, so the Flink job publishes to Kafka and this consumer upserts:

* trending-results       (append, one row per window and movie) -> live_trending, top 10 per window
* live-user-engagement   (upsert-kafka, key = day + user_id)     -> live_user_engagement
* live-movie-engagement  (upsert-kafka, key = day + movie_id)    -> live_movie_engagement

Writes are upserts by primary key, so re-reading a topic after a restart is idempotent.
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from typing import Any

from cassandra.cluster import Session
from kafka import KafkaConsumer

from src.cassandra_store import wait_for_cassandra

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TREND_TOPIC = os.getenv("KAFKA_TREND_TOPIC", "trending-results")
USER_TOPIC = os.getenv("KAFKA_USER_ENGAGEMENT_TOPIC", "live-user-engagement")
MOVIE_TOPIC = os.getenv("KAFKA_MOVIE_ENGAGEMENT_TOPIC", "live-movie-engagement")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "movie_analytics")
TOP_N = 10


def from_ms(value: Any) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)


def rank_movies(movies: dict[str, dict]) -> list[dict]:
    ranked = sorted(
        movies.values(),
        key=lambda row: (-row["live_views"], -row["unique_viewers"], row["title"]),
    )[:TOP_N]
    return [{**movie, "rank": rank} for rank, movie in enumerate(ranked, start=1)]


class Materializer:
    def __init__(self, session: Session) -> None:
        self.session = session
        now = "toTimestamp(now())"
        self.trend = session.prepare(
            f"INSERT INTO {KEYSPACE}.live_trending (day, window_start, rank, movie_id, title, genre, "
            f"live_views, unique_viewers, last_sent_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, {now})"
        )
        self.user = session.prepare(
            f"INSERT INTO {KEYSPACE}.live_user_engagement (day, user_id, subscription_plan, views, "
            f"watch_minutes, completion_sum, last_sent_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, {now})"
        )
        self.movie = session.prepare(
            f"INSERT INTO {KEYSPACE}.live_movie_engagement (day, movie_id, title, genre, views, "
            f"watch_minutes, completion_sum, last_sent_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, {now})"
        )
        # Trending: rows of the newest window seen so far (the table keeps every window).
        self.active_window: int | None = None
        self.window_movies: dict[str, dict] = {}

    def write_window(self) -> None:
        if self.active_window is None or not self.window_movies:
            return
        window_start = from_ms(self.active_window)
        futures = [
            self.session.execute_async(self.trend, (
                window_start.date(), window_start, row["rank"], row["movie_id"], row["title"], row["genre"],
                row["live_views"], row["unique_viewers"], from_ms(row["last_sent_at_ms"]),
            ))
            for row in rank_movies(self.window_movies)
        ]
        for future in futures:
            future.result()

    def add_trend(self, result: dict) -> bool:
        """Collect one (window, movie) row; returns True when the active window changed."""
        window_start = int(result["window_start_ms"])
        if self.active_window is not None and window_start < self.active_window:
            return False  # a late result for an older window: Flink emits each window once
        if window_start != self.active_window:
            self.write_window()
            self.active_window, self.window_movies = window_start, {}
        movie_id = str(result["movie_id"])
        self.window_movies[movie_id] = {
            "movie_id": movie_id, "title": result["title"], "genre": result["genre"],
            "live_views": int(result["live_views"]), "unique_viewers": int(result["unique_viewers"]),
            "last_sent_at_ms": result.get("last_sent_at_ms"),
        }
        return True

    def upsert_engagement(self, topic: str, rows: dict[tuple, dict]) -> None:
        futures = []
        for row in rows.values():
            day = date.fromisoformat(row["day"])
            common = (int(row["views"]), int(row["watch_minutes"]), float(row["completion_sum"]),
                      from_ms(row.get("last_sent_at_ms")))
            if topic == USER_TOPIC:
                params = (day, row["user_id"], row.get("subscription_plan"), *common)
                futures.append(self.session.execute_async(self.user, params))
            else:
                params = (day, row["movie_id"], row.get("title"), row.get("genre"), *common)
                futures.append(self.session.execute_async(self.movie, params))
        for future in futures:
            future.result()


def decode(value: bytes | None) -> dict | None:
    return None if value is None else json.loads(value.decode("utf-8"))


def consume() -> None:
    materializer = Materializer(wait_for_cassandra())
    consumer = KafkaConsumer(
        TREND_TOPIC, USER_TOPIC, MOVIE_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=os.getenv("MATERIALIZER_GROUP", "movie-speed-materializer"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=decode,
    )
    try:
        while True:
            # Poll in batches: the last version of each key wins, one Cassandra write per key and poll.
            polled = consumer.poll(timeout_ms=1000, max_records=5000)
            trend_touched = False
            latest: dict[str, dict[tuple, dict]] = {USER_TOPIC: {}, MOVIE_TOPIC: {}}
            for partition, messages in polled.items():
                for message in messages:
                    if message.value is None:  # upsert-kafka tombstone: totals never shrink, ignore
                        continue
                    if partition.topic == TREND_TOPIC:
                        trend_touched |= materializer.add_trend(message.value)
                    else:
                        key_column = "user_id" if partition.topic == USER_TOPIC else "movie_id"
                        latest[partition.topic][(message.value["day"], message.value[key_column])] = message.value
            if trend_touched:
                materializer.write_window()
            for topic, rows in latest.items():
                if rows:
                    materializer.upsert_engagement(topic, rows)
    finally:
        consumer.close()


if __name__ == "__main__":
    consume()
