from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from cassandra.cluster import Session
from kafka import KafkaConsumer

from src.cassandra_store import wait_for_cassandra

TOPIC = os.getenv("KAFKA_TREND_TOPIC", "trending-results")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TABLE = os.getenv("TRENDING_TABLE", "live_trending")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "movie_analytics")
TOP_N = 10


def rank_movies(movies: dict[str, dict]) -> list[dict]:
    ranked = sorted(
        movies.values(),
        key=lambda row: (-row["live_views"], -row["unique_viewers"], row["title"]),
    )[:TOP_N]
    return [{**movie, "rank": rank} for rank, movie in enumerate(ranked, start=1)]


def write_snapshot(session: Session, window_start_ms: int, movies: dict[str, dict]) -> None:
    """Upsert the top-N rows of one window; ranks 1..N are overwritten in place so reruns are idempotent."""
    window_start = datetime.fromtimestamp(window_start_ms / 1000, tz=timezone.utc)
    statement = session.prepare(
        f"INSERT INTO {KEYSPACE}.{TABLE} "
        "(day, window_start, rank, movie_id, title, genre, live_views, unique_viewers) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    for row in rank_movies(movies):
        session.execute(
            statement,
            (window_start.date(), window_start, row["rank"], row["movie_id"], row["title"],
             row["genre"], row["live_views"], row["unique_viewers"]),
        )


def consume_results() -> None:
    session = wait_for_cassandra()
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="movie-trending-dashboard-materializer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    active_window: int | None = None
    current_movies: dict[str, dict] = {}
    try:
        while True:
            # Poll in batches: one Cassandra snapshot per poll instead of one per Kafka message.
            polled = consumer.poll(timeout_ms=1000, max_records=2000)
            touched = False
            for messages in polled.values():
                for message in messages:
                    result = message.value
                    window_start = int(result["window_start_ms"])
                    if active_window is None or window_start > active_window:
                        if touched and current_movies:
                            write_snapshot(session, active_window, current_movies)
                        active_window = window_start
                        current_movies = {}
                    if window_start < active_window:
                        continue
                    movie_id = str(result["movie_id"])
                    current_movies[movie_id] = {
                        "movie_id": movie_id,
                        "title": result["title"],
                        "genre": result["genre"],
                        "live_views": int(result["live_views"]),
                        "unique_viewers": int(result["unique_viewers"]),
                    }
                    touched = True
            if touched and active_window is not None:
                write_snapshot(session, active_window, current_movies)
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_results()
