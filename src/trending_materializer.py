from __future__ import annotations

import csv
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "data/processed"))
TOPIC = os.getenv("KAFKA_TREND_TOPIC", "trending-results")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FIELDS = ["window_start", "rank", "movie_id", "title", "genre", "live_views", "unique_viewers"]


def write_snapshot(window_start_ms: int, movies: dict[str, dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ranked = sorted(
        movies.values(),
        key=lambda row: (-row["live_views"], -row["unique_viewers"], row["title"]),
    )[:10]
    window_start = datetime.fromtimestamp(window_start_ms / 1000, tz=timezone.utc).isoformat()
    destination = OUTPUT_DIR / "live_trending.csv"
    with tempfile.NamedTemporaryFile(
        "w", newline="", encoding="utf-8", dir=OUTPUT_DIR, delete=False, suffix=".tmp"
    ) as temp_file:
        writer = csv.DictWriter(temp_file, fieldnames=FIELDS)
        writer.writeheader()
        for rank, movie in enumerate(ranked, start=1):
            writer.writerow({"window_start": window_start, "rank": rank, **movie})
        temp_path = Path(temp_file.name)
    os.replace(temp_path, destination)


def consume_results() -> None:
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
        for message in consumer:
            result = message.value
            window_start = int(result["window_start_ms"])
            if active_window is None or window_start > active_window:
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
            write_snapshot(active_window, current_movies)
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_results()
