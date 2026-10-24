from __future__ import annotations

import argparse
import csv
import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


def load_movies(path: Path) -> dict[str, dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        return {row["movie_id"]: row for row in csv.DictReader(file)}


def send_events(raw_dir: Path, limit: int, rate: float) -> None:
    movies = load_movies(raw_dir / "movies.csv")
    watches_path = raw_dir / "watch_history.csv"
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_WATCH_TOPIC", "watch-events")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=10,
    )
    sent = 0
    started = time.monotonic()
    try:
        while True:
            with watches_path.open(encoding="utf-8-sig", newline="") as file:
                for watch in csv.DictReader(file):
                    movie = movies.get(watch["movie_id"])
                    if movie is None:
                        continue
                    target_time = started + sent / rate
                    delay = target_time - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    event_timestamp_ms = int(time.time() * 1000)
                    producer.send(
                        topic,
                        key=watch["movie_id"].encode("utf-8"),
                        value={
                            "watch_id": watch["watch_id"],
                            "user_id": watch["user_id"],
                            "movie_id": watch["movie_id"],
                            "title": movie["title"],
                            "genre": movie["genre"],
                            "source_timestamp": watch["watch_timestamp"],
                        },
                        timestamp_ms=event_timestamp_ms,
                    )
                    sent += 1
                    if sent % 1000 == 0:
                        producer.flush()
                        print(f"Published {sent:,} watch events", flush=True)
                    if limit > 0 and sent % limit == 0:
                        print(f"Completed {limit:,} events; looping demo source", flush=True)
                        break
            if limit <= 0:
                break
    except KafkaError as error:
        raise RuntimeError(f"Kafka publish failed: {error}") from error
    finally:
        producer.flush()
        producer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish watch history continuously to Kafka.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    parser.add_argument("--limit", type=int, default=100_000, help="Events per replay cycle; 0 sends once")
    parser.add_argument("--rate", type=float, default=500, help="Target events per second")
    args = parser.parse_args()
    if args.limit < 0 or args.rate <= 0:
        parser.error("--limit must be >= 0 and --rate must be > 0")
    send_events(args.raw_dir, args.limit, args.rate)


if __name__ == "__main__":
    main()
