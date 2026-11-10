"""Replay the last day of watch_history into Kafka `watch-events`, in watch_timestamp order.

* Event time is the original `watch_timestamp` (field `event_time_ms`); the replay runs `--speed`
  times faster than real time on a simulated clock (see `src.replay_clock`).
* A heartbeat (`event_type = 'heartbeat'`, no ids) is sent every `--heartbeat-seconds` with the current
  simulated time, so the watermark keeps moving and the last windows close even when no view arrives.
  Engines count only `event_type = 'watch'`.
* Each event carries `sent_at_ms` (wall-clock send time) for end-to-end latency measurements.
* With Cassandra available the clock and the number of events sent are stored in `replay_clock`;
  a restarted producer resumes after the last flushed event instead of replaying the day again.
  `--reset` starts a new replay.

The dashboard's "record a view" form sends the same event shape, stamped with the simulated time.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.replay_clock import ReplayClock, load_clock, save_clock

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def to_int(value: Any) -> int | None:
    """Keep malformed input from stopping the replay; consumers ignore nulls."""
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def parse_timestamp(value: str) -> datetime | None:
    """Raw timestamps are naive UTC or ISO with an offset (same rule as the batch curation)."""
    try:
        parsed = datetime.fromisoformat(value.strip())
    except (AttributeError, ValueError):
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def watch_event(
    watch: dict[str, Any], movie: dict[str, Any], user: dict[str, Any] | None, event_time: datetime
) -> dict[str, Any]:
    """The Kafka message for one view (shared with the dashboard form)."""
    return {
        "event_type": "watch",
        "watch_id": watch["watch_id"],
        "user_id": watch["user_id"],
        "movie_id": watch["movie_id"],
        "title": movie["title"],
        "genre": movie["genre"],
        "subscription_plan": (user or {}).get("subscription_plan"),
        "device": watch.get("device"),
        "watch_duration": to_int(watch.get("watch_duration")),
        "movie_duration": to_int(movie.get("duration")),
        "watch_timestamp": event_time.strftime(TIMESTAMP_FORMAT),
        "event_time_ms": int(event_time.timestamp() * 1000),
        "sent_at_ms": int(time.time() * 1000),
    }


def heartbeat_event(event_time_ms: int) -> dict[str, Any]:
    return {"event_type": "heartbeat", "event_time_ms": event_time_ms, "sent_at_ms": int(time.time() * 1000)}


def load_csv(path: Path, key: str) -> dict[str, dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        return {row[key]: row for row in csv.DictReader(file)}


def last_day(watches_path: Path) -> date:
    now = datetime.now(timezone.utc)
    with watches_path.open(encoding="utf-8-sig", newline="") as file:
        days = (parse_timestamp(row["watch_timestamp"]) for row in csv.DictReader(file))
        return max(stamp for stamp in days if stamp is not None and stamp <= now).date()


def day_events(watches_path: Path, replay_day: date, movies: dict) -> list[tuple[datetime, dict[str, str]]]:
    """Views of `replay_day` whose movie exists, sorted by (watch_timestamp, watch_id)."""
    events = []
    with watches_path.open(encoding="utf-8-sig", newline="") as file:
        for row in csv.DictReader(file):
            stamp = parse_timestamp(row["watch_timestamp"])
            if stamp is not None and stamp.date() == replay_day and row["movie_id"] in movies:
                events.append((stamp, row))
    events.sort(key=lambda item: (item[0], item[1]["watch_id"]))
    return events


def connect_clock_store():
    """Cassandra session for the replay clock, or None when CASSANDRA_HOST is not set."""
    if not os.getenv("CASSANDRA_HOST"):
        return None
    from src.cassandra_store import wait_for_cassandra

    return wait_for_cassandra()


def replay(raw_dir: Path, replay_day: date | None, speed: float, heartbeat_seconds: float, reset: bool) -> None:
    movies = load_csv(raw_dir / "movies.csv", "movie_id")
    users = load_csv(raw_dir / "users.csv", "user_id")
    watches_path = raw_dir / "watch_history.csv"
    replay_day = replay_day or last_day(watches_path)
    events = day_events(watches_path, replay_day, movies)

    session = connect_clock_store()
    clock = None if session is None or reset else load_clock(session)
    if clock is None or clock.replay_day != replay_day or clock.total != len(events):
        clock = ReplayClock.start(replay_day, speed, len(events))
        print(f"Starting replay of {replay_day}: {len(events):,} events at x{speed:g}", flush=True)
    else:
        print(f"Resuming replay of {replay_day} after {clock.sent:,}/{clock.total:,} events", flush=True)
    if session is not None:
        save_clock(session, clock)

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=5,
    )
    topic = os.getenv("KAFKA_WATCH_TOPIC", "watch-events")
    next_heartbeat = 0.0
    try:
        while True:
            sim_ms = clock.sim_ms()
            while clock.sent < clock.total and int(events[clock.sent][0].timestamp() * 1000) <= sim_ms:
                stamp, watch = events[clock.sent]
                producer.send(
                    topic, key=watch["movie_id"].encode("utf-8"),
                    value=watch_event(watch, movies[watch["movie_id"]], users.get(watch["user_id"]), stamp),
                )
                clock.sent += 1
            if time.monotonic() >= next_heartbeat:
                # Heartbeat after every event up to sim_ms is queued, so event time never goes backwards.
                producer.send(topic, value=heartbeat_event(sim_ms))
                producer.flush()  # progress is saved only for acknowledged events
                if session is not None:
                    save_clock(session, clock)
                print(f"sim={clock.sim_now().strftime(TIMESTAMP_FORMAT)} sent={clock.sent:,}/{clock.total:,}", flush=True)
                next_heartbeat = time.monotonic() + heartbeat_seconds
            time.sleep(0.05)
    except KafkaError as error:
        raise RuntimeError(f"Kafka publish failed: {error}") from error
    finally:
        producer.flush()
        producer.close()
        if session is not None:
            save_clock(session, clock)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay the last day of watch_history into Kafka.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    parser.add_argument("--replay-day", default=os.getenv("REPLAY_DAY", "auto"), help="'auto' or YYYY-MM-DD")
    parser.add_argument("--speed", type=float, default=float(os.getenv("REPLAY_SPEED", "60")),
                        help="simulated seconds per real second (60: one day in 24 minutes)")
    parser.add_argument("--heartbeat-seconds", type=float, default=1.0, help="real seconds between heartbeats")
    parser.add_argument("--reset", action="store_true", help="ignore a saved clock and start the replay again")
    args = parser.parse_args()
    if args.speed <= 0 or args.heartbeat_seconds <= 0:
        parser.error("--speed and --heartbeat-seconds must be > 0")
    replay_day = None if args.replay_day == "auto" else date.fromisoformat(args.replay_day)
    replay(args.raw_dir, replay_day, args.speed, args.heartbeat_seconds, args.reset)


if __name__ == "__main__":
    main()
