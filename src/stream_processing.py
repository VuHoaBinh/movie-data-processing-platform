"""Simulate event-time stream processing with five-minute windows.

In production, Flink would consume these events from Kafka. For the classroom
demo, this script replays watch_history.csv in timestamp order and produces the
same kind of windowed trending result without extra infrastructure.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def run(raw_dir: Path, output_dir: Path, window: str, recent_events: int) -> None:
    watches = pd.read_csv(raw_dir / "watch_history.csv", parse_dates=["watch_timestamp"])
    movies = pd.read_csv(raw_dir / "movies.csv", usecols=["movie_id", "title", "genre"])

    events = watches.sort_values("watch_timestamp").tail(recent_events).copy()
    events["window_start"] = events["watch_timestamp"].dt.floor(window)
    trends = (
        events.groupby(["window_start", "movie_id"], as_index=False)
        .agg(live_views=("watch_id", "count"), unique_viewers=("user_id", "nunique"))
        .merge(movies, on="movie_id", how="left")
    )
    trends["rank"] = trends.groupby("window_start")["live_views"].rank(
        method="first", ascending=False
    )
    trends = trends[trends["rank"] <= 10].sort_values(
        ["window_start", "rank"], ascending=[False, True]
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / "live_trending.csv"
    trends.to_csv(destination, index=False)
    print(f"Replayed {len(events):,} events; wrote {len(trends):,} window results to {destination}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay watch events into time windows.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/processed"))
    parser.add_argument("--window", default="5min")
    parser.add_argument("--recent-events", type=int, default=100_000)
    args = parser.parse_args()
    run(args.raw_dir, args.output_dir, args.window, args.recent_events)


if __name__ == "__main__":
    main()
