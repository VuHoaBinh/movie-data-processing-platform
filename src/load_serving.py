"""Load the Flink batch branch into its Cassandra serving keyspace (movie_analytics_flink).

Flink SQL has no Cassandra sink, so (like the speed layer's materializer) this loader reads the Flink
analytics zone (`warehouse/flink-warehouse/flink_analytics/<table>`, Parquet), reshapes it with the same
spec as the Spark branch (src/serving_spec.py), upserts it under a new batch version and publishes that
version in the keyspace's `serving_state`.

    python -m src.load_serving [--lake-dir data/warehouse/flink-warehouse] [--keyspace movie_analytics_flink]
"""
from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src import settings
from src.serving_spec import SERVING_TABLES, serving_columns

ANALYTICS_TABLES = sorted({spec["source"] for spec in SERVING_TABLES.values()})


def read_analytics(lake_dir: Path) -> dict[str, pd.DataFrame]:
    """The Flink analytics tables (Parquet directories; hidden / in-progress files are skipped by pyarrow)."""
    tables = {}
    for name in ANALYTICS_TABLES:
        directory = lake_dir / "flink_analytics" / name
        if not directory.is_dir():
            raise FileNotFoundError(f"{directory} not found: run the Flink batch job first")
        tables[name] = pd.read_parquet(directory)
    return tables


def serving_frames(tables: dict[str, pd.DataFrame], version: str) -> dict[str, pd.DataFrame]:
    """Pandas twin of spark_batch_job.serving_frames (same spec, same columns)."""
    frames = {}
    for table, spec in SERVING_TABLES.items():
        frame = tables[spec["source"]]
        if spec.get("rated"):
            frame = frame[frame["weighted_rating"].notna()]
        frame = frame.assign(bucket=version)
        frames[table] = frame[serving_columns(table, [c for c in frame.columns if c != "bucket"])]
    return frames


def plain(value: Any) -> Any:
    """Python values the Cassandra driver accepts (numpy scalars, NaN/NaT -> None, Timestamp -> date)."""
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):  # numpy scalar
        value = value.item()
        if isinstance(value, float) and math.isnan(value):
            return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def write_frames(session, frames: dict[str, pd.DataFrame], concurrency: int = 64) -> dict[str, int]:
    from cassandra.concurrent import execute_concurrent_with_args

    counts = {}
    for table, frame in frames.items():
        columns = list(frame.columns)
        statement = session.prepare(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})"
        )
        rows = [tuple(plain(value) for value in row) for row in frame.itertuples(index=False, name=None)]
        results = execute_concurrent_with_args(session, statement, rows, concurrency=concurrency,
                                               raise_on_first_error=True)
        counts[table] = sum(1 for success, _ in results if success)
        print(f"Cassandra {session.keyspace}.{table}: {counts[table]:,} rows", flush=True)
    return counts


def load(lake_dir: Path, boundary_path: Path, keyspace: str) -> None:
    from src.cassandra_store import connect, publish_batch_version

    started = time.perf_counter()
    boundary = json.loads(boundary_path.read_text()) if boundary_path.is_file() else {}
    as_date = lambda value: date.fromisoformat(value) if value else None
    tables = read_analytics(lake_dir)
    version = datetime.now(timezone.utc).strftime("v%Y%m%dT%H%M%S")
    session = connect(keyspace=keyspace, timeout=30)
    try:
        write_frames(session, serving_frames(tables, version))
        publish_batch_version(
            session, version, sorted(tables["genre_popularity"]["genre"]),
            as_date(boundary.get("replay_day")), as_date(boundary.get("batch_cutoff")),
        )
    finally:
        session.cluster.shutdown()
    print(f"TIMING flink_cassandra_load {time.perf_counter() - started:.1f}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load the Flink batch output into Cassandra.")
    parser.add_argument("--lake-dir", type=Path, default=Path(os.getenv("FLINK_LAKE_DIR", "data/warehouse/flink-warehouse")))
    parser.add_argument("--boundary", type=Path, default=Path("data/warehouse/batch_boundary.json"))
    parser.add_argument("--keyspace", default=settings.FLINK_KEYSPACE)
    args = parser.parse_args()
    load(args.lake_dir, args.boundary, args.keyspace)


if __name__ == "__main__":
    main()
