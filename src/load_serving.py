"""Load the Flink batch branch into its Cassandra serving keyspace (movie_analytics_flink).

Flink SQL has no Cassandra sink, so (like the speed layer's materializer) this loader reads the Flink batch
output (`data/warehouse/flink/analytics/<table>/`: headerless CSV parts, column names in `_schema.json`,
written by flink/flink_batch_sql_job.py), restores the column types, reshapes it with the same spec as the
Spark branch (src/serving_spec.py), upserts it under a new batch version and publishes that version in the
keyspace's `serving_state`.

    python -m src.load_serving [--analytics-dir data/warehouse/flink/analytics] [--keyspace movie_analytics_flink]
"""
from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src import settings
from src.serving_spec import SERVING_TABLES, serving_columns

ANALYTICS_TABLES = sorted({spec["source"] for spec in SERVING_TABLES.values()})

# CSV loses the column types. Every analytics column name has one type in all tables
# (see TABLES in flink/flink_batch_sql_job.py); any column not listed here is DOUBLE.
STRING_COLUMNS = {
    "movie_id", "title", "genre", "content_type", "language", "maturity_rating", "subscription_plan", "user_id",
    "region", "segment", "favorite_genres", "scope", "key", "period",
}
INTEGER_COLUMNS = {
    "release_year", "duration", "views", "unique_viewers", "rating_count", "watch_minutes", "active_days", "users",
    "hour", "movies", "ratings", "rank", "r1", "r2", "r3", "r4", "r5", "prev_views",
}
DATE_COLUMNS = {"day", "month", "period_start"}
BOOLEAN_COLUMNS = {"is_low_engagement"}


def column_kind(name: str) -> str:
    if name in STRING_COLUMNS:
        return "STRING"
    if name in INTEGER_COLUMNS:
        return "INTEGER"
    if name in DATE_COLUMNS:
        return "DATE"
    if name in BOOLEAN_COLUMNS:
        return "BOOLEAN"
    return "DOUBLE"


def _boolean(text: str) -> bool:
    lowered = text.strip().lower()
    if lowered not in ("true", "false"):
        raise ValueError(f"not a boolean: {text!r}")
    return lowered == "true"


PARSERS: dict[str, Callable[[str], Any]] = {
    "STRING": str,
    "INTEGER": lambda text: int(float(text)) if "." in text or "e" in text.lower() else int(text),
    "DOUBLE": float,
    "DATE": lambda text: date.fromisoformat(text[:10]),
    "BOOLEAN": _boolean,
}


def typed(frame: pd.DataFrame) -> pd.DataFrame:
    """Text columns -> Python values of the column's type; an empty field (Flink's NULL) -> None."""
    columns = {}
    for name in frame.columns:
        parse = PARSERS[column_kind(name)]
        columns[name] = pd.Series([None if text == "" else parse(text) for text in frame[name]],
                                  index=frame.index, dtype=object)
    return pd.DataFrame(columns, columns=list(frame.columns))


def read_table(directory: Path, columns: list[str]) -> pd.DataFrame:
    """All CSV parts of one Flink output table (hidden / in-progress files are skipped)."""
    files = [path for path in sorted(directory.rglob("*")) if path.is_file()
             and not path.name.startswith((".", "_")) and path.stat().st_size > 0]
    frames = [pd.read_csv(path, header=None, names=columns, dtype=str, keep_default_na=False) for path in files]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns, dtype=str)


def read_analytics(analytics_dir: Path) -> dict[str, pd.DataFrame]:
    """The Flink analytics tables used by the serving layer, with their column types restored."""
    schema_path = analytics_dir / "_schema.json"
    if not schema_path.is_file():
        raise FileNotFoundError(f"{schema_path} not found: run the Flink batch job first")
    schema = json.loads(schema_path.read_text())
    tables = {}
    for name in ANALYTICS_TABLES:
        directory = analytics_dir / name
        if name not in schema or not directory.is_dir():
            raise FileNotFoundError(f"{directory} not found: run the Flink batch job first")
        tables[name] = typed(read_table(directory, schema[name]))
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
    """Python values the Cassandra driver accepts (numpy scalars, NaN/NaT/NA -> None, Timestamp -> date)."""
    if value is None or value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if hasattr(value, "item"):  # numpy scalar
        value = value.item()
        if isinstance(value, float) and math.isnan(value):
            return None
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


def load(analytics_dir: Path, boundary_path: Path, keyspace: str) -> None:
    from src.cassandra_store import connect, publish_batch_version

    started = time.perf_counter()
    boundary = json.loads(boundary_path.read_text()) if boundary_path.is_file() else {}
    as_date = lambda value: date.fromisoformat(value) if value else None
    tables = read_analytics(analytics_dir)
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
    parser.add_argument("--analytics-dir", type=Path,
                        default=Path(os.getenv("FLINK_ANALYTICS_DIR", "data/warehouse/flink/analytics")))
    parser.add_argument("--boundary", type=Path, default=Path("data/warehouse/batch_boundary.json"))
    parser.add_argument("--keyspace", default=settings.FLINK_KEYSPACE)
    args = parser.parse_args()
    load(args.analytics_dir, args.boundary, args.keyspace)


if __name__ == "__main__":
    main()
