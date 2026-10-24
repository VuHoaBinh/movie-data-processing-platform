from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src import batch_processing, data_quality, spark_batch_job

Tables = dict[str, pd.DataFrame]


def pandas_tables(raw_dir: Path, now: datetime | None = None) -> tuple[Tables, data_quality.Report]:
    curated, report = batch_processing.curate(*batch_processing.read_raw(raw_dir), now=now)
    return batch_processing.build_tables(*curated), report


def spark_tables(spark, raw_dir: Path, now: datetime | None = None) -> tuple[Tables, data_quality.Report]:
    frames, quality = spark_batch_job.curate(*spark_batch_job.read_raw(spark, raw_dir), now=now)
    tables = spark_batch_job.build_tables(*frames)
    report = data_quality.report_from_rows([tuple(row) for row in quality.collect()])
    return {name: to_pandas(frame) for name, frame in tables.items()}, report


def to_pandas(frame) -> pd.DataFrame:
    return pd.DataFrame([row.asDict() for row in frame.collect()], columns=frame.columns)


def _plain(value):
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    if isinstance(value, (date, pd.Timestamp)):
        return str(value)
    return value.item() if hasattr(value, "item") else value


def records(frame: pd.DataFrame, sort: bool = False) -> list[tuple]:
    """Comparable rows: NaN/NaT -> None, dates -> str, numpy numbers -> python numbers."""
    rows = [tuple(_plain(value) for value in row) for row in frame.itertuples(index=False, name=None)]
    return sorted(rows, key=repr) if sort else rows
