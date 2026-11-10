"""The Spark-vs-Flink comparison accepts identical tables and reports real differences."""
from __future__ import annotations

import json

import pandas as pd

from src import compare_batch_engines as compare
from tests.helpers import pandas_tables


def write_warehouse(tmp_path, tables: dict, report: dict) -> None:
    """Spark side as Parquet (Hive layout), Flink side as headerless CSV parts plus _schema.json."""
    quality = pd.DataFrame(
        [(table, reason, count) for table, counts in report.items()
         for reason, count in counts.items() if reason not in ("input",) and count],
        columns=["table_name", "reason", "rows"],
    ).replace({"reason": {"output": "accepted"}})
    frames = {**tables, "quality_report": quality}
    schema = {}
    for name, frame in frames.items():
        database = "curated" if name == "quality_report" else "analytics"
        spark_dir = tmp_path / "spark-warehouse" / f"{database}.db" / name
        spark_dir.mkdir(parents=True)
        frame.astype({c: str for c in frame.select_dtypes("category").columns}).to_parquet(spark_dir / "part-0.parquet")
        flink_dir = tmp_path / "flink" / "analytics" / name
        flink_dir.mkdir(parents=True)
        frame.to_csv(flink_dir / "part-0-0.csv", header=False, index=False)
        schema[name] = list(frame.columns)
    (tmp_path / "flink" / "analytics" / "_schema.json").write_text(json.dumps(schema))


def compare_all(warehouse) -> dict:
    schema = json.loads((warehouse / "flink" / "analytics" / "_schema.json").read_text())
    return {
        table: compare.compare_table(
            compare.read_spark(warehouse, table), compare.read_flink(warehouse, table, schema[table]), keys, 1e-9
        )
        for table, keys in compare.KEYS.items()
    }


def test_identical_outputs_match(tmp_path, fixture_dir):
    tables, report = pandas_tables(fixture_dir)
    write_warehouse(tmp_path, tables, report)
    for table, result in compare_all(tmp_path).items():
        assert result["columns_match"], table
        assert result["spark_rows"] == result["flink_rows"], table
        assert not (result["missing_in_flink"] or result["extra_in_flink"] or result["mismatched_rows"]), table


def test_a_changed_value_and_a_missing_row_are_reported(tmp_path, fixture_dir):
    tables, report = pandas_tables(fixture_dir)
    write_warehouse(tmp_path, tables, report)
    flink = tmp_path / "flink" / "analytics"
    performance = tables["movie_performance"].copy()
    performance.loc[performance["movie_id"] == "M1", "weighted_rating"] = 3.94
    performance.to_csv(flink / "movie_performance" / "part-0-0.csv", header=False, index=False)
    tables["daily_views"].iloc[1:].to_csv(flink / "daily_views" / "part-0-0.csv", header=False, index=False)
    results = compare_all(tmp_path)
    assert results["movie_performance"]["mismatched_rows"] == 1
    assert abs(results["movie_performance"]["max_abs_diff"] - 0.01) < 1e-9
    assert results["daily_views"]["missing_in_flink"] == 1
