"""Compare the Spark batch output (Hive tables, Parquet) with the Flink batch output (CSV).

Both engines read the same raw files and batch boundary (see flink/flink_batch_sql_job.py). Rows are
aligned on each table's key; numbers must agree within `--tolerance`, everything else exactly.

    python -m src.compare_batch_engines [--warehouse-dir data/warehouse] [--tolerance 1e-9]

Exit code 0 when every table matches, 1 otherwise. `--report` writes the per-table summary as CSV.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import pandas as pd

KEYS = {
    "movie_performance": ["movie_id"],
    "low_engagement_movies": ["movie_id"],
    "genre_popularity": ["genre"],
    "subscription_views": ["subscription_plan"],
    "user_engagement": ["user_id"],
    "user_segments": ["segment"],
    "user_preferences": ["user_id"],
    "daily_views": ["day"],
    "hourly_views": ["hour"],
    "summary_metrics": [],
    "user_recommendations": ["user_id", "rank"],
    "rating_distribution": ["movie_id"],
    "genre_monthly": ["genre", "month"],
    "view_trends": ["scope", "key", "period", "period_start"],
    "quality_report": ["table_name", "reason"],
}


def normalize(value):
    """Comparable value: None for missing, lower-case text for booleans, float for numbers, else text."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, bool):
        return str(value).lower()
    text = str(value).strip()
    if text == "" or text.lower() in ("nan", "none", "null", "nat"):
        return None
    if text.lower() in ("true", "false"):
        return text.lower()
    try:
        return float(text)
    except ValueError:
        return text


def same(left, right, tolerance: float) -> bool:
    if isinstance(left, float) and isinstance(right, float):
        return abs(left - right) <= tolerance
    return left == right


def read_spark(warehouse: Path, table: str) -> pd.DataFrame:
    database = "curated" if table == "quality_report" else "analytics"
    return pd.read_parquet(warehouse / "spark-warehouse" / f"{database}.db" / table)


def read_flink(warehouse: Path, table: str, columns: list[str]) -> pd.DataFrame:
    directory = warehouse / "flink" / "analytics" / table
    files = [path for path in sorted(directory.rglob("*")) if path.is_file()
             and not path.name.startswith((".", "_")) and path.stat().st_size > 0]
    frames = [pd.read_csv(path, header=None, names=columns, dtype=str, keep_default_na=False) for path in files]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columns)


def compare_table(spark: pd.DataFrame, flink: pd.DataFrame, keys: list[str], tolerance: float) -> dict:
    columns = list(spark.columns)
    result = {"spark_rows": len(spark), "flink_rows": len(flink), "missing_in_flink": 0, "extra_in_flink": 0,
              "mismatched_rows": 0, "max_abs_diff": 0.0, "columns_match": list(flink.columns) == columns}
    if not result["columns_match"]:
        return result
    as_rows = lambda frame: {
        tuple(normalize(row[key]) for key in keys): tuple(normalize(row[column]) for column in columns)
        for row in frame.to_dict("records")
    }
    left, right = as_rows(spark), as_rows(flink)
    result["missing_in_flink"] = len(left.keys() - right.keys())
    result["extra_in_flink"] = len(right.keys() - left.keys())
    for key in left.keys() & right.keys():
        pairs = list(zip(left[key], right[key]))
        if not all(same(a, b, tolerance) for a, b in pairs):
            result["mismatched_rows"] += 1
        for a, b in pairs:
            if isinstance(a, float) and isinstance(b, float):
                result["max_abs_diff"] = max(result["max_abs_diff"], abs(a - b))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare Spark and Flink batch analytics tables.")
    parser.add_argument("--warehouse-dir", type=Path, default=Path("data/warehouse"))
    parser.add_argument("--tolerance", type=float, default=1e-9, help="allowed absolute difference for numbers")
    parser.add_argument("--report", type=Path, default=None, help="optional CSV file for the summary")
    args = parser.parse_args()

    schema = json.loads((args.warehouse_dir / "flink" / "analytics" / "_schema.json").read_text())
    rows = []
    for table, keys in KEYS.items():
        summary = compare_table(
            read_spark(args.warehouse_dir, table), read_flink(args.warehouse_dir, table, schema[table]), keys,
            args.tolerance,
        )
        summary["table"] = table
        summary["match"] = (
            summary["columns_match"] and summary["spark_rows"] == summary["flink_rows"]
            and not (summary["missing_in_flink"] or summary["extra_in_flink"] or summary["mismatched_rows"])
        )
        rows.append(summary)
    report = pd.DataFrame(rows)[[
        "table", "match", "spark_rows", "flink_rows", "missing_in_flink", "extra_in_flink", "mismatched_rows",
        "max_abs_diff", "columns_match",
    ]]
    print(report.to_string(index=False))
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        report.to_csv(args.report, index=False)
    sys.exit(0 if report["match"].all() else 1)


if __name__ == "__main__":
    main()
