"""Data-quality rules shared by the Pandas and Spark pipelines, with per-rule rejected-row counts.

A row is rejected for the *first* rule it fails, in this order, so the counts add up exactly:

1. duplicate_id      - the id was already seen earlier in the file (the first occurrence is kept)
2. invalid_timestamp - the timestamp text is present but cannot be parsed (flagged in `_bad_ts`)
3. null_value        - a required column is empty
4. invalid_value     - watch_duration <= 0, movie duration <= 0, or rating outside 1..5
5. future_timestamp  - timestamp later than the run time (`now`, naive UTC)
6. orphan_user       - user_id is not in the (cleaned) users table (it would be lost in the join)
7. orphan_movie      - movie_id is not in the (cleaned) movies table (idem)
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

REASONS = (
    "duplicate_id", "invalid_timestamp", "null_value", "invalid_value", "future_timestamp",
    "orphan_user", "orphan_movie",
)
TABLES = ("users", "movies", "ratings", "watch_history")
ID_COLUMN = {"users": "user_id", "movies": "movie_id", "ratings": "rating_id", "watch_history": "watch_id"}
TIME_COLUMN = {"ratings": "timestamp", "watch_history": "watch_timestamp"}
# Dimension tables only need the columns the analytics use; fact tables must be complete.
REQUIRED = {
    "users": ["user_id", "subscription_plan", "region"],
    "movies": ["movie_id", "title", "genre", "release_year", "duration"],
    "ratings": None,
    "watch_history": None,
}
Report = dict[str, dict[str, int]]


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def _empty_counts(rows: int) -> dict[str, int]:
    return {"input": rows, **dict.fromkeys(REASONS, 0), "output": rows}


# ---------------------------------------------------------------- Pandas


def _tag_pandas(frame: pd.DataFrame, table: str, rules: dict[str, pd.Series]) -> pd.Series:
    """First failing rule per row (NaN = accepted)."""
    reason = pd.Series(pd.NA, index=frame.index, dtype="object")
    for name in REASONS:
        if name in rules:
            reason = reason.where(reason.notna() | ~rules[name], name)
    return reason


def _clean_table_pandas(
    frame: pd.DataFrame, table: str, now: datetime, users: pd.DataFrame | None, movies: pd.DataFrame | None
) -> tuple[pd.DataFrame, dict[str, int]]:
    required = REQUIRED[table] or [c for c in frame.columns if not c.startswith("_")]
    rules: dict[str, pd.Series] = {
        "duplicate_id": frame.duplicated(ID_COLUMN[table], keep="first"),
        "null_value": frame[required].isna().any(axis=1),
    }
    if "_bad_ts" in frame:
        rules["invalid_timestamp"] = frame["_bad_ts"]
    if table == "movies":
        rules["invalid_value"] = frame["duration"] <= 0
    elif table == "ratings":
        rules["invalid_value"] = ~frame["rating"].between(1, 5)
    elif table == "watch_history":
        rules["invalid_value"] = frame["watch_duration"] <= 0
    if table in TIME_COLUMN:
        rules["future_timestamp"] = frame[TIME_COLUMN[table]] > pd.Timestamp(now)
    if users is not None and "user_id" in frame and table in TIME_COLUMN:
        rules["orphan_user"] = ~frame["user_id"].isin(users["user_id"])
    if movies is not None and table in TIME_COLUMN:
        rules["orphan_movie"] = ~frame["movie_id"].isin(movies["movie_id"])
    reason = _tag_pandas(frame, table, rules)
    counts = _empty_counts(len(frame))
    for name, count in reason.value_counts().items():
        counts[name] = int(count)
    kept = frame[reason.isna()].drop(columns=["_bad_ts"], errors="ignore")
    counts["output"] = len(kept)
    for column in ("rating", "watch_duration", "duration", "release_year"):
        if column in kept:
            kept = kept.astype({column: "int64"})
    return kept, counts


def clean_pandas(
    users: pd.DataFrame, movies: pd.DataFrame, ratings: pd.DataFrame, watches: pd.DataFrame,
    now: datetime | None = None,
) -> tuple[tuple[pd.DataFrame, ...], Report]:
    """Apply the rules to parsed frames; returns the cleaned frames and the rejected-row report."""
    now = now or utc_now()
    clean_users, users_counts = _clean_table_pandas(users, "users", now, None, None)
    clean_movies, movies_counts = _clean_table_pandas(movies, "movies", now, None, None)
    clean_ratings, ratings_counts = _clean_table_pandas(ratings, "ratings", now, clean_users, clean_movies)
    clean_watches, watches_counts = _clean_table_pandas(watches, "watch_history", now, clean_users, clean_movies)
    report = {
        "users": users_counts, "movies": movies_counts,
        "ratings": ratings_counts, "watch_history": watches_counts,
    }
    return (clean_users, clean_movies, clean_ratings, clean_watches), report


# ---------------------------------------------------------------- Spark


def _tag_spark(frame: "DataFrame", rules: dict) -> "DataFrame":
    from pyspark.sql import functions as F

    reason = F.lit(None).cast("string")
    for name in reversed(REASONS):  # build the when-chain so the earliest rule wins
        if name in rules:
            reason = F.when(rules[name], F.lit(name)).otherwise(reason)
    return frame.withColumn("_reason", reason)


def _tag_table_spark(
    frame: "DataFrame", table: str, now: datetime, users: "DataFrame | None", movies: "DataFrame | None"
) -> "DataFrame":
    """Add a `_reason` column (null = accepted) holding the first rule each row fails."""
    from functools import reduce
    from operator import or_

    from pyspark.sql import Window, functions as F

    id_column = ID_COLUMN[table]
    required = REQUIRED[table] or [c for c in frame.columns if not c.startswith("_")]
    order = Window.partitionBy(id_column).orderBy(F.monotonically_increasing_id())
    frame = frame.withColumn("_rn", F.row_number().over(order))
    # Orphan checks: flag columns come from left joins against the distinct valid ids.
    if users is not None and table in TIME_COLUMN:
        frame = frame.join(users.select("user_id").withColumn("_has_user", F.lit(True)), "user_id", "left")
    if movies is not None and table in TIME_COLUMN:
        frame = frame.join(movies.select("movie_id").withColumn("_has_movie", F.lit(True)), "movie_id", "left")
    rules = {
        "duplicate_id": F.col("_rn") > 1,
        "null_value": reduce(or_, [F.col(c).isNull() for c in required]),
    }
    if "_bad_ts" in frame.columns:
        rules["invalid_timestamp"] = F.col("_bad_ts")
    if table == "movies":
        rules["invalid_value"] = F.col("duration") <= 0
    elif table == "ratings":
        rules["invalid_value"] = ~F.col("rating").between(1, 5)
    elif table == "watch_history":
        rules["invalid_value"] = F.col("watch_duration") <= 0
    if table in TIME_COLUMN:
        rules["future_timestamp"] = F.col(TIME_COLUMN[table]) > F.lit(now.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
        rules["orphan_user"] = F.col("_has_user").isNull()
        rules["orphan_movie"] = F.col("_has_movie").isNull()
    tagged = _tag_spark(frame, rules)
    helper = [c for c in ("_rn", "_has_user", "_has_movie") if c in tagged.columns]
    return tagged.drop(*helper)


def clean_spark(
    users: "DataFrame", movies: "DataFrame", ratings: "DataFrame", watches: "DataFrame",
    now: datetime | None = None,
) -> tuple[tuple["DataFrame", ...], "DataFrame"]:
    """Spark twin of `clean_pandas`. The report DataFrame (table, reason, rows) is lazy: it costs an
    extra pass only when it is collected."""
    from pyspark.sql import functions as F

    now = now or utc_now()
    columns = {
        name: [c for c in frame.columns if not c.startswith("_")]
        for name, frame in zip(TABLES, (users, movies, ratings, watches))
    }
    tagged: dict = {}
    clean: dict = {}
    for table, frame in zip(TABLES, (users, movies, ratings, watches)):
        # Fact tables are checked against the already-cleaned dimension tables.
        tagged[table] = _tag_table_spark(frame, table, now, clean.get("users"), clean.get("movies"))
        clean[table] = tagged[table].where(F.col("_reason").isNull()).select(*columns[table])
    report = None
    for table in TABLES:
        counts = tagged[table].groupBy(F.coalesce("_reason", F.lit("accepted")).alias("reason")).agg(
            F.count(F.lit(1)).alias("rows")
        ).select(F.lit(table).alias("table_name"), "reason", "rows")
        report = counts if report is None else report.unionByName(counts)
    return tuple(clean[table] for table in TABLES), report


def report_from_rows(rows: list[tuple[str, str, int]]) -> Report:
    """Turn collected (table, reason, rows) tuples into the same dict `clean_pandas` returns."""
    report: Report = {table: _empty_counts(0) for table in TABLES}
    for table, reason, count in rows:
        key = "output" if reason == "accepted" else reason
        report[table][key] = int(count)
    for counts in report.values():
        counts["input"] = counts["output"] + sum(counts[reason] for reason in REASONS)
    return report


# ---------------------------------------------------------------- CLI


def format_report(report: Report) -> str:
    header = f"{'table':<14}{'input':>10}" + "".join(f"{reason:>18}" for reason in REASONS) + f"{'output':>10}"
    lines = [header]
    for table, counts in report.items():
        lines.append(
            f"{table:<14}{counts['input']:>10,}"
            + "".join(f"{counts[reason]:>18,}" for reason in REASONS)
            + f"{counts['output']:>10,}"
        )
    return "\n".join(lines)


def main() -> None:
    from src.batch_processing import read_raw

    parser = argparse.ArgumentParser(description="Report rows the cleaning rules would reject (Pandas).")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    _, report = clean_pandas(*read_raw(args.raw_dir))
    print(format_report(report))


if __name__ == "__main__":
    main()
