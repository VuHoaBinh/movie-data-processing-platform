"""Profile the raw CSV files: sizes, nulls, keys, ranges, categories, time pattern and skew.

Writes `data_profile.csv` (one row per metric: dataset, section, metric, value) and `data_profile.md`
(the same metrics as tables). Run: `python -m src.profile_data [--raw-dir data] [--output-dir benchmarks/results]`.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

from src.batch_processing import RAW_FILES, read_raw

Row = tuple[str, str, str, object]  # dataset, section, metric, value

TABLES = ("users", "movies", "ratings", "watch_history")
PRIMARY_KEY = {"users": "user_id", "movies": "movie_id", "ratings": "rating_id", "watch_history": "watch_id"}
FOREIGN_KEYS = (
    ("ratings", "user_id", "users"), ("ratings", "movie_id", "movies"),
    ("watch_history", "user_id", "users"), ("watch_history", "movie_id", "movies"),
)
CATEGORIES = {
    "users": ("gender", "region", "subscription_plan", "activity_profile"),
    "movies": ("genre", "content_type", "language", "maturity_rating"),
    "watch_history": ("device",),
}
NUMERIC = {
    "users": ("age",),
    "movies": ("release_year", "duration"),
    "ratings": ("rating",),
    "watch_history": ("watch_duration",),
}
TIME_COLUMN = {"ratings": "timestamp", "watch_history": "watch_timestamp"}
QUANTILES = (0.1, 0.5, 0.9, 0.99)
SEGMENT_LIMITS = (10, 35)  # BR9 thresholds: <= 10 Inactive, <= 35 Casual


def _round(value: float, digits: int = 4) -> float:
    return round(float(value), digits)


def gini(values: pd.Series | np.ndarray) -> float:
    """Gini coefficient (0 = equal, 1 = one item has everything)."""
    sorted_values = np.sort(np.asarray(values, dtype="float64"))
    total = sorted_values.sum()
    if len(sorted_values) == 0 or total == 0:
        return 0.0
    ranks = np.arange(1, len(sorted_values) + 1)
    return float(2 * (ranks * sorted_values).sum() / (len(sorted_values) * total) - (len(sorted_values) + 1) / len(sorted_values))


def top_share(values: pd.Series, fraction: float) -> float:
    """Share of the total taken by the top `fraction` of items (at least one item)."""
    count = max(1, math.ceil(len(values) * fraction))
    total = values.sum()
    return float(values.nlargest(count).sum() / total) if total else 0.0


def describe(values: pd.Series) -> dict[str, float]:
    return {
        "min": _round(values.min()), "mean": _round(values.mean()), "p50": _round(values.quantile(0.5)),
        "p90": _round(values.quantile(0.9)), "max": _round(values.max()),
    }


def profile_table(name: str, frame: pd.DataFrame, path: Path) -> list[Row]:
    rows: list[Row] = [
        (name, "overview", "rows", len(frame)),
        (name, "overview", "columns", frame.shape[1]),
        (name, "overview", "file_size_mb", _round(path.stat().st_size / 1024 / 1024, 2)),
    ]
    for column in frame.columns:
        rows.append((name, "types", column, str(frame[column].dtype)))
        rows.append((name, "null_pct", column, _round(frame[column].isna().mean() * 100, 2)))
        rows.append((name, "distinct", column, int(frame[column].nunique())))
    key = PRIMARY_KEY[name]
    rows.append((name, "keys", f"{key}_duplicates", int(frame[key].duplicated().sum())))
    rows.append((name, "keys", f"{key}_unique", bool(frame[key].is_unique)))
    return rows


def profile_integrity(frames: dict[str, pd.DataFrame]) -> list[Row]:
    rows: list[Row] = []
    for table, column, parent in FOREIGN_KEYS:
        orphans = ~frames[table][column].isin(frames[parent][column])
        rows.append((table, "orphans", f"{column}_not_in_{parent}", int(orphans.sum())))
    return rows


def profile_values(frames: dict[str, pd.DataFrame]) -> list[Row]:
    rows: list[Row] = []
    for table, columns in NUMERIC.items():
        for column in columns:
            values = frames[table][column].dropna()
            rows += [(table, "ranges", f"{column}_{stat}", value) for stat, value in describe(values).items()]
    ratings = frames["ratings"]["rating"].value_counts()
    for score in range(1, 6):
        count = int(ratings.get(score, 0))
        rows.append(("ratings", "rating_histogram", f"rating={score}", count))
        rows.append(("ratings", "rating_histogram_pct", f"rating={score}", _round(count / len(frames["ratings"]) * 100, 2)))
    watches = frames["watch_history"].merge(frames["movies"][["movie_id", "duration"]], on="movie_id")
    completion = watches["watch_duration"] / watches["duration"]
    rows += [("watch_history", "completion_ratio", f"completion_{stat}", value) for stat, value in describe(completion).items()]
    rows.append(("watch_history", "completion_ratio", "completion_share_ge_0.9", _round((completion >= 0.9).mean())))
    for table, columns in CATEGORIES.items():
        for column in columns:
            counts = frames[table][column].value_counts(dropna=False)
            for value, count in counts.items():
                rows.append((table, f"category_{column}", str(value), int(count)))
                rows.append((table, f"category_{column}_pct", str(value), _round(count / len(frames[table]) * 100, 2)))
    joined = frames["watch_history"].merge(frames["movies"][["movie_id", "genre"]], on="movie_id")
    for genre, count in joined["genre"].value_counts().items():
        rows.append(("watch_history", "views_by_genre", str(genre), int(count)))
        rows.append(("watch_history", "views_by_genre_pct", str(genre), _round(count / len(joined) * 100, 2)))
    return rows


def profile_time(frames: dict[str, pd.DataFrame]) -> list[Row]:
    rows: list[Row] = []
    for table, column in TIME_COLUMN.items():
        stamps = frames[table][column].dropna()
        rows += [
            (table, "time", "min_timestamp", str(stamps.min())),
            (table, "time", "max_timestamp", str(stamps.max())),
            (table, "time", "span_days", int((stamps.max().normalize() - stamps.min().normalize()).days + 1)),
        ]
        rows.append((table, "time", "unparseable_timestamps", int(frames[table][column].isna().sum())))
    stamps = frames["watch_history"]["watch_timestamp"].dropna()
    per_day = stamps.dt.date.value_counts().sort_index()
    rows += [
        ("watch_history", "time", "days_with_events", len(per_day)),
        ("watch_history", "events_per_day", "min", int(per_day.min())),
        ("watch_history", "events_per_day", "median", _round(per_day.median(), 1)),
        ("watch_history", "events_per_day", "max", int(per_day.max())),
    ]
    by_hour = stamps.dt.hour.value_counts()
    for hour in range(24):
        count = int(by_hour.get(hour, 0))
        rows.append(("watch_history", "events_by_hour", f"{hour:02d}", count))
        rows.append(("watch_history", "events_by_hour_pct", f"{hour:02d}", _round(count / len(stamps) * 100, 2)))
    weekend = stamps.dt.day_of_week >= 5
    days = pd.Series(stamps.dt.date.unique())
    weekend_days = int((pd.to_datetime(days).dt.day_of_week >= 5).sum())
    rows += [
        ("watch_history", "weekday_vs_weekend", "weekend_events", int(weekend.sum())),
        ("watch_history", "weekday_vs_weekend", "weekend_share_pct", _round(weekend.mean() * 100, 2)),
        ("watch_history", "weekday_vs_weekend", "weekend_days", weekend_days),
        ("watch_history", "weekday_vs_weekend", "avg_events_per_weekend_day", _round(weekend.sum() / max(1, weekend_days), 1)),
        ("watch_history", "weekday_vs_weekend", "avg_events_per_weekday_day", _round((~weekend).sum() / max(1, len(days) - weekend_days), 1)),
    ]
    return rows


def profile_skew(frames: dict[str, pd.DataFrame]) -> list[Row]:
    watches = frames["watch_history"]
    movie_views = watches["movie_id"].value_counts().reindex(frames["movies"]["movie_id"], fill_value=0)
    user_views = watches["user_id"].value_counts().reindex(frames["users"]["user_id"], fill_value=0)
    rows: list[Row] = [
        ("movies", "skew", "movies_without_views", int((movie_views == 0).sum())),
        ("movies", "skew", "top_1pct_movies_view_share_pct", _round(top_share(movie_views, 0.01) * 100, 2)),
        ("movies", "skew", "top_10pct_movies_view_share_pct", _round(top_share(movie_views, 0.10) * 100, 2)),
        ("movies", "skew", "gini_views_per_movie", _round(gini(movie_views))),
        ("users", "skew", "users_without_views", int((user_views == 0).sum())),
        ("users", "skew", "users_without_views_pct", _round((user_views == 0).mean() * 100, 2)),
        ("users", "skew", "gini_views_per_user", _round(gini(user_views))),
        ("users", "skew", f"users_with_views_le_{SEGMENT_LIMITS[0]}_pct", _round((user_views <= SEGMENT_LIMITS[0]).mean() * 100, 2)),
        ("users", "skew", f"users_with_views_le_{SEGMENT_LIMITS[1]}_pct", _round((user_views <= SEGMENT_LIMITS[1]).mean() * 100, 2)),
        ("users", "skew", "top_10pct_users_view_share_pct", _round(top_share(user_views, 0.10) * 100, 2)),
    ]
    for quantile in QUANTILES:
        label = f"p{int(quantile * 100)}"
        rows.append(("users", "views_per_user", label, _round(user_views.quantile(quantile), 1)))
        rows.append(("movies", "views_per_movie", label, _round(movie_views.quantile(quantile), 1)))
    return rows


def profile_coverage(frames: dict[str, pd.DataFrame]) -> list[Row]:
    watches, ratings = frames["watch_history"], frames["ratings"]
    rated_pairs = pd.MultiIndex.from_frame(ratings[["user_id", "movie_id"]])
    watch_pairs = pd.MultiIndex.from_frame(watches[["user_id", "movie_id"]])
    with_rating = watch_pairs.isin(rated_pairs)
    per_movie = ratings["movie_id"].value_counts()
    rows: list[Row] = [
        ("ratings", "coverage", "ratings_per_watch_pct", _round(len(ratings) / len(watches) * 100, 2)),
        ("ratings", "coverage", "watches_with_rated_user_movie_pair_pct", _round(with_rating.mean() * 100, 2)),
        ("ratings", "coverage", "movies_without_ratings", int((~frames["movies"]["movie_id"].isin(per_movie.index)).sum())),
        ("ratings", "coverage", "users_without_ratings", int((~frames["users"]["user_id"].isin(ratings["user_id"])).sum())),
    ]
    for quantile in QUANTILES:
        rows.append(("ratings", "ratings_per_movie", f"p{int(quantile * 100)}", _round(per_movie.quantile(quantile), 1)))
    rows.append(("ratings", "ratings_per_movie", "min", int(per_movie.min())))
    rows.append(("ratings", "ratings_per_movie", "max", int(per_movie.max())))
    return rows


def profile(raw_dir: Path) -> list[Row]:
    """All metrics for the four raw files in `raw_dir` (timestamps parsed as UTC, nothing is cleaned)."""
    parsed = read_raw(raw_dir)
    frames = {name: frame.drop(columns=["_bad_ts"], errors="ignore") for name, frame in zip(TABLES, parsed)}
    rows: list[Row] = []
    for name, file_name in zip(TABLES, RAW_FILES):
        rows += profile_table(name, frames[name], raw_dir / file_name)
    for build in (profile_integrity, profile_values, profile_time, profile_skew, profile_coverage):
        rows += build(frames)
    return rows


def _format(value: object) -> str:
    return f"{value:,}" if isinstance(value, (int, float)) and not isinstance(value, bool) else str(value)


def to_markdown(rows: list[Row]) -> str:
    """One table per (dataset, section); the CSV holds the same values."""
    lines = ["# Data profile of the raw CSV files", "",
             "Generated by `python -m src.profile_data`; every value also appears in `data_profile.csv`.", ""]
    sections: dict[tuple[str, str], list[Row]] = {}
    for row in sorted(rows, key=lambda item: TABLES.index(item[0])):  # stable: keeps the section order
        sections.setdefault((row[0], row[1]), []).append(row)
    current = None
    for (dataset, section), items in sections.items():
        if dataset != current:
            lines += [f"## {dataset}", ""]
            current = dataset
        lines += [f"### {section}", "", "| metric | value |", "|---|---|"]
        lines += [f"| {metric} | {_format(value)} |" for _, _, metric, value in items]
        lines.append("")
    return "\n".join(lines)


def write_outputs(rows: list[Row], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["dataset", "section", "metric", "value"]).to_csv(
        output_dir / "data_profile.csv", index=False
    )
    (output_dir / "data_profile.md").write_text(to_markdown(rows), encoding="utf-8", newline="\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile the raw movie-platform CSV files.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("benchmarks/results"))
    args = parser.parse_args()
    rows = profile(args.raw_dir)
    write_outputs(rows, args.output_dir)
    print(f"Wrote {len(rows)} metrics to {args.output_dir / 'data_profile.md'} and data_profile.csv")


if __name__ == "__main__":
    main()
