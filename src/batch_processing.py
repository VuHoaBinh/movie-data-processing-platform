from __future__ import annotations

import argparse
from decimal import ROUND_HALF_UP, Decimal
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src import business_rules as rules
from src.data_quality import Report, clean_pandas, format_report
from src.recommendations import recommend_pandas


RAW_FILES = ("users.csv", "movies.csv", "ratings.csv", "watch_history.csv")


def read_raw(raw_dir: Path) -> tuple[pd.DataFrame, ...]:
    missing = [name for name in RAW_FILES if not (raw_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing raw files in {raw_dir}: {', '.join(missing)}")

    users = pd.read_csv(raw_dir / "users.csv")
    movies = pd.read_csv(raw_dir / "movies.csv")
    ratings = pd.read_csv(raw_dir / "ratings.csv")
    watches = pd.read_csv(raw_dir / "watch_history.csv")
    for frame, column in ((ratings, "timestamp"), (watches, "watch_timestamp")):
        parsed = parse_timestamps(frame[column])
        frame["_bad_ts"] = frame[column].notna() & parsed.isna()  # text present but unparseable
        frame[column] = parsed
    return users, movies, ratings, watches


def parse_timestamps(values: pd.Series) -> pd.Series:
    """Parse mixed naive/ISO-with-offset strings; naive values are treated as UTC."""
    return pd.to_datetime(values, format="mixed", utc=True, errors="coerce").dt.tz_localize(None)


def round_half_up(values: pd.Series, digits: int) -> pd.Series:
    """Round like Spark's F.round (HALF_UP on the decimal text); pandas rounds half to even."""
    quantum = Decimal(1).scaleb(-digits)
    return values.map(
        lambda value: value if pd.isna(value)
        else float(Decimal(repr(float(value))).quantize(quantum, rounding=ROUND_HALF_UP))
    )


def nearest_rank(values: pd.Series, percent: int) -> float:
    """Percentile without interpolation (see `business_rules.nearest_rank_position`); NaN when empty."""
    ordered = np.sort(values.dropna().to_numpy())
    if len(ordered) == 0:
        return float("nan")
    return ordered[rules.nearest_rank_position(len(ordered), percent) - 1]


def curate(
    users: pd.DataFrame,
    movies: pd.DataFrame,
    ratings: pd.DataFrame,
    watches: pd.DataFrame,
    now: datetime | None = None,
) -> tuple[tuple[pd.DataFrame, ...], Report]:
    """Raw -> curated: apply the data-quality rules and return the clean frames plus rejected counts."""
    return clean_pandas(users, movies, ratings, watches, now)


def growth_trending(movies: pd.DataFrame, joined: pd.DataFrame) -> pd.DataFrame:
    """BR2 "rising now": views in the last 7 days vs the 7 days before, per movie.

    The reference date is the latest watch date in the data (not the wall clock), so the result is
    reproducible. Only movies with at least `TREND_MIN_RECENT_VIEWS` recent views are ranked, by
    growth = (recent - previous) / (previous + TREND_DAMPING), rounded to 3 decimals.
    """
    columns = ["movie_id", "title", "genre", "views_recent", "views_previous", "growth", "reference_date"]
    days = joined["watch_timestamp"].dt.normalize()
    if days.empty:
        return pd.DataFrame(columns=columns)
    reference = days.max()
    age = (reference - days).dt.days
    window = rules.TREND_WINDOW_DAYS
    recent = joined[age < window].groupby("movie_id").size().rename("views_recent")
    previous = joined[(age >= window) & (age < 2 * window)].groupby("movie_id").size().rename("views_previous")
    trend = (
        movies[["movie_id", "title", "genre"]]
        .merge(recent, on="movie_id")
        .merge(previous, on="movie_id", how="left")
    )
    trend["views_previous"] = trend["views_previous"].fillna(0).astype("int64")
    trend = trend[trend["views_recent"] >= rules.TREND_MIN_RECENT_VIEWS].copy()
    trend["growth"] = round_half_up(
        (trend["views_recent"] - trend["views_previous"]) / (trend["views_previous"] + rules.TREND_DAMPING), 3
    )
    trend["reference_date"] = reference.date()
    return trend.sort_values(["growth", "movie_id"], ascending=[False, True])[columns].reset_index(drop=True)


def peer_low_engagement(movie_performance: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """BR8: flag movies that are low *relative to the other movies of their genre*.

    Per genre (never-watched movies included): `low_views` = views <= the genre's p10 views;
    `low_completion` = at least 5 views and completion_rate <= the p10 completion of the genre's titles
    that have at least 5 views. Returns (per-genre peer thresholds and counts, flagged movies).
    """
    peers = []
    for genre, group in movie_performance.groupby("genre"):
        reliable = group[group["views"] >= rules.PEER_MIN_VIEWS_FOR_COMPLETION]
        peers.append({
            "genre": genre,
            "movies": len(group),
            "views_p10": int(nearest_rank(group["views"], rules.PEER_VIEWS_PERCENTILE)),
            "completion_p10": nearest_rank(reliable["completion_rate"], rules.PEER_COMPLETION_PERCENTILE),
        })
    peers = pd.DataFrame(peers, columns=["genre", "movies", "views_p10", "completion_p10"])
    scored = movie_performance.merge(peers, on="genre")
    low_views = scored["views"] <= scored["views_p10"]
    low_completion = (scored["views"] >= rules.PEER_MIN_VIEWS_FOR_COMPLETION) & (
        scored["completion_rate"] <= scored["completion_p10"]
    )
    scored["reason"] = np.select(
        [low_views & low_completion, low_views, low_completion],
        [rules.REASON_BOTH, rules.REASON_LOW_VIEWS, rules.REASON_LOW_COMPLETION],
        default="",
    )
    scored["priority"] = scored["reason"].map(rules.REASON_PRIORITY)
    flagged = scored[scored["reason"] != ""].rename(
        columns={"views_p10": "genre_views_p10", "completion_p10": "genre_completion_p10"}
    )
    flagged = flagged.sort_values(["priority", "views", "movie_id"])[[
        "movie_id", "title", "genre", "views", "completion_rate",
        "genre_views_p10", "genre_completion_p10", "reason", "priority",
    ]].reset_index(drop=True)
    flagged["priority"] = flagged["priority"].astype("int64")
    count_columns = ["low_views", "low_completion", "low_both"]
    counts = (
        scored.groupby(["genre", "reason"]).size().unstack(fill_value=0)
        .reindex(columns=[rules.REASON_LOW_VIEWS, rules.REASON_LOW_COMPLETION, rules.REASON_BOTH], fill_value=0)
    )
    counts.columns = count_columns
    peers = peers.merge(counts.reset_index(), on="genre", how="left")
    peers[count_columns] = peers[count_columns].fillna(0).astype("int64")
    peers["flagged"] = peers[count_columns].sum(axis=1)
    return peers, flagged


def window_activity(joined: pd.DataFrame) -> pd.DataFrame:
    """Alert inputs: views, users with a view and titles with a view in the last 28 days and the 28 days before.

    The windows end at the latest watch date; a window without views has no row.
    """
    columns = ["period", "start_date", "end_date", "views", "active_users", "active_titles"]
    days = joined["watch_timestamp"].dt.normalize()
    if days.empty:
        return pd.DataFrame(columns=columns)
    reference = days.max()
    age = (reference - days).dt.days
    width = rules.ACTIVITY_WINDOW_DAYS
    rows = []
    for label, mask, first_age, last_age in (
        (rules.PREVIOUS, (age >= width) & (age < 2 * width), 2 * width - 1, width),
        (rules.RECENT, age < width, width - 1, 0),
    ):
        part = joined[mask]
        if part.empty:
            continue
        rows.append({
            "period": label,
            "start_date": (reference - pd.Timedelta(days=first_age)).date(),
            "end_date": (reference - pd.Timedelta(days=last_age)).date(),
            "views": len(part),
            "active_users": part["user_id"].nunique(),
            "active_titles": part["movie_id"].nunique(),
        })
    return pd.DataFrame(rows, columns=columns)


def view_trends(movies: pd.DataFrame, joined: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """BR10: daily views per genre (complete calendar, 7-day moving average), per movie, weekday vs weekend."""
    genre_columns = ["day", "genre", "views", "views_ma7"]
    movie_columns = ["movie_id", "day", "views"]
    weekday_columns = ["genre", "day_type", "days", "views", "avg_daily_views"]
    if joined.empty:
        return (pd.DataFrame(columns=genre_columns), pd.DataFrame(columns=movie_columns),
                pd.DataFrame(columns=weekday_columns))
    watched = joined.assign(day=joined["watch_timestamp"].dt.normalize())
    calendar = pd.date_range(watched["day"].min(), watched["day"].max(), freq="D")
    grid = pd.MultiIndex.from_product([sorted(movies["genre"].unique()), calendar], names=["genre", "day"])
    genre_daily = (
        watched.groupby(["genre", "day"]).size().reindex(grid, fill_value=0).rename("views").reset_index()
    )
    window = rules.MOVING_AVERAGE_DAYS
    moving_sum = genre_daily.groupby("genre")["views"].transform(lambda series: series.rolling(window).sum())
    genre_daily["views_ma7"] = round_half_up(moving_sum / window, 2)  # empty until a full window exists
    genre_daily_views = genre_daily.sort_values(["day", "genre"])[genre_columns].reset_index(drop=True)
    genre_daily_views["day"] = genre_daily_views["day"].dt.date

    movie_daily_views = watched.groupby(["movie_id", "day"], as_index=False).agg(views=("watch_id", "count"))
    movie_daily_views["day"] = movie_daily_views["day"].dt.date

    by_day = genre_daily.groupby("day", as_index=False)["views"].sum().assign(genre=rules.ALL_GENRES)
    daily = pd.concat([genre_daily[["genre", "day", "views"]], by_day[["genre", "day", "views"]]])
    daily["day_type"] = np.where(daily["day"].dt.day_of_week >= 5, rules.WEEKEND, rules.WEEKDAY)
    weekday_weekend_views = daily.groupby(["genre", "day_type"], as_index=False).agg(
        days=("day", "count"), views=("views", "sum")
    )
    weekday_weekend_views["avg_daily_views"] = round_half_up(
        weekday_weekend_views["views"] / weekday_weekend_views["days"], 1
    )
    return genre_daily_views, movie_daily_views, weekday_weekend_views[weekday_columns]


def build_tables(
    users: pd.DataFrame,
    movies: pd.DataFrame,
    ratings: pd.DataFrame,
    watches: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Build the dashboard tables from curated frames (see `curate`)."""
    joined = watches.merge(movies, on="movie_id", validate="many_to_one")
    joined = joined.merge(users, on="user_id", validate="many_to_one")

    rating_stats = (
        ratings.groupby("movie_id", as_index=False)
        .agg(avg_rating=("rating", "mean"), rating_count=("rating_id", "count"))
    )
    # Bayesian average: shrink each movie's mean toward the global mean C, with
    # prior weight m = median rating count, so titles with few ratings cannot top BR3/BR4.
    global_mean = ratings["rating"].mean()
    min_votes = rating_stats["rating_count"].median()
    rating_stats["weighted_rating"] = round_half_up(
        (rating_stats["rating_count"] * rating_stats["avg_rating"] + min_votes * global_mean)
        / (rating_stats["rating_count"] + min_votes),
        2,
    )
    watch_stats = (
        joined.groupby("movie_id", as_index=False)
        .agg(
            views=("watch_id", "count"),
            watch_minutes=("watch_duration", "sum"),
            avg_watch_minutes=("watch_duration", "mean"),
            unique_viewers=("user_id", "nunique"),
        )
    )
    movie_performance = (
        movies[["movie_id", "title", "genre", "release_year", "duration"]]
        .merge(watch_stats, on="movie_id", how="left")
        .merge(rating_stats, on="movie_id", how="left")
    )
    movie_performance["views"] = movie_performance["views"].fillna(0).astype("int64")
    movie_performance["watch_minutes"] = movie_performance["watch_minutes"].fillna(0)
    movie_performance["unique_viewers"] = movie_performance["unique_viewers"].fillna(0).astype("int64")
    movie_performance["watch_hours"] = round_half_up(movie_performance["watch_minutes"] / 60, 1)
    movie_performance["completion_rate"] = round_half_up(
        (movie_performance["avg_watch_minutes"] / movie_performance["duration"] * 100).clip(upper=100), 1
    )
    movie_performance["avg_rating"] = round_half_up(movie_performance["avg_rating"], 2)
    movie_performance = movie_performance.drop(columns=["watch_minutes", "avg_watch_minutes"])
    lowest_viewed_movies = movie_performance.sort_values(
        ["views", "title"], ascending=[True, True]
    )[["movie_id", "title", "genre", "views", "completion_rate"]]
    genre_engagement_peers, low_engagement_movies = peer_low_engagement(movie_performance)
    trending_growth = growth_trending(movies, joined)

    genre_popularity = (
        joined.groupby("genre", as_index=False)
        .agg(views=("watch_id", "count"), unique_viewers=("user_id", "nunique"))
        .sort_values("views", ascending=False)
    )
    subscription_views = (
        joined.groupby("subscription_plan", as_index=False)
        .agg(views=("watch_id", "count"), unique_viewers=("user_id", "nunique"))
    )

    engagement_stats = (
        joined.groupby("user_id", as_index=False)
        .agg(
            views=("watch_id", "count"),
            watch_minutes=("watch_duration", "sum"),
            active_days=("watch_timestamp", lambda values: values.dt.date.nunique()),
        )
    )
    user_engagement = users[["user_id", "subscription_plan", "region"]].merge(
        engagement_stats, on="user_id", how="left"
    )
    for column in ("views", "watch_minutes", "active_days"):
        user_engagement[column] = user_engagement[column].fillna(0).astype("int64")
    segment_thresholds = pd.DataFrame({
        "percentile": [rules.SEGMENT_LOW_PERCENTILE, rules.SEGMENT_HIGH_PERCENTILE],
        "views_threshold": [
            int(nearest_rank(user_engagement["views"], percent))
            for percent in (rules.SEGMENT_LOW_PERCENTILE, rules.SEGMENT_HIGH_PERCENTILE)
        ],
        "users": len(user_engagement),
    })
    low_cut, high_cut = segment_thresholds["views_threshold"]
    user_engagement["segment"] = pd.Categorical(
        np.where(
            user_engagement["views"] > high_cut, "Active",
            np.where(user_engagement["views"] > low_cut, "Casual", "Inactive"),
        ),
        categories=rules.SEGMENTS,
    )
    user_segments = (
        user_engagement.groupby("segment", observed=False, as_index=False)
        .agg(users=("user_id", "count"), views=("views", "sum"))
    )
    segment_agreement = (
        users[["user_id", "activity_profile"]]
        .merge(user_engagement[["user_id", "segment"]], on="user_id")
        .assign(activity_profile=lambda frame: pd.Categorical(frame["activity_profile"], categories=rules.SEGMENTS))
        .groupby(["activity_profile", "segment"], observed=False)
        .size()
        .rename("users")
        .reset_index()
    )

    genre_by_user = (
        joined.groupby(["user_id", "genre"], as_index=False)
        .size()
        .sort_values(["user_id", "size"], ascending=[True, False])
    )
    user_preferences = (
        genre_by_user.groupby("user_id", as_index=False)
        .head(2)
        .groupby("user_id", as_index=False)["genre"]
        .agg(favorite_genres=lambda values: "|".join(values))
    )

    user_recommendations = recommend_pandas(users, movie_performance, user_preferences, watches)

    daily_views = (
        joined.assign(day=joined["watch_timestamp"].dt.date)
        .groupby("day", as_index=False)
        .agg(views=("watch_id", "count"), unique_viewers=("user_id", "nunique"))
    )
    hourly_views = (
        joined.assign(hour=joined["watch_timestamp"].dt.hour)
        .groupby("hour", as_index=False)
        .agg(views=("watch_id", "count"))
    )
    genre_daily_views, movie_daily_views, weekday_weekend_views = view_trends(movies, joined)
    activity_windows = window_activity(joined)
    summary_metrics = pd.DataFrame(
        [{
            "users": len(users),
            "movies": len(movies),
            "views": len(watches),
            "ratings": len(ratings),
            "average_rating": round_half_up(pd.Series([ratings["rating"].mean()]), 2).iloc[0],
        }]
    )

    return {
        "movie_performance": movie_performance,
        "trending_growth": trending_growth,
        "low_engagement_movies": low_engagement_movies,
        "lowest_viewed_movies": lowest_viewed_movies,
        "genre_engagement_peers": genre_engagement_peers,
        "genre_popularity": genre_popularity,
        "subscription_views": subscription_views,
        "user_engagement": user_engagement,
        "user_segments": user_segments,
        "segment_thresholds": segment_thresholds,
        "segment_agreement": segment_agreement,
        "user_preferences": user_preferences,
        "user_recommendations": user_recommendations,
        "daily_views": daily_views,
        "genre_daily_views": genre_daily_views,
        "movie_daily_views": movie_daily_views,
        "weekday_weekend_views": weekday_weekend_views,
        "activity_windows": activity_windows,
        "hourly_views": hourly_views,
        "summary_metrics": summary_metrics,
    }


def run(raw_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    curated, report = curate(*read_raw(raw_dir))
    print(format_report(report))
    tables = build_tables(*curated)
    for name, table in tables.items():
        table.to_csv(output_dir / f"{name}.csv", index=False)
        print(f"Wrote {name}.csv ({len(table):,} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build processed analytics tables.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/processed"))
    args = parser.parse_args()
    run(args.raw_dir, args.output_dir)


if __name__ == "__main__":
    main()
