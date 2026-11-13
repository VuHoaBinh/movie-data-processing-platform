from __future__ import annotations

import argparse
import os
from decimal import ROUND_HALF_UP, Decimal
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src import settings
from src.data_quality import Report, clean_pandas, format_report, utc_now
from src.replay_clock import batch_cutoff as compute_batch_cutoff


RAW_FILES = ("users.csv", "movies.csv", "ratings.csv", "watch_history.csv")
WATCH_COLUMNS = ["watch_id", "user_id", "movie_id", "watch_timestamp", "watch_duration", "device"]


def read_raw(
    raw_dir: Path,
    replay_day: date | None = None,
    batch_cutoff: date | None = None,
    archive_dir: Path | None = None,
) -> tuple[pd.DataFrame, ...]:
    """Read the raw CSVs (and the archived live events) and parse numbers and timestamps.

    With `replay_day`, the CSV rows of that day are left to the speed layer and archived events of
    days before `batch_cutoff` are added instead (see `select_watch_events`).
    """
    missing = [name for name in RAW_FILES if not (raw_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing raw files in {raw_dir}: {', '.join(missing)}")

    # Match Spark CSV: empty fields are missing; tokens such as NA and NULL are text.
    users, movies, ratings, watches = (
        pd.read_csv(raw_dir / name, keep_default_na=False, na_values=[""])
        for name in RAW_FILES
    )
    watches = select_watch_events(watches, read_archive(archive_dir), replay_day, batch_cutoff)
    for frame, columns in ((movies, ("release_year", "duration")),
                           (ratings, ("rating",)), (watches, ("watch_duration",))):
        for column in columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for frame, column in ((ratings, "timestamp"), (watches, "watch_timestamp")):
        parsed = parse_timestamps(frame[column])
        frame["_bad_ts"] = frame[column].notna() & parsed.isna()  # text present but unparseable
        frame[column] = parsed
    return users, movies, ratings, watches


def read_archive(archive_dir: Path | None) -> pd.DataFrame | None:
    """Live events archived from Kafka by `src.stream_archiver` (headerless CSV, partitioned by watch_date)."""
    if archive_dir is None or not (archive_dir / "_spark_metadata").is_dir():
        return None
    files = sorted(
        path for path in archive_dir.rglob("*.csv")
        if not any(part.startswith(("_", ".")) for part in path.relative_to(archive_dir).parts)
    )
    frames = [
        pd.read_csv(path, header=None, names=WATCH_COLUMNS, keep_default_na=False, na_values=[""])
        for path in files if path.stat().st_size > 0
    ]
    return pd.concat(frames, ignore_index=True) if frames else None


def event_days(values: pd.Series) -> pd.Series:
    """UTC calendar day of raw timestamp text (NaT when it cannot be parsed)."""
    return parse_timestamps(values).dt.normalize()


def select_watch_events(
    watches: pd.DataFrame, archived: pd.DataFrame | None, replay_day: date | None, batch_cutoff: date | None
) -> pd.DataFrame:
    """Batch input = CSV history without the replay day + archived live events of closed days.

    The replay day reaches the batch only through the archive, so an event is never counted twice;
    re-sent events share their watch_id and are removed by the duplicate_id rule.
    """
    if replay_day is not None:
        watches = watches[event_days(watches["watch_timestamp"]) != pd.Timestamp(replay_day)]
    if archived is not None:
        cutoff = batch_cutoff or replay_day
        if cutoff is not None:
            archived = archived[event_days(archived["watch_timestamp"]) < pd.Timestamp(cutoff)]
        watches = pd.concat([watches, archived[WATCH_COLUMNS]], ignore_index=True)
    return watches.reset_index(drop=True)


def detect_replay_day(raw_dir: Path, now: datetime | None = None) -> date | None:
    """Last day of watch_history (ignoring timestamps in the future): the day replayed into Kafka."""
    stamps = parse_timestamps(pd.read_csv(raw_dir / "watch_history.csv", usecols=["watch_timestamp"])["watch_timestamp"])
    stamps = stamps[stamps <= pd.Timestamp(now or utc_now())]
    return None if stamps.empty else stamps.max().date()


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


def curate(
    users: pd.DataFrame,
    movies: pd.DataFrame,
    ratings: pd.DataFrame,
    watches: pd.DataFrame,
    now: datetime | None = None,
) -> tuple[tuple[pd.DataFrame, ...], Report]:
    """Raw -> curated: apply the data-quality rules and return the clean frames plus rejected counts."""
    return clean_pandas(users, movies, ratings, watches, now)


def build_tables(
    users: pd.DataFrame,
    movies: pd.DataFrame,
    ratings: pd.DataFrame,
    watches: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Build the dashboard tables from curated frames (see `curate`)."""
    joined = watches.merge(movies, on="movie_id", validate="many_to_one")
    joined = joined.merge(users, on="user_id", validate="many_to_one")
    # Completion of one view, capped at 1 (watch_duration and duration are both minutes).
    joined["completion"] = (joined["watch_duration"] / joined["duration"]).clip(upper=1.0)
    joined["capped_minutes"] = joined[["watch_duration", "duration"]].min(axis=1)

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
            capped_minutes=("capped_minutes", "sum"),
            movie_duration=("duration", "first"),
            unique_viewers=("user_id", "nunique"),
        )
    )
    # Match both engines: per movie, divide the exact integer total only once.
    watch_stats["completion_sum"] = watch_stats["capped_minutes"] / watch_stats["movie_duration"]
    watch_stats["avg_completion"] = watch_stats["capped_minutes"] / (
        watch_stats["movie_duration"] * watch_stats["views"]
    )
    watch_stats = watch_stats.drop(columns=["capped_minutes", "movie_duration"])
    movie_performance = (
        movies[["movie_id", "title", "genre", "release_year", "duration", "content_type", "language",
                "maturity_rating"]]
        .merge(watch_stats, on="movie_id", how="left")
        .merge(rating_stats, on="movie_id", how="left")
    )
    movie_performance["views"] = movie_performance["views"].fillna(0).astype("int64")
    movie_performance["watch_minutes"] = movie_performance["watch_minutes"].fillna(0)
    movie_performance["unique_viewers"] = movie_performance["unique_viewers"].fillna(0).astype("int64")
    movie_performance["watch_hours"] = round_half_up(movie_performance["watch_minutes"] / 60, 1)
    movie_performance["completion_rate"] = round_half_up(movie_performance["avg_completion"], 3)
    movie_performance["avg_rating"] = round_half_up(movie_performance["avg_rating"], 2)
    # BR8: additive sums so the serving layer can merge batch history with the speed layer's live days.
    low_engagement_movies = movie_performance[["movie_id", "title", "genre", "views"]].copy()
    low_engagement_movies["watch_minutes"] = movie_performance["watch_minutes"].astype("int64")
    low_engagement_movies["completion_sum"] = round_half_up(movie_performance["completion_sum"].fillna(0.0), 4)
    low_engagement_movies["completion_rate"] = movie_performance["completion_rate"]
    low_engagement_movies["is_low_engagement"] = (
        (movie_performance["views"] >= settings.MIN_VIEWS)
        & (movie_performance["avg_completion"] < settings.LOW_COMPLETION_THRESHOLD)
    )
    low_engagement_movies = low_engagement_movies.sort_values("movie_id").reset_index(drop=True)
    movie_performance = movie_performance.drop(
        columns=["watch_minutes", "completion_sum", "avg_completion"]
    )

    genre_popularity, genre_monthly = build_genre_tables(joined, ratings, movies)
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
            completion_sum=("completion", "sum"),
            avg_completion=("completion", "mean"),
        )
    )
    user_engagement = users[["user_id", "subscription_plan", "region"]].merge(
        engagement_stats, on="user_id", how="left"
    )
    for column in ("views", "watch_minutes", "active_days"):
        user_engagement[column] = user_engagement[column].fillna(0).astype("int64")
    user_engagement["completion_sum"] = round_half_up(user_engagement["completion_sum"].fillna(0.0), 4)
    user_engagement["avg_completion"] = round_half_up(user_engagement["avg_completion"], 3)
    user_engagement["segment"] = pd.cut(
        user_engagement["views"],
        bins=[-1, 10, 35, float("inf")],
        labels=["Inactive", "Casual", "Active"],
    )
    user_segments = (

        user_engagement.groupby("segment", observed=False, as_index=False)
        .agg(users=("user_id", "count"), views=("views", "sum"))
    )

    genre_by_user = (
        joined.groupby(["user_id", "genre"], as_index=False)
        .size()
        .sort_values(["user_id", "size", "genre"], ascending=[True, False, True])
    )
    user_preferences = (
        genre_by_user.groupby("user_id", as_index=False)
        .head(2)
        .groupby("user_id", as_index=False)["genre"]
        .agg(favorite_genres=lambda values: "|".join(values))
    )

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
        "low_engagement_movies": low_engagement_movies,
        "genre_popularity": genre_popularity,
        "subscription_views": subscription_views,
        "user_engagement": user_engagement,
        "user_segments": user_segments,
        "user_preferences": user_preferences,
        "daily_views": daily_views,
        "hourly_views": hourly_views,
        "summary_metrics": summary_metrics,
        "user_recommendations": build_recommendations(joined, user_preferences, movie_performance),
        "rating_distribution": build_rating_distribution(ratings, movie_performance),
        "genre_monthly": genre_monthly,
        "view_trends": build_view_trends(joined),
    }


def _month_start(stamps: pd.Series) -> pd.Series:
    return stamps.dt.to_period("M").dt.start_time.dt.date


def build_genre_tables(
    joined: pd.DataFrame, ratings: pd.DataFrame, movies: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """BR5: per genre, overall and per month: views, unique viewers, watch hours, average rating.

    The average rating of a month uses the ratings given in that month to movies of the genre.
    """
    rated = ratings.merge(movies[["movie_id", "genre"]], on="movie_id")

    def summarise(views: pd.DataFrame, rated_part: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        frame = views.groupby(keys, as_index=False).agg(
            views=("watch_id", "count"), unique_viewers=("user_id", "nunique"),
            watch_minutes=("watch_duration", "sum"),
        )
        frame["watch_hours"] = round_half_up(frame["watch_minutes"] / 60, 1)
        rating = rated_part.groupby(keys, as_index=False).agg(avg_rating=("rating", "mean"))
        frame = frame.merge(rating, on=keys, how="left")
        frame["avg_rating"] = round_half_up(frame["avg_rating"], 2)
        return frame[keys + ["views", "unique_viewers", "watch_hours", "avg_rating"]]

    overall = summarise(joined, rated, ["genre"]).sort_values("views", ascending=False).reset_index(drop=True)
    monthly = summarise(
        joined.assign(month=_month_start(joined["watch_timestamp"])),
        rated.assign(month=_month_start(rated["timestamp"])),
        ["genre", "month"],
    ).sort_values(["genre", "month"]).reset_index(drop=True)
    return overall, monthly


def build_recommendations(
    joined: pd.DataFrame, user_preferences: pd.DataFrame, movie_performance: pd.DataFrame,
    top_n: int = settings.RECOMMENDATIONS_TOP_N,
) -> pd.DataFrame:
    """BR3: per user, the top N rated movies of the (up to two) favourite genres not watched yet.

    Exact without a users x movies cross join: a user who watched w movies of a genre finds its top N
    unwatched movies among the genre's top N + w, so only those candidates are joined.
    """
    order = ["weighted_rating", "rating_count", "movie_id"]
    rated = movie_performance.dropna(subset=["weighted_rating"]).sort_values(
        ["genre"] + order, ascending=[True, False, False, True]
    )
    rated = rated.assign(genre_rank=rated.groupby("genre").cumcount() + 1)[
        ["genre", "genre_rank", "movie_id", "title", "weighted_rating", "rating_count"]
    ]
    favorites = user_preferences.assign(genre=user_preferences["favorite_genres"].str.split("|")).explode("genre")
    watched = joined[["user_id", "movie_id", "genre"]].drop_duplicates()
    watched_in_genre = watched.groupby(["user_id", "genre"], as_index=False).size().rename(columns={"size": "watched"})
    favorites = favorites[["user_id", "genre"]].merge(watched_in_genre, on=["user_id", "genre"], how="left")
    favorites["watched"] = favorites["watched"].fillna(0)
    candidates = favorites.merge(rated, on="genre")
    candidates = candidates[candidates["genre_rank"] <= top_n + candidates["watched"]]
    candidates = candidates.merge(watched[["user_id", "movie_id"]], on=["user_id", "movie_id"], how="left", indicator=True)
    candidates = candidates[candidates["_merge"] == "left_only"].sort_values(
        ["user_id"] + order, ascending=[True, False, False, True]
    )
    candidates["rank"] = candidates.groupby("user_id").cumcount() + 1
    result = candidates[candidates["rank"] <= top_n]
    return result[
        ["user_id", "rank", "movie_id", "title", "genre", "weighted_rating", "rating_count"]
    ].astype({"rank": "int64", "rating_count": "int64"}).reset_index(drop=True)


def build_rating_distribution(ratings: pd.DataFrame, movie_performance: pd.DataFrame) -> pd.DataFrame:
    """BR4: number of ratings per star (1-5) for every rated movie, with its count and average."""
    counts = (
        ratings.assign(star=ratings["rating"].astype("int64"))
        .pivot_table(index="movie_id", columns="star", values="rating_id", aggfunc="count", fill_value=0)
        .reindex(columns=range(1, 6), fill_value=0)
    )
    counts.columns = [f"r{star}" for star in counts.columns]
    rated = movie_performance.dropna(subset=["rating_count"])[["movie_id", "title", "genre", "rating_count", "avg_rating"]]
    result = rated.merge(counts.reset_index(), on="movie_id").sort_values("movie_id").reset_index(drop=True)
    return result.astype({"rating_count": "int64", **{f"r{star}": "int64" for star in range(1, 6)}})


TREND_PERIODS = {"all": ("day", "week", "month"), "genre": ("day", "week", "month"), "movie": ("week", "month")}


def _period_start(stamps: pd.Series, period: str) -> pd.Series:
    days = stamps.dt.normalize()
    if period == "day":
        return days.dt.date
    if period == "week":  # ISO weeks start on Monday
        return (days - pd.to_timedelta(days.dt.weekday, unit="D")).dt.date
    return _month_start(stamps)


def _previous_start(starts: pd.Series, period: str) -> pd.Series:
    stamps = pd.to_datetime(starts)
    offset = {"day": pd.DateOffset(days=1), "week": pd.DateOffset(weeks=1), "month": pd.DateOffset(months=1)}[period]
    return (stamps - offset).dt.date


def build_view_trends(joined: pd.DataFrame) -> pd.DataFrame:
    """BR10: views per period (day / week / month) for the platform, each genre and each movie
    (movies: week and month only, to keep the table small), with growth versus the previous period.

    growth = (views - prev_views) / prev_views, null when the previous period had no views.
    """
    keyed = {
        "all": joined.assign(key="all"),
        "genre": joined.assign(key=joined["genre"]),
        "movie": joined.assign(key=joined["movie_id"]),
    }
    frames = []
    for scope, periods in TREND_PERIODS.items():
        for period in periods:
            counts = (
                keyed[scope].assign(period_start=_period_start(keyed[scope]["watch_timestamp"], period))
                .groupby(["key", "period_start"], as_index=False).agg(views=("watch_id", "count"))
            )
            counts["prev_start"] = _previous_start(counts["period_start"], period)
            previous = counts[["key", "period_start", "views"]].rename(
                columns={"period_start": "prev_start", "views": "prev_views"}
            )
            counts = counts.merge(previous, on=["key", "prev_start"], how="left")
            frames.append(counts.assign(scope=scope, period=period))
    trends = pd.concat(frames, ignore_index=True)
    trends["prev_views"] = trends["prev_views"].fillna(0).astype("int64")
    trends["growth"] = round_half_up(
        ((trends["views"] - trends["prev_views"]) / trends["prev_views"]).where(trends["prev_views"] > 0), 3
    )
    return trends.sort_values(["scope", "key", "period", "period_start"])[
        ["scope", "key", "period", "period_start", "views", "prev_views", "growth"]
    ].reset_index(drop=True)


def resolve_replay_day(value: str, raw_dir: Path) -> date | None:
    """'auto' = last day of watch_history, 'none' = no live day (batch reads every CSV row)."""
    if value.lower() == "none":
        return None
    return detect_replay_day(raw_dir) if value.lower() == "auto" else date.fromisoformat(value)


def run(
    raw_dir: Path,
    output_dir: Path,
    replay_day: date | None = None,
    archive_dir: Path | None = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    archived = read_archive(archive_dir)
    archived_max_day = None if archived is None else event_days(archived["watch_timestamp"]).max().date()
    cutoff = compute_batch_cutoff(replay_day, archived_max_day)
    print(f"replay_day={replay_day} batch_cutoff={cutoff}")
    curated, report = curate(*read_raw(raw_dir, replay_day, cutoff, archive_dir))
    print(format_report(report))
    tables = build_tables(*curated)
    for name, table in tables.items():
        table.to_csv(output_dir / f"{name}.csv", index=False)
        print(f"Wrote {name}.csv ({len(table):,} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build processed analytics tables.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/processed"))
    parser.add_argument(
        "--replay-day", default=os.getenv("REPLAY_DAY", "auto"),
        help="day owned by the speed layer: 'auto' (last day of watch_history), 'none' or YYYY-MM-DD",
    )
    parser.add_argument("--archive-dir", type=Path, default=settings.ARCHIVE_DIR)
    args = parser.parse_args()
    run(args.raw_dir, args.output_dir, resolve_replay_day(args.replay_day, args.raw_dir), args.archive_dir)


if __name__ == "__main__":
    main()
