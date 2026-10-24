from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


RAW_FILES = ("users.csv", "movies.csv", "ratings.csv", "watch_history.csv")


def read_raw(raw_dir: Path) -> tuple[pd.DataFrame, ...]:
    missing = [name for name in RAW_FILES if not (raw_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing raw files in {raw_dir}: {', '.join(missing)}")

    users = pd.read_csv(raw_dir / "users.csv")
    movies = pd.read_csv(raw_dir / "movies.csv")
    ratings = pd.read_csv(raw_dir / "ratings.csv", parse_dates=["timestamp"])
    watches = pd.read_csv(raw_dir / "watch_history.csv", parse_dates=["watch_timestamp"])
    return users, movies, ratings, watches


def build_tables(
    users: pd.DataFrame,
    movies: pd.DataFrame,
    ratings: pd.DataFrame,
    watches: pd.DataFrame,
) -> dict[str, pd.DataFrame]:

    watches = watches.drop_duplicates("watch_id").dropna()
    ratings = ratings.drop_duplicates("rating_id").dropna()

    joined = watches.merge(movies, on="movie_id", validate="many_to_one")
    joined = joined.merge(users, on="user_id", validate="many_to_one")

    rating_stats = (
        ratings.groupby("movie_id", as_index=False)
        .agg(avg_rating=("rating", "mean"), rating_count=("rating_id", "count"))
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
    movie_performance["watch_hours"] = (movie_performance["watch_minutes"] / 60).round(1)
    movie_performance["completion_rate"] = (
        movie_performance["avg_watch_minutes"] / movie_performance["duration"] * 100
    ).clip(upper=100).round(1)
    movie_performance["avg_rating"] = movie_performance["avg_rating"].round(2)
    movie_performance = movie_performance.drop(columns=["watch_minutes", "avg_watch_minutes"])
    low_engagement_movies = movie_performance.sort_values(
        ["views", "title"], ascending=[True, True]
    )[["movie_id", "title", "genre", "views", "completion_rate"]]

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
        .sort_values(["user_id", "size"], ascending=[True, False])
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
            "average_rating": round(ratings["rating"].mean(), 2),
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
    }


def run(raw_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    tables = build_tables(*read_raw(raw_dir))
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
