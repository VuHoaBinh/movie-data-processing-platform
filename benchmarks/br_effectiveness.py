"""Application effectiveness: run each business requirement's dashboard queries against Cassandra and
check the success criterion against an independent Pandas computation from the raw CSVs.

    CASSANDRA_HOST=localhost python -m benchmarks.br_effectiveness        (stack up, batch loaded)
"""
from __future__ import annotations

import argparse
import math
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from benchmarks.common import write_results
from src import batch_processing
from src.cassandra_store import connect, query_frame

TOP_SHARE = 0.30


def timed(session, cql: str, params: list | None = None) -> tuple[pd.DataFrame, float]:
    started = time.perf_counter()
    frame = query_frame(session, cql, params)
    return frame, (time.perf_counter() - started) * 1000


def check(rows: list[dict], br: str, group: str, criterion: str, measured: str, passed: bool, ms: float) -> None:
    rows.append({"br": br, "user_group": group, "criterion": criterion, "measured": measured,
                 "pass": "yes" if passed else "NO", "query_ms": round(ms, 1)})


def main() -> None:
    parser = argparse.ArgumentParser(description="Per-BR success criteria measured against Cassandra.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    parser.add_argument("--sample-users", type=int, default=200)
    args = parser.parse_args()

    curated, _ = batch_processing.curate(*batch_processing.read_raw(args.raw_dir))
    expected = batch_processing.build_tables(*curated)
    performance = expected["movie_performance"]
    session = connect()
    rows: list[dict] = []

    # BR1: catalogue by genre is complete and the title search finds known titles.
    genres = sorted(expected["genre_popularity"]["genre"])
    started = time.perf_counter()
    catalog = pd.concat([
        query_frame(session, "SELECT movie_id, title, genre, views FROM movie_performance WHERE genre = %s", [g])
        for g in genres
    ])
    catalog_ms = (time.perf_counter() - started) * 1000
    check(rows, "BR1", "Viewer", "browse-all returns every movie (8 genre partitions)",
          f"{len(catalog):,} of {len(performance):,} movies", len(catalog) == len(performance), catalog_ms)
    hits = catalog[catalog["title"].str.contains("spider", case=False, regex=False)]
    check(rows, "BR1", "Viewer", "title search 'spider' finds the Spider-Man titles",
          f"{len(hits)} titles", len(hits) >= 2, catalog_ms)

    # BR2: batch top-10 equals the independent ranking; live window is fresh with ranks 1..10.
    top, ms = timed(session, "SELECT movie_id, views FROM top_viewed_movies WHERE bucket = 'all' LIMIT 10")
    reference = performance.sort_values(["views", "movie_id"], ascending=[False, True]).head(10)
    check(rows, "BR2", "Viewer", "all-time top-10 equals the Pandas ranking",
          f"{(top['movie_id'].tolist() == reference['movie_id'].tolist())}", top["movie_id"].tolist() == reference["movie_id"].tolist(), ms)
    today = datetime.now(timezone.utc).date()
    live = pd.DataFrame()
    for day in (today, today - timedelta(days=1)):
        live, ms = timed(session, "SELECT window_start, rank FROM live_trending WHERE day = %s LIMIT 10", [day])
        if not live.empty:
            break
    age = (datetime.now(timezone.utc) - pd.Timestamp(live["window_start"].max()).tz_localize("UTC")).total_seconds() / 60 if not live.empty else math.inf
    check(rows, "BR2", "Viewer", "live window has ranks 1..10 and is < 10 min old",
          f"{len(live)} rows, newest window {age:.1f} min old", sorted(live.get("rank", [])) == list(range(1, 11)) and age < 10, ms)

    # BR3: lists are complete, never contain a watched title and match the independent Pandas computation.
    watched = set(zip(curated[3]["user_id"], curated[3]["movie_id"]))
    users = random.Random(3).sample(sorted(expected["user_engagement"]["user_id"]), args.sample_users)
    reference_lists = expected["user_recommendations"].groupby("user_id")["movie_id"].apply(list).to_dict()
    prefs = expected["user_preferences"].set_index("user_id")["favorite_genres"]
    for label, table in (("genre-based", "user_recommendations"), ("ALS", "user_recommendations_als")):
        listed = served = leaks = bad_ranks = wrong_genre = matches = 0
        total_ms = 0.0
        for user in users:
            recs, ms = timed(session, f"SELECT rank, movie_id, genre, source FROM {table} WHERE user_id = %s", [user])
            total_ms += ms
            if recs.empty:
                continue
            served += 1
            listed += len(recs)
            leaks += sum((user, movie) in watched for movie in recs["movie_id"])
            bad_ranks += int(recs["rank"].tolist() != list(range(1, len(recs) + 1)))
            if table == "user_recommendations":
                favorite = prefs.get(user, "").split("|")
                wrong_genre += int(not recs.loc[recs["source"] == "genre", "genre"].isin(favorite).all())
                matches += int(recs["movie_id"].tolist() == reference_lists.get(user))
        check(rows, "BR3", "Viewer", f"{label}: no watched title is recommended ({args.sample_users} random users)",
              f"{leaks} watched titles in {listed:,} recommendations for {served} users with a list",
              leaks == 0 and bad_ranks == 0 and served > 0, total_ms / args.sample_users)
        if table == "user_recommendations":
            check(rows, "BR3", "Viewer", "genre-based: favourite-genre rows stay in the user's top-2 genres and lists equal the Pandas lists",
                  f"{wrong_genre} users with a foreign genre; {matches}/{served} lists identical",
                  wrong_genre == 0 and matches == served == args.sample_users, total_ms / args.sample_users)
    genre_served = expected["user_recommendations"].query("source == 'genre'")["user_id"].nunique() / len(expected["user_engagement"])
    check(rows, "BR3", "Viewer", "share of users that get favourite-genre recommendations (the rest are cold-start, shown popular titles)",
          f"{genre_served:.2%} genre-based; every user has a list: {len(reference_lists) == len(expected['user_engagement'])}",
          genre_served > 0.95 and len(reference_lists) == len(expected["user_engagement"]), 0.0)
    als_users, ms = timed(session, "SELECT DISTINCT user_id FROM user_recommendations_als")
    with_history = curated[3]["user_id"].nunique()
    check(rows, "BR3", "Viewer", "ALS serves every user with watch history (cold-start users fall back to the genre-based list)",
          f"{len(als_users):,} of {with_history:,} users with history ({len(als_users) / len(expected['user_engagement']):.2%} of all users)",
          len(als_users) == with_history, ms)

    # BR4: Bayesian ranking equals Pandas.
    rated, ms = timed(session, "SELECT movie_id FROM top_rated_movies WHERE bucket = 'all' LIMIT 10")
    reference = performance.dropna(subset=["weighted_rating"]).sort_values(
        ["weighted_rating", "rating_count", "movie_id"], ascending=[False, False, True]).head(10)
    check(rows, "BR4", "Analyst", "top-10 by weighted rating equals the Pandas ranking",
          f"{rated['movie_id'].tolist() == reference['movie_id'].tolist()}", rated["movie_id"].tolist() == reference["movie_id"].tolist(), ms)
    min_ratings = int(performance.loc[performance["movie_id"].isin(rated["movie_id"]), "rating_count"].min())
    check(rows, "BR4", "Analyst", "no top-10 title rests on very few ratings", f"min rating_count in top-10 = {min_ratings}",
          min_ratings >= 10, ms)

    # BR5: genre views add up to all views; unique viewers per genre equal the Pandas distinct count.
    genre_frame, ms = timed(session, "SELECT genre, views, unique_viewers FROM genre_popularity")
    summary = query_frame(session, "SELECT views FROM summary_metrics WHERE bucket = 'all'").iloc[0]["views"]
    check(rows, "BR5", "Analyst", "sum of genre views = total views", f"{int(genre_frame['views'].sum()):,} vs {int(summary):,}",
          int(genre_frame["views"].sum()) == int(summary), ms)
    reference = expected["genre_popularity"].set_index("genre")["unique_viewers"]
    viewers = genre_frame.set_index("genre")["unique_viewers"]
    check(rows, "BR5", "Analyst", "unique viewers per genre equal the Pandas distinct count and never exceed the user count",
          f"{len(viewers)} genres, max {int(viewers.max()):,} of {len(expected['user_engagement']):,} users",
          viewers.sort_index().tolist() == reference.sort_index().tolist() and int(viewers.max()) <= len(expected["user_engagement"]), ms)

    # BR6: one scan returns views, watch minutes and active days of every user; all three add up.
    engagement, ms = timed(
        session, "SELECT user_id, subscription_plan, region, views, watch_minutes, active_days FROM user_engagement")
    reference = expected["user_engagement"].set_index("user_id").sort_index()
    scanned = engagement.set_index("user_id").sort_index()
    check(rows, "BR6", "Analyst", "engagement scan covers all users; views add up",
          f"{len(engagement):,} users, {int(engagement['views'].sum()):,} views",
          len(engagement) == len(reference) and int(engagement["views"].sum()) == int(summary), ms)
    check(rows, "BR6", "Analyst", "watch minutes and active days per user equal the Pandas values",
          f"{int(engagement['watch_minutes'].sum()):,} minutes, active days 0-{int(engagement['active_days'].max())} "
          f"(mean {engagement['active_days'].mean():.1f}, median {engagement['active_days'].median():.0f}), "
          f"median watch minutes {engagement['watch_minutes'].median():.0f}",
          scanned["watch_minutes"].tolist() == reference["watch_minutes"].tolist()
          and scanned["active_days"].tolist() == reference["active_days"].tolist()
          and int(engagement["watch_minutes"].sum()) == int(curated[3]["watch_duration"].sum())
          and int(engagement["active_days"].max()) <= curated[3]["watch_timestamp"].dt.date.nunique(), ms)

    # BR7: top-30% scatter has the expected movies.
    n_top = math.ceil(len(performance) * TOP_SHARE)
    top_share, ms = timed(session, "SELECT movie_id, views FROM top_viewed_movies WHERE bucket = 'all' LIMIT %s", [n_top])
    check(rows, "BR7", "Analyst", "top 30% of movies by views are returned, with the minimum views above the 70th percentile",
          f"{len(top_share):,} movies, min views {int(top_share['views'].min())}",
          len(top_share) == n_top and top_share["views"].min() >= performance["views"].quantile(0.69), ms)

    # BR8: lowest-engagement list equals Pandas and includes never-watched movies.
    low, ms = timed(session, "SELECT movie_id, views FROM low_engagement_movies WHERE bucket = 'all' LIMIT 10")
    reference = expected["low_engagement_movies"].head(10)
    check(rows, "BR8", "Manager", "10 lowest-viewed titles equal the Pandas list",
          f"views {low['views'].tolist()}", low["movie_id"].tolist() == reference["movie_id"].tolist(), ms)

    # BR9: segments partition all users.
    segments, ms = timed(session, "SELECT segment, users, views FROM user_segments")
    check(rows, "BR9", "Manager", "segments cover all users exactly once",
          f"{int(segments['users'].sum()):,} users in {len(segments)} segments",
          int(segments["users"].sum()) == len(expected["user_engagement"]) and len(segments) == 3, ms)

    # BR10: one point per day, daily views add up.
    daily, ms = timed(session, "SELECT day, views FROM daily_views WHERE bucket = 'all'")
    check(rows, "BR10", "Manager", "daily series has one point per day and adds up to total views",
          f"{len(daily)} days, {int(daily['views'].sum()):,} views",
          len(daily) == len(expected["daily_views"]) and int(daily["views"].sum()) == int(summary), ms)

    write_results(
        "br_effectiveness", rows, "Business-requirement success criteria measured on the running stack",
        ["Each criterion is evaluated on Cassandra and compared with an independent Pandas computation from the raw CSVs "
         "(`query_ms`: client-side time of the Cassandra query/queries involved, 1M watch rows)."],
    )


if __name__ == "__main__":
    main()
