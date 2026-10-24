"""Offline evaluation of the BR3 recommenders on a time-based split (needs Spark + Java).

    python -m benchmarks.recommender_eval                 (host with Java, or the Docker `benchmark` service)

Split: the watch events of the last `--holdout-days` days (relative to the newest event) are the test set;
every recommender only sees events and ratings before that cutoff. For each user with at least one *new*
title in the test period (watched in the test period, not watched before it) the recommender returns ten
unwatched titles; a hit is a recommended title that user watched in the test period.

Methods: the popularity baseline (most-viewed unwatched titles), the genre-based table (BR3 as deployed,
`src/recommendations.py`) and Spark MLlib ALS (`src/recommender_als.py`), plus ALS with other hyperparameters
to show how sensitive it is (reported, not used to choose the deployed setting).
"""
from __future__ import annotations

import argparse
import math
import tempfile
import time
from pathlib import Path

import pandas as pd

from benchmarks.common import write_results
from src import batch_processing, recommender_als
from src.recommendations import RECOMMENDATION_SIZE, recommend_pandas

K = RECOMMENDATION_SIZE


def metrics(lists: dict[str, list[str]], truth: dict[str, set[str]], catalogue: int) -> dict:
    ideal = [sum(1 / math.log2(i + 2) for i in range(min(len(items), K))) for items in truth.values()]
    precision = recall = ndcg = 0.0
    hit_users = full = 0
    recommended: set[str] = set()
    for (user, wanted), best in zip(truth.items(), ideal):
        recs = lists.get(user, [])
        recommended.update(recs)
        hits = [i for i, movie in enumerate(recs) if movie in wanted]
        precision += len(hits) / K
        recall += len(hits) / len(wanted)
        ndcg += sum(1 / math.log2(i + 2) for i in hits) / best
        hit_users += bool(hits)
        full += len(recs) == K
    n = len(truth)
    return {
        "users_evaluated": n,
        "precision_at_10": round(precision / n, 4),
        "recall_at_10": round(recall / n, 4),
        "ndcg_at_10": round(ndcg / n, 4),
        "hit_rate_at_10": round(hit_users / n, 4),
        "catalogue_coverage": round(len(recommended) / catalogue, 4),
        "users_with_full_list": round(full / n, 4),
    }


def grouped(frame: pd.DataFrame) -> dict[str, list[str]]:
    ordered = frame.sort_values(["user_id", "rank"])
    return ordered.groupby("user_id")["movie_id"].apply(list).to_dict()


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline evaluation of the BR3 recommenders.")
    parser.add_argument("--raw-dir", type=Path, default=Path("data"))
    parser.add_argument("--holdout-days", type=int, default=30)
    parser.add_argument("--no-variants", action="store_true", help="skip the ALS sensitivity rows")
    args = parser.parse_args()

    users, movies, ratings, watches = batch_processing.curate(*batch_processing.read_raw(args.raw_dir))[0]
    newest = watches["watch_timestamp"].max()
    cutoff = newest - pd.Timedelta(days=args.holdout_days)
    train = watches[watches["watch_timestamp"] < cutoff]
    test = watches[watches["watch_timestamp"] >= cutoff]
    train_ratings = ratings[ratings["timestamp"] < cutoff]

    seen = set(zip(train["user_id"], train["movie_id"]))
    truth: dict[str, set[str]] = {}
    for user, movie in zip(test["user_id"], test["movie_id"]):
        if (user, movie) not in seen:
            truth.setdefault(user, set()).add(movie)
    train_views = train["movie_id"].value_counts()
    held_out_titles = {movie for wanted in truth.values() for movie in wanted}
    unseen_titles = sum(1 for movie in held_out_titles if movie not in train_views.index)
    cold_users = sum(1 for user in truth if user not in set(train["user_id"]))
    catalogue = len(movies)

    tables = batch_processing.build_tables(users, movies, train_ratings, train)
    rows: list[dict] = []

    def add(name: str, lists: dict[str, list[str]], fit: float, score: float) -> None:
        rows.append({"method": name, **metrics(lists, truth, catalogue),
                     "fit_seconds": round(fit, 1), "score_seconds": round(score, 1)})
        print(rows[-1], flush=True)

    # Popularity: most-viewed unwatched titles (the "filler" of the genre-based table, for everybody).
    started = time.perf_counter()
    ranking = tables["movie_performance"].sort_values(["views", "movie_id"], ascending=[False, True])["movie_id"].tolist()
    fit = time.perf_counter() - started
    started = time.perf_counter()
    popular_lists = {}
    watched_by: dict[str, set[str]] = {}
    for user, movie in seen:
        watched_by.setdefault(user, set()).add(movie)
    for user in truth:
        owned = watched_by.get(user, ())
        popular_lists[user] = [movie for movie in ranking if movie not in owned][:K]
    add("Popularity baseline", popular_lists, fit, time.perf_counter() - started)

    # Genre-based: recomputed from the same training data with the production function.
    started = time.perf_counter()
    genre_table = recommend_pandas(users, tables["movie_performance"], tables["user_preferences"], train)
    add("Genre-based (deployed)", grouped(genre_table), 0.0, time.perf_counter() - started)

    # ALS: train on the same events, score with the same exclusion of watched titles.
    from src.spark_batch_job import create_spark

    spark = create_spark(app_name="recommender-eval")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    try:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "train.csv"
            train[["user_id", "movie_id"]].to_csv(path, index=False)
            interactions = spark.read.option("header", True).csv(str(path)).persist()
            interactions.count()
            variants = [("ALS (deployed)", {})]
            if not args.no_variants:
                variants += [("ALS rank 16", {"rank": 16}), ("ALS rank 64", {"rank": 64}), ("ALS alpha 10", {"alpha": 10.0})]
            for name, overrides in variants:
                timings: dict = {}
                result = recommender_als.als_recommendations(interactions, timings=timings, **overrides)
                als_table = pd.DataFrame([row.asDict() for row in result.collect()])
                result.unpersist()
                add(name, grouped(als_table), timings["fit_seconds"], timings["score_seconds"])
    finally:
        spark.stop()

    params = ", ".join(f"{key}={value}" for key, value in recommender_als.ALS_PARAMS.items())
    write_results(
        "recommender_eval", rows, "BR3 recommenders: offline evaluation on a time-based split",
        [
            f"**Split.** Events before {cutoff:%Y-%m-%d %H:%M} UTC are training data ({len(train):,} views, "
            f"{len(train_ratings):,} ratings); the last {args.holdout_days} days up to {newest:%Y-%m-%d} are the test period "
            f"({len(test):,} views). No recommender sees anything from the test period.",
            f"**Evaluated users.** {len(truth):,} users watched at least one title in the test period that they had not "
            f"watched before; {cold_users:,} of them have no training history (cold start). Held-out titles per user: "
            f"mean {sum(len(w) for w in truth.values()) / len(truth):.1f}. {unseen_titles:,} of the {len(held_out_titles):,} "
            f"distinct held-out titles had no training view (new releases), which no recommender can know.",
            f"**Metrics (K = {K}, averaged over evaluated users, a user without a list scores 0).** "
            "precision@10 = hits / 10; recall@10 = hits / number of held-out titles of the user; "
            "ndcg@10 = DCG of the hits with log2 discount / the best possible DCG (binary relevance); "
            "hit_rate@10 = share of users with at least one hit; "
            f"catalogue_coverage = distinct recommended titles / {catalogue:,} titles; "
            "users_with_full_list = share of evaluated users that got ten titles from the method itself "
            "(ALS cannot serve users without training history; the dashboard then shows the genre-based list).",
            "**Candidates.** Every method excludes the titles the user watched before the cutoff, as the deployed tables do.",
            f"**ALS.** Implicit feedback, strength = number of watches of the title by the user; {params}, "
            f"{recommender_als.NUM_BLOCKS} user and item blocks. The 'deployed' setting was fixed before the evaluation; the "
            "other ALS rows only show sensitivity.",
            "**Times.** `fit_seconds`: ALS training (popularity: sorting the titles; genre-based: none, it reuses the "
            "favourite-genre and rating aggregates the batch already computes for BR1-BR10). `score_seconds`: building all "
            "lists (ALS: all users with history; popularity: evaluated users only; genre-based: all users). Pandas for the "
            "first two, Spark `local[*]` for ALS, so the times are indicative, not a like-for-like engine comparison.",
            "Synthetic data: the generator draws 43% of a user's views from one preferred genre and the rest mostly "
            "from blockbusters and hits, so the numbers describe this dataset, not real viewers.",
        ],
    )


if __name__ == "__main__":
    main()
