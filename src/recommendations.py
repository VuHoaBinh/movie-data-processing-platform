"""BR3 genre-based recommendations, precomputed for every user (Pandas and Spark twins).

For each user: the top `size` unwatched movies from the user's two most-watched genres, ranked by the
Bayesian `weighted_rating` (ties: more ratings first, then movie_id). Users with fewer than `size` such
titles (cold-start users have none) are filled with the most-viewed titles they have not watched. Titles
the user already watched are never recommended; titles that were only rated are not excluded, because the
ratings table is a subset of the watch history in this dataset.

Candidates are bounded, not a user x movie cross join: the pool of a genre only needs
`size + (most distinct titles any user watched in that genre)` titles, because that is the most a user
can already have seen among the best ones. The result does not depend on the pool size.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import DataFrame

RECOMMENDATION_SIZE = 10
RECOMMENDATION_COLUMNS = [
    "user_id", "rank", "movie_id", "title", "genre", "source", "weighted_rating", "avg_rating", "views",
]
DETAIL_COLUMNS = ["movie_id", "title", "genre", "weighted_rating", "avg_rating", "views"]


def recommend_pandas(
    users: "pd.DataFrame",
    movie_performance: "pd.DataFrame",
    user_preferences: "pd.DataFrame",
    watches: "pd.DataFrame",
    size: int = RECOMMENDATION_SIZE,
) -> "pd.DataFrame":
    import pandas as pd

    seen = watches[["user_id", "movie_id"]].drop_duplicates()
    seen_by_genre = seen.merge(movie_performance[["movie_id", "genre"]], on="movie_id")
    per_genre_max = int(seen_by_genre.groupby(["user_id", "genre"]).size().max()) if len(seen_by_genre) else 0

    def unseen(pairs: pd.DataFrame) -> pd.DataFrame:
        marked = pairs.merge(seen.assign(_seen=1), on=["user_id", "movie_id"], how="left")
        return marked[marked["_seen"].isna()].drop(columns="_seen")

    rated = movie_performance[movie_performance["weighted_rating"].notna()]
    order, direction = ["weighted_rating", "rating_count", "movie_id"], [False, False, True]
    wanted = user_preferences.assign(genre=user_preferences["favorite_genres"].str.split("|")).explode("genre")
    parts = []
    for genre, genre_users in wanted.groupby("genre"):
        pool = rated[rated["genre"] == genre].sort_values(order, ascending=direction).head(size + per_genre_max)
        pairs = unseen(genre_users[["user_id"]].merge(pool[DETAIL_COLUMNS + ["rating_count"]], how="cross"))
        parts.append(pairs.sort_values(["user_id"] + order, ascending=[True] + direction).groupby("user_id").head(size))
    columns = ["user_id"] + DETAIL_COLUMNS + ["rating_count"]
    genre_rows = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=columns)
    genre_rows = genre_rows.assign(source="genre", _tier=0, _k1=genre_rows["weighted_rating"], _k2=genre_rows["rating_count"])

    have = genre_rows.groupby("user_id").size().clip(upper=size)
    need = size - users["user_id"].map(have).fillna(0).astype(int)
    deficient = pd.DataFrame({"user_id": users["user_id"], "need": need})
    deficient = deficient[deficient["need"] > 0]
    popular = pd.DataFrame(columns=genre_rows.columns)
    if len(deficient):
        pop_extra = seen[seen["user_id"].isin(deficient["user_id"])].groupby("user_id").size().max()
        pool = movie_performance.sort_values(["views", "movie_id"], ascending=[False, True]).head(
            size + (0 if pd.isna(pop_extra) else int(pop_extra))
        )
        pairs = deficient.merge(pool[DETAIL_COLUMNS], how="cross")
        pairs = unseen(pairs)
        pairs = pairs.merge(genre_rows[["user_id", "movie_id"]].assign(_taken=1), on=["user_id", "movie_id"], how="left")
        pairs = pairs[pairs["_taken"].isna()].drop(columns="_taken")
        pairs = pairs.sort_values(["user_id", "views", "movie_id"], ascending=[True, False, True])
        pairs = pairs[pairs.groupby("user_id").cumcount() < pairs["need"]]
        popular = pairs.assign(source="popular", _tier=1, _k1=pairs["views"].astype("float64"), _k2=0.0)

    both = pd.concat([genre_rows, popular], ignore_index=True)
    both = both.sort_values(["user_id", "_tier", "_k1", "_k2", "movie_id"], ascending=[True, True, False, False, True])
    both["rank"] = both.groupby("user_id").cumcount() + 1
    both = both[both["rank"] <= size]
    result = both[RECOMMENDATION_COLUMNS].reset_index(drop=True)
    result["rank"] = result["rank"].astype("int64")
    result["views"] = result["views"].astype("int64")
    for column in ("weighted_rating", "avg_rating"):
        result[column] = result[column].astype("float64")
    return result


def recommend_spark(
    users: "DataFrame",
    movie_performance: "DataFrame",
    user_preferences: "DataFrame",
    watches: "DataFrame",
    size: int = RECOMMENDATION_SIZE,
) -> "DataFrame":
    from pyspark.sql import Window, functions as F

    seen = watches.select("user_id", "movie_id").distinct()
    per_genre_max = (
        seen.join(movie_performance.select("movie_id", "genre"), "movie_id")
        .groupBy("user_id", "genre").count().agg(F.max("count")).first()[0] or 0
    )
    order = [F.desc("weighted_rating"), F.desc("rating_count"), F.asc("movie_id")]

    rated = movie_performance.where(F.col("weighted_rating").isNotNull())
    pool = (
        rated.withColumn("_pos", F.row_number().over(Window.partitionBy("genre").orderBy(*order)))
        .where(F.col("_pos") <= size + per_genre_max)
        .drop("_pos")
    )
    wanted = user_preferences.select("user_id", F.explode(F.split("favorite_genres", r"\|")).alias("genre"))
    candidates = wanted.join(pool, "genre").join(seen, ["user_id", "movie_id"], "left_anti")
    per_genre = Window.partitionBy("user_id", "genre").orderBy(*order)
    genre_rows = (
        candidates.withColumn("_pos", F.row_number().over(per_genre))
        .where(F.col("_pos") <= size)
        .select(
            "user_id", "movie_id", "title", "genre", "weighted_rating", "avg_rating", "views",
            F.lit("genre").alias("source"), F.lit(0).alias("_tier"),
            F.col("weighted_rating").alias("_k1"), F.col("rating_count").cast("double").alias("_k2"),
        )
    )

    have = genre_rows.groupBy("user_id").agg(F.least(F.count(F.lit(1)), F.lit(size)).alias("have"))
    deficient = (
        users.select("user_id").join(have, "user_id", "left")
        .withColumn("need", F.lit(size) - F.coalesce(F.col("have"), F.lit(0)))
        .where(F.col("need") > 0)
        .select("user_id", "need")
    )
    pop_extra = seen.join(deficient, "user_id").groupBy("user_id").count().agg(F.max("count")).first()[0] or 0
    pool = movie_performance.select(*DETAIL_COLUMNS).orderBy(F.desc("views"), F.asc("movie_id")).limit(size + pop_extra)
    popular = (
        deficient.crossJoin(pool)
        .join(seen, ["user_id", "movie_id"], "left_anti")
        .join(genre_rows.select("user_id", "movie_id"), ["user_id", "movie_id"], "left_anti")
        .withColumn("_pos", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("views"), F.asc("movie_id"))))
        .where(F.col("_pos") <= F.col("need"))
        .select(
            "user_id", "movie_id", "title", "genre", "weighted_rating", "avg_rating", "views",
            F.lit("popular").alias("source"), F.lit(1).alias("_tier"),
            F.col("views").cast("double").alias("_k1"), F.lit(0.0).alias("_k2"),
        )
    )
    final = Window.partitionBy("user_id").orderBy(F.asc("_tier"), F.desc("_k1"), F.desc("_k2"), F.asc("movie_id"))
    return (
        genre_rows.unionByName(popular)
        .withColumn("rank", F.row_number().over(final))
        .where(F.col("rank") <= size)
        .select(*RECOMMENDATION_COLUMNS)
        .orderBy("user_id", "rank")
    )
