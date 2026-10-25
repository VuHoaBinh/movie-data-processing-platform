"""BR3 alternative: collaborative filtering with Spark MLlib ALS (Spark only, no Pandas twin).

Implicit feedback: the strength of a (user, movie) pair is the number of times the user watched the movie
(`watch_history` rows), the signal the dashboard is trying to predict. ALS (Hu, Koren, Volinsky 2008) learns
`rank` latent factors per user and per movie; a movie's score for a user is the dot product of the two.
Scoring is done here with NumPy on the driver-broadcast item matrix instead of `recommendForAllUsers`, so
the watched titles can be masked before the top-N: `recommendForAllUsers(N)` would need N above the longest
watch history of any user (several hundred titles) to guarantee ten unwatched ones.

Fixed seed and fixed block counts make a run repeatable: identical input gives identical recommendations.
A user needs at least one watch to have factors, and a movie needs at least one watch to have factors, so
cold-start users get nothing from ALS (the dashboard falls back to the genre-based table) and never-watched
movies are never recommended.
"""
from __future__ import annotations

import time
from typing import TYPE_CHECKING

from src.recommendations import RECOMMENDATION_COLUMNS, RECOMMENDATION_SIZE

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Chosen before looking at any evaluation numbers: alpha = 40 is the value suggested by the ALS paper,
# rank 32 is a middle size for 20k items, regParam 0.1 and 10 iterations are Spark's usual starting point.
ALS_PARAMS = {"rank": 32, "regParam": 0.1, "alpha": 40.0, "maxIter": 10, "seed": 42}
NUM_BLOCKS = 8
CANDIDATES_PER_USER = 4  # top-N is taken from the best CANDIDATES_PER_USER * N scores, then ordered exactly


def als_recommendations(
    watches: "DataFrame",
    size: int = RECOMMENDATION_SIZE,
    timings: dict | None = None,
    **overrides,
) -> "DataFrame":
    """Return (user_id, rank, movie_id): the `size` best unwatched movies per user with watch history."""
    import numpy as np
    from pyspark.ml.recommendation import ALS
    from pyspark.sql import Window, functions as F
    from pyspark.sql.types import IntegerType, LongType, StructField, StructType

    params = {**ALS_PARAMS, **overrides}
    spark = watches.sparkSession

    plays = watches.groupBy("user_id", "movie_id").agg(F.count(F.lit(1)).alias("plays"))
    user_index = plays.select("user_id").distinct().withColumn(
        "user_idx", F.row_number().over(Window.orderBy("user_id")) - 1
    ).persist()
    item_index = plays.select("movie_id").distinct().withColumn(
        "item_idx", F.row_number().over(Window.orderBy("movie_id")) - 1
    ).persist()
    ratings = (
        plays.join(user_index, "user_id").join(item_index, "movie_id")
        .select("user_idx", "item_idx", F.col("plays").cast("float").alias("plays"))
        .repartition(NUM_BLOCKS, "user_idx")
        .persist()
    )

    started = time.perf_counter()
    model = ALS(
        userCol="user_idx", itemCol="item_idx", ratingCol="plays", implicitPrefs=True,
        numUserBlocks=NUM_BLOCKS, numItemBlocks=NUM_BLOCKS, **params,
    ).fit(ratings)
    fit_seconds = time.perf_counter() - started

    started = time.perf_counter()
    item_rows = model.itemFactors.orderBy("id").collect()
    item_ids = np.array([row["id"] for row in item_rows], dtype=np.int64)
    item_matrix = np.array([row["features"] for row in item_rows], dtype=np.float32)
    shared = spark.sparkContext.broadcast((item_ids, item_matrix))
    seen = ratings.groupBy("user_idx").agg(F.collect_list("item_idx").alias("seen"))
    user_rows = model.userFactors.withColumnRenamed("id", "user_idx").join(seen, "user_idx")

    def top_items(rows):
        ids, matrix = shared.value
        for row in rows:
            scores = matrix @ np.asarray(row["features"], dtype=np.float32)
            scores[np.searchsorted(ids, np.asarray(row["seen"], dtype=np.int64))] = -np.inf
            take = min(size, int(np.isfinite(scores).sum()))
            if take == 0:
                continue
            width = min(len(scores), CANDIDATES_PER_USER * size)
            best = np.argpartition(-scores, width - 1)[:width]
            best = best[np.lexsort((ids[best], -scores[best]))][:take]  # score desc, then item index asc
            for rank, position in enumerate(best, start=1):
                yield int(row["user_idx"]), rank, int(ids[position])

    schema = StructType([
        StructField("user_idx", LongType()), StructField("rank", IntegerType()), StructField("item_idx", LongType()),
    ])
    top = spark.createDataFrame(user_rows.rdd.mapPartitions(top_items), schema)
    result = (
        top.join(user_index, "user_idx").join(item_index, "item_idx")
        .select("user_id", "rank", "movie_id")
        .orderBy("user_id", "rank")
        .persist()
    )
    result.count()  # materialise: training and scoring run once, later actions reuse the cached rows
    score_seconds = time.perf_counter() - started
    shared.unpersist()
    for frame in (user_index, item_index, ratings):
        frame.unpersist()
    if timings is not None:
        timings.update(fit_seconds=fit_seconds, score_seconds=score_seconds)
    return result


def build_als_table(
    watches: "DataFrame", movie_performance: "DataFrame", size: int = RECOMMENDATION_SIZE, **overrides
) -> "DataFrame":
    """`user_recommendations_als`: the same columns as the genre-based table (`source` = 'als')."""
    from pyspark.sql import functions as F

    top = als_recommendations(watches, size, **overrides)
    details = movie_performance.select("movie_id", "title", "genre", "weighted_rating", "avg_rating", "views")
    return (
        top.join(details, "movie_id")
        .withColumn("source", F.lit("als"))
        .select(*RECOMMENDATION_COLUMNS)
        .orderBy("user_id", "rank")
    )
