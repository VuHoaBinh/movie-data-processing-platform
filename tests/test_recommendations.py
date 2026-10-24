"""BR3 recommendations: hand-derived lists, no watched title ever recommended, ALS determinism.

The genre-based table is computed by both engines (`recommend_pandas` / `recommend_spark`) and compared
cell by cell in test_parity.py; here its rules are checked against a dataset small enough to derive by hand.
ALS is Spark only, so it has its own tests.
"""
from __future__ import annotations

import pandas as pd
import pytest

from src import recommendations
from tests.helpers import pandas_tables, spark_tables

USERS = "user_id,age,gender,region,registration_date,subscription_plan,activity_profile\n" + "".join(
    f"{user},30,Male,Vietnam,2025-01-01,Basic,Active\n" for user in "PQRS"
)
MOVIES = "movie_id,title,genre,release_year,duration,content_type,language,maturity_rating\n" + "".join(
    f"{movie},T{movie},{genre},2020,100,Movie,English,P\n"
    for movie, genre in (
        ("A1", "Action"), ("A2", "Action"), ("A3", "Action"), ("A4", "Action"),
        ("D1", "Drama"), ("D2", "Drama"), ("D3", "Drama"), ("C1", "Comedy"),
    )
)
# Two ratings per movie, so the Bayesian weighted rating orders movies exactly like their average:
# A1 5.0, C1 5.0 | A2 4.5, D1 4.5 | A3 4.0, D2 4.0 | A4 3.0 | D3 2.0.  Ties break on movie_id.
_RATINGS = {
    "A1": (5, 5), "A2": (5, 4), "A3": (4, 4), "A4": (3, 3), "D1": (4, 5), "D2": (4, 4), "D3": (2, 2), "C1": (5, 5),
}
RATINGS = "rating_id,user_id,movie_id,rating,timestamp\n" + "".join(
    f"R{n}{k},{'PQ'[k]},{movie},{score},2026-01-01 12:00:00\n"
    for n, (movie, scores) in enumerate(_RATINGS.items()) for k, score in enumerate(scores)
)
# P: A1, D3 x2, C1 -> top genres Drama (2 views), then Action/Comedy tie -> Action (alphabetical)
# Q: A1, A2        -> Action              S: D1 x3, D2 -> Drama              R: nothing (cold start)
_WATCHES = [("P", "A1"), ("P", "D3"), ("P", "D3"), ("P", "C1"), ("Q", "A1"), ("Q", "A2"),
            ("S", "D1"), ("S", "D1"), ("S", "D1"), ("S", "D2")]
WATCHES = "watch_id,user_id,movie_id,watch_timestamp,watch_duration,device\n" + "".join(
    f"W{n:02},{user},{movie},2026-01-0{1 + n % 5} 10:00:00,10,Web\n" for n, (user, movie) in enumerate(_WATCHES)
)

# (user, rank, movie_id, source). Views: D1 3 | A1 2, D3 2 | A2 1, C1 1, D2 1 | A3 0, A4 0.
EXPECTED = [
    # P's genre candidates: Action A2 4.5, A3 4.0, A4 3.0 and Drama D1 4.5, D2 4.0 (A1, D3 are watched, C1 is
    # not a favourite genre). Ties A2/D1 and A3/D2 go to the smaller movie_id. Everything else is watched.
    ("P", 1, "A2", "genre"), ("P", 2, "D1", "genre"), ("P", 3, "A3", "genre"), ("P", 4, "D2", "genre"),
    ("P", 5, "A4", "genre"),
    # Q: only A3, A4 are unwatched Action titles; the filler is the most viewed unwatched title (D1 3, D3 2,
    # then C1 before D2 on the movie_id tie).
    ("Q", 1, "A3", "genre"), ("Q", 2, "A4", "genre"), ("Q", 3, "D1", "popular"), ("Q", 4, "D3", "popular"),
    ("Q", 5, "C1", "popular"), ("Q", 6, "D2", "popular"),
    # R never watched anything: the popularity ranking itself.
    ("R", 1, "D1", "popular"), ("R", 2, "A1", "popular"), ("R", 3, "D3", "popular"), ("R", 4, "A2", "popular"),
    ("R", 5, "C1", "popular"), ("R", 6, "D2", "popular"), ("R", 7, "A3", "popular"), ("R", 8, "A4", "popular"),
    # S: D3 is the only unwatched Drama title; then popular titles S has not seen.
    ("S", 1, "D3", "genre"), ("S", 2, "A1", "popular"), ("S", 3, "A2", "popular"), ("S", 4, "C1", "popular"),
    ("S", 5, "A3", "popular"), ("S", 6, "A4", "popular"),
]


@pytest.fixture(scope="module")
def rec_dir(tmp_path_factory):
    directory = tmp_path_factory.mktemp("recs")
    for name, text in (("users", USERS), ("movies", MOVIES), ("ratings", RATINGS), ("watch_history", WATCHES)):
        (directory / f"{name}.csv").write_text(text, encoding="utf-8")
    return directory


def compact(frame: pd.DataFrame) -> list[tuple]:
    return [(row.user_id, row.rank, row.movie_id, row.source) for row in frame.itertuples()]


@pytest.fixture(scope="module", params=["pandas", pytest.param("spark", marks=pytest.mark.spark)])
def engine_tables(request, rec_dir):
    if request.param == "pandas":
        return pandas_tables(rec_dir)[0]
    return spark_tables(request.getfixturevalue("spark"), rec_dir)[0]


def test_lists_match_hand_derivation(engine_tables):
    assert compact(engine_tables["user_recommendations"]) == EXPECTED


def test_list_length_is_capped_by_size(rec_dir):
    tables = pandas_tables(rec_dir)[0]
    args = (
        pd.read_csv(rec_dir / "users.csv"), tables["movie_performance"], tables["user_preferences"],
        pd.read_csv(rec_dir / "watch_history.csv"),
    )
    capped = recommendations.recommend_pandas(*args, size=3)
    assert compact(capped) == [row for row in EXPECTED if row[1] <= 3]


def watched(raw_dir) -> set[tuple[str, str]]:
    history = pd.read_csv(raw_dir / "watch_history.csv")
    return set(zip(history["user_id"], history["movie_id"]))


def check_lists(frame: pd.DataFrame, raw_dir, size: int = 10) -> None:
    seen = watched(raw_dir)
    assert not [(u, m) for u, m in zip(frame["user_id"], frame["movie_id"]) if (u, m) in seen]
    assert not frame.duplicated(["user_id", "movie_id"]).any()
    for user, group in frame.groupby("user_id"):
        assert group["rank"].tolist() == list(range(1, len(group) + 1)) and len(group) <= size, user


@pytest.mark.spark
@pytest.mark.parametrize("dataset", ["clean_dir", "dirty_dir", "sparse_dir"])
def test_no_watched_title_is_recommended_and_every_user_is_served(request, dataset, spark):
    raw_dir = request.getfixturevalue(dataset)
    for tables in (pandas_tables(raw_dir)[0], spark_tables(spark, raw_dir)[0]):
        frame = tables["user_recommendations"]
        check_lists(frame, raw_dir)
        # Every user of the cleaned user table has a full list (the catalogue is far larger than 10 + history).
        assert frame["user_id"].nunique() == len(tables["user_engagement"])
        assert (frame.groupby("user_id").size() == 10).all()


def test_genre_rows_come_from_the_favourite_genres(clean_dir):
    tables = pandas_tables(clean_dir)[0]
    favourite = dict(zip(tables["user_preferences"]["user_id"], tables["user_preferences"]["favorite_genres"].str.split("|")))
    genre_rows = tables["user_recommendations"].query("source == 'genre'")
    assert len(genre_rows) > 0
    assert all(genre in favourite[user] for user, genre in zip(genre_rows["user_id"], genre_rows["genre"]))
    # Within a user's list genre rows come first and are ordered by weighted rating.
    for _, group in tables["user_recommendations"].groupby("user_id"):
        sources = group["source"].tolist()
        assert sources == sorted(sources, key=["genre", "popular"].index)
        rated = group.query("source == 'genre'")["weighted_rating"].tolist()
        assert rated == sorted(rated, reverse=True)


# ---------------------------------------------------------------- ALS (Spark only)

@pytest.fixture(scope="module")
def als_inputs(spark, clean_dir):
    from src import spark_batch_job

    frames, _ = spark_batch_job.curate(*spark_batch_job.read_raw(spark, clean_dir))
    watches = frames[3].persist()
    return watches, spark_batch_job.build_tables(*frames)["movie_performance"]


@pytest.mark.spark
def test_als_lists_are_full_unwatched_and_repeatable(als_inputs, clean_dir):
    from src.recommender_als import als_recommendations

    watches, _ = als_inputs
    timings: dict = {}
    first = _collect(als_recommendations(watches, timings=timings))
    again = _collect(als_recommendations(watches))
    assert timings["fit_seconds"] > 0 and timings["score_seconds"] > 0
    assert first.equals(again), "same data and seed must give the same recommendations"
    check_lists(first, clean_dir)
    # 400 users, 200 movies, 8000 views: every user with history has at least 10 unwatched titles left.
    users_with_history = pd.read_csv(clean_dir / "watch_history.csv")["user_id"].nunique()
    assert first["user_id"].nunique() == users_with_history
    assert (first.groupby("user_id").size() == 10).all()


@pytest.mark.spark
def test_als_table_has_the_genre_based_shape_and_a_different_seed_changes_it(als_inputs):
    from src.recommender_als import als_recommendations, build_als_table

    watches, performance = als_inputs
    table = _collect(build_als_table(watches, performance))
    assert list(table.columns) == recommendations.RECOMMENDATION_COLUMNS
    assert set(table["source"]) == {"als"}
    other = _collect(als_recommendations(watches, seed=7))
    assert not other[["user_id", "rank", "movie_id"]].equals(table[["user_id", "rank", "movie_id"]])


def _collect(frame) -> pd.DataFrame:
    return pd.DataFrame([row.asDict() for row in frame.collect()], columns=frame.columns)
