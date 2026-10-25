"""The read-time per-movie series must use the same moving-average definition as the batch genre series."""
from __future__ import annotations

import pandas as pd

from src.trends import dense_series
from tests.helpers import pandas_tables, records


def test_missing_days_count_as_zero_and_average_needs_seven_days(peer_dir):
    tables = pandas_tables(peer_dir)[0]
    movie = tables["movie_daily_views"].query("movie_id == 'C05'")  # views on 03-09..03-15 only: 2,2,2,1,1,1,1
    series = dense_series(movie, "2026-03-05", "2026-03-15")
    # 11 calendar days, 03-05..03-08 filled with 0; the first full 7-day window ends on 03-11
    assert series["views"].tolist() == [0, 0, 0, 0, 2, 2, 2, 1, 1, 1, 1]
    assert series["views_ma7"][:6].isna().all()
    # 03-11: 0+0+0+0+2+2+2 = 6 -> 0.857 -> 0.86;  03-15: 2+2+2+1+1+1+1 = 10 -> 1.428 -> 1.43
    assert series["views_ma7"][6] == 0.86 and series["views_ma7"].iloc[-1] == 1.43


def test_same_definition_as_the_batch_genre_series(peer_dir):
    """Densifying the movie rows of a one-movie genre must reproduce the batch `genre_daily_views` average."""
    tables = pandas_tables(peer_dir)[0]
    genre_rows = tables["genre_daily_views"].query("genre == 'Comedy'")
    sparse = genre_rows[genre_rows["views"] > 0][["day", "views"]]
    rebuilt = dense_series(sparse, genre_rows["day"].min(), genre_rows["day"].max())
    assert records(rebuilt[["views", "views_ma7"]]) == records(genre_rows[["views", "views_ma7"]])
