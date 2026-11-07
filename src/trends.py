"""Read-time helper for the BR10 per-movie series (the batch stores only the days with views).

`movie_daily_views` keeps one row per (movie, day with views): a dense 180-day series for 20,000 movies would
be 3.6M mostly-zero rows. The dashboard reads one partition (at most 180 rows), fills the missing days with 0
and applies the same 7-day trailing average the batch stores for genres (`genre_daily_views.views_ma7`).
"""
from __future__ import annotations

import pandas as pd

from src import business_rules as rules
from src.batch_processing import round_half_up


def dense_series(days_views: pd.DataFrame, first_day, last_day) -> pd.DataFrame:
    """Columns day, views, views_ma7 for every calendar day first_day..last_day.

    `days_views` has the columns `day` and `views` (days without views are absent). Missing days count as 0;
    the average is the sum of the 7 trailing days (the day itself included) divided by 7, and stays empty
    (NaN) until 7 days of history exist, exactly like the batch genre series.
    """
    window = rules.MOVING_AVERAGE_DAYS
    calendar = pd.date_range(pd.Timestamp(str(first_day)), pd.Timestamp(str(last_day)), freq="D", name="day")
    observed = pd.Series(
        days_views["views"].to_numpy(),
        index=pd.to_datetime([str(day) for day in days_views["day"]]),
    )
    series = observed.reindex(calendar, fill_value=0).astype("int64").rename("views").reset_index()
    series["views_ma7"] = round_half_up(series["views"].rolling(window).sum() / window, 2)
    return series
