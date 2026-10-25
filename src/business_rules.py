"""Parameters of the business rules shared by the Pandas and Spark batch pipelines.

Both engines read their constants from here, so the two implementations cannot drift apart.
`docs/business_requirements.md` explains why each value was chosen.
"""
from __future__ import annotations

# BR2 "rising now": views in the last 7 days vs the 7 days before, relative to the latest watch date.
TREND_WINDOW_DAYS = 7
TREND_MIN_RECENT_VIEWS = 10  # volume floor: fewer recent views than this cannot enter the ranking
TREND_DAMPING = 10  # pseudo-count in the denominator: growth = (recent - previous) / (previous + 10)

# BR8 low engagement relative to genre peers (all movies of the same genre, never-watched included).
PEER_VIEWS_PERCENTILE = 10  # low views: views <= the genre's p10
PEER_COMPLETION_PERCENTILE = 10  # low completion: completion_rate <= the genre's p10
PEER_MIN_VIEWS_FOR_COMPLETION = 5  # completion is an average over sessions; fewer than 5 is noise
REASON_BOTH, REASON_LOW_VIEWS, REASON_LOW_COMPLETION = "both", "low_views", "low_completion"
REASON_PRIORITY = {REASON_BOTH: 1, REASON_LOW_VIEWS: 2, REASON_LOW_COMPLETION: 3}  # 1 = review first

# BR9 segments from quantiles of views per user: <= p25 Inactive, <= p75 Casual, above Active.
SEGMENT_LOW_PERCENTILE = 25
SEGMENT_HIGH_PERCENTILE = 75
SEGMENTS = ("Inactive", "Casual", "Active")  # also the ActivityProfile labels used by generate_data.py

# BR10 trends.
MOVING_AVERAGE_DAYS = 7
ALL_GENRES = "All genres"
WEEKDAY, WEEKEND = "Weekday", "Weekend"


def nearest_rank_position(count: int, percent: int) -> int:
    """1-based position of the nearest-rank percentile in a sorted list of `count` values.

    ceil(percent * count / 100) in integer arithmetic, so there is no interpolation and no floating-point
    rounding: the percentile is always an observed value, identical in Pandas and Spark.
    """
    return max(1, -(-percent * count // 100))
