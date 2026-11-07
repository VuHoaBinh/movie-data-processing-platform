"""Manager alerts: each compares two time windows of an absolute count, so it moves when the platform gets worse.

Shared by the dashboard, the tests that degrade data on purpose, and `benchmarks/br_effectiveness.py`. The limits are
named constants in `src/business_rules.py`; docs/business_requirements.md section 2.3 gives the reason for each.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src import business_rules as rules


@dataclass(frozen=True)
class Alert:
    key: str
    change: float | None  # relative change against the earlier window; None when it cannot be computed
    limit: float
    firing: bool


def window_change(windows: pd.DataFrame, column: str) -> float | None:
    """recent / previous - 1 for one column of `activity_windows`; None without both windows or a previous of 0."""
    by_period = windows.set_index("period")[column] if len(windows) else pd.Series(dtype="float64")
    if rules.RECENT not in by_period.index or rules.PREVIOUS not in by_period.index or not by_period[rules.PREVIOUS]:
        return None
    return float(by_period[rules.RECENT]) / float(by_period[rules.PREVIOUS]) - 1


def _alert(key: str, change: float | None, limit: float) -> Alert:
    return Alert(key, change, limit, change is not None and change < -limit)


def activity_alerts(windows: pd.DataFrame) -> list[Alert]:
    """BR9: users with a view fell; BR8: titles with a view fell (last 28 days against the 28 before)."""
    return [
        _alert("active_users", window_change(windows, "active_users"), rules.ACTIVE_USERS_DROP_ALERT),
        _alert("active_titles", window_change(windows, "active_titles"), rules.ACTIVE_TITLES_DROP_ALERT),
    ]


def genre_week_changes(grid: pd.DataFrame, days: int = rules.MOVING_AVERAGE_DAYS) -> pd.Series:
    """Per genre: views of the last `days` days of `genre_daily_views` against the `days` before (NaN if none before)."""
    ordered = sorted(pd.to_datetime(grid["day"].astype(str)).unique())[-2 * days:]
    frame = grid.assign(day=pd.to_datetime(grid["day"].astype(str)))
    recent = frame[frame["day"].isin(ordered[days:])].groupby("genre")["views"].sum()
    earlier = frame[frame["day"].isin(ordered[:days])].groupby("genre")["views"].sum()
    return recent / earlier.where(earlier > 0) - 1


def genre_alert(grid: pd.DataFrame) -> tuple[Alert, pd.Series]:
    """BR10: the worst genre against the limit, and the per-genre changes."""
    changes = genre_week_changes(grid).dropna()
    worst = float(changes.min()) if len(changes) else None
    return _alert("genre_views", worst, rules.GENRE_DROP_ALERT), changes
