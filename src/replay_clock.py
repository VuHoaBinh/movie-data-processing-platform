"""Simulated clock of the live-day replay, shared by the producer, the dashboard and the batch job.

The last day of `watch_history` is replayed into Kafka in `watch_timestamp` order, `speed` times faster
than real time. Event time is the original `watch_timestamp`, so every event produced during the demo
(including the dashboard form) must be stamped with the *simulated* time; a real wall-clock time would
push the watermark months ahead and make every remaining replayed event late.

    sim_now = anchor_sim + (wall_now - anchor_wall) * speed

The clock is stored in the Cassandra table `replay_clock` (one row, id = 'demo') so that the dashboard
can read it and the producer can resume after a restart without re-sending events.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

CLOCK_ID = "demo"


def utc(value: datetime) -> datetime:
    """Driver timestamps come back naive (UTC); make them aware."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def day_start(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


@dataclass
class ReplayClock:
    replay_day: date
    anchor_wall: datetime
    anchor_sim: datetime
    speed: float
    sent: int = 0
    total: int = 0

    @classmethod
    def start(cls, replay_day: date, speed: float, total: int, wall: datetime | None = None) -> "ReplayClock":
        return cls(replay_day, wall or datetime.now(timezone.utc), day_start(replay_day), speed, 0, total)

    def sim_now(self, wall: datetime | None = None) -> datetime:
        wall = wall or datetime.now(timezone.utc)
        return self.anchor_sim + (wall - self.anchor_wall) * self.speed

    def sim_ms(self, wall: datetime | None = None) -> int:
        return int(self.sim_now(wall).timestamp() * 1000)

    @property
    def done(self) -> bool:
        return self.sent >= self.total


def load_clock(session: Any, keyspace: str = "movie_analytics") -> ReplayClock | None:
    row = session.execute(
        f"SELECT replay_day, anchor_wall, anchor_sim, speed, sent, total FROM {keyspace}.replay_clock WHERE id = %s",
        [CLOCK_ID],
    ).one()
    if row is None:
        return None
    get = row.get if isinstance(row, dict) else lambda name: getattr(row, name)
    replay_day = get("replay_day")
    if not isinstance(replay_day, date):  # cassandra.util.Date
        replay_day = replay_day.date()
    return ReplayClock(
        replay_day, utc(get("anchor_wall")), utc(get("anchor_sim")), float(get("speed")),
        int(get("sent") or 0), int(get("total") or 0),
    )


def save_clock(session: Any, clock: ReplayClock, keyspace: str = "movie_analytics") -> None:
    session.execute(
        f"INSERT INTO {keyspace}.replay_clock (id, replay_day, anchor_wall, anchor_sim, speed, sent, total, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        [CLOCK_ID, clock.replay_day, clock.anchor_wall, clock.anchor_sim, clock.speed, clock.sent, clock.total,
         datetime.now(timezone.utc)],
    )


def batch_cutoff(replay_day: date | None, archived_max_day: date | None = None, sim_day: date | None = None) -> date | None:
    """First day the batch does NOT cover; the serving layer takes days >= cutoff from the speed tables.

    * CSV rows on `replay_day` belong to the speed layer (they reach the batch through the archive).
    * Archived events are included only for days that are closed: before the simulated day when the
      replay clock is known, otherwise before the newest archived day (it may still be filling).
    """
    if replay_day is None:
        return None
    closed = sim_day if sim_day is not None else archived_max_day if archived_max_day is not None else replay_day
    return max(replay_day, closed)


def days_between(first: date, last: date, limit: int = 60) -> list[date]:
    """Days first..last inclusive (at most `limit`, newest kept)."""
    count = (last - first).days + 1
    days = [first + timedelta(days=offset) for offset in range(max(count, 0))]
    return days[-limit:]
