"""Lambda boundary: the replay day belongs to the speed layer, archived live events return to the batch."""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from src import batch_processing
from src.kafka_event_producer import day_events, heartbeat_event, watch_event
from src.replay_clock import ReplayClock, batch_cutoff, days_between
from tests.helpers import pandas_tables, records

ROOT = Path(__file__).resolve().parent.parent
LIVE_DAY = date(2026, 1, 4)  # last day of the conftest fixture: 32 WA* + 9 WB* views


def archived_frame(watch_ids_and_stamps: list[tuple[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(
        [(watch_id, "U2", "M3", stamp, 25.0, "TV") for watch_id, stamp in watch_ids_and_stamps],
        columns=batch_processing.WATCH_COLUMNS,
    )


def test_clock_runs_speed_times_faster_from_the_start_of_the_replay_day():
    wall = datetime(2026, 11, 10, 8, 0, tzinfo=timezone.utc)
    clock = ReplayClock.start(LIVE_DAY, speed=60, total=10, wall=wall)
    assert clock.sim_now(wall) == datetime(2026, 1, 4, tzinfo=timezone.utc)
    assert clock.sim_now(wall + timedelta(minutes=5)) == datetime(2026, 1, 4, 5, 0, tzinfo=timezone.utc)
    assert not clock.done


def test_batch_cutoff_includes_only_closed_days():
    assert batch_cutoff(None) is None
    assert batch_cutoff(LIVE_DAY) == LIVE_DAY  # nothing archived yet: the live day is speed-only
    assert batch_cutoff(LIVE_DAY, archived_max_day=LIVE_DAY) == LIVE_DAY  # newest archived day may be open
    assert batch_cutoff(LIVE_DAY, archived_max_day=date(2026, 1, 6)) == date(2026, 1, 6)
    assert batch_cutoff(LIVE_DAY, archived_max_day=date(2026, 1, 6), sim_day=date(2026, 1, 5)) == date(2026, 1, 5)
    assert days_between(LIVE_DAY, date(2026, 1, 6)) == [LIVE_DAY, date(2026, 1, 5), date(2026, 1, 6)]


def test_replay_day_is_left_to_the_speed_layer(fixture_dir):
    tables, _ = pandas_tables(fixture_dir, replay_day=LIVE_DAY, batch_cutoff=LIVE_DAY)
    daily = records(tables["daily_views"])
    assert [row[0] for row in daily] == ["2026-01-01", "2026-01-02", "2026-01-03"]
    assert tables["summary_metrics"].iloc[0]["views"] == 9


def test_archived_events_of_closed_days_join_the_batch_once(fixture_dir, tmp_path):
    full, _ = pandas_tables(fixture_dir)
    # The archive holds the replayed day (same watch_ids as the CSV, one of them sent twice after a
    # producer restart) plus one form event on the next, still open, day.
    watches = pd.read_csv(fixture_dir / "watch_history.csv")
    replayed = watches[watches["watch_timestamp"].str.startswith("2026-01-04")]
    archived = pd.concat([replayed, replayed.head(1)], ignore_index=True)
    archived = pd.concat([archived, archived_frame([("FORM-1", "2026-01-05 00:03:00")])], ignore_index=True)
    archive_dir = tmp_path / "archive"
    (archive_dir / "_spark_metadata").mkdir(parents=True)
    for day, rows in archived.groupby(archived["watch_timestamp"].str[:10]):
        (archive_dir / f"watch_date={day}").mkdir()
        rows.to_csv(archive_dir / f"watch_date={day}" / "part-00000.csv", header=False, index=False)

    tables, report = pandas_tables(
        fixture_dir, replay_day=LIVE_DAY, batch_cutoff=date(2026, 1, 5), archive_dir=archive_dir
    )
    for name in ("daily_views", "movie_performance", "user_engagement", "low_engagement_movies"):
        assert records(tables[name]) == records(full[name]), name  # the form event (Jan 5) is still live-only
    assert report["watch_history"]["duplicate_id"] == 1


def test_producer_replays_the_day_in_event_time_order(fixture_dir):
    movies = {"M2": {"title": "Beta", "genre": "Action", "duration": "100"},
              "M3": {"title": "Gamma", "genre": "Drama", "duration": "50"}}
    events = day_events(fixture_dir / "watch_history.csv", LIVE_DAY, movies)
    assert len(events) == 41
    assert [stamp for stamp, _ in events] == sorted(stamp for stamp, _ in events)
    stamp, row = events[0]
    event = watch_event(row, movies[row["movie_id"]], {"subscription_plan": "Basic"}, stamp)
    assert event["event_time_ms"] == int(datetime(2026, 1, 4, 8, tzinfo=timezone.utc).timestamp() * 1000)
    assert event["watch_timestamp"] == "2026-01-04 08:00:00" and event["event_type"] == "watch"
    assert heartbeat_event(1)["event_type"] == "heartbeat"


def test_engines_read_every_field_the_producer_sends():
    """Producer, Spark speed layer and Flink source table must agree on the event schema."""
    event = watch_event(
        {"watch_id": "W", "user_id": "U", "movie_id": "M", "watch_duration": "1", "device": "TV"},
        {"title": "T", "genre": "G", "duration": "2"}, {"subscription_plan": "Basic"},
        datetime(2026, 1, 4, tzinfo=timezone.utc),
    )
    produced = set(json.loads(json.dumps(event)))
    spark_source = (ROOT / "src" / "spark_speed_layer.py").read_text()
    spark_fields = set(re.findall(r'StructField\("(\w+)"', spark_source))
    flink_source = (ROOT / "flink" / "flink_speed_sql_job.py").read_text()
    flink_table = flink_source[flink_source.index("CREATE TABLE watch_events"):flink_source.index("event_time AS")]
    flink_fields = set(re.findall(r"(\w+) (?:STRING|INT|BIGINT)", flink_table))
    assert spark_fields <= produced
    assert flink_fields <= produced
    assert {"event_time_ms", "sent_at_ms", "watch_duration", "movie_duration"} <= spark_fields & flink_fields


@pytest.mark.spark
def test_spark_and_pandas_select_the_same_batch_input(spark, fixture_dir):
    from src import spark_batch_job

    archived = archived_frame([("W1", "2026-01-01 10:00:00"), ("LIVE-1", "2026-01-04 09:30:00"),
                               ("LIVE-2", "2026-01-05 00:10:00")])
    expected = batch_processing.select_watch_events(
        pd.read_csv(fixture_dir / "watch_history.csv"), archived, LIVE_DAY, date(2026, 1, 5)
    )
    raw = spark_batch_job.read_raw(spark, fixture_dir)[3]
    archived_spark = spark.createDataFrame(archived, schema=spark_batch_job.RAW_SCHEMAS["watch_history"])
    selected = spark_batch_job.select_watch_events(raw, archived_spark, LIVE_DAY, date(2026, 1, 5))
    assert sorted(row.watch_id for row in selected.collect()) == sorted(expected["watch_id"])
