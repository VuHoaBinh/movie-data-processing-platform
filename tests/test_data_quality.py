"""Rejected-row counts: hand-made edge cases and the generator's --dirty-rate injection."""
from __future__ import annotations

from pathlib import Path

import pytest

from src.data_quality import REASONS
from tests.helpers import pandas_tables, spark_tables

HAND_WATCHES = """watch_id,user_id,movie_id,watch_timestamp,watch_duration,device
W1,U1,M1,2026-01-01 10:00:00,60,Web
W1,U1,M1,2026-01-01 10:00:00,60,Web
W2,U1,M1,2026-01-01 10:00:00,,Web
W2,U1,M1,2026-01-01 10:00:00,60,Web
W3,U1,M1,2026-01-01 10:00:00,0,Web
W4,U1,M1,2031-01-01 10:00:00,60,Web
W5,U9,M1,2026-01-01 10:00:00,60,Web
W6,U1,M9,2026-01-01 10:00:00,60,Web
W7,U9,M9,2026-01-01 10:00:00,-5,Web
W8,U1,M1,not-a-date,60,Web
"""
HAND_RATINGS = """rating_id,user_id,movie_id,rating,timestamp
R1,U1,M1,5,2026-01-01 10:00:00
R2,U1,M1,0,2026-01-01 10:00:00
R3,U1,M1,6,2026-01-01 10:00:00
R4,U1,M1,,2026-01-01 10:00:00
R5,U1,M1,3,2031-01-01 10:00:00
R5,U1,M1,3,2026-01-01 10:00:00
R6,U9,M1,3,2026-01-01 10:00:00
R7,U1,M9,3,2026-01-01 10:00:00
R8,U1,M1,3,31/02/2026
"""


@pytest.fixture()
def hand_dir(tmp_path: Path, fixture_dir: Path) -> Path:
    for name in ("users.csv", "movies.csv"):
        (tmp_path / name).write_text((fixture_dir / name).read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / "watch_history.csv").write_text(HAND_WATCHES, encoding="utf-8")
    (tmp_path / "ratings.csv").write_text(HAND_RATINGS, encoding="utf-8")
    return tmp_path


def expected_hand_report() -> dict:
    # Each row is counted once, under the first rule it fails (order: REASONS).
    return {
        "watch_history": {
            "input": 10, "duplicate_id": 2, "invalid_timestamp": 1,  # 2nd W1 and 2nd W2 (kept first W2 has the null -> rejected as null)
            "null_value": 1, "invalid_value": 2, "future_timestamp": 1,  # W2(first), W3 + W7, W4
            "orphan_user": 1, "orphan_movie": 1, "output": 1,  # W5, W6; only W1 survives
        },
        "ratings": {
            "input": 9, "duplicate_id": 1, "invalid_timestamp": 1, "null_value": 1, "invalid_value": 2,
            "future_timestamp": 1, "orphan_user": 1, "orphan_movie": 1, "output": 1,
        },
    }


def test_rule_precedence_pandas(hand_dir):
    _, report = pandas_tables(hand_dir)
    for table, expected in expected_hand_report().items():
        assert report[table] == expected, table
    assert report["users"]["output"] == 4 and report["movies"]["output"] == 4


@pytest.mark.spark
def test_rule_precedence_spark(spark, hand_dir):
    _, report = spark_tables(spark, hand_dir)
    for table, expected in expected_hand_report().items():
        assert report[table] == expected, table


def test_clean_data_loses_no_rows(clean_dir):
    _, report = pandas_tables(clean_dir)
    for counts in report.values():
        assert counts["output"] == counts["input"] and all(counts[reason] == 0 for reason in REASONS)


def test_generator_dirty_rate_is_detected_exactly(dirty_dir, clean_dir):
    import generate_data  # noqa: F401  (documents the injected kinds)

    _, report = pandas_tables(dirty_dir)
    # 6% of 8000 watches / 3000 ratings spread over the seven kinds -> 68 and 25 rows each.
    for table, per_kind in (("watch_history", 68), ("ratings", 25)):
        assert {reason: report[table][reason] for reason in REASONS} == dict.fromkeys(REASONS, per_kind)
    clean_report = pandas_tables(clean_dir)[1]
    assert report["watch_history"]["output"] == clean_report["watch_history"]["output"] == 8000
    assert report["ratings"]["output"] == clean_report["ratings"]["output"] == 3000


def test_dirty_rate_does_not_change_the_clean_rows(dirty_dir, clean_dir):
    for name in ("users.csv", "movies.csv"):
        assert (dirty_dir / name).read_bytes() == (clean_dir / name).read_bytes()
    clean_lines = (clean_dir / "watch_history.csv").read_text(encoding="utf-8").splitlines()
    dirty_lines = (dirty_dir / "watch_history.csv").read_text(encoding="utf-8").splitlines()
    assert dirty_lines[: len(clean_lines)] == clean_lines
