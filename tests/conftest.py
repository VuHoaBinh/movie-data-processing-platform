from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # not the Windows Store `python3` shim
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
sys.path.insert(0, str(ROOT))

USERS = """user_id,age,gender,region,registration_date,subscription_plan,activity_profile
U1,30,Male,Vietnam,2025-01-01,Basic,Active
U2,25,Female,Japan,2025-02-01,Premium,Casual
U3,41,Other,Korea,2025-03-01,Standard,Active
U4,19,Male,Thailand,2025-04-01,Basic,Inactive
"""
MOVIES = """movie_id,title,genre,release_year,duration,content_type,language,maturity_rating
M1,Alpha,Action,2020,100,Movie,English,P
M2,Beta,Action,2021,100,Movie,English,T13
M3,Gamma,Drama,2019,50,Series,English,T16
M4,Delta,Drama,2022,80,Movie,Korean,T18
"""
RATINGS = """rating_id,user_id,movie_id,rating,timestamp
R1,U1,M1,5,2026-01-01 12:00:00
R2,U2,M1,4,2026-01-02 12:00:00
R3,U3,M1,3,2026-01-03 12:00:00
R4,U1,M2,2,2026-01-02 12:00:00
R5,U3,M3,5,2026-01-03 12:00:00
R6,U1,M3,4,2026-01-03 12:00:00
"""
# 9 hand-picked views, one with an ISO offset (12:00+07:00 is 05:00 UTC),
# plus 32 views of M2 by U1 (hour 8) and 9 views of M3 by U3 (hour 9) on 2026-01-04, 10 minutes each.
WATCHES = ["watch_id,user_id,movie_id,watch_timestamp,watch_duration,device"] + [
    "W1,U1,M1,2026-01-01 10:00:00,60,Web",
    "W2,U1,M1,2026-01-01 22:30:00,100,TV",
    "W3,U2,M1,2026-01-02 10:15:00,40,Mobile",
    "W4,U1,M2,2026-01-02 11:00:00,50,Web",
    "W5,U3,M3,2026-01-02 23:00:00,25,Tablet",
    "W6,U3,M3,2026-01-03 10:00:00,50,Web",
    "W7,U1,M3,2026-01-03 11:00:00,10,Mobile",
    "W8,U2,M2,2026-01-03 12:00:00,100,TV",
    "W9,U2,M1,2026-01-03T12:00:00+07:00,20,Web",
] + [f"WA{i:02},U1,M2,2026-01-04 08:00:00,10,Web" for i in range(32)] + [
    f"WB{i:02},U3,M3,2026-01-04 09:00:00,10,Mobile" for i in range(9)
]


# Second hand-made dataset for the rules that need history and peers (BR2 growth windows, BR8 peer
# thresholds, BR10 moving average). One viewer; latest watch date (reference date) = 2026-03-15.
PEER_USERS = """user_id,age,gender,region,registration_date,subscription_plan,activity_profile
P1,30,Male,Vietnam,2025-01-01,Basic,Active
"""
PEER_MOVIES = ["movie_id,title,genre,release_year,duration,content_type,language,maturity_rating"] + [
    f"C{n:02},Comedy {n:02},Comedy,2020,100,Movie,English,P" for n in range(1, 14)
] + ["D1,Drama 1,Drama,2020,100,Movie,English,P", "D2,Drama 2,Drama,2020,100,Movie,English,P"]
PEER_RATINGS = """rating_id,user_id,movie_id,rating,timestamp
R1,P1,C05,5,2026-03-01 12:00:00
"""
# movie -> (total views, minutes per view, views 03-09..03-15, views 03-02..03-08); the rest are older
# (03-01 and before). Completion rate = minutes, because every duration is 100.
PEER_VIEWS = {
    "C02": (5, 40, 0, 0), "C03": (6, 20, 3, 3), "C04": (8, 80, 4, 4), "C05": (10, 70, 10, 0),
    "C06": (12, 60, 9, 3), "C07": (25, 75, 10, 15), "C08": (20, 50, 0, 0), "C09": (25, 85, 12, 12),
    "C10": (30, 65, 20, 10), "C11": (40, 95, 30, 10), "C12": (60, 55, 40, 0), "C13": (5, 90, 5, 0),
}


def peer_watch_rows() -> list[str]:
    rows = ["watch_id,user_id,movie_id,watch_timestamp,watch_duration,device"]
    for movie, (total, minutes, recent, previous) in PEER_VIEWS.items():
        days = (
            [f"2026-03-{9 + i % 7:02}" for i in range(recent)]
            + [f"2026-03-{2 + i % 7:02}" for i in range(previous)]
            + [(datetime(2026, 3, 1) - timedelta(days=i % 5)).strftime("%Y-%m-%d") for i in range(total - recent - previous)]
        )
        rows += [f"W_{movie}_{i:02},P1,{movie},{day} 12:00:00,{minutes},Web" for i, day in enumerate(days)]
    return rows


@pytest.fixture(scope="session")
def peer_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("peer")
    (directory / "users.csv").write_text(PEER_USERS, encoding="utf-8")
    (directory / "movies.csv").write_text("\n".join(PEER_MOVIES) + "\n", encoding="utf-8")
    (directory / "ratings.csv").write_text(PEER_RATINGS, encoding="utf-8")
    (directory / "watch_history.csv").write_text("\n".join(peer_watch_rows()) + "\n", encoding="utf-8")
    return directory


# Second hand-made dataset for the rules that need history and peers (BR2 growth windows, BR8 peer
# thresholds, BR10 moving average). One viewer; latest watch date (reference date) = 2026-03-15.
PEER_USERS = """user_id,age,gender,region,registration_date,subscription_plan,activity_profile
P1,30,Male,Vietnam,2025-01-01,Basic,Active
"""
PEER_MOVIES = ["movie_id,title,genre,release_year,duration,content_type,language,maturity_rating"] + [
    f"C{n:02},Comedy {n:02},Comedy,2020,100,Movie,English,P" for n in range(1, 14)
] + ["D1,Drama 1,Drama,2020,100,Movie,English,P", "D2,Drama 2,Drama,2020,100,Movie,English,P"]
PEER_RATINGS = """rating_id,user_id,movie_id,rating,timestamp
R1,P1,C05,5,2026-03-01 12:00:00
"""
# movie -> (total views, minutes per view, views 03-09..03-15, views 03-02..03-08); the rest are older
# (03-01 and before). Completion rate = minutes, because every duration is 100.
PEER_VIEWS = {
    "C02": (5, 40, 0, 0), "C03": (6, 20, 3, 3), "C04": (8, 80, 4, 4), "C05": (10, 70, 10, 0),
    "C06": (12, 60, 9, 3), "C07": (25, 75, 10, 15), "C08": (20, 50, 0, 0), "C09": (25, 85, 12, 12),
    "C10": (30, 65, 20, 10), "C11": (40, 95, 30, 10), "C12": (60, 55, 40, 0), "C13": (5, 90, 5, 0),
}


def peer_watch_rows() -> list[str]:
    rows = ["watch_id,user_id,movie_id,watch_timestamp,watch_duration,device"]
    for movie, (total, minutes, recent, previous) in PEER_VIEWS.items():
        days = (
            [f"2026-03-{9 + i % 7:02}" for i in range(recent)]
            + [f"2026-03-{2 + i % 7:02}" for i in range(previous)]
            + [(datetime(2026, 3, 1) - timedelta(days=i % 5)).strftime("%Y-%m-%d") for i in range(total - recent - previous)]
        )
        rows += [f"W_{movie}_{i:02},P1,{movie},{day} 12:00:00,{minutes},Web" for i, day in enumerate(days)]
    return rows


@pytest.fixture(scope="session")
def peer_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp("peer")
    (directory / "users.csv").write_text(PEER_USERS, encoding="utf-8")
    (directory / "movies.csv").write_text("\n".join(PEER_MOVIES) + "\n", encoding="utf-8")
    (directory / "ratings.csv").write_text(PEER_RATINGS, encoding="utf-8")
    (directory / "watch_history.csv").write_text("\n".join(peer_watch_rows()) + "\n", encoding="utf-8")
    return directory


@pytest.fixture(scope="session")
def fixture_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Tiny hand-made dataset whose analytics values are derived by hand in test_fixture_tables.py."""
    directory = tmp_path_factory.mktemp("fixture")
    (directory / "users.csv").write_text(USERS, encoding="utf-8")
    (directory / "movies.csv").write_text(MOVIES, encoding="utf-8")
    (directory / "ratings.csv").write_text(RATINGS, encoding="utf-8")
    (directory / "watch_history.csv").write_text("\n".join(WATCHES) + "\n", encoding="utf-8")
    return directory


@pytest.fixture(scope="module")
def spark():
    """A local session per test module. `spark_batch_job.run` stops the active session when it finishes
    (test_parity calls it), so a session shared by the whole run would be dead for the modules after it;
    `getOrCreate` hands every module a live one, which is stopped when the module ends."""
    pytest.importorskip("pyspark")
    from src.spark_batch_job import create_spark

    session = create_spark(app_name="tests")
    session.conf.set("spark.sql.shuffle.partitions", "4")
    yield session
    session.stop()  # no plain session may stay active: test_storage_equivalence needs one with Hive support


@pytest.fixture(scope="session", autouse=True)
def _stop_spark():
    yield
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()


def generate(directory: Path, dirty_rate: float = 0.0, **sizes: int) -> Path:
    """Run generate_data.py into `directory` (deterministic: seed 42) and return it."""
    import generate_data

    argv = ["generate_data.py", "--out-dir", str(directory), "--dirty-rate", str(dirty_rate)]
    for name, value in {"users": 400, "movies": 200, "ratings": 3000, "watches": 8000, **sizes}.items():
        argv += [f"--{name}", str(value)]
    old_argv, sys.argv = sys.argv, argv
    try:
        generate_data.main()
    finally:
        sys.argv = old_argv
    return directory


@pytest.fixture(scope="session")
def clean_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return generate(tmp_path_factory.mktemp("clean"))


@pytest.fixture(scope="session")
def dirty_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return generate(tmp_path_factory.mktemp("dirty"), dirty_rate=0.06)


@pytest.fixture(scope="session")
def sparse_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Few views per user: no Casual/Active segment exists, which both engines must still report."""
    return generate(tmp_path_factory.mktemp("sparse"), users=100, movies=50, ratings=100, watches=300)
