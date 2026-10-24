"""Cassandra and Hive/Parquet must answer every dashboard query identically.

Needs the Cassandra tables and the Hive warehouse written by the same batch run. The warehouse
metastore stores container paths, so run it inside the stack:
    docker compose run --rm --no-deps -e RUN_HIVE_TESTS=1 benchmark pytest tests/test_storage_equivalence.py
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.cassandra_store import CassandraUnavailable, connect
from src.dashboard_queries import DASHBOARD_QUERIES, run_cql, run_sql

pytestmark = [pytest.mark.integration, pytest.mark.spark]


@pytest.fixture(scope="module")
def hive():
    if not os.getenv("RUN_HIVE_TESTS"):
        pytest.skip("set RUN_HIVE_TESTS=1 (inside the Docker stack) to query the Hive warehouse")
    from src.spark_batch_job import create_spark

    session = create_spark(Path(os.getenv("WAREHOUSE_DIR", "data/warehouse")), app_name="tests-hive")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def cassandra():
    try:
        return connect(timeout=5)
    except CassandraUnavailable as error:
        pytest.skip(str(error))


@pytest.mark.parametrize("query", DASHBOARD_QUERIES, ids=lambda query: query.name)
def test_query_results_are_identical(hive, cassandra, query):
    from_hive = run_sql(hive, query)
    from_cassandra = run_cql(cassandra, query)[list(from_hive.columns)]
    columns = list(from_hive.columns)
    left = from_cassandra.astype(str).sort_values(columns).reset_index(drop=True)
    right = from_hive.astype(str).sort_values(columns).reset_index(drop=True)
    assert len(left) == len(right) > 0
    assert left.equals(right)
