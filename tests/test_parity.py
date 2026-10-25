"""Pandas and Spark pipelines must produce exactly the same analytics tables and the same rejected counts."""
from __future__ import annotations

import pytest

from tests.helpers import pandas_tables, records, spark_tables

pytestmark = pytest.mark.spark


@pytest.mark.parametrize("dataset", ["fixture_dir", "peer_dir", "clean_dir", "dirty_dir", "sparse_dir"])
def test_pandas_and_spark_outputs_are_identical(request, spark, dataset):
    raw_dir = request.getfixturevalue(dataset)
    pandas_result, pandas_report = pandas_tables(raw_dir)
    spark_result, spark_report = spark_tables(spark, raw_dir)
    assert set(pandas_result) == set(spark_result) and len(pandas_result) == 19
    for name, table in pandas_result.items():
        assert list(table.columns) == list(spark_result[name].columns), name
        assert records(table, sort=True) == records(spark_result[name], sort=True), name
    assert pandas_report == spark_report


def test_csv_flag_still_writes_pandas_compatible_files(spark, clean_dir, tmp_path):
    import pandas as pd

    from src import batch_processing, spark_batch_job

    spark_batch_job.run(clean_dir, warehouse_dir=None, cassandra_host=None, csv_output_dir=tmp_path / "spark")
    batch_processing.run(clean_dir, tmp_path / "pandas")
    for name in pandas_names(tmp_path / "pandas"):
        left = pd.read_csv(tmp_path / "pandas" / f"{name}.csv")
        right = pd.read_csv(tmp_path / "spark" / f"{name}.csv")
        assert records(left, sort=True) == records(right, sort=True), name


def pandas_names(directory):
    return sorted(path.stem for path in directory.glob("*.csv"))
