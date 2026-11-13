"""Serving tables: both branches use src/serving_spec.py, and every column exists in the CQL schema."""
from __future__ import annotations

import re
from pathlib import Path

from src import load_serving
from src.serving_spec import GENRE_VERSIONED_TABLES, SERVING_TABLES, VERSIONED_TABLES
from tests.helpers import pandas_tables

SCHEMA = (Path(__file__).resolve().parent.parent / "sql" / "cassandra_schema.cql").read_text()


def cql_tables() -> dict[str, tuple[set[str], set[str]]]:
    """table -> (columns, primary-key columns) parsed from the schema file."""
    tables = {}
    for name, body in re.findall(r"CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\n\)", SCHEMA, re.S):
        columns = set(re.findall(r"^\s+(\w+) \w+,?(?:\s+--.*)?$", body, re.M))
        key = re.search(r"PRIMARY KEY \((.*)\)", body)
        inline = re.search(r"^\s+(\w+) \w+ PRIMARY KEY", body, re.M)
        if inline:
            columns.add(inline.group(1))
            key_columns = {inline.group(1)}
        else:
            key_columns = set(re.findall(r"\w+", key.group(1)))
        tables[name] = (columns, key_columns)
    return tables


def test_every_serving_frame_fits_its_cassandra_table(fixture_dir):
    tables, _ = pandas_tables(fixture_dir)
    frames = load_serving.serving_frames(tables, "v1")
    schema = cql_tables()
    assert set(frames) == set(SERVING_TABLES)
    for name, frame in frames.items():
        columns, key = schema[name]
        assert set(frame.columns) <= columns, (name, set(frame.columns) - columns)
        assert key <= set(frame.columns), (name, key - set(frame.columns))
        assert (frame["bucket"] == "v1").all() if "bucket" in frame else True


def test_versioned_tables_have_the_version_in_their_partition_key():
    schema = cql_tables()
    for name in (*VERSIONED_TABLES, *GENRE_VERSIONED_TABLES):
        assert "bucket" in schema[name][1], name


def test_rated_tables_only_hold_movies_with_a_weighted_rating(fixture_dir):
    tables, _ = pandas_tables(fixture_dir)
    frames = load_serving.serving_frames(tables, "v1")
    assert frames["top_rated_movies"]["weighted_rating"].notna().all()
    assert set(frames["top_rated_movies"]["movie_id"]) == {"M1", "M2", "M3"}  # M4 has no rating


def flink_types() -> dict[str, list[tuple[str, str]]]:
    """TABLES of flink/flink_batch_sql_job.py, read without importing pyflink."""
    import ast

    source = (Path(__file__).resolve().parent.parent / "flink" / "flink_batch_sql_job.py").read_text()
    for node in ast.parse(source).body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == "TABLES":
            return ast.literal_eval(node.value)
    raise AssertionError("TABLES not found in the Flink batch job")


def test_loader_types_match_the_flink_output_schema():
    kinds = {"STRING": "STRING", "INT": "INTEGER", "BIGINT": "INTEGER", "DOUBLE": "DOUBLE", "DATE": "DATE",
             "BOOLEAN": "BOOLEAN"}
    tables = flink_types()
    assert set(load_serving.ANALYTICS_TABLES) <= set(tables)
    for table in load_serving.ANALYTICS_TABLES:  # quality_report is not served
        for name, kind in tables[table]:
            assert load_serving.column_kind(name) == kinds[kind], (table, name, kind)


def test_loader_reads_the_flink_csv_output(fixture_dir, tmp_path):
    """Write the fixture tables like the Flink job (headerless CSV parts + _schema.json) and load them back."""
    import json
    from datetime import date

    tables, _ = pandas_tables(fixture_dir)
    analytics = tmp_path / "flink" / "analytics"
    schema = {}
    for name, frame in tables.items():
        (analytics / name).mkdir(parents=True)
        text = frame.copy()
        for column in text.columns:
            if load_serving.column_kind(column) == "BOOLEAN":  # Flink writes lower-case booleans
                text[column] = text[column].map({True: "true", False: "false"})
        half = len(text) // 2  # several part files, like a parallel Flink sink
        text.iloc[:half].to_csv(analytics / name / "part-0-0.csv", header=False, index=False)
        text.iloc[half:].to_csv(analytics / name / "part-1-0.csv", header=False, index=False)
        (analytics / name / ".part-2-0.inprogress").write_text("ignored")
        schema[name] = list(frame.columns)
    (analytics / "_schema.json").write_text(json.dumps(schema))

    loaded = load_serving.read_analytics(analytics)
    assert set(loaded) == set(load_serving.ANALYTICS_TABLES)
    for name, frame in loaded.items():
        assert list(frame.columns) == list(tables[name].columns)
        assert len(frame) == len(tables[name])
        for column in frame.columns:
            kind = load_serving.column_kind(column)
            expected = {"STRING": str, "INTEGER": int, "DOUBLE": float, "DATE": date, "BOOLEAN": bool}[kind]
            values = [value for value in frame[column] if value is not None]
            assert all(type(value) is expected for value in values), (name, column, kind)

    performance = loaded["movie_performance"].set_index("movie_id")
    source = tables["movie_performance"].set_index("movie_id")
    assert performance.loc["M1", "views"] == source.loc["M1", "views"]
    assert performance.loc["M4", "weighted_rating"] is None  # M4 has no rating: empty field -> None
    frames = load_serving.serving_frames(loaded, "v1")
    assert set(frames["top_rated_movies"]["movie_id"]) == {"M1", "M2", "M3"}
    rows = [tuple(load_serving.plain(v) for v in row) for row in frames["daily_views"].itertuples(index=False)]
    assert all(isinstance(row[1], date) for row in rows)
