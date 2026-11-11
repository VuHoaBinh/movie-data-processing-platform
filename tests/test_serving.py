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
