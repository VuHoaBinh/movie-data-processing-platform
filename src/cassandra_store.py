"""Thin Cassandra access layer shared by the dashboard, materializer and tests."""
from __future__ import annotations

import argparse
import os
import re
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

from cassandra.cluster import Cluster, Session
from cassandra.query import dict_factory

if TYPE_CHECKING:
    import pandas as pd

KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "movie_analytics")
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "cassandra_schema.cql"
BATCH_BUCKET = "all"


class CassandraUnavailable(RuntimeError):
    """Raised when the cluster cannot be reached, with a message fit for the UI."""


def contact_points() -> list[str]:
    return os.getenv("CASSANDRA_HOST", "localhost").split(",")


def connect(keyspace: str | None = KEYSPACE, timeout: float = 30.0) -> Session:
    # The default reactor works on Python 3.12 with cassandra-driver 3.29.2 (AsyncioConnection times out there).
    cluster = Cluster(
        contact_points(),
        port=int(os.getenv("CASSANDRA_PORT", "9042")),
        connect_timeout=timeout,
        protocol_version=5,
    )
    try:
        session = cluster.connect(keyspace) if keyspace else cluster.connect()
    except Exception as error:
        cluster.shutdown()
        raise CassandraUnavailable(
            f"Cannot reach Cassandra at {','.join(contact_points())}: {error}"
        ) from error
    session.row_factory = dict_factory
    session.default_fetch_size = 5000
    session.default_timeout = 60.0
    return session


def wait_for_cassandra(retries: int = 60, delay: float = 3.0) -> Session:
    for attempt in range(retries):
        try:
            return connect(keyspace=None)
        except CassandraUnavailable:
            if attempt == retries - 1:
                raise
            time.sleep(delay)
    raise CassandraUnavailable("unreachable")


# Tables whose rows cannot be rebuilt by the batch (TTL'd stream windows, run history): a changed definition is
# reported instead of being dropped.
KEEP_ON_CHANGE = frozenset({"live_trending", "live_trending_spark", "batch_runs"})
CREATE_TABLE = re.compile(r"^CREATE TABLE IF NOT EXISTS\s+(\w+)", re.IGNORECASE)


class SchemaConflict(RuntimeError):
    """A protected table exists with a different definition than sql/cassandra_schema.cql."""


def schema_statements(path: Path = SCHEMA_PATH, keyspace: str = KEYSPACE) -> list[str]:
    """The statements of the CQL file, with the keyspace name replaced when a different one is wanted."""
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if not line.strip().startswith("--")]
    statements = [statement.strip() for statement in "\n".join(lines).split(";") if statement.strip()]
    return [statement.replace("movie_analytics", keyspace) for statement in statements]


def _columns(session: Session, keyspace: str, table: str) -> set[tuple]:
    rows = session.execute(
        "SELECT column_name, kind, type, clustering_order, position FROM system_schema.columns "
        "WHERE keyspace_name = %s AND table_name = %s",
        [keyspace, table],
    )
    return {(row["column_name"], row["kind"], row["type"], row["clustering_order"], row["position"]) for row in rows}


def changed_tables(session: Session, path: Path = SCHEMA_PATH, keyspace: str = KEYSPACE) -> list[str]:
    """Existing tables whose columns, primary key or clustering order differ from the CQL file.

    Each definition is created in a scratch keyspace and compared with the live one in system_schema, so the
    comparison needs no CQL parsing and sees every property that decides how rows are stored.
    """
    scratch = f"{keyspace}_schema_check"
    session.execute(f"DROP KEYSPACE IF EXISTS {scratch}")
    session.execute(
        f"CREATE KEYSPACE {scratch} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
    )
    changed: list[str] = []
    try:
        for statement in schema_statements(path, keyspace):
            match = CREATE_TABLE.match(statement)
            if not match:
                continue
            table = match.group(1)
            live = _columns(session, keyspace, table)
            if not live:
                continue  # new table: plain CREATE below
            session.execute(statement.replace(f"EXISTS {table}", f"EXISTS {scratch}.{table}", 1))
            if live != _columns(session, scratch, table):
                changed.append(table)
    finally:
        session.execute(f"DROP KEYSPACE IF EXISTS {scratch}")
    return changed


def apply_schema(session: Session, path: Path = SCHEMA_PATH, keyspace: str = KEYSPACE) -> list[str]:
    """Create what is missing and rebuild serving tables whose definition changed; returns the rebuilt tables.

    `CREATE TABLE IF NOT EXISTS` never alters an existing table, so a volume created by an older version of
    the schema would keep the old primary key and the batch writes would fail. Serving tables are derived
    data (the batch truncates and refills them), so a changed one is dropped and recreated empty; the next
    batch run fills it. Tables in KEEP_ON_CHANGE are never dropped.
    """
    statements = schema_statements(path, keyspace)
    session.execute(statements[0])  # CREATE KEYSPACE IF NOT EXISTS
    rebuilt = changed_tables(session, path, keyspace)
    blocked = [table for table in rebuilt if table in KEEP_ON_CHANGE]
    if blocked:
        raise SchemaConflict(
            f"{', '.join(blocked)} exist with a different definition and hold data the batch cannot rebuild; "
            "migrate them by hand or drop the Cassandra volume (docker compose down -v)"
        )
    for table in rebuilt:
        session.execute(f"DROP TABLE {keyspace}.{table}")
    for statement in statements:
        if statement.upper().startswith("USE "):
            session.set_keyspace(statement.split()[1])
        else:
            session.execute(statement)
    return rebuilt


def query_frame(session: Session, cql: str, params: Sequence[Any] | None = None) -> "pd.DataFrame":
    import pandas as pd  # not installed in the stream image

    return pd.DataFrame(list(session.execute(cql, params or [])))


def table_counts(session: Session, tables: Sequence[str]) -> dict[str, int]:
    return {table: session.execute(f"SELECT COUNT(*) AS n FROM {table}").one()["n"] for table in tables}


def main() -> None:
    parser = argparse.ArgumentParser(description="Cassandra schema helper.")
    parser.add_argument("command", choices=["init"], help="init: wait for Cassandra and apply the schema")
    parser.parse_args()
    session = wait_for_cassandra()
    rebuilt = apply_schema(session)
    print(f"Schema applied to keyspace {KEYSPACE}" + (f"; rebuilt changed tables: {', '.join(rebuilt)}" if rebuilt else ""))


if __name__ == "__main__":
    main()
