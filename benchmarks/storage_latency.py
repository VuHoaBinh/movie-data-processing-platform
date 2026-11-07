"""Dashboard query latency: Cassandra vs Hive/Parquet (Spark SQL), plus Cassandra concurrent throughput.

Run inside the Docker stack, after the batch loaded both stores:
    docker compose --profile tools run --rm --no-deps benchmark python -m benchmarks.storage_latency
"""
from __future__ import annotations

import argparse
import random
import threading
import time
from pathlib import Path

from benchmarks.common import RESULTS_DIR, summarize, write_results
from src.cassandra_store import connect
from src.dashboard_queries import ADHOC_SQL, DASHBOARD_QUERIES, DashboardQuery
from src.spark_batch_job import create_spark

USERS = 50_000
RANDOM_USER_QUERY = DashboardQuery(
    "user_favorite_genres_random_user", "BR3", "point",
    "SELECT favorite_genres FROM user_preferences WHERE user_id = %s",
    "SELECT favorite_genres FROM analytics.user_preferences WHERE user_id = '{user}'",
)


def time_calls(call, iterations: int, warmup: int) -> list[float]:
    for _ in range(warmup):
        call()
    samples = []
    for _ in range(iterations):
        started = time.perf_counter()
        call()
        samples.append((time.perf_counter() - started) * 1000)
    return samples


def cassandra_throughput(session, threads: int, seconds: float) -> float:
    """Queries per second for a mixed point / top-N workload from several client threads."""
    point = session.prepare("SELECT favorite_genres FROM user_preferences WHERE user_id = ?")
    top = session.prepare("SELECT title, genre, weighted_rating FROM top_rated_movies WHERE bucket = 'all' LIMIT 10")
    counts = [0] * threads
    stop = time.perf_counter() + seconds

    def worker(index: int) -> None:
        rng = random.Random(index)
        while time.perf_counter() < stop:
            if rng.random() < 0.7:
                list(session.execute(point, [f"U{rng.randint(1, USERS):05}"]))
            else:
                list(session.execute(top))
            counts[index] += 1

    pool = [threading.Thread(target=worker, args=(index,)) for index in range(threads)]
    for thread in pool:
        thread.start()
    for thread in pool:
        thread.join()
    return sum(counts) / seconds


def main() -> None:
    parser = argparse.ArgumentParser(description="Cassandra vs Hive/Parquet dashboard query latency.")
    parser.add_argument("--cassandra-iterations", type=int, default=200)
    parser.add_argument("--hive-iterations", type=int, default=20)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--warehouse-dir", type=Path, default=Path("data/warehouse"))
    args = parser.parse_args()

    session = connect()
    spark = create_spark(args.warehouse_dir, app_name="storage-latency")
    rng = random.Random(7)
    rows = []
    for query in [*DASHBOARD_QUERIES, RANDOM_USER_QUERY]:
        random_user = query is RANDOM_USER_QUERY
        prepared = session.prepare(query.cql.replace("%s", "?"))

        def run_cassandra() -> None:
            params = [f"U{rng.randint(1, USERS):05}"] if random_user else list(query.params)
            list(session.execute(prepared, params))

        def run_hive() -> None:
            sql = query.sql.format(user=f"U{rng.randint(1, USERS):05}") if random_user else query.sql
            spark.sql(sql).collect()

        scan = query.kind == "scan"
        samples = {
            "cassandra": time_calls(run_cassandra, 30 if scan else args.cassandra_iterations, 10),
            "hive_parquet_sparksql": time_calls(run_hive, args.hive_iterations, 3),
        }
        for store, values in samples.items():
            rows.append({"query": query.name, "br": query.br, "kind": query.kind, "store": store, **summarize(values)})
        print(f"{query.name}: cassandra p50 {rows[-2]['p50_ms']} ms, hive p50 {rows[-1]['p50_ms']} ms", flush=True)

    adhoc = time_calls(lambda: spark.sql(ADHOC_SQL).collect(), args.hive_iterations, 3)
    rows.append({"query": "adhoc_week_from_curated_events", "br": "ad hoc", "kind": "scan",
                 "store": "hive_parquet_sparksql", **summarize(adhoc)})
    qps = cassandra_throughput(session, args.threads, 10.0)
    notes = [
        "Cassandra 5.0 (1 node, RF=1, prepared statements, python cassandra-driver) vs Spark 3.5.3 SQL on Parquet "
        "Hive tables (`analytics.*`, local[*], one warm session, results collected to the driver); same "
        f"container, same machine. Latency includes fetching every row. Cassandra: {args.cassandra_iterations} "
        f"runs (30 for scans), Hive: {args.hive_iterations} runs, after warm-up.",
        f"Cassandra concurrent throughput: {qps:,.0f} queries/s with {args.threads} client threads "
        "(70% random-user point lookups, 30% top-10 lists, 10 s).",
        "`adhoc_week_from_curated_events` has no Cassandra table: it aggregates one week of "
        "`curated.watch_events` (partition pruning by watch_date).",
    ]
    write_results("storage_latency", rows, "Dashboard query latency: Cassandra vs Hive/Parquet", notes)
    (RESULTS_DIR / "storage_throughput_qps.txt").write_text(f"{qps:.0f}\n", encoding="utf-8")
    spark.stop()


if __name__ == "__main__":
    main()
