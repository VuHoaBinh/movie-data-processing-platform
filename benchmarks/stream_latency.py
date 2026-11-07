"""Flink vs Spark Structured Streaming: result latency, throughput and correctness on one Kafka topic.

Both engines read the same topic (`bench-events`) and compute 10-second tumbling-window top-10 trending,
written to Cassandra (`live_trending_bench_flink` via Kafka + materializer, `live_trending_bench_spark`
directly). The producer computes the expected top-10 of every window itself, so each engine's output is
checked exactly and a window's latency is the time from the window end until its complete, correct top-10
is visible in Cassandra (all clocks are the host's).

Scenarios (in this order; the burst must come first because its event times are in the past):
  burst  - events carrying past timestamps are sent as fast as possible; throughput = events / time until
           the last window's result is complete.
  <rate> - real-time events at N per second for --seconds seconds; latency per fully contained window.

Start only the base services first:
    docker compose up -d kafka kafka-init cassandra cassandra-init flink-jobmanager flink-taskmanager
    python -m benchmarks.stream_latency          (run on the host; needs the Docker CLI)
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import subprocess
import threading
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka import KafkaProducer

from benchmarks.common import percentile, write_results
from src.cassandra_store import connect

WINDOW_S = 10
TOPIC = "bench-events"
# engine -> (Cassandra table, Kafka result topic or None when the engine writes to Cassandra itself)
ENGINES = {
    "flink-python": ("live_trending_bench_flink", "bench-trending"),
    "flink-sql": ("live_trending_bench_flinksql", "bench-trending-sql"),
    "spark": ("live_trending_bench_spark", None),
}
DDL = (
    "CREATE TABLE IF NOT EXISTS {table} (day date, window_start timestamp, rank int, movie_id text, title text, "
    "genre text, live_views bigint, unique_viewers bigint, PRIMARY KEY ((day), window_start, rank)) "
    "WITH CLUSTERING ORDER BY (window_start DESC, rank ASC)"
)
CONTAINERS = ("bench-flink", "bench-materializer", "bench-flink-sql", "bench-materializer-sql", "bench-spark")


def run(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(list(args), check=check, capture_output=True, text=True)


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return run("docker", "compose", *args, check=check)


def load_movies(limit: int = 2000) -> list[tuple[str, str, str]]:
    with Path("data/movies.csv").open(encoding="utf-8-sig", newline="") as handle:
        return [(row["movie_id"], row["title"], row["genre"]) for row in csv.DictReader(handle)][:limit]


def setup(session) -> None:
    teardown()
    kafka = ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092"]
    topics = [TOPIC] + [topic for _, topic in ENGINES.values() if topic]
    for topic in topics:
        compose(*kafka, "--delete", "--topic", topic, check=False)
    time.sleep(3)
    for topic in topics:
        compose(*kafka, "--create", "--if-not-exists", "--topic", topic, "--partitions", "1", "--replication-factor", "1")
    for table, _ in ENGINES.values():
        session.execute(DDL.format(table=table))
        session.execute(f"TRUNCATE {table}")
    window = {"TREND_WINDOW_SECONDS": str(WINDOW_S)}
    env = lambda values: [arg for key, value in values.items() for arg in ("-e", f"{key}={value}")]
    flink_command = (
        "until curl -fsS http://flink-jobmanager:8081/overview >/dev/null; do sleep 2; done; "
        "flink run -m flink-jobmanager:8081 -py /opt/flink/usrlib/%s"
    )
    for name, script, container, materializer in (
        ("flink-python", "flink_trending_job.py", "bench-flink", "bench-materializer"),
        ("flink-sql", "flink_trending_sql_job.py", "bench-flink-sql", "bench-materializer-sql"),
    ):
        table, topic = ENGINES[name]
        compose("run", "-d", "--no-deps", "--name", container,
                *env({**window, "KAFKA_WATCH_TOPIC": TOPIC, "KAFKA_TREND_TOPIC": topic}),
                "flink-trending-job", "bash", "-lc", flink_command % script)
        compose("run", "-d", "--no-deps", "--name", materializer,
                *env({"KAFKA_TREND_TOPIC": topic, "TRENDING_TABLE": table, "MATERIALIZER_GROUP": f"bench-{name}"}),
                "trending-materializer")
    compose("--profile", "alt", "run", "-d", "--no-deps", "--name", "bench-spark",
            *env({**window, "KAFKA_WATCH_TOPIC": TOPIC, "TRENDING_TABLE": ENGINES["spark"][0],
                  "STARTING_OFFSETS": "earliest", "TRIGGER_SECONDS": "1",
                  "CHECKPOINT_DIR": "/tmp/bench-checkpoint"}), "spark-streaming")
    deadline = time.time() + 240
    while time.time() < deadline:  # wait for both Flink jobs, then give Spark time to start its query
        jobs = json.load(urllib.request.urlopen("http://localhost:8081/jobs/overview"))["jobs"]
        if sum(job["state"] == "RUNNING" for job in jobs) >= 2:
            break
        time.sleep(2)
    else:
        raise RuntimeError("Flink jobs did not start")
    time.sleep(45)


def teardown() -> None:
    for name in CONTAINERS:
        run("docker", "rm", "-f", name, check=False)


class Poller(threading.Thread):
    """Poll one Cassandra table and remember when each window's rows changed."""

    def __init__(self, session, table: str, days: list):
        super().__init__(daemon=True)
        self.statement = session.prepare(f"SELECT window_start, rank, movie_id, live_views, unique_viewers FROM {table} WHERE day = ?")
        self.session, self.days, self.stop = session, days, threading.Event()
        self.history: dict[int, list[tuple[float, tuple]]] = defaultdict(list)

    def run(self) -> None:
        while not self.stop.is_set():
            groups: dict[int, list] = defaultdict(list)
            for day in self.days:
                for row in self.session.execute(self.statement, [day]):
                    groups[int(row["window_start"].replace(tzinfo=timezone.utc).timestamp() * 1000)].append(
                        (row["rank"], row["movie_id"], row["live_views"], row["unique_viewers"])
                    )
            seen = time.time()
            for window, rows in groups.items():
                signature = tuple(sorted(rows))
                if not self.history[window] or self.history[window][-1][1] != signature:
                    self.history[window].append((seen, signature))
            time.sleep(0.05)


class Truth:
    """Expected per-window top-10, accumulated from the events the producer sent."""

    def __init__(self) -> None:
        self.views: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.users: dict[int, dict[str, set]] = defaultdict(lambda: defaultdict(set))
        self.titles: dict[str, str] = {}

    def add(self, timestamp_ms: int, movie_id: str, title: str, user: str) -> None:
        window = timestamp_ms // (WINDOW_S * 1000) * WINDOW_S * 1000
        self.views[window][movie_id] += 1
        self.users[window][movie_id].add(user)
        self.titles[movie_id] = title

    def top10(self, window: int) -> tuple:
        movies = self.views[window]
        ranked = sorted(movies, key=lambda m: (-movies[m], -len(self.users[window][m]), self.titles[m]))[:10]
        return tuple(sorted(
            (rank, movie, movies[movie], len(self.users[window][movie])) for rank, movie in enumerate(ranked, 1)
        ))


def make_sender(producer: KafkaProducer, movies, truth: Truth, rng: random.Random):
    # Pre-encoded JSON fragments keep the producer fast enough to be faster than the engines under test.
    fragments = [(m[0], m[1], json.dumps(m[1]), json.dumps(m[2])) for m in movies]
    count = len(movies)

    def send(timestamp_ms: int, serial: int) -> None:
        movie_id, title, title_json, genre_json = fragments[int(count * rng.random() ** 2)]
        user = "U%05d" % rng.randint(1, 50_000)
        payload = (
            '{"watch_id": "B%d", "user_id": "%s", "movie_id": "%s", "title": %s, "genre": %s}'
            % (serial, user, movie_id, title_json, genre_json)
        ).encode()
        producer.send(TOPIC, key=movie_id.encode(), value=payload, timestamp_ms=timestamp_ms)
        truth.add(timestamp_ms, movie_id, title, user)
    return send


def completion_times(poller: Poller, truth: Truth, windows: list[int]) -> dict[int, float | None]:
    result: dict[int, float | None] = {}
    for window in windows:
        expected = truth.top10(window)
        result[window] = next((seen for seen, rows in list(poller.history.get(window, [])) if rows == expected), None)
    return result


def summarize_rows(engine: str, scenario: str, events: int, achieved: float, windows: list[int],
                   done: dict[int, float | None], window_end_s) -> dict:
    latencies = [done[w] - window_end_s(w) for w in windows if done[w] is not None]
    row = {"engine": engine, "scenario": scenario, "events": events, "achieved_events_per_s": round(achieved),
           "windows": len(windows), "windows_exact": len(latencies)}
    if latencies:
        row.update(p50_latency_s=round(percentile(latencies, 50), 2), p95_latency_s=round(percentile(latencies, 95), 2),
                   max_latency_s=round(max(latencies), 2))
    else:
        row.update(p50_latency_s="", p95_latency_s="", max_latency_s="")
    return row


class Run:
    """Producer side of the benchmark: sends events, keeps the ground truth, waits for the engines."""

    def __init__(self, producer: KafkaProducer, pollers: dict[str, Poller], movies) -> None:
        self.truth = Truth()
        self.producer, self.pollers = producer, pollers
        self.send = make_sender(producer, movies, self.truth, random.Random(11))
        self.serial = 0

    def emit(self, timestamp_ms: int) -> None:
        self.send(timestamp_ms, self.serial)
        self.serial += 1

    def realtime(self, rate: int, seconds: int) -> tuple[list[int], int, float]:
        """Send `rate` events/s with current timestamps; return the fully contained windows."""
        first, sent = time.time(), 0
        while (elapsed := time.time() - first) < seconds:
            target = int(rate * elapsed)
            while sent < target:
                self.emit(int(time.time() * 1000))
                sent += 1
            time.sleep(0.002)
        self.producer.flush()
        length = time.time() - first
        step = WINDOW_S * 1000
        start_w = (int(first * 1000) // step + 2) * step  # skip the partially filled first window
        end_w = int(time.time() * 1000) // step * step - step
        return list(range(start_w, end_w, step)), sent, sent / length

    def drain(self, windows: list[int], timeout: float = 300.0) -> None:
        """Trickle 5 events/s (their later timestamps close the open windows) until both engines are exact."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            self.emit(int(time.time() * 1000))
            if all(all(completion_times(p, self.truth, windows).values()) for p in self.pollers.values()):
                return
            time.sleep(0.2)

    def report_mismatches(self, engine: str, windows: list[int]) -> None:
        poller = self.pollers[engine]
        for window in windows:
            if completion_times(poller, self.truth, [window])[window] is None:
                seen = poller.history.get(window, [])
                print(f"  {engine} window {window}: expected {self.truth.top10(window)[:2]} ... "
                      f"last seen {seen[-1][1][:2] if seen else 'nothing'}", flush=True)
                return


def main() -> None:
    parser = argparse.ArgumentParser(description="Flink vs Spark Structured Streaming benchmark.")
    parser.add_argument("--rates", type=int, nargs="*", default=[250, 500, 1000, 2000, 4000, 8000])
    parser.add_argument("--seconds", type=int, default=60, help="duration of each real-time scenario")
    parser.add_argument("--burst-events", type=int, default=100_000)
    parser.add_argument("--keep", action="store_true", help="leave the benchmark containers running")
    args = parser.parse_args()

    session = connect()
    setup(session)
    producer = KafkaProducer(bootstrap_servers="localhost:9092", acks=1, linger_ms=5, batch_size=64_000)
    now = datetime.now(timezone.utc)
    days = sorted({(now - timedelta(hours=2)).date(), now.date(), (now + timedelta(hours=1)).date()})
    pollers = {name: Poller(session, table, days) for name, (table, _) in ENGINES.items()}
    for poller in pollers.values():
        poller.start()
    run = Run(producer, pollers, load_movies())
    rows: list[dict] = []
    try:
        # ---- burst: past event times, as fast as possible (must be the first scenario)
        burst_windows = 20
        step = WINDOW_S * 1000
        base = (int(time.time() * 1000) - 3_600_000) // step * step
        span = burst_windows * step
        started = time.time()
        for index in range(args.burst_events):
            run.emit(base + index * span // args.burst_events)
        run.emit(base + span + 1000)  # closes the last burst window
        producer.flush()
        produced = time.time()
        windows = [base + i * step for i in range(burst_windows)]
        run.drain(windows)
        for name, poller in pollers.items():
            done = completion_times(poller, run.truth, windows)
            finish = max(done.values()) if all(done.values()) else None
            row = summarize_rows(name, "burst", args.burst_events, 0, windows, done, lambda w: started)
            row.update(
                scenario=f"burst {args.burst_events:,} events (produced in {produced - started:.0f}s)",
                achieved_events_per_s=round(args.burst_events / (finish - started)) if finish else 0,
                p50_latency_s="", p95_latency_s="", max_latency_s=round(finish - produced, 1) if finish else "",
            )
            rows.append(row)
            run.report_mismatches(name, windows)
            print(row, flush=True)

        # ---- real-time rates; each scenario starts only when both engines have caught up
        for rate in args.rates:
            windows, sent, achieved = run.realtime(rate, args.seconds)
            run.drain(windows)
            print(f"rate {rate}: sent {sent:,} ({achieved:,.0f}/s)", flush=True)
            for name, poller in pollers.items():
                done = completion_times(poller, run.truth, windows)
                rows.append(summarize_rows(name, f"{rate} events/s x {args.seconds}s", sent, achieved, windows, done,
                                           lambda w: (w + step) / 1000))
                run.report_mismatches(name, windows)
                print(rows[-1], flush=True)
    finally:
        for poller in pollers.values():
            poller.stop.set()
        if not args.keep:
            teardown()
    write_results(
        "stream_latency", rows, "Flink vs Spark Structured Streaming (10 s tumbling windows, top-10 per window)",
        [
            "Same Kafka topic, same events, both engines running concurrently on one Docker Desktop (WSL2) host; "
            "flink-python = the project's PyFlink DataStream job and flink-sql = the Flink SQL tumbling-window job "
            "(Flink 2.1, parallelism 1, event time = Kafka record timestamp, monotonous watermarks) "
            "-> Kafka -> materializer -> Cassandra; Spark 3.5.3 Structured Streaming (local[*], 1 s trigger, "
            "0 s watermark) -> Cassandra.",
            "Real-time rows: latency = time from the window end until its complete top-10 equals the expected "
            "result computed by the producer; `windows_exact` of `windows` checks exactness. Each scenario starts "
            "after both engines drained the previous one (a 5 events/s trickle closes open windows); "
            "`achieved_events_per_s` is the producer's rate.",
            "Burst row: `achieved_events_per_s` = events / time from the first event until the engine's last "
            "window was complete (processing throughput, 0 = not finished within 5 min); `max_latency_s` = time "
            "from the end of production until the last window was complete (backlog drain time).",
        ],
    )


if __name__ == "__main__":
    main()
