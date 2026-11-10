"""End-to-end streaming latency read back from Cassandra.

Live rows carry `last_sent_at` (wall-clock send time of the newest event included, set by the producer
as sent_at_ms) and `updated_at` (Cassandra write time set by the engine/materializer).
latency = updated_at - last_sent_at. Each sample keeps only rows whose updated_at moved since the
previous sample, so every write is counted once.

Caveats (state them in the report):
  * live_trending*: a 5-minute window can only be emitted once the watermark passes its end
    (WATERMARK_SECONDS = 60 simulated seconds, i.e. 60/speed real seconds) so its latency includes
    that wait; it is not comparable to the engagement tables. Rows are re-written while a window is
    open or re-ranked, only the written version is measured.
  * Engagement rows are upserts: latency is that of the newest event folded into the row.
  * updated_at and last_sent_at come from different processes; clock skew on one host is negligible,
    across hosts it is not.
  * The sampling interval can miss intermediate rewrites of the same row (undercounting, not bias).
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

BENCH_DIR = Path(__file__).resolve().parent
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "movie_analytics")
# table -> (engine, key columns identifying a row)
TABLES: Dict[str, Tuple[str, Tuple[str, ...]]] = {
    "live_trending": ("flink", ("window_start", "rank")),
    "live_trending_spark": ("spark", ("window_start", "rank")),
    "live_user_engagement": ("flink", ("user_id",)),
    "live_user_engagement_spark": ("spark", ("user_id",)),
    "live_movie_engagement": ("flink", ("movie_id",)),
    "live_movie_engagement_spark": ("spark", ("movie_id",)),
}


def percentile(values: Sequence[float], q: float) -> float:
    """Linear-interpolated percentile, q in [0, 100]."""
    if not values:
        return float("nan")
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q / 100.0
    low = int(pos)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def to_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def latency_seconds(updated_at: Optional[datetime], last_sent_at: Optional[datetime]) -> Optional[float]:
    if updated_at is None or last_sent_at is None:
        return None
    return (to_utc(updated_at) - to_utc(last_sent_at)).total_seconds()


def summarize(samples: Iterable[dict]) -> List[dict]:
    groups: Dict[Tuple[str, str], List[float]] = {}
    for s in samples:
        groups.setdefault((s["table"], s["engine"]), []).append(float(s["latency_s"]))
    out = []
    for (table, engine), vals in sorted(groups.items()):
        out.append({"table": table, "engine": engine, "count": len(vals),
                    "p50": percentile(vals, 50), "p95": percentile(vals, 95),
                    "p99": percentile(vals, 99), "max": max(vals), "min": min(vals)})
    return out


def collect_new(rows: Iterable[dict], key_cols: Sequence[str], seen: Dict[tuple, datetime],
                table: str, engine: str, sample_time: datetime) -> List[dict]:
    """Return samples for rows whose updated_at is newer than what `seen` recorded; updates `seen`."""
    samples = []
    for row in rows:
        updated = row.get("updated_at")
        lat = latency_seconds(updated, row.get("last_sent_at"))
        if updated is None or lat is None:
            continue
        key = tuple(str(row.get(c)) for c in key_cols)
        if key in seen and seen[key] >= updated:
            continue
        seen[key] = updated
        samples.append({"sampled_at": sample_time.isoformat(), "table": table, "engine": engine,
                        "key": "|".join(key), "updated_at": to_utc(updated).isoformat(),
                        "last_sent_at": to_utc(row["last_sent_at"]).isoformat(), "latency_s": round(lat, 3)})
    return samples


def fetch_day(session, table: str, day: date) -> List[dict]:
    cols = "last_sent_at, updated_at, " + ", ".join(TABLES[table][1])
    return list(session.execute(f"SELECT {cols} FROM {KEYSPACE}.{table} WHERE day = %s", [day]))


def resolve_day(session, explicit: Optional[str]) -> date:
    if explicit:
        return date.fromisoformat(explicit)
    row = session.execute(f"SELECT replay_day FROM {KEYSPACE}.replay_clock WHERE id = 'demo'").one()
    if row is None:
        raise SystemExit("replay_clock is empty: start the producer or pass --day YYYY-MM-DD")
    value = row["replay_day"] if isinstance(row, dict) else row.replay_day
    return value if isinstance(value, date) and not hasattr(value, "date") else value.date()


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--duration", type=float, default=300, help="seconds to sample")
    p.add_argument("--interval", type=float, default=2.0, help="seconds between samples")
    p.add_argument("--label", default="run", help="run label, e.g. 'speed60'")
    p.add_argument("--day", help="UTC day to read (default: replay_day from replay_clock)")
    p.add_argument("--tables", nargs="+", default=list(TABLES), choices=list(TABLES))
    p.add_argument("--results-dir", type=Path, default=BENCH_DIR / "results")
    p.add_argument("--include-existing", action="store_true",
                   help="count rows already present at the first sample (default: ignore them)")
    args = p.parse_args(argv)

    sys.path.insert(0, str(BENCH_DIR.parent))
    from src.cassandra_store import connect  # same connection settings as the project (CASSANDRA_HOST, ...)

    session = connect()
    day = resolve_day(session, args.day)
    args.results_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    samples_path = args.results_dir / f"latency_{args.label}_{stamp}.csv"
    summary_path = args.results_dir / f"latency_{args.label}_{stamp}_summary.csv"
    print(f"Sampling {args.tables} for day {day} during {args.duration:.0f}s every {args.interval:.1f}s")

    seen: Dict[str, Dict[tuple, datetime]] = {t: {} for t in args.tables}
    samples: List[dict] = []
    deadline = time.monotonic() + args.duration
    first = True
    while True:
        now = datetime.now(timezone.utc)
        for table in args.tables:
            engine, keys = TABLES[table]
            rows = [dict(r) if isinstance(r, dict) else r._asdict() for r in fetch_day(session, table, day)]
            new = collect_new(rows, keys, seen[table], table, engine, now)
            if not (first and not args.include_existing):  # first pass only primes `seen`
                samples.extend(new)
        first = False
        if time.monotonic() >= deadline:
            break
        time.sleep(max(0.0, min(args.interval, deadline - time.monotonic())))

    fields = ["sampled_at", "table", "engine", "key", "updated_at", "last_sent_at", "latency_s"]
    with samples_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(samples)
    summary = summarize(samples)
    with summary_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["label", "table", "engine", "count", "p50", "p95", "p99", "max", "min"])
        writer.writeheader()
        for row in summary:
            writer.writerow({"label": args.label, **{k: (round(v, 3) if isinstance(v, float) else v) for k, v in row.items()}})
    print(f"\n{'table':<30}{'engine':<7}{'n':>7}{'p50':>9}{'p95':>9}{'p99':>9}{'max':>9}  (seconds)")
    for r in summary:
        print(f"{r['table']:<30}{r['engine']:<7}{r['count']:>7}{r['p50']:>9.2f}{r['p95']:>9.2f}{r['p99']:>9.2f}{r['max']:>9.2f}")
    if not summary:
        print("No updated rows observed: is the producer running and the speed layer up?")
    print("\nNote: live_trending* latency includes the watermark wait (60 simulated s) after the window end.")
    print(f"Samples: {samples_path}\nSummary: {summary_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
