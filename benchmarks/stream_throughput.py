"""Throughput of the speed layer at one replay speed: produced vs processed events per second.

Produced  = `sent` counter of table replay_clock (events written to Kafka by the producer).
Processed = sum of `views` over the replay day in live_movie_engagement (Flink) and
            live_movie_engagement_spark (Spark). Engagement tables count every event once, so the sum equals
            the number of events folded in (events dropped as late/invalid are not counted).
Lag       = produced - processed at the sample time (events). Rates use the first and last sample of the
            steady window and the per-interval rates are written to the CSV.

Start this script just BEFORE (or right after) `python -m src.kafka_event_producer --speed X --reset`.
If the live tables already hold data from a previous run, either use a fresh stack or rely on the
baseline taken at the first sample (processed counts are reported relative to it).
Heartbeats are not events and are not counted by the producer's `sent`.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

BENCH_DIR = Path(__file__).resolve().parent
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "movie_analytics")
ENGINE_TABLES = {"flink": "live_movie_engagement", "spark": "live_movie_engagement_spark"}


def rate(delta: float, seconds: float) -> float:
    return delta / seconds if seconds > 0 else float("nan")


def analyse(samples: List[dict], engines: Sequence[str]) -> Dict[str, float]:
    """Overall rates between first and last sample (processed relative to the first sample's baseline)."""
    if len(samples) < 2:
        return {}
    a, b = samples[0], samples[-1]
    seconds = b["t"] - a["t"]
    out = {"seconds": seconds, "produced_per_s": rate(b["sent"] - a["sent"], seconds),
           "final_sent": b["sent"], "final_total": b["total"]}
    for e in engines:
        out[f"{e}_processed_per_s"] = rate(b[e] - a[e], seconds)
        out[f"{e}_final_lag"] = b["sent"] - b[e]
        out[f"{e}_max_lag"] = max(s["sent"] - s[e] for s in samples)
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--label", required=True, help="e.g. 'speed60'")
    p.add_argument("--duration", type=float, default=120)
    p.add_argument("--interval", type=float, default=5.0)
    p.add_argument("--day", help="UTC day (default: replay_day from replay_clock)")
    p.add_argument("--engines", nargs="+", default=["spark", "flink"], choices=list(ENGINE_TABLES))
    p.add_argument("--until-done", action="store_true", help="stop early when the producer has sent everything "
                   "and the lag of every engine is 0 (duration stays an upper bound)")
    p.add_argument("--results-dir", type=Path, default=BENCH_DIR / "results")
    args = p.parse_args(argv)

    sys.path.insert(0, str(BENCH_DIR.parent))
    from src.cassandra_store import connect

    session = connect()
    clock = session.execute(f"SELECT replay_day, speed, sent, total FROM {KEYSPACE}.replay_clock WHERE id='demo'").one()
    if clock is None:
        raise SystemExit("replay_clock is empty: start the producer first")
    get = (lambda r, k: r[k]) if isinstance(clock, dict) else getattr
    day = date.fromisoformat(args.day) if args.day else get(clock, "replay_day")
    day = day if not hasattr(day, "date") else day.date()
    speed = float(get(clock, "speed"))
    args.results_dir.mkdir(parents=True, exist_ok=True)
    out = args.results_dir / f"throughput_{args.label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    def processed(engine: str) -> int:
        row = session.execute(f"SELECT sum(views) AS n FROM {KEYSPACE}.{ENGINE_TABLES[engine]} WHERE day = %s", [day]).one()
        value = (row["n"] if isinstance(row, dict) else row.n) if row else 0
        return int(value or 0)

    samples: List[dict] = []
    start = time.monotonic()
    print(f"label={args.label} speed={speed:g} day={day}")
    while True:
        c = session.execute(f"SELECT sent, total FROM {KEYSPACE}.replay_clock WHERE id='demo'").one()
        sent = int((c["sent"] if isinstance(c, dict) else c.sent) or 0)
        total = int((c["total"] if isinstance(c, dict) else c.total) or 0)
        sample = {"t": time.monotonic() - start, "wall": datetime.now(timezone.utc).isoformat(), "sent": sent, "total": total}
        for e in args.engines:
            sample[e] = processed(e)
        samples.append(sample)
        lags = " ".join(f"{e}_lag={sent - sample[e]}" for e in args.engines)
        print(f"t={sample['t']:6.1f}s sent={sent}/{total} " + " ".join(f"{e}={sample[e]}" for e in args.engines) + f" {lags}", flush=True)
        if time.monotonic() - start >= args.duration:
            break
        if args.until_done and total and sent >= total and all(sent - sample[e] <= 0 for e in args.engines):
            break
        time.sleep(args.interval)

    fields = ["label", "speed", "t_s", "wall", "sent", "total"] + [f"{e}_processed" for e in args.engines] + \
             [f"{e}_lag" for e in args.engines] + ["produced_per_s"] + [f"{e}_processed_per_s" for e in args.engines]
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        prev = None
        for s in samples:
            row = {"label": args.label, "speed": speed, "t_s": round(s["t"], 2), "wall": s["wall"],
                   "sent": s["sent"], "total": s["total"]}
            dt = s["t"] - prev["t"] if prev else 0
            row["produced_per_s"] = round(rate(s["sent"] - prev["sent"], dt), 2) if prev else ""
            for e in args.engines:
                row[f"{e}_processed"] = s[e]
                row[f"{e}_lag"] = s["sent"] - s[e]
                row[f"{e}_processed_per_s"] = round(rate(s[e] - prev[e], dt), 2) if prev else ""
            writer.writerow(row)
            prev = s
    summary = analyse(samples, args.engines)
    print("\nSummary (first to last sample):")
    for k, v in summary.items():
        print(f"  {k}: {v:.2f}" if isinstance(v, float) else f"  {k}: {v}")
    print("Caveat: processed counts include only events the engine accepted (late events are dropped).")
    print(f"Samples: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
