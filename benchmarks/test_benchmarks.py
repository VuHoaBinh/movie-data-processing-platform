"""Offline checks of the parsing/summary helpers: python benchmarks/test_benchmarks.py"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import batch_benchmark as bb
import resource_usage as ru
import stream_latency as sl
import stream_throughput as st


def test_all() -> None:
    assert bb.parse_timings("x\nTIMING read 1.5\nTIMING write 2.0\nTIMING read 0.5\nTIMING bad\n") == {"read": 2.0, "write": 2.0}
    assert bb.scaled_sizes(5) == (250_000, 20_000, 2_500_000, 5_000_000)
    rows = [{"engine": "e", "scale": 1, "status": "ok", "wall_seconds": w, "stage_a": w / 2} for w in (10, 20)]
    rows.append({"engine": "e", "scale": 1, "status": "failed", "wall_seconds": 99})
    s = {r["metric"]: r for r in bb.summarize(rows)}
    assert s["wall_seconds"]["mean"] == 15 and s["stage_a"]["runs"] == 2
    assert sl.percentile([1, 2, 3, 4, 5], 50) == 3 and sl.percentile([1, 2], 95) == 1.95
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    seen: dict = {}
    r1 = [{"user_id": "u1", "last_sent_at": t0.replace(tzinfo=None), "updated_at": t0 + timedelta(seconds=2)}]
    assert sl.collect_new(r1, ["user_id"], seen, "t", "spark", t0)[0]["latency_s"] == 2.0
    assert sl.collect_new(r1, ["user_id"], seen, "t", "spark", t0) == []  # unchanged row not counted twice
    r1[0]["updated_at"] += timedelta(seconds=5)
    assert len(sl.collect_new(r1, ["user_id"], seen, "t", "spark", t0)) == 1
    assert sl.summarize([{"table": "t", "engine": "spark", "latency_s": 2.0}])[0]["count"] == 1
    line = '{"Name":"kafka","CPUPerc":"12.5%","MemUsage":"1.5GiB / 8GiB","MemPerc":"18.75%"}'
    p = ru.parse_stats_line(line)
    assert p["cpu_percent"] == 12.5 and abs(p["mem_mib"] - 1536) < 1e-6 and ru.parse_stats_line("garbage") is None
    a = {"t": 0, "sent": 0, "total": 9, "spark": 0}
    b = {"t": 10, "sent": 100, "total": 9, "spark": 80}
    r = st.analyse([a, b], ["spark"])
    assert r["produced_per_s"] == 10 and r["spark_processed_per_s"] == 8 and r["spark_final_lag"] == 20
    print("ok")


if __name__ == "__main__":
    test_all()
