"""Sample `docker stats` on the host and report CPU% / memory per container.

Run on the host (not inside a container). Writes benchmarks/results/resources_<label>_<ts>.csv with one
row per container per sample, and prints mean/max per container.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence

BENCH_DIR = Path(__file__).resolve().parent
_UNITS = {"b": 1, "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3, "tib": 1024 ** 4,
          "kb": 1000, "mb": 1000 ** 2, "gb": 1000 ** 3, "tb": 1000 ** 4}


def parse_size(text: str) -> float:
    """'512.3MiB' -> bytes."""
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text or "")
    if not m or m.group(2).lower() not in _UNITS:
        return float("nan")
    return float(m.group(1)) * _UNITS[m.group(2).lower()]


def parse_stats_line(line: str) -> Optional[dict]:
    """Parse one JSON line of `docker stats --format '{{json .}}'`."""
    try:
        d = json.loads(line)
        used = d["MemUsage"].split("/")[0]
        return {"container": d["Name"], "cpu_percent": float(d["CPUPerc"].strip().rstrip("%")),
                "mem_mib": parse_size(used) / 1024 ** 2, "mem_percent": float(d["MemPerc"].strip().rstrip("%"))}
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def summarize(rows: List[dict]) -> List[dict]:
    by: Dict[str, List[dict]] = {}
    for r in rows:
        by.setdefault(r["container"], []).append(r)
    return [{"container": name, "samples": len(v),
             "cpu_mean": statistics.mean(x["cpu_percent"] for x in v), "cpu_max": max(x["cpu_percent"] for x in v),
             "mem_mean_mib": statistics.mean(x["mem_mib"] for x in v), "mem_max_mib": max(x["mem_mib"] for x in v)}
            for name, v in sorted(by.items())]


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--duration", type=float, default=120, help="seconds")
    p.add_argument("--interval", type=float, default=5.0, help="seconds between samples (docker stats itself takes ~1-2 s)")
    p.add_argument("--label", default="run")
    p.add_argument("--results-dir", type=Path, default=BENCH_DIR / "results")
    args = p.parse_args(argv)

    args.results_dir.mkdir(parents=True, exist_ok=True)
    out = args.results_dir / f"resources_{args.label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    rows: List[dict] = []
    deadline = time.monotonic() + args.duration
    while True:
        started = time.monotonic()
        try:
            proc = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"],
                                  capture_output=True, text=True, check=True)
        except (OSError, subprocess.CalledProcessError) as error:
            print(f"docker stats failed: {error}", file=sys.stderr)
            return 1
        stamp = datetime.now().isoformat(timespec="seconds")
        for line in proc.stdout.splitlines():
            parsed = parse_stats_line(line)
            if parsed:
                rows.append({"timestamp": stamp, **parsed})
        if time.monotonic() >= deadline:
            break
        time.sleep(max(0.0, args.interval - (time.monotonic() - started)))
    with out.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "container", "cpu_percent", "mem_mib", "mem_percent"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"{'container':<36}{'n':>4}{'cpu_mean%':>11}{'cpu_max%':>10}{'mem_mean_MiB':>14}{'mem_max_MiB':>13}")
    for s in summarize(rows):
        print(f"{s['container']:<36}{s['samples']:>4}{s['cpu_mean']:>11.1f}{s['cpu_max']:>10.1f}"
              f"{s['mem_mean_mib']:>14.1f}{s['mem_max_mib']:>13.1f}")
    print("Note: CPU% is relative to one core (can exceed 100).")
    print(f"Samples: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
