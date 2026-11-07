"""Pandas vs Spark batch runtime on generated data of growing size.

Data is generated into benchmarks/tmp/ (never into data/). Each engine runs in a fresh process; the
same raw CSVs go in and the same ten CSV tables come out, which are compared for exact equality.

    python generate_data.py ... (done by --generate)   then, inside the Docker image:
    docker compose --profile tools run --rm --no-deps benchmark python -m benchmarks.batch_engines
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from benchmarks.common import write_results

TMP = Path(__file__).resolve().parent / "tmp"
SIZES = (100_000, 1_000_000, 5_000_000)


def dataset_dir(watches: int) -> Path:
    return TMP / f"data_{watches}"


def generate(watches: int) -> None:
    target = dataset_dir(watches)
    if (target / "watch_history.csv").exists():
        return
    started = time.perf_counter()
    subprocess.run(
        [sys.executable, "generate_data.py", "--users", "50000", "--movies", "20000",
         "--watches", str(watches), "--ratings", str(watches // 2), "--out-dir", str(target)],
        check=True,
    )
    print(f"generated {watches:,} watches in {time.perf_counter() - started:.0f}s", flush=True)


def child(engine: str, raw_dir: Path, output_dir: Path) -> None:
    """Run one engine end to end and print its wall time as JSON."""
    started = time.perf_counter()
    if engine == "pandas":
        from src import batch_processing

        batch_processing.run(raw_dir, output_dir)
    else:
        from src import spark_batch_job

        spark_batch_job.run(raw_dir, warehouse_dir=None, cassandra_host=None, csv_output_dir=output_dir, als=False)
    print("RESULT " + json.dumps({"seconds": time.perf_counter() - started}))


def process_tree_rss_mb(root_pid: int) -> float:
    """Resident memory of a process and all its descendants, read from /proc (Linux)."""
    parents, rss = {}, {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = dict(line.split(":", 1) for line in (entry / "status").read_text().splitlines() if ":" in line)
            parents[int(entry.name)] = int(fields["PPid"])
            rss[int(entry.name)] = int(fields.get("VmRSS", "0 kB").split()[0])
        except (OSError, KeyError, ValueError):
            continue
    family, changed = {root_pid}, True
    while changed:
        changed = False
        for pid, parent in parents.items():
            if parent in family and pid not in family:
                family.add(pid)
                changed = True
    return sum(rss.get(pid, 0) for pid in family) / 1024


def run_child(engine: str, raw_dir: Path, output_dir: Path) -> dict:
    env = dict(os.environ, PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell", PYTHONPATH=".")
    process = subprocess.Popen(
        [sys.executable, "-m", "benchmarks.batch_engines", "--child", engine, str(raw_dir), str(output_dir)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env,
    )
    output: list[str] = []
    reader = threading.Thread(target=lambda: output.extend(process.stdout), daemon=True)
    reader.start()
    peak = 0.0
    while process.poll() is None:
        peak = max(peak, process_tree_rss_mb(process.pid))
        time.sleep(0.5)
    reader.join()
    for line in output:
        if line.startswith("RESULT "):
            return {**json.loads(line[7:]), "peak_mb": peak}
    raise RuntimeError(f"{engine} failed:\n{''.join(output)[-3000:]}")


def tables_identical(left: Path, right: Path) -> bool:
    import pandas as pd

    for path in sorted(left.glob("*.csv")):
        a, b = pd.read_csv(path), pd.read_csv(right / path.name)
        if list(a.columns) != list(b.columns) or len(a) != len(b):
            return False
        key = list(a.columns)
        if not a.sort_values(key).reset_index(drop=True).equals(b.sort_values(key).reset_index(drop=True)):
            return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Pandas vs Spark batch runtime.")
    parser.add_argument("--child", nargs=3, metavar=("ENGINE", "RAW", "OUT"))
    parser.add_argument("--sizes", type=int, nargs="*", default=list(SIZES))
    parser.add_argument("--runs", type=int, default=3, help="repetitions for sizes up to 1M (5M runs once)")
    parser.add_argument("--generate", action="store_true", help="only generate the datasets and exit")
    args = parser.parse_args()
    if args.child:
        child(args.child[0], Path(args.child[1]), Path(args.child[2]))
        return
    for size in args.sizes:
        generate(size)
    if args.generate:
        return

    rows = []
    for size in args.sizes:
        raw = dataset_dir(size)
        repeats = args.runs if size <= 1_000_000 else 1
        outputs = {}
        for engine in ("pandas", "spark"):
            times, peaks = [], []
            for run in range(repeats):
                output = TMP / f"out_{engine}_{size}"
                result = run_child(engine, raw, output)
                times.append(result["seconds"])
                peaks.append(result["peak_mb"])
                print(f"{engine} {size:,}: {result['seconds']:.1f}s peak {result['peak_mb']:.0f} MB", flush=True)
            outputs[engine] = output
            rows.append({
                "watch_rows": size, "ratings_rows": size // 2, "engine": engine, "runs": repeats,
                "median_s": round(sorted(times)[len(times) // 2], 1), "min_s": round(min(times), 1),
                "max_s": round(max(times), 1), "peak_mem_mb": round(max(peaks)),
            })
        identical = tables_identical(outputs["pandas"], outputs["spark"])
        for row in rows[-2:]:
            row["outputs_identical"] = identical
    write_results(
        "batch_engines", rows, "Pandas vs Spark batch (end to end: read CSV, clean, 10 tables, write CSV)",
        [
            "Environment: Docker Desktop (WSL2) container, python:3.12 + Java 17, Spark local[*] with 4 GB driver "
            "memory (PySpark 3.5.3), Pandas 2.x; users=50,000, movies=20,000, ratings = watches / 2.",
            "Spark time includes JVM start-up. `outputs_identical` compares the ten CSV tables of both engines.",
            "Peak memory = highest summed resident set of the process tree (Python + JVM), sampled every 0.5 s from /proc.",
        ],
    )


if __name__ == "__main__":
    main()
