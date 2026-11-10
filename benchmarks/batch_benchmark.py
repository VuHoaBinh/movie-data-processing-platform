"""Batch runtime benchmark at several data sizes (x1, x5, x10).

Generates datasets with generate_data.py, runs each engine --repeat times per scale and writes one raw
CSV row per run (wall time + parsed `TIMING <stage> <seconds>` lines) to benchmarks/results/.
Prints mean/std per (engine, scale). Run from anywhere; commands execute with cwd = repository root.
"""
from __future__ import annotations

import argparse
import csv
import math
import shlex
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
BENCH_DIR = Path(__file__).resolve().parent
CSV_FILES = ("users.csv", "movies.csv", "ratings.csv", "watch_history.csv")
# x1 = generate_data.py defaults: users, movies, ratings, watches.
BASE_SIZES = (50_000, 20_000, 500_000, 1_000_000)


def scaled_sizes(scale: int, base: Sequence[int] = BASE_SIZES) -> Tuple[int, int, int, int]:
    """Scale users, ratings and watches by `scale`; the movie catalogue stays fixed (a catalogue does not
    grow tenfold when traffic does, and it keeps ratings <= watches valid)."""
    users, movies, ratings, watches = base
    return users * scale, movies, ratings * scale, watches * scale


def parse_timings(output: str) -> Dict[str, float]:
    """Parse `TIMING <stage> <seconds>` lines; a repeated stage name is summed."""
    timings: Dict[str, float] = {}
    for line in output.splitlines():
        parts = line.strip().split()
        if len(parts) == 3 and parts[0] == "TIMING":
            try:
                timings[parts[1]] = timings.get(parts[1], 0.0) + float(parts[2])
            except ValueError:
                continue
    return timings


def summarize(rows: List[dict]) -> List[dict]:
    """Mean/std of wall time and of every stage per (engine, scale), successful runs only."""
    groups: Dict[Tuple[str, int], List[dict]] = {}
    for row in rows:
        if row["status"] == "ok":
            groups.setdefault((row["engine"], int(row["scale"])), []).append(row)
    out = []
    for (engine, scale), runs in sorted(groups.items()):
        keys = ["wall_seconds"] + sorted({k for r in runs for k in r if k.startswith("stage_")})
        for key in keys:
            values = [float(r[key]) for r in runs if r.get(key) not in (None, "")]
            if not values:
                continue
            out.append({
                "engine": engine, "scale": scale, "metric": key, "runs": len(values),
                "mean": statistics.mean(values),
                "std": statistics.stdev(values) if len(values) > 1 else float("nan"),
            })
    return out


def dataset_ready(directory: Path) -> bool:
    return all((directory / name).exists() for name in CSV_FILES)


def ensure_dataset(scale: int, sizes: Sequence[int], tmp_dir: Path, python: str, dirty_rate: float) -> Path:
    directory = tmp_dir / f"data_x{scale}"
    if dataset_ready(directory):
        print(f"[data] x{scale}: reusing {directory}", flush=True)
        return directory
    users, movies, ratings, watches = sizes
    directory.mkdir(parents=True, exist_ok=True)
    cmd = [python, "generate_data.py", "--users", str(users), "--movies", str(movies),
           "--ratings", str(ratings), "--watches", str(watches), "--out-dir", str(directory),
           "--dirty-rate", str(dirty_rate)]
    print(f"[data] x{scale}: {' '.join(cmd)}", flush=True)
    started = time.perf_counter()
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)
    print(f"[data] x{scale}: generated in {time.perf_counter() - started:.1f}s", flush=True)
    return directory


def build_command(engine: str, scale: int, raw_dir: Path, tmp_dir: Path, args: argparse.Namespace) -> List[str]:
    empty_archive = tmp_dir / "empty_archive"  # keep archived live events of a previous demo out of the timing
    if engine == "spark":
        cmd = [args.python, "-m", "src.spark_batch_job", "--raw-dir", str(raw_dir),
               "--warehouse-dir", str(tmp_dir / f"warehouse_x{scale}"),
               "--replay-day", "none", "--archive-dir", str(empty_archive)]
        if args.cassandra_host:
            cmd += ["--cassandra-host", args.cassandra_host]
        return cmd
    if engine == "pandas":
        return [args.python, "-m", "src.batch_processing", "--raw-dir", str(raw_dir),
                "--output-dir", str(tmp_dir / f"processed_x{scale}"),
                "--replay-day", "none", "--archive-dir", str(empty_archive)]
    if not args.command:
        raise SystemExit(f"engine {engine!r} needs --command")
    text = args.command.format(raw_dir=raw_dir, scale=scale, tmp_dir=tmp_dir, python=args.python)
    return shlex.split(text)


def run_once(cmd: List[str], timeout: Optional[float]) -> Tuple[float, str, str, int]:
    started = time.perf_counter()
    try:
        proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout)
        output, code, status = proc.stdout + "\n" + proc.stderr, proc.returncode, "ok" if proc.returncode == 0 else "failed"
    except subprocess.TimeoutExpired as error:
        output = (error.stdout or b"").decode() if isinstance(error.stdout, bytes) else (error.stdout or "")
        code, status = -1, "timeout"
    return time.perf_counter() - started, output, status, code


def write_rows(path: Path, rows: List[dict]) -> None:
    base = ["timestamp", "engine", "scale", "run", "status", "returncode", "wall_seconds"]
    extra = sorted({k for r in rows for k in r if k not in base})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=base + extra)
        writer.writeheader()
        writer.writerows(rows)


def print_summary(summary: List[dict]) -> None:
    if not summary:
        print("No successful runs to summarise.")
        return
    print(f"\n{'engine':<10}{'scale':>6}  {'metric':<28}{'runs':>5}{'mean_s':>10}{'std_s':>9}")
    for row in summary:
        std = "n/a" if math.isnan(row["std"]) else f"{row['std']:.2f}"
        print(f"{row['engine']:<10}{'x' + str(row['scale']):>6}  {row['metric']:<28}{row['runs']:>5}"
              f"{row['mean']:>10.2f}{std:>9}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scales", type=int, nargs="+", default=[1, 5, 10], help="size factors (default: 1 5 10)")
    p.add_argument("--engines", nargs="+", default=["spark", "pandas"],
                   help="spark, pandas, or any other name (needs --command; the name is only a label)")
    p.add_argument("--command", help="command template for a custom engine; placeholders {raw_dir} {scale} "
                                     "{tmp_dir} {python}. Timed by wall clock (TIMING lines are parsed too).")
    p.add_argument("--repeat", type=int, default=3, help="runs per (engine, scale)")
    p.add_argument("--sizes", type=int, nargs=4, metavar=("USERS", "MOVIES", "RATINGS", "WATCHES"),
                   default=list(BASE_SIZES), help="override the x1 sizes (e.g. for a smoke test)")
    p.add_argument("--dirty-rate", type=float, default=0.0, help="passed to generate_data.py")
    p.add_argument("--cassandra-host", default=None, help="also load Cassandra in the spark engine (adds load time)")
    p.add_argument("--python", default=sys.executable, help="interpreter used for the children")
    p.add_argument("--tmp-dir", type=Path, default=BENCH_DIR / "tmp")
    p.add_argument("--results-dir", type=Path, default=BENCH_DIR / "results")
    p.add_argument("--timeout", type=float, default=None, help="seconds per run")
    p.add_argument("--keep-output", action="store_true", help="keep warehouse/processed dirs between runs")
    args = p.parse_args(argv)
    if args.repeat < 1:
        p.error("--repeat must be >= 1")
    if args.sizes[2] > args.sizes[3]:
        p.error("RATINGS cannot exceed WATCHES (generate_data.py rule)")

    args.tmp_dir = args.tmp_dir.resolve()
    args.tmp_dir.mkdir(parents=True, exist_ok=True)
    (args.tmp_dir / "empty_archive").mkdir(exist_ok=True)
    args.results_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = args.results_dir / f"batch_{stamp}.csv"

    rows: List[dict] = []
    for scale in args.scales:
        raw_dir = ensure_dataset(scale, scaled_sizes(scale, args.sizes), args.tmp_dir, args.python, args.dirty_rate)
        for engine in args.engines:
            for run in range(1, args.repeat + 1):
                if not args.keep_output:  # every run starts from an empty warehouse / output dir
                    for name in (f"warehouse_x{scale}", f"processed_x{scale}"):
                        shutil.rmtree(args.tmp_dir / name, ignore_errors=True)
                cmd = build_command(engine, scale, raw_dir, args.tmp_dir, args)
                print(f"[run] {engine} x{scale} #{run}: {' '.join(cmd)}", flush=True)
                wall, output, status, code = run_once(cmd, args.timeout)
                row = {"timestamp": datetime.now().isoformat(timespec="seconds"), "engine": engine,
                       "scale": scale, "run": run, "status": status, "returncode": code,
                       "wall_seconds": round(wall, 3)}
                for stage, seconds in parse_timings(output).items():
                    row[f"stage_{stage}"] = seconds
                if status != "ok":
                    print(output[-2000:], file=sys.stderr)
                print(f"      -> {status} in {wall:.1f}s", flush=True)
                rows.append(row)
                write_rows(out_path, rows)  # persist after every run so a crash keeps earlier results
    print(f"\nRaw results: {out_path}")
    print_summary(summarize(rows))
    return 0 if all(r["status"] == "ok" for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
