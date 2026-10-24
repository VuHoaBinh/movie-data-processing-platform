"""Time the full batch (CSV -> Hive/Parquet zones -> Cassandra) stage by stage.

Run inside the stack (this reloads Hive and Cassandra from data/):
    docker compose --profile tools run --rm --no-deps benchmark python -m benchmarks.load_times
"""
from __future__ import annotations

import os
import re
import subprocess
import sys

from benchmarks.common import write_results

STAGES = {
    "hive_raw": "CSV -> raw Parquet tables (4)",
    "hive_curated": "raw -> curated Parquet (cleaning, curated.watch_events partitioned by date)",
    "hive_analytics": "curated -> 10 analytics Parquet tables",
    "cassandra_load": "analytics -> 13 Cassandra serving tables (truncate + insert)",
}


def main() -> None:
    env = dict(os.environ, PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell")
    completed = subprocess.run(
        [sys.executable, "-m", "src.spark_batch_job"], capture_output=True, text=True, env=env, check=True
    )
    timings = dict(re.findall(r"TIMING (\w+) ([\d.]+)", completed.stdout))
    rows = [{"stage": stage, "what": what, "seconds": timings[stage]} for stage, what in STAGES.items()]
    rows.append({"stage": "total", "what": "sum of the stages above", "seconds": round(sum(float(r["seconds"]) for r in rows), 1)})
    write_results(
        "load_times", rows, "Batch load times (1M watch rows)",
        ["Spark local[*], 4 GB driver, Docker Desktop; Cassandra writes go through spark-cassandra-connector 3.5.1."],
    )


if __name__ == "__main__":
    main()
