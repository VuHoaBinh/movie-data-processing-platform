"""On-disk size of the lake (Parquet) and the serving layer (Cassandra), measured from the host.

    python -m benchmarks.storage_footprint
"""
from __future__ import annotations

import subprocess
from pathlib import Path

from benchmarks.common import write_results

WAREHOUSE = Path("data/warehouse/spark-warehouse")
CASSANDRA = "movie-data-processing-platform-cassandra-1"


def directory_mb(path: Path) -> float:
    return round(sum(file.stat().st_size for file in path.rglob("*") if file.is_file()) / 1024**2, 1)


def cassandra_mb() -> float:
    subprocess.run(["docker", "exec", CASSANDRA, "nodetool", "flush", "movie_analytics"], check=True, capture_output=True)
    out = subprocess.run(
        ["docker", "exec", CASSANDRA, "du", "-sk", "/var/lib/cassandra/data/movie_analytics"],
        check=True, capture_output=True, text=True,
    ).stdout
    return round(int(out.split()[0]) / 1024, 1)


def main() -> None:
    raw_csv = round(sum(path.stat().st_size for path in Path("data").glob("*.csv")) / 1024**2, 1)
    rows = [{"store": "raw CSV (data/*.csv)", "size_mb": raw_csv}]
    for zone in ("raw", "curated", "analytics"):
        rows.append({"store": f"Hive/Parquet {zone}.db", "size_mb": directory_mb(WAREHOUSE / f"{zone}.db")})
    rows.append({"store": "Cassandra movie_analytics (13 serving tables, RF=1)", "size_mb": cassandra_mb()})
    write_results(
        "storage_footprint", rows, "Storage footprint",
        ["Measured as file sizes after `nodetool flush`; Cassandra data is LZ4-compressed SSTables."],
    )


if __name__ == "__main__":
    main()
