"""Shared helpers for the benchmark scripts: percentiles and result files."""
from __future__ import annotations

import csv
import statistics
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"


def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile (pct in 0..100)."""
    ordered = sorted(values)
    rank = max(1, round(pct / 100 * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


def summarize(values_ms: list[float]) -> dict[str, float]:
    return {
        "runs": len(values_ms),
        "p50_ms": round(percentile(values_ms, 50), 3),
        "p95_ms": round(percentile(values_ms, 95), 3),
        "mean_ms": round(statistics.fmean(values_ms), 3),
        "max_ms": round(max(values_ms), 3),
    }


def write_results(name: str, rows: list[dict], title: str, notes: list[str]) -> None:
    """Write results/<name>.csv and results/<name>.md (a markdown table plus the notes)."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    columns = list(rows[0])
    with (RESULTS_DIR / f"{name}.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    lines = [f"# {title}", "", "| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(str(row[column]) for column in columns) + " |" for row in rows]
    lines += ["", *notes, ""]
    (RESULTS_DIR / f"{name}.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
