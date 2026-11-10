# Performance evaluation scripts (report chapter 6)

Rule: **never invent numbers.** Every figure in the report must come from the CSV/console output of these
scripts, from repeated runs (report the mean and std, state the number of runs and the machine).
Raw outputs go to `benchmarks/results/`; generated datasets and warehouses to `benchmarks/tmp/` (git-ignored).

| Script | Measures |
|---|---|
| `batch_benchmark.py` | Batch runtime at x1/x5/x10 data size, per engine, wall time + per-stage times |
| `stream_latency.py` | End-to-end latency of the speed layer: `updated_at - last_sent_at` in the live tables |
| `stream_throughput.py` | Events/s produced vs processed per engine at one replay speed, and lag |
| `resource_usage.py` | CPU% and memory per container (`docker stats`), host only |

Each script supports `--help`. Run commands from the repository root. In the compose stack replace
`python` by `docker compose run --rm --no-deps benchmark python` (the service mounts `./benchmarks`,
`./src`, `./data`, uses image `movie-analytics-app`, `PYTHONPATH=/app`, `CASSANDRA_HOST=cassandra`).
`resource_usage.py` needs the docker CLI, so run it on the host only.

## 1. Batch runtime

```bash
# Spark and the Pandas twin, x1/x5/x10, 3 runs each (needs Java for Spark)
python benchmarks/batch_benchmark.py --scales 1 5 10 --engines spark pandas --repeat 3
# Spark including the Cassandra load
python benchmarks/batch_benchmark.py --engines spark --cassandra-host localhost --repeat 3
# Flink SQL batch (flink/flink_batch_sql_job.py). The datasets must live under ./data, which is mounted
# into the Flink containers at /opt/flink/data; placeholders {raw_dir} {scale} {tmp_dir} {python}
python benchmarks/batch_benchmark.py --engines flink --tmp-dir data/bench --command \
  "docker compose --profile flink-batch run --rm flink-batch flink run -m flink-jobmanager:8081 -py /opt/flink/usrlib/flink_batch_sql_job.py --data-dir /opt/flink/{raw_dir}"
# Smoke test with tiny sizes (users movies ratings watches for x1)
python benchmarks/batch_benchmark.py --scales 1 2 --engines pandas --repeat 2 --sizes 500 200 3000 6000 --tmp-dir /tmp/bm
```

* Scale: users, ratings and watches are multiplied by the factor; the movie catalogue (20k) stays fixed
  (a catalogue does not grow tenfold with traffic, and ratings <= watches stays valid). Say so in the report.
* Datasets are generated once per scale (`tmp/data_x{N}`) and reused. Every run starts from an empty
  warehouse/output dir and an empty archive dir, with `--replay-day none`, so no live-day data is excluded or added.
* The Spark job prints `TIMING <stage> <seconds>`; these become `stage_*` columns. Custom engines are timed by
  wall clock (and `TIMING` lines are parsed too if they print them). Wall time includes JVM/Spark start-up.
* Output: `results/batch_<timestamp>.csv` (one row per run; written after every run) and a mean/std table.
  The first Spark run may download the Cassandra connector jar: discard it or run once beforehand.
  Do not compare engines that wrote different things (Cassandra load vs CSV only).
* Aggregating several invocations: concatenate the CSVs (pandas `read_csv` + `groupby(['engine','scale'])`).

## 2. Streaming latency

Start the stack (`docker compose up`) and then, in a second shell, a run per replay speed:

```bash
docker compose run --rm event-producer python -m src.kafka_event_producer --speed 60 --reset   # shell 1
python benchmarks/stream_latency.py --label speed60 --duration 300 --interval 2                 # shell 2
```

Latency = `updated_at - last_sent_at` for rows rewritten since the previous sample, per table; summary has
count, p50, p95, p99, max per table and engine (`results/latency_<label>_<ts>.csv` and `_summary.csv`).
`CASSANDRA_HOST` selects the node (default localhost).

Caveats to state in the report:
* `live_trending*` latency includes waiting for the watermark (60 simulated seconds after the window end,
  i.e. `60 / speed` real seconds); do not compare it directly with the engagement tables.
* Engagement tables are upserts, the sample keeps the latest write per row per sample interval, so
  intermediate rewrites between two samples are not seen.
* `last_sent_at` and `updated_at` come from different processes; on one host the clock skew is negligible.
* Events by the dashboard form are also measured if they land in the sampled day.
* Spark micro-batch latency is bounded below by the trigger (`TRIGGER_SECONDS`, default 5 s).

## 3. Throughput at several replay speeds

```bash
for SPEED in 60 300 1200; do
  docker compose run --rm event-producer python -m src.kafka_event_producer --speed $SPEED --reset &
  python benchmarks/stream_throughput.py --label speed$SPEED --duration 600 --interval 5 --until-done
  wait   # or stop the producer; then clear the live tables / recreate the stack before the next speed
done
```

`stream_throughput.py` samples `replay_clock.sent` (produced) and the sum of `views` for the replay day in
`live_movie_engagement` (Flink) and `live_movie_engagement_spark` (Spark) (processed), prints the lag
(produced - processed) and per-interval rates, and writes `results/throughput_<label>_<ts>.csv`.
The processed sum is cumulative, so **reset state between speeds** (`docker compose down -v` and up again, or
truncate the live tables and restart the streaming jobs with fresh checkpoints); otherwise old totals distort the
numbers. The speed at which lag grows without bound is the saturation point. Heartbeats are not counted.
Rates over the whole run are a simple average; use the per-interval columns to see saturation.

## 4. CPU / memory

```bash
python benchmarks/resource_usage.py --label spark_speed60 --duration 300 --interval 5   # host, while a run is active
```

Prints mean/max CPU% (100% = one core) and memory per container; CSV in `results/resources_<label>_<ts>.csv`.
Run it alongside a batch (`spark-batch`) or a streaming run and keep the label identical to the matching
latency/throughput run.

## Repeating runs

Repeat each configuration at least 3 times (`--repeat` for batch; re-run the streaming scripts with the
same label from a clean state), report mean and std and the number of runs, and note hardware, Docker memory
limit and data sizes. Report values exactly as printed; mark anything not measured as not measured.
