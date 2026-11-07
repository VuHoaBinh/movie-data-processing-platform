# Movie Streaming Analytics Platform

A reference data-processing project covering BR1-BR10: movie search, trending,
recommendations, ratings, genre popularity, engagement, movie performance,
low-engagement detection, user segmentation, and time-based trends.

## Architecture

```text
CSV source files
      |
      +--> Spark batch --> Hive/Parquet lake (raw -> curated -> analytics) --+
      |                                                                      v
      |                                         Cassandra serving tables --> Streamlit
      |                                                  ^
      +--> Kafka --> Flink 5-minute windows --> Kafka --> materializer
```

Data management (see [docs/storage_decision.md](docs/storage_decision.md) for the ADR):

* **Serving layer: Apache Cassandra 5.0.** One table per dashboard query
  ([sql/cassandra_schema.cql](sql/cassandra_schema.cql)); the live trending stream is a time-series
  table with a 3-day TTL. The dashboard reads only from Cassandra.
* **Lake layer: Hive tables on Parquet** (Spark SQL with Hive support, embedded Derby metastore,
  warehouse in `data/warehouse/`): `raw.*` (schema-explicit copy of the CSVs), `curated.*`
  (UTC timestamps, de-duplicated; `curated.watch_events` partitioned by `watch_date`) and
  `analytics.*` (the dashboard tables, including the BR3 recommendation tables).

One Spark run reads the CSVs, builds the lake zones, computes the dashboard tables and loads them into
Cassandra. The Pandas pipeline (`src/batch_processing.py`) is a lightweight twin that writes the same
tables as CSV.

## Business requirement coverage

| BR | Requirement | Implementation |
|---|---|---|
| BR1 | Movie Search & Filtering | Viewer searches titles and filters by genre from the movie catalogue. |
| BR2 | Trending Movie Detection | Viewer sees all-time popular movies, titles rising now (batch: last 7 days vs the 7 days before, with a volume floor and damping) and the latest five-minute Kafka/Flink window. |
| BR3 | Personalized Movie Recommendation | Viewer gets ten unwatched titles, precomputed per user: genre-based (best-rated titles in their top two watched genres, popular titles as filler) or Spark MLlib ALS (similar viewers); the page lets the viewer pick the recommender (ALS is the default, genre-based the alternative and the fallback for users without watch history). |
| BR4 | Movie Rating Analytics | Analyst sees the overall rating metric and highest-rated movies with rating counts. |
| BR5 | Genre Popularity Analysis | Analyst compares views and unique viewers by genre (two charts and a table with views per viewer). |
| BR6 | User Engagement Analysis | Analyst reviews views, watch minutes and active days per user (KPI cards with mean and median, three histograms, filters by plan and region), subscription usage and viewing hours. |
| BR7 | Movie Performance Analysis | Analyst compares views, rating, completion rate, and watch hours. |
| BR8 | Low-Engagement Movie Detection | Manager sees titles that are low relative to their genre peers (views at or below the genre p10, or completion at or below the genre p10), with the reason and per-genre counts; never-watched titles are included. |
| BR9 | User Segmentation | Manager views Inactive, Casual, and Active user counts from quantile thresholds (p25/p75 of views per user, stored and shown), including users with no views, plus the agreement with the generator's `activity_profile`. |
| BR10 | Time-Based Movie Trend Analysis | Manager views daily views per genre and per movie, a 7-day moving average and a weekday vs weekend comparison; analyst can also inspect hourly activity. |

BR2 uses a Flink event-time window over Kafka watch events. For the local demo,
a producer continuously replays CSV history into Kafka with current event
timestamps. Flink publishes window results back to Kafka, and a materializer
updates the dashboard snapshot. This is real Kafka-to-Flink stream processing;
the producer is a repeatable demo source. The other batch tables are produced
by one Pandas or Spark run, not ten independent jobs.

## Run the full stack with Docker (recommended)

Install and start Docker Desktop (8 GB memory recommended), then run:

```powershell
docker compose up --build
```

This starts Kafka, Cassandra (+ `cassandra-init`, which applies the schema), a Flink JobManager and
TaskManager, the BR2 Flink job, a continuous CSV event producer, the result materializer, a one-shot
`spark-batch` service (CSV -> Hive/Parquet -> ALS -> Cassandra, about 6 minutes; 5-10 on a busy machine) and the Streamlit
dashboard, which waits for the batch to finish. Open the dashboard at `http://localhost:8501`, the
Flink UI at `http://localhost:8081`; Cassandra is on `localhost:9042`.

* The first live BR2 results appear after the first five-minute event window closes.
* The dashboard has four workspaces: **Viewer** (search, trending, recommendations), **Analyst** (filters, BR4-BR7),
  **Manager** (KPI cards and alerts, then BR8-BR10: titles low against their genre with the reason, segment cut-offs and the agreement matrix, trends by genre / title / weekday vs weekend) and **Data Engineering** (last batch run and stage times, rejected rows per
  cleaning rule, age of the newest live window, rows per Cassandra table). Every chart and table carries a one-line
  caption with the business question it answers. If the background batch fails, the sidebar shows the tail of its log
  and a "Retry batch" button.
* The "Record a movie view" form (Viewer workspace) appends the event to `data/watch_history.csv`, sends it to Kafka and
  starts the Spark batch **in the background** (status shown in the sidebar); "Refresh batch (Spark)"
  does the same on demand.
* Stop with `Ctrl+C`, then `docker compose down` (add `-v` to also drop the Cassandra volume).
* Upgrading an existing stack: `cassandra-init` (`python -m src.cassandra_store init`) compares every table in
  `sql/cassandra_schema.cql` with the live one and drops and recreates the serving tables whose columns, primary key
  or clustering order changed (for example `low_engagement_movies`, which gained `priority` in the BR8 upgrade); the next
  batch refills them. `live_trending*` and `batch_runs` are never dropped: if their definition ever changes, init stops
  with `SchemaConflict` and you migrate by hand or run `docker compose down -v`. No manual step is needed for a volume
  created by an earlier version. `tests/test_schema_migration.py` checks it against the old table definition.

The compose setup is a single-machine demo: one Kafka broker/partition, one Flink TaskManager, one
Cassandra node (RF=1).

### Alternatives for comparison

* **Spark Structured Streaming instead of Flink**: `docker compose --profile alt up -d spark-streaming`
  computes the same 5-minute top-10 from `watch-events` into the Cassandra table `live_trending_spark`
  (`src/spark_streaming_trending.py`). Flink keeps writing `live_trending`; both tables hold identical
  rows for the windows both engines processed.
* **Hive/Parquet instead of Cassandra for serving**: `src/dashboard_queries.py` holds every dashboard
  query twice (CQL and Spark SQL over `analytics.*`); results are identical.
  The compose stack runs the Flink SQL job; the PyFlink DataStream job cannot keep up with the 500 events/s demo
  producer (see [docs/evaluation.md](docs/evaluation.md)); submit it with `-py /opt/flink/usrlib/flink_trending_job.py` to compare.
  Set `TREND_WINDOW_SECONDS` to shorten the window for experiments (Flink job and Spark job).

## Run pieces locally

Use Python 3.11-3.13. The dashboard needs Cassandra (`docker compose up -d cassandra cassandra-init`):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt -r requirements-spark.txt   # Spark needs Java 11/17

python generate_data.py                                  # optional: recreate demo data
python -m src.spark_batch_job --cassandra-host localhost # lake + Cassandra (needs internet once for the connector jar)
python -m src.spark_batch_job --no-hive --csv-output-dir data/processed   # Spark, CSV only
python -m src.batch_processing                           # Pandas twin, CSV only (data/processed)
$env:CASSANDRA_HOST = "localhost"; streamlit run app.py
```

Spark batch flags: `--warehouse-dir` (Hive location, default `data/warehouse`), `--no-hive`,
`--cassandra-host` (default `$CASSANDRA_HOST`), `--csv-output-dir` (optional CSV copy of the 19 analytics tables),
`--no-als` (skip the ALS recommendations, which add training time to the batch).

## Tests, data quality and benchmarks

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest                      # unit + Pandas/Spark parity tests (needs Java for the Spark ones)
$env:CASSANDRA_HOST = "localhost"; python -m pytest tests/test_cassandra_counts.py   # stack must be up and loaded

# Everything, including Cassandra-vs-Hive equivalence, inside the Docker stack:
docker compose --profile tools run --rm --no-deps -e RUN_HIVE_TESTS=1 benchmark python -m pytest -q
```

* `tests/test_fixture_tables.py` - a hand-made 4-user / 4-movie / 50-view dataset with the expected value of
  every cell of the 20 tables derived by hand, run against both engines. `tests/test_fixture_peer_tables.py` adds a
  second hand-made dataset (13 Comedy + 2 Drama titles over 3 weeks) for the growth windows, genre-peer thresholds and
  the 7-day moving average.
* `tests/test_parity.py` - Pandas and Spark produce exactly the same 20 tables (and rejected-row counts) on the
  fixture, the peer dataset and generated clean / dirty / sparse data. The ALS table is Spark only and is not part of the parity.
* `tests/test_recommendations.py` - BR3: a second hand-derived dataset (genre ties, filler, cold start), no watched
  title in any list on generated data (both engines), and the ALS tests (ten ranks per user, no watched title,
  identical output on a second run with the same seed).
* `tests/test_data_quality.py` + `src/data_quality.py` - the cleaning rules (duplicate ids, nulls, invalid
  values, future timestamps, orphan foreign keys) with the number of rejected rows per rule. See them on the
  real data with `python -m src.data_quality`; Spark logs the same report and stores it in
  `curated.quality_report`.
* `generate_data.py --dirty-rate 0.05 --out-dir some/dir` appends bad rows (opt-in) so cleaning can be
  demonstrated; `--out-dir` also keeps experiments away from `data/`.
* `tests/test_cassandra_counts.py`, `tests/test_storage_equivalence.py` - Cassandra row counts match the
  batch output and both stores answer every dashboard query identically.
* `tests/test_alerts.py` - the Manager alerts fire on degraded data (users and titles that stop watching, a genre that loses a
  third of its views) and stay quiet on healthy data; each alert compares two time windows of an absolute count.
* `tests/test_schema_migration.py` - an old Cassandra volume (the PR #2 `low_engagement_movies`) is upgraded by `cassandra-init`.
* `tests/test_app_pages.py` - Streamlit `AppTest` renders the four pages on the loaded stack without errors; the user and
  title pickers (a search box that narrows to at most 50 matches, never the 50,000 users or 20,000 titles) stay capped.
  The Manager alert limits are named constants in `src/business_rules.py` (`ACTIVE_USERS_DROP_ALERT`, `ACTIVE_TITLES_DROP_ALERT`,
  `GENRE_DROP_ALERT`), justified in `docs/business_requirements.md` section 2.3.
* `python -m src.profile_data` profiles the raw CSVs (`benchmarks/results/data_profile.md` / `.csv`, tested in
  `tests/test_profile_data.py`); `python -m src.data_quality` reports rejected rows on them.
* `benchmarks/` - `batch_engines` (Pandas vs Spark at 100k/1M/5M), `storage_latency` (Cassandra vs
  Hive/Parquet), `stream_latency` (Flink vs Spark Structured Streaming), `load_times`, `storage_footprint`,
  `br_effectiveness`, `recommender_eval` (BR3: popularity vs genre-based vs ALS on a time-based split; needs Spark,
  run it in the Docker `benchmark` service). Results are committed in `benchmarks/results/`; the analysis is in
  [docs/evaluation.md](docs/evaluation.md). Generated benchmark data goes to `benchmarks/tmp/` (git-ignored).

## Project structure

```text
data/                        Raw CSV input
data/warehouse/              Hive warehouse + Derby metastore (generated, git-ignored)
sql/cassandra_schema.cql     Cassandra keyspace and per-query tables
docs/storage_decision.md     ADR: why Cassandra + Hive/Parquet (Vietnamese)
docs/business_requirements.md Business context, stakeholders, BR1-BR10 rationale and traceability (Vietnamese)
docs/data_characteristics.md Data sources, schema, Big Data characteristics, design consequences (Vietnamese)
src/spark_batch_job.py       Spark batch: lake zones, dashboard tables, Cassandra load, run metadata
src/recommendations.py       BR3 genre-based top-10 per user, watched titles excluded (Pandas and Spark twins)
src/recommender_als.py       BR3 alternative: Spark MLlib ALS (Spark only)
src/batch_processing.py      Pandas twin of the batch pipeline (CSV output)
src/cassandra_store.py       Cassandra connection / schema helper
src/data_quality.py          Cleaning rules + rejected-row report (Pandas and Spark)
src/profile_data.py          Data profiler: sizes, nulls, keys, skew, time pattern (writes benchmarks/results/data_profile.*)
src/trending_materializer.py Kafka result -> Cassandra live_trending
src/spark_streaming_trending.py Spark Structured Streaming alternative to Flink
src/dashboard_queries.py     Dashboard queries as CQL and Spark SQL (storage comparison)
src/kafka_event_producer.py  CSV-to-Kafka demo producer
src/stream_processing.py     Offline stream replay utility
flink/flink_trending_sql_job.py Flink SQL 5-minute trending job (default; ~15x the throughput)
flink/flink_trending_job.py  PyFlink DataStream version (kept for comparison, ~500 events/s max)
app.py                       Streamlit presentation layer (Viewer / Analyst / Manager / Data Engineering, reads Cassandra)
tests/                       pytest suite (fixture, parity, data quality, integration)
benchmarks/                  Benchmark scripts and committed results
docs/evaluation.md           Correctness / performance / effectiveness evaluation
data_dictionary.md           Raw data definitions
```
