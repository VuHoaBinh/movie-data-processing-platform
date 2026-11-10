# Movie Streaming Analytics Platform

A reference data-processing project covering BR1-BR10: movie search, trending,
recommendations, ratings, genre popularity, engagement, movie performance,
low-engagement detection, user segmentation, and time-based trends.

## Architecture

Lambda architecture:

```text
watch_history.csv (days before the replay day) ---------------------------+
                                                                          v
                         raw zone archive (CSV) ------> Spark batch --> Hive (raw -> curated -> analytics)
                                  ^                                       |
                                  | stream-archiver                       v
last day of watch_history --> Kafka watch-events --+--> Spark Structured Streaming --> Cassandra *_spark --+
  (producer, x60 replay)          ^                |                                                       |
  dashboard form -----------------+                +--> Flink SQL --> Kafka --> materializer --> Cassandra -+--> Streamlit
                                                                                                          |   (batch + live
                                                          Cassandra batch serving tables -----------------+    merged)
```

* **Live day.** The last day of `watch_history` (`REPLAY_DAY`, default `auto`) belongs to the speed
  layer. The producer replays it in `watch_timestamp` order, `REPLAY_SPEED` (default 60) times faster than
  real time, on a simulated clock stored in Cassandra (`replay_clock`). Event time is always
  `watch_timestamp`; the dashboard form stamps new views with the simulated time. Heartbeats keep the
  watermark (1 minute) moving.
* **No double counting.** The batch reads the CSV *without* the replay day, plus the archived Kafka
  events of closed days (`stream-archiver`, headerless CSV in `data/warehouse/landing/`, de-duplicated by
  `watch_id`). It records `batch_cutoff` in `summary_metrics`; the dashboard adds the speed tables only
  for days >= `batch_cutoff`.
* **Speed layer (BR2, BR6, BR8)**, same logic in both engines: BR2 = 5-minute tumbling window, top 10;
  BR6 = per (day, user) views / watch minutes / completion sum; BR8 = per (day, movie). Completion of a
  view = min(1, watch_duration / duration). Spark writes `live_*_spark`, Flink writes `live_*`.

Data management (see [docs/storage_decision.md](docs/storage_decision.md) for the ADR):

* **Serving layer: Apache Cassandra 5.0.** One table per dashboard query
  ([sql/cassandra_schema.cql](sql/cassandra_schema.cql)); the live trending stream is a time-series
  table with a 3-day TTL. The dashboard reads only from Cassandra.
* **Lake layer: Hive tables on Parquet** (Spark SQL with Hive support, embedded Derby metastore,
  warehouse in `data/warehouse/`): `raw.*` (schema-explicit copy of the CSVs), `curated.*`
  (UTC timestamps, de-duplicated; `curated.watch_events` partitioned by `watch_date`) and
  `analytics.*` (the 14 analytics tables).

One Spark run reads the CSVs, builds the lake zones, computes the dashboard tables and loads them into
Cassandra. The Pandas pipeline (`src/batch_processing.py`) is a lightweight twin that writes the same
tables as CSV.

## Business requirement coverage

| BR | Requirement | Implementation |
|---|---|---|
| BR1 | Movie Search & Filtering | Viewer searches titles and filters by genre, release year, language, content type and maturity rating. |
| BR2 | Trending Movie Detection | Speed layer: latest closed five-minute event-time window, top 10 (Spark or Flink). |
| BR3 | Personalized Movie Recommendation | Batch top 8 per user: best weighted-rated movies of the top two watched genres, excluding movies already watched. |
| BR4 | Movie Rating Analytics | Count, average, 1-5 star distribution and weighted rating per movie; top weighted-rated list. |
| BR5 | Genre Popularity Analysis | Views, unique viewers, watch hours and average rating per genre, overall and per month. |
| BR6 | User Engagement Analysis | Batch: views, watch time, active days, avg completion per user. Speed: per (user, day); the lookup merges both. |
| BR7 | Movie Performance Analysis | Analyst compares views, rating, completion rate, and watch hours. |
| BR8 | Low-Engagement Movie Detection | Flag views >= 20 and avg completion < 30% over batch history + live days (`src/settings.py`). |
| BR9 | User Segmentation | Manager views Inactive, Casual, and Active user counts, including users with no views. |
| BR10 | Time-Based Movie Trend Analysis | Views per day/week/month for the platform and each genre (movies: week/month) with growth vs the previous period. |

The batch tables are produced by one Spark run (Pandas twin for tests), not ten independent jobs.

**Batch serving versions.** Each Spark run writes its Cassandra rows under a new version and then
switches `serving_state` to it, so the dashboard never reads a half-written table; the version before
the previous one is deleted.

**Flink batch (alternative engine).** `flink/flink_batch_sql_job.py` recomputes the same analytics tables
with Flink SQL in BATCH mode from the same raw files and boundary (`data/warehouse/batch_boundary.json`,
written by Spark). Run it after a Spark batch and compare:

```powershell
docker compose --profile flink-batch run --rm flink-batch
docker compose run --rm --no-deps benchmark python -m src.compare_batch_engines
```

Known difference: for a duplicated id Spark keeps the first row in file order, Flink (no row order in a
file source) the first by value; both agree when duplicates are identical copies.

## Run the full stack with Docker (recommended)

Install and start Docker Desktop (8 GB memory recommended), then run:

```powershell
docker compose up --build
```

This starts Kafka, Cassandra (+ `cassandra-init`, which applies the schema), the live-day producer,
the Spark speed layer (`spark-streaming`), the Flink JobManager/TaskManager with the Flink speed job and
its materializer, the `stream-archiver`, a one-shot `spark-batch` service (raw -> Hive/Parquet ->
Cassandra, about 4-5 minutes) and the Streamlit dashboard, which waits for the batch to finish. Open the
dashboard at `http://localhost:8501`, the Flink UI at `http://localhost:8081`; Cassandra is on
`localhost:9042`.

* At x60 the replay of one day takes 24 minutes; a five-minute window closes every ~5 seconds. The
  sidebar shows the simulated time and the replay progress. Restarting the producer resumes the replay;
  `docker compose run --rm event-producer python -m src.kafka_event_producer --reset` starts it again.
* The "Record a movie view" form sends one watch event to Kafka at the simulated time. BR6/BR8 live
  totals change within seconds, BR2 when the window closes. The archiver stores it in the raw zone;
  "Refresh batch (Spark)" (or the next daily run) moves closed days from the speed layer into the batch.
* The sidebar switch selects the speed-layer engine (Spark or Flink) for BR2, BR6 and BR8.
* **After a schema change** (`sql/cassandra_schema.cql`) recreate the serving layer: `docker compose down -v`.
  Cassandra only holds derived data; `data/warehouse/landing/` keeps the archived live events.
* Stop with `Ctrl+C`, then `docker compose down` (add `-v` to also drop the Cassandra volume).

The compose setup is a single-machine demo: one Kafka broker/partition, one Flink TaskManager, one
Cassandra node (RF=1).

### Alternatives for comparison

* **Spark vs Flink speed layer**: both run by default on the same topic and parameters
  (`src/spark_speed_layer.py`, `flink/flink_speed_sql_job.py`) and write separate tables
  (`live_*_spark` vs `live_*`), so their rows can be compared window by window and day by day.
* **Hive/Parquet instead of Cassandra for serving**: `src/dashboard_queries.py` holds every dashboard
  query twice (CQL and Spark SQL over `analytics.*`); results are identical.
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
`--cassandra-host` (default `$CASSANDRA_HOST`), `--csv-output-dir` (optional CSV copy of the 10 tables).

## Project structure

```text
data/                        Raw CSV input
data/warehouse/              Hive warehouse + Derby metastore (generated, git-ignored)
sql/cassandra_schema.cql     Cassandra keyspace and per-query tables
docs/storage_decision.md     ADR: why Cassandra + Hive/Parquet (Vietnamese)
src/spark_batch_job.py       Spark batch: lake zones, dashboard tables, Cassandra load
src/batch_processing.py      Pandas twin of the batch pipeline (CSV output)
src/cassandra_store.py       Cassandra connection / schema helper
src/settings.py              Shared parameters (BR8 thresholds, watermark, window, topics, archive path)
src/replay_clock.py          Simulated clock of the live-day replay, batch/speed boundary
src/speed_materializer.py    Flink results (Kafka) -> Cassandra live tables
src/spark_speed_layer.py     Spark Structured Streaming speed layer (BR2, BR6, BR8)
src/stream_archiver.py       Kafka watch-events -> raw zone (CSV) for the next batch run
src/dashboard_queries.py     Dashboard queries as CQL and Spark SQL (storage comparison)
src/kafka_event_producer.py  Live-day replay producer (event time = watch_timestamp, heartbeats)
src/stream_processing.py     Offline stream replay utility
flink/flink_speed_sql_job.py Flink SQL speed layer (BR2, BR6, BR8)
flink/flink_batch_sql_job.py Flink SQL batch layer (alternative engine, same tables as Spark)
src/compare_batch_engines.py Spark vs Flink batch output comparison
benchmarks/                  Performance measurement scripts (see benchmarks/README.md)
app.py                       Streamlit presentation layer (reads Cassandra)
data_dictionary.md           Raw data definitions
```
