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
  `analytics.*` (the 10 dashboard tables).

One Spark run reads the CSVs, builds the lake zones, computes the dashboard tables and loads them into
Cassandra. The Pandas pipeline (`src/batch_processing.py`) is a lightweight twin that writes the same
tables as CSV.

## Business requirement coverage

| BR | Requirement | Implementation |
|---|---|---|
| BR1 | Movie Search & Filtering | Viewer searches titles and filters by genre from the movie catalogue. |
| BR2 | Trending Movie Detection | Viewer sees popular movies and the latest five-minute Kafka/Flink window. |
| BR3 | Personalized Movie Recommendation | Viewer gets highly rated movies in their top two watched genres. |
| BR4 | Movie Rating Analytics | Analyst sees the overall rating metric and highest-rated movies with rating counts. |
| BR5 | Genre Popularity Analysis | Analyst compares views and unique viewers by genre. |
| BR6 | User Engagement Analysis | Analyst reviews views, watch time, active days, and subscription usage. |
| BR7 | Movie Performance Analysis | Analyst compares views, rating, completion rate, and watch hours. |
| BR8 | Low-Engagement Movie Detection | Manager sees the ten lowest-viewed titles; movies with zero views are included. |
| BR9 | User Segmentation | Manager views Inactive, Casual, and Active user counts, including users with no views. |
| BR10 | Time-Based Movie Trend Analysis | Manager views daily viewing trends; analyst can also inspect hourly activity. |

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
`spark-batch` service (CSV -> Hive/Parquet -> Cassandra, about 4-5 minutes) and the Streamlit
dashboard, which waits for the batch to finish. Open the dashboard at `http://localhost:8501`, the
Flink UI at `http://localhost:8081`; Cassandra is on `localhost:9042`.

* The first live BR2 results appear after the first five-minute event window closes.
* The "Record a movie view" form appends the event to `data/watch_history.csv`, sends it to Kafka and
  starts the Spark batch **in the background** (status shown in the sidebar); "Refresh batch (Spark)"
  does the same on demand.
* Stop with `Ctrl+C`, then `docker compose down` (add `-v` to also drop the Cassandra volume).

The compose setup is a single-machine demo: one Kafka broker/partition, one Flink TaskManager, one
Cassandra node (RF=1).

### Alternatives for comparison

* **Spark Structured Streaming instead of Flink**: `docker compose --profile alt up -d spark-streaming`
  computes the same 5-minute top-10 from `watch-events` into the Cassandra table `live_trending_spark`
  (`src/spark_streaming_trending.py`). Flink keeps writing `live_trending`; both tables hold identical
  rows for the windows both engines processed.
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
src/trending_materializer.py Kafka result -> Cassandra live_trending
src/spark_streaming_trending.py Spark Structured Streaming alternative to Flink
src/dashboard_queries.py     Dashboard queries as CQL and Spark SQL (storage comparison)
src/kafka_event_producer.py  CSV-to-Kafka demo producer
src/stream_processing.py     Offline stream replay utility
flink/flink_trending_job.py  PyFlink event-time trending job
app.py                       Streamlit presentation layer (reads Cassandra)
data_dictionary.md           Raw data definitions
```
