# Movie Streaming Analytics Platform

A reference data-processing project covering BR1-BR10: movie search, trending,
recommendations, ratings, genre popularity, engagement, movie performance,
low-engagement detection, user segmentation, and time-based trends.

## Architecture

```text
CSV source files
      |
      +--> Spark batch --> 10 dashboard tables ---------+
      |                                                  |
      +--> Kafka --> Flink 5-minute windows --> Kafka ---+--> Streamlit
                                  |                       |
                           BR2 live trending       materializer
```

The batch pipeline creates shared analytics tables for the dashboard. It is
not one Spark job per BR: one run reads the source data and builds the tables
used by multiple requirements. Pandas is available for lightweight local
development; the full Docker stack uses Spark for batch. Both batch engines
write compatible files to `data/processed/`.

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

## Run locally

Use Python 3.12 or 3.13. The dashboard and Pandas batch pipeline need the base
requirements:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# Optional: recreate demo data.
python generate_data.py

# Batch-only local development with Pandas.
python -m src.batch_processing
streamlit run app.py
```

To run batch processing with Spark locally, install Java and the optional
Spark dependency, then run:

```powershell
java -version
python -m pip install -r requirements-spark.txt
python -m src.spark_batch_job
streamlit run app.py
```

Spark and Flink can be started independently. For Spark, run the batch command
above in one terminal; it reads the source CSV files and writes the analytics
CSVs. Start the dashboard in another terminal with `streamlit run app.py`.
Submitting a view event in the dashboard appends it to
`data/watch_history.csv` and reruns Spark so the batch charts include it.

For only the streaming path, start the Kafka and Flink services in Docker:

```powershell
docker compose up --build -d kafka kafka-init flink-jobmanager flink-taskmanager flink-trending-job event-producer trending-materializer
```

That path runs separately from Spark. The form also publishes each submitted
event to Kafka when Kafka is running. Flink updates the live BR2 CSV after its
five-minute event-time window closes, and the dashboard refreshes the live
panel every ten seconds. Start the dashboard separately with
`streamlit run app.py` from the project folder so it can read the shared
`data/processed/` files. Stop only the streaming stack with `docker compose down`.

Both batch commands write ten dashboard-compatible CSV tables to
`data/processed/`. The full Kafka/Flink stream stack is started with Docker
Compose below.

## Run the full Spark + Flink stack with Docker

Install and start Docker Desktop, then run:

```powershell
docker compose up --build
```

This starts Kafka, a Flink JobManager and TaskManager, the BR2 Flink job, a
continuous CSV event producer, a result materializer, and the Streamlit
dashboard. The dashboard container runs the Spark batch job at startup and
writes its ten batch tables to `data/processed/`. A dashboard form can append
a view event to `data/watch_history.csv`, send it to Kafka, and rerun the Spark
batch job. Flink consumes
`watch-events`, computes five-minute event-time windows, and publishes to
`trending-results`. The materializer updates
`data/processed/live_trending.csv`; the dashboard refreshes that panel every
ten seconds.

Open the dashboard at `http://localhost:8501` and the Flink UI at
`http://localhost:8081`. The first live results appear after the first
five-minute event window closes. Stop the stack with `Ctrl+C`, then run:

```powershell
docker compose down
```

The compose setup is a single-machine learning/demo deployment. Kafka has one
broker and one partition; Flink has one TaskManager. The stream replays a
limited slice of the input CSV continuously, while Flink performs Kafka-backed
stream processing. It is not configured for production availability or durable
state across container removal.

## Project structure

```text
data/                       Raw CSV input
data/processed/             Dashboard-ready analytical tables
src/batch_processing.py     Local Pandas batch pipeline
src/spark_batch_job.py      Spark batch pipeline for the same tables
src/stream_processing.py    Offline stream replay utility
src/kafka_event_producer.py CSV-to-Kafka demo producer
src/trending_materializer.py Kafka result-to-dashboard CSV sink
flink/flink_trending_job.py PyFlink event-time trending job
flink/Dockerfile            Flink image with Kafka connector
requirements-spark.txt      Optional PySpark dependency
requirements-stream.txt     Kafka producer/materializer dependency
sql/bigquery_schema.sql     Warehouse table definitions
app.py                      Streamlit presentation layer
data_dictionary.md          Raw data definitions
```

`data/processed` is reproducible batch output. Rerun the batch command after
regenerating raw data. The Docker stack also writes live trending snapshots to
`live_trending.csv`. `sql/bigquery_schema.sql` remains a BigQuery schema
reference; this stack currently stores dashboard tables as local CSV files.
