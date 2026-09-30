# Movie Streaming Analytics Platform

A reference data-processing project covering BR1-BR10: movie search, trending,
recommendations, ratings, genre popularity, engagement, movie performance,
low-engagement detection, user segmentation, and time-based trends.

## Architecture

```text
users / movies / ratings / watch_history
                  |
                  v
          data/ (raw CSV files)
                  |
        +---------+----------+
        |                    |
        v                    v
  Batch processing      Stream replay
  Pandas / Spark        Flink-like windows
        |                    |
        +---------+----------+
                  v
          data/processed/
                  |
        BigQuery-ready tables
                  |
                  v
         Streamlit dashboard
       Viewer / Analyst / Manager
```

The default local pipeline uses Pandas so it runs without a cluster. The
equivalent Spark aggregation is in `src/spark_batch_job.py`; the event-time
window simulation in `src/stream_processing.py` represents the Flink use case.

## Run locally

Use Python 3.12 or 3.13 for the broadest compatibility with Pandas, Streamlit,
PySpark, and PyFlink. A virtual environment is recommended.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# Optional: recreate 1,570,000 raw rows with the default sizes.
python generate_data.py

# Required: build dashboard-ready tables and simulated live trends.
python -m src.batch_processing
python -m src.stream_processing

streamlit run app.py
```

## Run with Docker on Windows

Use this option when Windows Smart App Control blocks `streamlit.exe` or a
Pandas `.pyd` file. Start Docker Desktop, then run:

```powershell
docker compose up --build
```

Open `http://localhost:8501`. The container runs the batch and stream jobs
before starting Streamlit. Processed CSV files are written back to
`data/processed` through the mounted data directory.

Stop the application with `Ctrl+C`, followed by:

```powershell
docker compose down
```

For a smaller dataset:

```powershell
python generate_data.py --users 2000 --movies 1000 --ratings 20000 --watches 50000
```

## Project structure

```text
data/                       Raw CSV input
data/processed/             Generated analytical tables
src/batch_processing.py     Local batch ETL pipeline
src/spark_batch_job.py      Spark implementation example
src/stream_processing.py    Event-time streaming simulation
sql/bigquery_schema.sql     Warehouse table definitions
app.py                      Streamlit presentation layer
data_dictionary.md          Raw data definitions
```

`data/processed` is reproducible output: rerun the two processing commands
after regenerating raw data. In production, write these outputs to BigQuery
and replace the CSV reads in `app.py` with parameterized BigQuery queries.
