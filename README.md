# Movie Streaming Analytics Platform

Demo scope: BR1–BR10 — search/filter, trending, recommendations, rating analytics, genre popularity, engagement, performance, low-engagement detection, segmentation, and time trends.

```powershell
python generate_data.py
python -m pip install -r requirements.txt
streamlit run app.py
```

Pipeline: `CSV raw data → Spark (daily aggregates/recommendations) + Flink (live events) → BigQuery → Streamlit`. The demo reads CSV locally; replace the loader with BigQuery queries once tables exist.
