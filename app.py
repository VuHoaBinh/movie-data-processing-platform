import csv
import json
import math
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src.cassandra_store import BATCH_BUCKET, CassandraUnavailable, connect, query_frame

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None


st.set_page_config(page_title="Movie Analytics Platform", page_icon="🎬", layout="wide")
RAW_DIR = Path("data")
BATCH_LOG = RAW_DIR / "batch_refresh.log"
TOP_SHARE = 0.30  # BR7 shows the top 30% of movies by views.


@st.cache_resource
def get_session():
    return connect()


@st.cache_data(ttl=60, show_spinner=False)
def cql(statement: str, params: tuple = ()) -> pd.DataFrame:
    """Run one read-only CQL query against the serving layer (cached for a minute)."""
    return query_frame(get_session(), statement, list(params))


try:
    get_session()
except CassandraUnavailable as error:
    st.error(f"Serving layer unavailable. {error}")
    st.caption("Start it with `docker compose up -d cassandra cassandra-init`, then run the Spark batch job.")
    st.stop()

metrics_frame = cql("SELECT * FROM summary_metrics WHERE bucket = %s", (BATCH_BUCKET,))
if metrics_frame.empty:
    st.error("The serving tables are empty. Run the Spark batch job to load Cassandra.")
    st.code("python -m src.spark_batch_job --cassandra-host localhost")
    st.stop()
metrics = metrics_frame.iloc[0]
genres = sorted(cql("SELECT genre FROM genre_popularity")["genre"])


@st.cache_resource
def batch_runner() -> dict:
    return {"process": None, "started": None, "dirty": False}


def start_batch() -> None:
    """Start the Spark batch in the background; the UI keeps responding while it runs."""
    runner = batch_runner()
    if runner["process"] is not None and runner["process"].poll() is None:
        runner["dirty"] = True  # rerun once the current batch finishes
        return
    log = BATCH_LOG.open("w", encoding="utf-8")
    runner["process"] = subprocess.Popen(
        [sys.executable, "-m", "src.spark_batch_job"], stdout=log, stderr=subprocess.STDOUT
    )
    runner["started"] = datetime.now(timezone.utc)
    runner["dirty"] = False


@st.fragment(run_every="5s")
def render_batch_status() -> None:
    runner = batch_runner()
    process = runner["process"]
    if process is None:
        st.caption("Batch: idle")
        return
    code = process.poll()
    if code is None:
        elapsed = int((datetime.now(timezone.utc) - runner["started"]).total_seconds())
        st.info(f"Spark batch running in the background ({elapsed}s). Charts update when it finishes.")
    elif runner["dirty"]:
        start_batch()
    elif code == 0:
        st.success("Spark batch finished; Cassandra serving tables are fresh.")
        if runner.pop("announced", None) != process.pid:
            runner["announced"] = process.pid
            st.cache_data.clear()
            st.rerun()
    else:
        st.error(f"Spark batch failed (exit code {code}). See {BATCH_LOG}.")


@st.fragment(run_every="10s")
def render_live_trending() -> None:
    st.subheader("BR2 - Latest 5-minute event window")
    today = datetime.now(timezone.utc).date()
    live = pd.DataFrame()
    for day in (today, today - timedelta(days=1)):
        live = query_frame(
            get_session(),
            "SELECT window_start, rank, title, genre, live_views, unique_viewers "
            "FROM live_trending WHERE day = %s LIMIT 10",
            [day],
        )
        if not live.empty:
            break
    if live.empty:
        st.info("Waiting for Flink to complete the first five-minute window.")
        return
    latest_time = pd.Timestamp(live["window_start"].max()).tz_localize("UTC")
    if pd.Timestamp.now(tz="UTC") - latest_time > pd.Timedelta(minutes=10):
        st.info(f"Waiting for a fresh Flink result. Last result: {latest_time.isoformat()}")
        return
    st.caption(f"Window started: {latest_time.isoformat()} - refreshes every 10 seconds")
    st.dataframe(
        live[["rank", "title", "genre", "live_views", "unique_viewers"]],
        hide_index=True,
        use_container_width=True,
    )


def catalog_for(genre: str) -> pd.DataFrame:
    selected = genres if genre == "All" else [genre]
    frames = [
        cql(
            "SELECT movie_id, title, genre, release_year, duration, views, avg_rating, completion_rate "
            "FROM movie_performance WHERE genre = %s",
            (name,),
        )
        for name in selected
    ]
    return pd.concat(frames, ignore_index=True).sort_values("views", ascending=False)


st.title("Movie Streaming Analytics Platform")
page = st.sidebar.radio("Workspace", ["Viewer", "Analyst", "Manager"])
if st.sidebar.button("Refresh batch (Spark)"):
    start_batch()
with st.sidebar:
    render_batch_status()

with st.expander("Record a movie view (demo input)"):
    st.caption(
        "Submit appends the event to the raw CSV, sends it to Kafka for BR2 and refreshes the batch "
        "tables with Spark in the background."
    )
    users = cql("SELECT user_id FROM user_engagement")["user_id"].sort_values().tolist()
    movie_options = cql("SELECT movie_id, title, genre, duration FROM movie_performance").sort_values("movie_id")
    with st.form("record_watch"):
        form_user = st.selectbox("User", users)
        form_movie = st.selectbox(
            "Movie", movie_options["movie_id"].tolist(),
            format_func=lambda movie_id: movie_options.loc[movie_options["movie_id"] == movie_id, "title"].iloc[0],
        )
        movie_duration = int(movie_options.loc[movie_options["movie_id"] == form_movie, "duration"].iloc[0])
        form_minutes = st.slider("Minutes watched", min_value=1, max_value=max(1, movie_duration), value=max(1, movie_duration // 2))
        form_device = st.selectbox("Device", ["Web", "Mobile", "Tablet", "TV"])
        submitted = st.form_submit_button("Save view event")

    if submitted:
        # Match the raw CSV format (naive UTC, "YYYY-MM-DD HH:MM:SS") so batch parsing stays uniform.
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        event = {
            "watch_id": f"FORM-{uuid.uuid4().hex[:12]}",
            "user_id": form_user,
            "movie_id": form_movie,
            "watch_timestamp": timestamp,
            "watch_duration": form_minutes,
            "device": form_device,
        }
        with (RAW_DIR / "watch_history.csv").open("a", newline="", encoding="utf-8") as csv_file:
            csv.writer(csv_file).writerow(event.values())

        kafka_sent = False
        if KafkaProducer is not None:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                    value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                    acks="all",
                )
                movie_row = movie_options.loc[movie_options["movie_id"] == form_movie].iloc[0]
                producer.send(
                    os.getenv("KAFKA_WATCH_TOPIC", "watch-events"),
                    key=str(form_movie).encode("utf-8"),
                    value={
                        "watch_id": event["watch_id"], "user_id": form_user,
                        "movie_id": form_movie, "title": movie_row["title"],
                        "genre": movie_row["genre"], "source_timestamp": timestamp,
                    },
                    timestamp_ms=int(datetime.now(timezone.utc).timestamp() * 1000),
                ).get(timeout=10)
                producer.close()
                kafka_sent = True
            except Exception as error:
                st.warning(f"Saved to CSV, but could not send to Kafka: {error}")

        start_batch()
        st.success(
            f"Event {event['watch_id']} saved to watch_history.csv; the Spark batch is refreshing in the background. "
            + ("Sent to Kafka; BR2 changes when Flink closes the 5-minute window." if kafka_sent else "BR2 live needs Kafka/Flink running.")
        )

if page == "Viewer":
    st.subheader("BR1 - Movie Search & Filtering")
    search_col, genre_col = st.columns([2, 1])
    query = search_col.text_input("Search title")
    genre = genre_col.selectbox("Genre", ["All"] + genres)

    # Cassandra has no substring search: read the genre partition(s) by key, filter titles here.
    catalog = catalog_for(genre)
    catalog = catalog[catalog["title"].str.contains(query, case=False, na=False, regex=False)]
    st.dataframe(catalog, hide_index=True, use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.subheader("BR2 - Trending Movie Detection")
        st.dataframe(
            cql(
                "SELECT title, genre, views, avg_rating FROM top_viewed_movies WHERE bucket = %s LIMIT 10",
                (BATCH_BUCKET,),
            ),
            hide_index=True,
            use_container_width=True,
        )

    with right:
        selected_user = st.selectbox("Recommendations for", users)
        preference = cql("SELECT favorite_genres FROM user_preferences WHERE user_id = %s", (selected_user,))
        favorite_genres = preference.iloc[0]["favorite_genres"].split("|") if not preference.empty else []
        st.subheader("BR3 - Personalized Movie Recommendation")
        if favorite_genres:
            # Top 8 of the union is contained in the union of each genre's top 8.
            recommendations = pd.concat(
                [
                    cql(
                        "SELECT title, genre, weighted_rating, avg_rating, rating_count, views "
                        "FROM movies_by_genre_rating WHERE genre = %s LIMIT 8",
                        (name,),
                    )
                    for name in favorite_genres
                ]
            ).nlargest(8, "weighted_rating")
        else:
            st.caption("No viewing history yet; showing popular picks.")
            recommendations = cql(
                "SELECT title, genre, avg_rating, views FROM top_viewed_movies WHERE bucket = %s LIMIT 8",
                (BATCH_BUCKET,),
            )
        st.dataframe(recommendations, hide_index=True, use_container_width=True)

    render_live_trending()

elif page == "Analyst":
    st.subheader("Analyst dashboard - BR4 to BR7")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Users", f"{int(metrics['users']):,}")
    col2.metric("Movies", f"{int(metrics['movies']):,}")
    col3.metric("Views", f"{int(metrics['views']):,}")
    col4.metric("Average rating", f"{metrics['average_rating']:.2f} / 5")

    st.markdown("**BR4 - Movie Rating Analytics**")
    st.caption(
        "Ranked by Bayesian weighted rating: each average is pulled toward the overall mean "
        "until a title has about the median number of ratings."
    )
    st.dataframe(
        cql(
            "SELECT title, genre, weighted_rating, avg_rating, rating_count "
            "FROM top_rated_movies WHERE bucket = %s LIMIT 10",
            (BATCH_BUCKET,),
        ),
        hide_index=True,
        use_container_width=True,
    )

    st.markdown("**BR5 - Genre Popularity Analysis**")
    left, right = st.columns(2)
    left.plotly_chart(
        px.bar(
            cql("SELECT genre, views, unique_viewers FROM genre_popularity").sort_values("views", ascending=False),
            x="genre",
            y="views",
            color="genre",
            title="Genre popularity",
            template="plotly_white",
        ),
        use_container_width=True,
    )
    st.markdown("**BR7 - Movie Performance Analysis**")
    top_n = math.ceil(int(metrics["movies"]) * TOP_SHARE)
    top_movies = cql(
        "SELECT title, genre, views, avg_rating, completion_rate, watch_hours "
        "FROM top_viewed_movies WHERE bucket = %s LIMIT %s",
        (BATCH_BUCKET, top_n),
    )
    performance_chart = px.scatter(
        top_movies,
        x="views",
        y="avg_rating",
        color="genre",
        hover_name="title",
        hover_data=["completion_rate", "watch_hours"],
        log_x=True,
        opacity=0.65,
        title="Movie performance - top 30% by views",
        template="plotly_white",
    )
    right.plotly_chart(performance_chart, use_container_width=True)

    st.markdown("**BR6 - User Engagement Analysis**")
    left, right = st.columns(2)
    left.plotly_chart(
        px.histogram(
            cql("SELECT views FROM user_engagement"), x="views", nbins=30, title="User engagement distribution"
        ),
        use_container_width=True,
    )
    right.plotly_chart(
        px.pie(
            cql("SELECT subscription_plan, views FROM subscription_views"),
            names="subscription_plan",
            values="views",
            title="Views by subscription plan",
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        px.bar(cql("SELECT hour, views FROM hourly_views").sort_values("hour"), x="hour", y="views", title="Viewing hours"),
        use_container_width=True,
    )

else:
    st.subheader("Manager dashboard")
    st.subheader("BR8 - Low-Engagement Movie Detection")
    st.dataframe(
        cql(
            "SELECT movie_id, title, genre, views, completion_rate "
            "FROM low_engagement_movies WHERE bucket = %s LIMIT 10",
            (BATCH_BUCKET,),
        ),
        hide_index=True,
        use_container_width=True,
    )

    st.markdown("**BR9 - User Segmentation**")
    left, right = st.columns(2)
    left.plotly_chart(
        px.pie(cql("SELECT segment, users FROM user_segments"), names="segment", values="users", title="User segments"),
        use_container_width=True,
    )
    st.markdown("**BR10 - Time-Based Movie Trend Analysis**")
    daily = cql("SELECT day, views FROM daily_views WHERE bucket = %s", (BATCH_BUCKET,))
    daily["day"] = pd.to_datetime(daily["day"].astype(str))
    right.plotly_chart(
        px.line(daily.sort_values("day"), x="day", y="views", title="Daily viewing trend"),
        use_container_width=True,
    )
