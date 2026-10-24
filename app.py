import csv
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None


st.set_page_config(page_title="Movie Analytics Platform", page_icon="🎬", layout="wide")
RAW_DIR = Path("data")
PROCESSED_DIR = RAW_DIR / "processed"


@st.cache_data
def load_data() -> dict[str, pd.DataFrame]:
    required = [
        "movie_performance",
        "low_engagement_movies",
        "genre_popularity",
        "subscription_views",
        "user_engagement",
        "user_segments",
        "user_preferences",
        "daily_views",
        "hourly_views",
        "summary_metrics",
    ]
    missing = [name for name in required if not (PROCESSED_DIR / f"{name}.csv").exists()]
    if missing:
        raise FileNotFoundError(
            "Processed data is missing. Run python -m src.batch_processing (Pandas) or python -m src.spark_batch_job (Spark)."
        )

    data = {name: pd.read_csv(PROCESSED_DIR / f"{name}.csv") for name in required}
    data["movies"] = pd.read_csv(RAW_DIR / "movies.csv")
    data["users"] = pd.read_csv(RAW_DIR / "users.csv", usecols=["user_id"])
    data["daily_views"]["day"] = pd.to_datetime(data["daily_views"]["day"])
    data["daily_views"] = data["daily_views"].sort_values("day")

    return data


try:
    data = load_data()
except FileNotFoundError as error:
    st.error(str(error))
    st.code("python -m src.batch_processing  # or python -m src.spark_batch_job")
    st.stop()


movies = data["movies"]
performance = data["movie_performance"]
metrics = data["summary_metrics"].iloc[0]


@st.fragment(run_every="10s")
def render_live_trending() -> None:
    live_path = PROCESSED_DIR / "live_trending.csv"
    st.subheader("BR2 - Latest 5-minute event window")
    if not live_path.exists() or live_path.stat().st_size == 0:
        st.info("Waiting for Flink to complete the first five-minute window.")
        return
    live = pd.read_csv(live_path)
    if live.empty:
        st.info("Waiting for Flink to complete the first five-minute window.")
        return
    parsed_windows = pd.to_datetime(live["window_start"], utc=True)
    latest_time = parsed_windows.max()
    if pd.Timestamp.now(tz="UTC") - latest_time > pd.Timedelta(minutes=10):
        st.info(f"Waiting for a fresh Flink result. Last result: {latest_time.isoformat()}")
        return
    latest = live.loc[parsed_windows == latest_time]
    st.caption(f"Window started: {latest_time.isoformat()} - refreshes every 10 seconds")
    st.dataframe(
        latest[["rank", "title", "genre", "live_views", "unique_viewers"]],
        hide_index=True,
        use_container_width=True,
    )


st.title("Movie Streaming Analytics Platform")
page = st.sidebar.radio("Workspace", ["Viewer", "Analyst", "Manager"])

with st.expander("Record a movie view (demo input)"):
    st.caption("Submit ghi sự kiện vào CSV, gửi vào Kafka cho BR2 và chạy lại batch để cập nhật các chart.")
    with st.form("record_watch"):
        form_user = st.selectbox("User", data["users"]["user_id"].tolist())
        form_movie = st.selectbox(
            "Movie", movies["movie_id"].tolist(),
            format_func=lambda movie_id: movies.loc[movies["movie_id"] == movie_id, "title"].iloc[0],
        )
        movie_duration = int(movies.loc[movies["movie_id"] == form_movie, "duration"].iloc[0])
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
        watches_path = RAW_DIR / "watch_history.csv"
        with watches_path.open("a", newline="", encoding="utf-8") as csv_file:
            csv.writer(csv_file).writerow(event.values())

        kafka_sent = False
        if KafkaProducer is not None:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                    value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                    acks="all",
                )
                movie_row = movies.loc[movies["movie_id"] == form_movie].iloc[0]
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
                st.warning(f"Đã lưu vào CSV, nhưng chưa gửi được Kafka: {error}")

        try:
            subprocess.run(
                [sys.executable, "-m", "src.spark_batch_job"],
                check=True, capture_output=True, text=True,
            )
            batch_engine = "Spark"
        except (subprocess.CalledProcessError, ModuleNotFoundError):
            subprocess.run([sys.executable, "-m", "src.batch_processing"], check=True)
            batch_engine = "Pandas (Spark chưa sẵn sàng)"
        st.cache_data.clear()
        st.success(
            f"Đã ghi event {event['watch_id']} vào watch_history.csv và cập nhật các bảng bằng {batch_engine}. "
            + ("Sự kiện đã gửi tới Kafka; BR2 sẽ đổi khi Flink hoàn tất cửa sổ 5 phút." if kafka_sent else "BR2 live cần Kafka/Flink đang chạy.")
        )
        st.rerun()

if page == "Viewer":
    st.subheader("BR1 - Movie Search & Filtering")
    search_col, genre_col = st.columns([2, 1])
    query = search_col.text_input("Search title")
    genre = genre_col.selectbox("Genre", ["All"] + sorted(movies["genre"].unique()))

    found = movies[movies["title"].str.contains(query, case=False, na=False, regex=False)]
    if genre != "All":
        found = found[found["genre"] == genre]
    catalog = found.merge(
        performance[["movie_id", "views", "avg_rating", "completion_rate"]],
        on="movie_id",
        how="left",
    ).sort_values("views", ascending=False)
    st.dataframe(catalog, hide_index=True, use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.subheader("BR2 - Trending Movie Detection")
        st.dataframe(
            performance.nlargest(10, "views")[["title", "genre", "views", "avg_rating"]],
            hide_index=True,
            use_container_width=True,
        )

    with right:
        selected_user = st.selectbox("Recommendations for", data["users"]["user_id"])
        preference = data["user_preferences"].query("user_id == @selected_user")
        favorite_genres = (
            preference.iloc[0]["favorite_genres"].split("|") if not preference.empty else []
        )
        st.subheader("BR3 - Personalized Movie Recommendation")
        if favorite_genres:
            recommendations = performance[
                performance["genre"].isin(favorite_genres)
            ].nlargest(8, "weighted_rating")
        else:
            st.caption("No viewing history yet; showing popular picks.")
            recommendations = performance.nlargest(8, "views")
        st.dataframe(
            recommendations[["title", "genre", "weighted_rating", "avg_rating", "rating_count", "views"]],
            hide_index=True,
            use_container_width=True,
        )

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
    rated_movies = performance.dropna(subset=["weighted_rating"]).sort_values(
        ["weighted_rating", "rating_count"], ascending=[False, False]
    )
    st.dataframe(
        rated_movies[["title", "genre", "weighted_rating", "avg_rating", "rating_count"]].head(10),
        hide_index=True,
        use_container_width=True,
    )

    st.markdown("**BR5 - Genre Popularity Analysis**")
    left, right = st.columns(2)
    left.plotly_chart(
        px.bar(
            data["genre_popularity"],
            x="genre",
            y="views",
            color="genre",
            title="Genre popularity",
            template="plotly_white",
        ),
        use_container_width=True,
    )
    st.markdown("**BR7 - Movie Performance Analysis**")
    top_movies = performance[performance["views"] >= performance["views"].quantile(0.70)]
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
            data["user_engagement"], x="views", nbins=30, title="User engagement distribution"
        ),
        use_container_width=True,
    )
    right.plotly_chart(
        px.pie(
            data["subscription_views"],
            names="subscription_plan",
            values="views",
            title="Views by subscription plan",
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        px.bar(data["hourly_views"], x="hour", y="views", title="Viewing hours"),
        use_container_width=True,
    )

else:
    st.subheader("Manager dashboard")
    st.subheader("BR8 - Low-Engagement Movie Detection")
    st.dataframe(
        data["low_engagement_movies"].head(10),
        hide_index=True,
        use_container_width=True,
    )

    st.markdown("**BR9 - User Segmentation**")
    left, right = st.columns(2)
    left.plotly_chart(
        px.pie(data["user_segments"], names="segment", values="users", title="User segments"),
        use_container_width=True,
    )
    st.markdown("**BR10 - Time-Based Movie Trend Analysis**")
    right.plotly_chart(
        px.line(data["daily_views"], x="day", y="views", title="Daily viewing trend"),
        use_container_width=True,
    )
