from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src import settings
from src.cassandra_store import CassandraUnavailable, connect, query_frame, serving_state
from src.replay_clock import ReplayClock, days_between, load_clock

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None


st.set_page_config(page_title="Movie Analytics Platform", page_icon="🎬", layout="wide")
RAW_DIR = Path("data")
BATCH_LOG = RAW_DIR / "batch_refresh.log"
TOP_SHARE = 0.30  # BR7 shows the top 30% of movies by views.
ENGINE_SUFFIX = {"Spark": "_spark", "Flink": ""}  # each engine writes its own live tables


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

state = serving_state(get_session())
if state is None:
    st.error("No batch version is published yet. Run the Spark batch job to load Cassandra.")
    st.code("python -m src.spark_batch_job --cassandra-host localhost")
    st.stop()
# Every batch-table read uses the published version, so a running batch never shows half-written data.
VERSION = state["version"]
metrics = cql("SELECT * FROM summary_metrics WHERE bucket = %s", (VERSION,)).iloc[0]
genres = sorted(cql("SELECT genre FROM genre_popularity")["genre"])


def as_date(value) -> date | None:
    """Cassandra `date` values come back as cassandra.util.Date."""
    if value is None or (not isinstance(value, (str, date)) and pd.isna(value)):
        return None
    return value if isinstance(value, date) else value.date()


# Lambda merge boundary: the batch covers days < batch_cutoff, the speed tables supply the rest.
BATCH_CUTOFF = as_date(state["batch_cutoff"])
REPLAY_DAY = as_date(state["replay_day"])


def replay_clock() -> ReplayClock | None:
    try:
        return load_clock(get_session())
    except Exception:  # table missing on an old schema, or Cassandra hiccup
        return None


def simulated_now() -> datetime:
    """Event time for new events: the replay clock when a replay runs, else the wall clock."""
    clock = replay_clock()
    return clock.sim_now() if clock is not None else datetime.now(timezone.utc)


def live_days() -> list[date]:
    """Days owned by the speed layer: batch_cutoff .. today on the simulated clock."""
    if BATCH_CUTOFF is None:
        return []
    return days_between(BATCH_CUTOFF, max(simulated_now().date(), BATCH_CUTOFF))


@st.cache_data(ttl=5, show_spinner=False)
def live_rows(table: str, days: tuple, key_column: str | None = None, key: str | None = None) -> pd.DataFrame:
    """Speed-layer rows for the given days (one partition per day), optionally for one key."""
    frames = []
    for day in days:
        if key is None:
            frames.append(query_frame(get_session(), f"SELECT * FROM {table} WHERE day = %s", [day]))
        else:
            frames.append(query_frame(
                get_session(), f"SELECT * FROM {table} WHERE day = %s AND {key_column} = %s", [day, key]
            ))
    frames = [frame for frame in frames if not frame.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def low_engagement(views: pd.Series, completion_sum: pd.Series) -> pd.Series:
    rate = (completion_sum / views).where(views > 0)
    return (views >= settings.MIN_VIEWS) & (rate < settings.LOW_COMPLETION_THRESHOLD)


def as_percent(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    return frame.assign(**{column: (frame[column] * 100).round(1)}).rename(columns={column: f"{column} %"})


def merged_movie_engagement(engine: str) -> tuple[pd.DataFrame, int]:
    """BR8 Lambda view: batch sums + live sums per movie, then the low-engagement flag."""
    batch = cql(
        "SELECT movie_id, title, genre, views, watch_minutes, completion_sum FROM low_engagement_movies "
        "WHERE bucket = %s",
        (VERSION,),
    ).set_index("movie_id")
    live = live_rows(f"live_movie_engagement{ENGINE_SUFFIX[engine]}", tuple(live_days()))
    merged = batch[["title", "genre"]].copy()
    for column in ("views", "watch_minutes", "completion_sum"):
        merged[f"batch_{column}"] = batch[column].astype(float)
        live_sum = live.groupby("movie_id")[column].sum() if not live.empty else pd.Series(dtype=float)
        merged[f"live_{column}"] = live_sum.reindex(merged.index).fillna(0.0)
        merged[column] = merged[f"batch_{column}"] + merged[f"live_{column}"]
    merged["completion_rate"] = (merged["completion_sum"] / merged["views"]).where(merged["views"] > 0)
    merged["is_low_engagement"] = low_engagement(merged["views"], merged["completion_sum"])
    merged["batch_flag"] = low_engagement(merged["batch_views"], merged["batch_completion_sum"])
    return merged.reset_index(), 0 if live.empty else int(live["views"].sum())


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
def render_replay_status() -> None:
    clock = replay_clock()
    if clock is None:
        st.caption("Live replay: not running")
        return
    state = "done, clock running" if clock.done else f"{clock.sent:,}/{clock.total:,} events"
    st.caption(
        f"Live replay of {clock.replay_day} at x{clock.speed:g}: {state}. "
        f"Simulated time {clock.sim_now().strftime('%Y-%m-%d %H:%M:%S')} UTC."
    )
    st.caption(f"Batch covers days before {BATCH_CUTOFF}; later days come from the speed layer.")


@st.fragment(run_every="10s")
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
def render_live_trending(engine: str) -> None:
    st.subheader("BR2 - Latest 5-minute event window (speed layer)")
    st.caption(f"{engine} results, refreshed every 10 seconds. Windows use event time (watch_timestamp).")
    render_engine_trending(f"live_trending{ENGINE_SUFFIX[engine]}", engine)


def render_engine_trending(table: str, engine: str) -> None:
    now = simulated_now()
    today = now.date()
    live = pd.DataFrame()
    for day in (today, today - timedelta(days=1)):
        live = query_frame(
            get_session(),
            "SELECT window_start, rank, title, genre, live_views, unique_viewers "
            f"FROM {table} WHERE day = %s LIMIT 10",
            [day],
        )
        if not live.empty:
            break
    if live.empty:
        st.info(f"Waiting for {engine} to complete the first five-minute window.")
        return
    latest_window = live["window_start"].max()
    live = live.loc[live["window_start"] == latest_window]
    latest_time = pd.Timestamp(latest_window).tz_localize("UTC")
    window = pd.Timedelta(seconds=settings.TREND_WINDOW_SECONDS)
    if pd.Timestamp(now) - latest_time > 3 * window + pd.Timedelta(seconds=settings.WATERMARK_SECONDS):
        st.info(f"Waiting for a fresh {engine} result. Last result: {latest_time.isoformat()}")
        return
    st.caption(f"Window started: {latest_time.isoformat()} (simulated now: {now.strftime('%Y-%m-%d %H:%M:%S')})")
    st.dataframe(
        live[["rank", "title", "genre", "live_views", "unique_viewers"]],
        hide_index=True,
        use_container_width=True,
    )


@st.fragment(run_every="10s")
def render_live_engagement(engine: str, user_ids: list[str]) -> None:
    """BR6 speed layer: per (user, day) totals, and one user's batch + live (Lambda merge)."""
    st.markdown(f"**BR6 - Live engagement per user and day ({engine} speed layer)**")
    days = live_days()
    if not days:
        st.info("No live day: the batch covers the whole history (replay_day = none).")
        return
    table = f"live_user_engagement{ENGINE_SUFFIX[engine]}"
    today = days[-1]
    live_today = live_rows(table, (today,))
    st.caption(f"Simulated day {today}: {len(live_today):,} active users so far.")
    if not live_today.empty:
        live_today["avg_completion"] = live_today["completion_sum"] / live_today["views"]
        top_users = live_today.sort_values(["views", "watch_minutes"], ascending=False).head(10)[
            ["user_id", "subscription_plan", "views", "watch_minutes", "avg_completion", "updated_at"]
        ]
        st.dataframe(as_percent(top_users, "avg_completion"), hide_index=True, use_container_width=True)
    user_id = st.selectbox("User lookup (batch history + live days)", user_ids, key="br6_user")
    batch = cql(
        "SELECT views, watch_minutes, active_days, completion_sum, segment FROM user_engagement WHERE user_id = %s",
        (user_id,),
    )
    live = live_rows(table, tuple(days), "user_id", user_id)
    batch_row = batch.iloc[0] if not batch.empty else pd.Series(
        {"views": 0, "watch_minutes": 0, "active_days": 0, "completion_sum": 0.0, "segment": "Inactive"}
    )
    live_views = int(live["views"].sum()) if not live.empty else 0
    views = int(batch_row["views"]) + live_views
    completion_sum = float(batch_row["completion_sum"] or 0) + (float(live["completion_sum"].sum()) if not live.empty else 0.0)
    columns = st.columns(4)
    columns[0].metric("Views", f"{views:,}", delta=f"+{live_views} live" if live_views else None)
    columns[1].metric(
        "Watch minutes",
        f"{int(batch_row['watch_minutes']) + (int(live['watch_minutes'].sum()) if not live.empty else 0):,}",
    )
    columns[2].metric("Active days", int(batch_row["active_days"]) + (int((live["views"] > 0).sum()) if not live.empty else 0))
    columns[3].metric("Avg completion", f"{completion_sum / views:.1%}" if views else "-")


def catalog_for(genre: str) -> pd.DataFrame:
    """BR1 catalogue: one Cassandra partition per (genre, version), sorted by views."""
    selected = genres if genre == "All" else [genre]
    frames = [
        cql(
            "SELECT movie_id, title, genre, release_year, duration, content_type, language, maturity_rating, "
            "views, avg_rating, completion_rate FROM movie_performance WHERE genre = %s AND bucket = %s",
            (name, VERSION),
        )
        for name in selected
    ]
    return pd.concat(frames, ignore_index=True).sort_values("views", ascending=False)


def filter_catalog(catalog: pd.DataFrame) -> pd.DataFrame:
    """BR1 filters on title, release year, language, content type and maturity rating."""
    query = st.text_input("Search title")
    columns = st.columns(4)
    years = catalog["release_year"].dropna()
    low, high = (int(years.min()), int(years.max())) if not years.empty else (1900, 2100)
    year_range = columns[0].slider("Release year", low, max(high, low + 1), (low, max(high, low + 1)))
    choices = {}
    for column, label, slot in (("language", "Language", 1), ("content_type", "Content type", 2),
                                ("maturity_rating", "Maturity rating", 3)):
        options = sorted(catalog[column].dropna().unique())
        choices[column] = columns[slot].multiselect(label, options)
    mask = catalog["title"].str.contains(query, case=False, na=False, regex=False)
    mask &= catalog["release_year"].between(*year_range)
    for column, selected in choices.items():
        if selected:
            mask &= catalog[column].isin(selected)
    return catalog[mask]


def render_rating_distribution(container, movie_id: str) -> None:
    """BR4 for one movie: rating count, average, weighted rating and the 1-5 star distribution."""
    distribution = cql("SELECT * FROM rating_distribution WHERE movie_id = %s", (movie_id,))
    if distribution.empty:
        container.info("This movie has no ratings yet.")
        return
    row = distribution.iloc[0]
    stars = pd.DataFrame({"stars": [1, 2, 3, 4, 5], "ratings": [int(row[f"r{star}"]) for star in range(1, 6)]})
    container.plotly_chart(
        px.bar(stars, x="stars", y="ratings",
               title=f"{row['title']}: {int(row['rating_count'])} ratings, average {row['avg_rating']:.2f}",
               template="plotly_white"),
        use_container_width=True,
    )


def batch_rows(statement: str, params: tuple = ()) -> pd.DataFrame:
    """Rows written by the published batch version (for tables that carry it as a column)."""
    frame = cql(statement, params)
    return frame[frame["bucket"] == VERSION].drop(columns="bucket") if not frame.empty else frame


st.title("Movie Streaming Analytics Platform")
page = st.sidebar.radio("Workspace", ["Viewer", "Analyst", "Manager"])
engine = st.sidebar.radio("Speed-layer engine (BR2, BR6, BR8)", ["Spark", "Flink"], horizontal=True)
if st.sidebar.button("Refresh batch (Spark)"):
    start_batch()
with st.sidebar:
    render_batch_status()
    render_replay_status()

with st.expander("Record a movie view (demo input)"):
    st.caption(
        "Sends a watch event to Kafka `watch-events`, stamped with the simulated replay time. The speed "
        "layer updates BR2/BR6/BR8 within seconds; the archiver stores it in the raw zone, so the next "
        "batch run includes it."
    )
    user_options = cql("SELECT user_id, subscription_plan FROM user_engagement")
    users = user_options["user_id"].sort_values().tolist()
    movie_options = catalog_for("All")[["movie_id", "title", "genre", "duration"]].sort_values("movie_id")
    with st.form("record_watch"):
        form_user = st.selectbox("User", users)
        form_movie = st.selectbox(
            "Movie", movie_options["movie_id"].tolist(),
            format_func=lambda movie_id: movie_options.loc[movie_options["movie_id"] == movie_id, "title"].iloc[0],
        )
        movie_duration = int(movie_options.loc[movie_options["movie_id"] == form_movie, "duration"].iloc[0])
        form_minutes = st.slider("Minutes watched", min_value=1, max_value=max(1, movie_duration), value=max(1, movie_duration // 2))
        form_device = st.selectbox("Device", ["Web", "Mobile", "Tablet", "TV"])
        submitted = st.form_submit_button("Send view event")

    if submitted:
        from src.kafka_event_producer import watch_event

        event_time = simulated_now().replace(microsecond=0)
        movie_row = movie_options.loc[movie_options["movie_id"] == form_movie].iloc[0]
        plan = user_options.loc[user_options["user_id"] == form_user, "subscription_plan"].iloc[0]
        event = watch_event(
            {"watch_id": f"FORM-{uuid.uuid4().hex[:12]}", "user_id": form_user, "movie_id": form_movie,
             "watch_duration": form_minutes, "device": form_device},
            {"title": movie_row["title"], "genre": movie_row["genre"], "duration": movie_duration},
            {"subscription_plan": plan},
            event_time,
        )
        if KafkaProducer is None:
            st.error("kafka-python is not installed; the event was not sent.")
        else:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                    value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                    acks="all",
                )
                producer.send(
                    settings.WATCH_TOPIC, key=str(form_movie).encode("utf-8"), value=event
                ).get(timeout=10)
                producer.close()
                st.success(
                    f"Event {event['watch_id']} sent at simulated time {event['watch_timestamp']}. "
                    "BR6/BR8 live totals update within seconds; BR2 when its 5-minute window closes."
                )
            except Exception as error:
                st.error(f"Could not send to Kafka: {error}")

if page == "Viewer":
    st.subheader("BR1 - Movie Search & Filtering")
    genre = st.selectbox("Genre", ["All"] + genres)
    # Cassandra has no substring search: read the genre partition(s) by key, filter the rest here.
    catalog = filter_catalog(catalog_for(genre))
    st.caption(f"{len(catalog):,} movies")
    st.dataframe(as_percent(catalog, "completion_rate"), hide_index=True, use_container_width=True)
    if not catalog.empty:
        st.markdown("**BR4 - Ratings of a movie**")
        shown = catalog.head(500)  # the 500 most viewed matches of the current search
        rated_movie = st.selectbox(
            "Movie", shown["movie_id"].tolist(), key="viewer_rating_movie",
            format_func=lambda movie_id: shown.loc[shown["movie_id"] == movie_id, "title"].iloc[0],
        )
        render_rating_distribution(st, rated_movie)

    left, right = st.columns(2)
    with left:
        st.subheader("Most viewed (batch, all history)")
        st.dataframe(
            cql(
                "SELECT title, genre, views, avg_rating FROM top_viewed_movies WHERE bucket = %s LIMIT 10",
                (VERSION,),
            ),
            hide_index=True,
            use_container_width=True,
        )

    with right:
        st.subheader("BR3 - Personalized Movie Recommendation")
        selected_user = st.selectbox("Recommendations for", users)
        preference = cql("SELECT favorite_genres FROM user_preferences WHERE user_id = %s", (selected_user,))
        recommendations = batch_rows(
            "SELECT rank, bucket, title, genre, weighted_rating, rating_count FROM user_recommendations "
            "WHERE user_id = %s",
            (selected_user,),
        )
        if not recommendations.empty:
            st.caption(
                f"Favourite genres: {preference.iloc[0]['favorite_genres'].replace('|', ', ')}. "
                "Movies already watched are excluded; ranked by weighted rating."
            )
        else:
            st.caption("No viewing history yet; showing popular picks.")
            recommendations = cql(
                "SELECT title, genre, avg_rating, views FROM top_viewed_movies WHERE bucket = %s LIMIT %s",
                (VERSION, settings.RECOMMENDATIONS_TOP_N),
            )
        st.dataframe(recommendations, hide_index=True, use_container_width=True)

    render_live_trending(engine)

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
    left, right = st.columns(2)
    top_rated = cql(
        "SELECT movie_id, title, genre, weighted_rating, avg_rating, rating_count "
        "FROM top_rated_movies WHERE bucket = %s LIMIT 10",
        (VERSION,),
    )
    left.dataframe(top_rated.drop(columns="movie_id"), hide_index=True, use_container_width=True)
    distribution_movie = right.selectbox(
        "Rating distribution of", top_rated["movie_id"].tolist(),
        format_func=lambda movie_id: top_rated.loc[top_rated["movie_id"] == movie_id, "title"].iloc[0],
    )
    render_rating_distribution(right, distribution_movie)

    st.markdown("**BR5 - Genre Popularity Analysis**")
    genre_table = cql("SELECT genre, views, unique_viewers, watch_hours, avg_rating FROM genre_popularity")
    left, right = st.columns(2)
    left.plotly_chart(
        px.bar(genre_table.sort_values("views", ascending=False), x="genre", y="views", color="genre",
               title="Views by genre", template="plotly_white"),
        use_container_width=True,
    )
    right.dataframe(genre_table.sort_values("views", ascending=False), hide_index=True, use_container_width=True)
    monthly = pd.concat(
        [cql("SELECT * FROM genre_monthly WHERE genre = %s", (name,)) for name in genres], ignore_index=True
    )
    if not monthly.empty:
        monthly["month"] = pd.to_datetime(monthly["month"].astype(str))
        measure = st.radio("Monthly measure", ["views", "unique_viewers", "watch_hours", "avg_rating"], horizontal=True)
        st.plotly_chart(
            px.line(monthly.sort_values("month"), x="month", y=measure, color="genre", markers=True,
                    title=f"Genre {measure.replace('_', ' ')} per month", template="plotly_white"),
            use_container_width=True,
        )
    st.markdown("**BR7 - Movie Performance Analysis**")
    top_n = math.ceil(int(metrics["movies"]) * TOP_SHARE)
    top_movies = cql(
        "SELECT title, genre, views, avg_rating, completion_rate, watch_hours "
        "FROM top_viewed_movies WHERE bucket = %s LIMIT %s",
        (VERSION, top_n),
    )
    performance_chart = px.scatter(
        top_movies,
        x="views",
        y="avg_rating",
        color="genre",
        hover_name="title",
        hover_data={"completion_rate": ":.1%", "watch_hours": True},
        log_x=True,
        opacity=0.65,
        title="Movie performance - top 30% by views",
        template="plotly_white",
    )
    st.plotly_chart(performance_chart, use_container_width=True)

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
    render_live_engagement(engine, users)

else:
    st.subheader("Manager dashboard")
    st.subheader("BR8 - Low-Engagement Movie Detection")
    merged, live_views = merged_movie_engagement(engine)
    flagged = merged[merged["is_low_engagement"]].sort_values(["completion_rate", "views"], ascending=[True, False])
    newly = flagged[~flagged["batch_flag"]]
    st.caption(
        f"Flag: views >= {settings.MIN_VIEWS} and average completion < {settings.LOW_COMPLETION_THRESHOLD:.0%} "
        f"(completion of a view = min(1, watch_duration / duration)). Batch history before {BATCH_CUTOFF} "
        f"+ {live_views:,} live views from the {engine} speed layer."
    )
    columns = st.columns(3)
    columns[0].metric("Low-engagement movies", f"{len(flagged):,}")
    columns[1].metric("Newly flagged by live views", f"{len(newly):,}")
    columns[2].metric("Movies with live views today", f"{int((merged['live_views'] > 0).sum()):,}")
    st.dataframe(
        as_percent(flagged[["movie_id", "title", "genre", "views", "live_views", "completion_rate"]].head(50),
                   "completion_rate"),
        hide_index=True, use_container_width=True,
    )

    st.markdown("**BR7 - Movie Performance (top 10 by views)**")
    top_performers = cql(
        "SELECT title, genre, views, unique_viewers, avg_rating, completion_rate, watch_hours "
        "FROM top_viewed_movies WHERE bucket = %s LIMIT 10", (VERSION,)
    )
    top_performers.insert(0, "rank", range(1, len(top_performers) + 1))
    st.dataframe(as_percent(top_performers, "completion_rate"), hide_index=True, use_container_width=True)

    st.markdown("**BR9 - User Segmentation**")
    st.caption("Total views over the history: Inactive 0-10, Casual 11-35, Active > 35 (users without views count).")
    st.plotly_chart(
        px.pie(cql("SELECT segment, users FROM user_segments"), names="segment", values="users", title="User segments"),
        use_container_width=True,
    )

    st.markdown("**BR10 - Time-Based Movie Trend Analysis**")
    columns = st.columns(3)
    scope = columns[0].selectbox("Scope", ["Platform", "Genre", "Movie"])
    if scope == "Platform":
        scope_key, key = "all", "all"
    elif scope == "Genre":
        scope_key, key = "genre", columns[1].selectbox("Genre ", genres)
    else:
        scope_key = "movie"
        key = columns[1].selectbox(
            "Movie", movie_options["movie_id"].tolist(),
            format_func=lambda movie_id: movie_options.loc[movie_options["movie_id"] == movie_id, "title"].iloc[0],
        )
    periods = ["week", "month"] if scope_key == "movie" else ["day", "week", "month"]
    period = columns[2].selectbox("Period", periods)
    trends = batch_rows(
        "SELECT period_start, views, prev_views, growth, bucket FROM view_trends "
        "WHERE scope = %s AND key = %s AND period = %s",
        (scope_key, key, period),
    )
    if trends.empty:
        st.info("No views for this selection.")
    else:
        trends["period_start"] = pd.to_datetime(trends["period_start"].astype(str))
        left, right = st.columns([2, 1])
        left.plotly_chart(
            px.line(trends, x="period_start", y="views", markers=True, title=f"Views per {period}",
                    template="plotly_white"),
            use_container_width=True,
        )
        latest = trends.sort_values("period_start", ascending=False).head(12)
        right.dataframe(as_percent(latest, "growth"), hide_index=True, use_container_width=True)
        st.caption(f"Growth = change versus the previous {period}. Days from {BATCH_CUTOFF} on are in the speed layer.")
