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

from src.cassandra_store import BATCH_BUCKET, CassandraUnavailable, connect, query_frame, table_counts
from src.data_quality import REASONS

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None


st.set_page_config(page_title="Movie Analytics Platform", page_icon="🎬", layout="wide")
RAW_DIR = Path("data")
BATCH_LOG = RAW_DIR / "batch_refresh.log"
TOP_SHARE = 0.30  # BR7 shows the top 30% of movies by views.
LIVE_STALE_MINUTES = 10  # BR2 live window older than this is "waiting for Flink".
INACTIVE_ALERT_SHARE = 0.40  # Manager alert: more than 40% of users are Inactive (<= 10 views).
LOW_ENGAGEMENT_SHARE = 0.10  # Manager: a title is low-engagement below 10% of the average views per title.
TREND_DROP_ALERT = 0.10  # Manager alert: last 7 days of views fell more than 10% against the 7 days before.
LOG_TAIL_LINES = 25
STAGE_ORDER = ["hive_raw", "hive_curated", "build_tables", "als", "hive_analytics", "cassandra_load"]

# One colour per genre, shared by every chart; segments and plans have fixed colours too.
ACCENT = "#4C78A8"
SEGMENT_COLORS = {"Inactive": "#E4572E", "Casual": "#F3A712", "Active": "#29A375"}
PLAN_COLORS = {"Basic": "#9DB4D8", "Standard": "#5C80BC", "Premium": "#2D4A82"}
RECOMMENDERS = {
    "Genre-based": (
        "user_recommendations",
        "Takes the user's two most-watched genres and lists the best Bayesian-rated titles in them that the user "
        "has not watched; users with no history get the most-viewed titles.",
    ),
    "Collaborative filtering (ALS)": (
        "user_recommendations_als",
        "Spark MLlib ALS learns taste factors from who watched what and suggests unwatched titles that similar "
        "viewers watched; it needs at least one watch.",
    ),
}


@st.cache_resource
def get_session():
    return connect()


@st.cache_data(ttl=60, show_spinner=False)
def cql(statement: str, params: tuple = ()) -> pd.DataFrame:
    """Run one read-only CQL query against the serving layer (cached for a minute)."""
    return query_frame(get_session(), statement, list(params))


@st.cache_data(ttl=300, show_spinner="Reading the engagement table...")
def engagement_scan() -> pd.DataFrame:
    """The one full scan of the dashboard (BR6): every user's views, watch minutes and active days at once."""
    return query_frame(
        get_session(),
        "SELECT user_id, subscription_plan, region, views, watch_minutes, active_days FROM user_engagement",
    )


def whole(value) -> str:
    return f"{int(value):,}"


def compact(value) -> str:
    """Whole numbers with separators; millions as 1.00M so the value fits a KPI card."""
    return f"{float(value) / 1e6:.2f}M" if abs(float(value)) >= 1e6 else f"{float(value):,.0f}"


def ask(code: str, question: str) -> None:
    """One-line caption naming the business requirement a chart or table answers."""
    st.caption(f"**{code}** · {question}")


def styled(figure, height: int = 330):
    figure.update_layout(template="plotly_white", height=height, margin=dict(l=10, r=10, t=45, b=10), legend_title_text="")
    return figure


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
GENRE_COLORS = {name: px.colors.qualitative.Safe[index % len(px.colors.qualitative.Safe)] for index, name in enumerate(genres)}


@st.cache_resource
def batch_runner() -> dict:
    return {"process": None, "started": None, "dirty": False, "error": None}


def start_batch() -> None:
    """Start the Spark batch in the background; the UI keeps responding while it runs."""
    runner = batch_runner()
    if runner["process"] is not None and runner["process"].poll() is None:
        runner["dirty"] = True  # rerun once the current batch finishes
        return
    try:
        with BATCH_LOG.open("w", encoding="utf-8") as log:
            runner["process"] = subprocess.Popen(
                [sys.executable, "-m", "src.spark_batch_job"], stdout=log, stderr=subprocess.STDOUT
            )
    except OSError as error:  # no write access to data/, interpreter missing...
        runner["process"], runner["error"] = None, str(error)
        return
    runner["started"] = datetime.now(timezone.utc)
    runner["dirty"] = False
    runner["error"] = None


def log_tail(lines: int = LOG_TAIL_LINES) -> str:
    try:
        text = BATCH_LOG.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        return f"(log not readable: {error})"
    return "\n".join(text.splitlines()[-lines:]) or "(the batch wrote no output)"


@st.fragment(run_every="5s")
def render_batch_status() -> None:
    runner = batch_runner()
    process = runner["process"]
    if runner["error"]:
        st.error(f"Could not start the Spark batch: {runner['error']}")
        return
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
        st.error(f"Spark batch failed (exit code {code}). Last lines of {BATCH_LOG}:")
        st.code(log_tail(), language="text")
        if st.button("Retry batch", key="retry_batch"):
            start_batch()
            st.rerun()


def latest_live_window() -> pd.DataFrame:
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
    return live


@st.fragment(run_every="10s")
def render_live_trending() -> None:
    ask("BR2", "What is being watched right now? Top 10 of the latest 5-minute window (Kafka + Flink).")
    live = latest_live_window()
    if live.empty:
        st.info("Waiting for Flink to complete the first five-minute window.")
        return
    latest_time = pd.Timestamp(live["window_start"].max()).tz_localize("UTC")
    if pd.Timestamp.now(tz="UTC") - latest_time > pd.Timedelta(minutes=LIVE_STALE_MINUTES):
        st.info(f"Waiting for a fresh Flink result. Last result: {latest_time.isoformat()}")
        return
    st.caption(f"Window started: {latest_time.isoformat()} - refreshes every 10 seconds")
    st.dataframe(
        live[["rank", "title", "genre", "live_views", "unique_viewers"]],
        hide_index=True,
        width="stretch",
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


def render_record_form() -> None:
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


# ------------------------------------------------------------------ Viewer


def viewer_page() -> None:
    st.header("Viewer: find something to watch")
    st.caption("Search the catalogue, see what is popular right now, and get titles picked for you.")
    browse_tab, trending_tab, for_you_tab = st.tabs(["Search & browse", "Trending", "Recommended for you"])

    with browse_tab:
        ask("BR1", "Which movies match this title or genre?")
        search_col, genre_col = st.columns([2, 1])
        query = search_col.text_input("Search title")
        genre = genre_col.selectbox("Genre", ["All"] + genres)

        # Cassandra has no substring search: read the genre partition(s) by key, filter titles here.
        catalog = catalog_for(genre)
        catalog = catalog[catalog["title"].str.contains(query, case=False, na=False, regex=False)]
        st.caption(f"{whole(len(catalog))} titles, most viewed first")
        st.dataframe(
            catalog,
            hide_index=True,
            width="stretch",
            height=380,
            column_config={
                "movie_id": "ID", "title": "Title", "genre": "Genre",
                "release_year": st.column_config.NumberColumn("Year", format="%d"),
                "duration": st.column_config.NumberColumn("Minutes", format="%d"),
                "views": st.column_config.NumberColumn("Views", format="%d"),
                "avg_rating": st.column_config.NumberColumn("Avg rating", format="%.2f"),
                "completion_rate": st.column_config.NumberColumn("Completion", format="%.1f%%"),
            },
        )

    with trending_tab:
        left, right = st.columns(2)
        with left:
            st.subheader("Most viewed of all time")
            ask("BR2", "Which titles have the most views overall? (batch)")
            st.dataframe(
                cql(
                    "SELECT title, genre, views, avg_rating FROM top_viewed_movies WHERE bucket = %s LIMIT 10",
                    (BATCH_BUCKET,),
                ),
                hide_index=True,
                width="stretch",
                column_config={
                    "title": "Title", "genre": "Genre",
                    "views": st.column_config.NumberColumn("Views", format="%d"),
                    "avg_rating": st.column_config.NumberColumn("Avg rating", format="%.2f"),
                },
            )
        with right:
            st.subheader("Right now")
            render_live_trending()

    with for_you_tab:
        users = cql("SELECT user_id FROM user_engagement")["user_id"].sort_values().tolist()
        ask("BR3", "What should this viewer watch next? Titles already watched are never suggested.")
        pick_col, method_col = st.columns([1, 2])
        with pick_col:
            selected_user = st.selectbox("Recommendations for", users)
        with method_col:
            method = st.radio("Recommender", list(RECOMMENDERS), horizontal=True)
            st.caption(RECOMMENDERS[method][1])
        preference = cql("SELECT favorite_genres FROM user_preferences WHERE user_id = %s", (selected_user,))
        favorite_genres = preference.iloc[0]["favorite_genres"].split("|") if not preference.empty else []
        st.markdown(
            "**Most-watched genres:** " + (", ".join(favorite_genres) if favorite_genres else "none yet (new viewer)")
        )

        columns = "rank, title, genre, source, weighted_rating, avg_rating, views"
        table = RECOMMENDERS[method][0]
        recommendations = read_or_none(f"SELECT {columns} FROM {table} WHERE user_id = %s", (selected_user,))
        if recommendations is not None and recommendations.empty and table != "user_recommendations":
            st.info("ALS needs at least one watch from this user, so the genre-based list is shown instead.")
            recommendations = read_or_none(f"SELECT {columns} FROM user_recommendations WHERE user_id = %s", (selected_user,))
        if recommendations is None:
            pass
        elif recommendations.empty:
            st.info("No recommendations for this user in the serving layer yet.")
        else:
            recommendations["source"] = recommendations["source"].map(
                {"genre": "Favourite genre", "popular": "Popular", "als": "Similar viewers"}
            )
            st.dataframe(
                recommendations,
                hide_index=True,
                width="stretch",
                column_config={
                    "rank": "#", "title": "Title", "genre": "Genre", "source": "Why",
                    "weighted_rating": st.column_config.NumberColumn("Weighted rating", format="%.2f"),
                    "avg_rating": st.column_config.NumberColumn("Avg rating", format="%.2f"),
                    "views": st.column_config.NumberColumn("Views", format="%d"),
                },
            )

    with st.expander("Record a movie view (demo input)"):
        render_record_form()


# ------------------------------------------------------------------ Analyst


def analyst_page() -> None:
    st.header("Analyst: ratings, genres, engagement, performance")
    st.caption("Filter the charts and compare titles, genres and user behaviour (BR4 to BR7).")

    with st.container(border=True):
        filter_cols = st.columns(3)
        chosen_genres = filter_cols[0].multiselect("Genres (BR4, BR5, BR7)", genres, default=genres) or genres
        engagement = engagement_scan()
        plans = sorted(engagement["subscription_plan"].dropna().unique())
        regions = sorted(engagement["region"].dropna().unique())
        chosen_plans = filter_cols[1].multiselect("Subscription plans (BR6)", plans, default=plans) or plans
        chosen_regions = filter_cols[2].multiselect("Regions (BR6)", regions, default=regions) or regions

    cards = st.columns(5)
    cards[0].metric("Users", whole(metrics["users"]), border=True)
    cards[1].metric("Movies", whole(metrics["movies"]), border=True)
    cards[2].metric("Views", compact(metrics["views"]), border=True)
    cards[3].metric("Ratings", whole(metrics["ratings"]), border=True)
    cards[4].metric("Average rating", f"{metrics['average_rating']:.2f} / 5", border=True)

    st.subheader("BR4 - Ratings")
    ask("BR4", "Which movies are rated highly by enough viewers to trust it? (Bayesian weighted rating)")
    show = st.slider("Titles shown", 5, 25, 10)
    if len(chosen_genres) == len(genres):
        rated = cql(
            "SELECT title, genre, weighted_rating, avg_rating, rating_count "
            "FROM top_rated_movies WHERE bucket = %s LIMIT %s",
            (BATCH_BUCKET, show),
        )
    else:
        rated = pd.concat([
            cql(
                "SELECT title, genre, weighted_rating, avg_rating, rating_count "
                "FROM movies_by_genre_rating WHERE genre = %s LIMIT %s",
                (name, show),
            )
            for name in chosen_genres
        ]).sort_values(["weighted_rating", "rating_count"], ascending=False).head(show)
    st.caption(
        "Each average is pulled toward the overall mean until a title has about the median number of ratings."
    )
    st.dataframe(
        rated,
        hide_index=True,
        width="stretch",
        column_config={
            "title": "Title", "genre": "Genre",
            "weighted_rating": st.column_config.NumberColumn("Weighted rating", format="%.2f"),
            "avg_rating": st.column_config.NumberColumn("Avg rating", format="%.2f"),
            "rating_count": st.column_config.NumberColumn("Ratings", format="%d"),
        },
    )

    st.subheader("BR5 - Genres")
    ask("BR5", "Which genres are watched most, and by how many different viewers?")
    genre_frame = cql("SELECT genre, views, unique_viewers FROM genre_popularity")
    genre_frame = genre_frame[genre_frame["genre"].isin(chosen_genres)].sort_values("views", ascending=False)
    left, right = st.columns(2)
    left.plotly_chart(
        styled(px.bar(genre_frame, x="genre", y="views", color="genre", color_discrete_map=GENRE_COLORS, title="Views by genre")),
        width="stretch", key="br5_views",
    )
    right.plotly_chart(
        styled(px.bar(
            genre_frame, x="genre", y="unique_viewers", color="genre", color_discrete_map=GENRE_COLORS,
            title="Unique viewers by genre",
        )),
        width="stretch", key="br5_viewers",
    )
    with st.expander("Genre table"):
        genre_frame = genre_frame.assign(views_per_viewer=(genre_frame["views"] / genre_frame["unique_viewers"]).round(1))
        st.dataframe(genre_frame, hide_index=True, width="stretch")

    st.subheader("BR6 - User engagement")
    ask("BR6", "How much do users watch, on how many days, and when? (filters: plan and region)")
    selected = engagement[
        engagement["subscription_plan"].isin(chosen_plans) & engagement["region"].isin(chosen_regions)
    ]
    if selected.empty:
        st.info("No users match these filters.")
    else:
        kpis = st.columns(4)
        kpis[0].metric("Users selected", whole(len(selected)), border=True)
        kpis[1].metric(
            "Views per user", f"{selected['views'].mean():.1f}", f"median {selected['views'].median():.0f}",
            delta_color="off", delta_arrow="off", border=True,
        )
        kpis[2].metric(
            "Watch hours per user", f"{selected['watch_minutes'].mean() / 60:.1f}",
            f"median {selected['watch_minutes'].median() / 60:.1f}", delta_color="off", delta_arrow="off", border=True,
        )
        kpis[3].metric(
            "Active days per user", f"{selected['active_days'].mean():.1f}",
            f"median {selected['active_days'].median():.0f}", delta_color="off", delta_arrow="off", border=True,
        )
        hist_cols = st.columns(3)
        for column, label, bins, holder in (
            ("views", "Views per user", 30, hist_cols[0]),
            ("watch_minutes", "Watch minutes per user", 30, hist_cols[1]),
            ("active_days", "Active days per user", 30, hist_cols[2]),
        ):
            holder.plotly_chart(
                styled(px.histogram(selected, x=column, nbins=bins, title=label, color_discrete_sequence=[ACCENT]).update_layout(
                    yaxis_title="users", xaxis_title=label
                )),
                width="stretch", key=f"br6_{column}",
            )
    plan_col, hour_col = st.columns(2)
    with plan_col:
        ask("BR6", "Which subscription plans generate the views? (all users)")
        plan_frame = cql("SELECT subscription_plan, views FROM subscription_views")
        plan_col.plotly_chart(
            styled(px.pie(
                plan_frame, names="subscription_plan", values="views", color="subscription_plan",
                color_discrete_map=PLAN_COLORS, title="Views by subscription plan",
            )),
            width="stretch", key="br6_plans",
        )
    with hour_col:
        ask("BR6", "At which hours of the day is the platform used? (all users, UTC)")
        hour_col.plotly_chart(
            styled(px.bar(
                cql("SELECT hour, views FROM hourly_views").sort_values("hour"), x="hour", y="views",
                title="Views by hour", color_discrete_sequence=[ACCENT],
            )),
            width="stretch", key="br6_hours",
        )

    st.subheader("BR7 - Movie performance")
    ask("BR7", "Which popular titles are also well rated and watched to the end? (top 30% by views)")
    top_n = math.ceil(int(metrics["movies"]) * TOP_SHARE)
    top_movies = cql(
        "SELECT title, genre, views, avg_rating, completion_rate, watch_hours "
        "FROM top_viewed_movies WHERE bucket = %s LIMIT %s",
        (BATCH_BUCKET, top_n),
    )
    top_movies = top_movies[top_movies["genre"].isin(chosen_genres)]
    st.plotly_chart(
        styled(px.scatter(
            top_movies, x="views", y="avg_rating", color="genre", color_discrete_map=GENRE_COLORS,
            hover_name="title", hover_data=["completion_rate", "watch_hours"], log_x=True, opacity=0.65,
            title=f"Top {whole(len(top_movies))} titles by views (log scale)",
        ), height=420),
        width="stretch", key="br7_scatter",
    )


# ------------------------------------------------------------------ Manager


def manager_page() -> None:
    st.header("Manager: what needs attention")
    st.caption("Headline numbers and alerts first, then the lists and charts behind them (BR8 to BR10).")

    segments = cql("SELECT segment, users, views FROM user_segments").set_index("segment")
    total_users = int(segments["users"].sum())
    inactive_share = int(segments.loc["Inactive", "users"]) / total_users if total_users and "Inactive" in segments.index else 0.0
    mean_views = int(metrics["views"]) / max(1, int(metrics["movies"]))
    low_threshold = max(1, math.ceil(mean_views * LOW_ENGAGEMENT_SHARE))
    low_count = int(cql(
        "SELECT COUNT(*) AS n FROM low_engagement_movies WHERE bucket = %s AND views < %s", (BATCH_BUCKET, low_threshold)
    ).iloc[0]["n"])
    daily = cql("SELECT day, views FROM daily_views WHERE bucket = %s", (BATCH_BUCKET,))
    daily["day"] = pd.to_datetime(daily["day"].astype(str))
    daily = daily.sort_values("day")
    last_week, previous_week = int(daily["views"].tail(7).sum()), int(daily["views"].iloc[-14:-7].sum())
    trend = (last_week - previous_week) / previous_week if previous_week else 0.0

    cards = st.columns(4)
    cards[0].metric("Users", whole(total_users), border=True)
    cards[1].metric("Inactive users", f"{inactive_share:.1%}", f"{whole(segments.loc['Inactive', 'users'])} users" if "Inactive" in segments.index else None,
                    delta_color="off", delta_arrow="off", border=True)
    cards[2].metric("Low-engagement titles", whole(low_count), f"under {low_threshold} views", delta_color="off", delta_arrow="off", border=True)
    cards[3].metric("Views, last 7 days", whole(last_week), f"{trend:+.1%} vs the 7 days before", border=True)

    with st.container(border=True):
        st.markdown("**Alerts**")
        if inactive_share > INACTIVE_ALERT_SHARE:
            st.warning(f"{inactive_share:.1%} of users are Inactive (10 views or fewer), above the {INACTIVE_ALERT_SHARE:.0%} limit: plan a win-back campaign (BR9).")
        else:
            st.success(f"Inactive users are {inactive_share:.1%}, within the {INACTIVE_ALERT_SHARE:.0%} limit (BR9).")
        share_low = low_count / max(1, int(metrics["movies"]))
        if low_count:
            st.warning(f"{whole(low_count)} titles ({share_low:.1%} of the catalogue) have fewer than {low_threshold} views, "
                       f"{LOW_ENGAGEMENT_SHARE:.0%} of the average of {mean_views:.0f}: review them before renewing licences (BR8).")
        else:
            st.success("No title is below the low-engagement threshold (BR8).")
        if trend < -TREND_DROP_ALERT:
            st.warning(f"Views fell {abs(trend):.1%} against the previous 7 days (BR10).")
        else:
            st.success(f"Views are {trend:+.1%} against the previous 7 days, within the {TREND_DROP_ALERT:.0%} drop limit (BR10).")

    left, right = st.columns(2)
    with left:
        st.subheader("BR8 - Low-engagement titles")
        ask("BR8", "Which titles are watched the least and are candidates to drop?")
        st.dataframe(
            cql(
                "SELECT movie_id, title, genre, views, completion_rate "
                "FROM low_engagement_movies WHERE bucket = %s LIMIT 10",
                (BATCH_BUCKET,),
            ),
            hide_index=True,
            width="stretch",
            column_config={
                "movie_id": "ID", "title": "Title", "genre": "Genre",
                "views": st.column_config.NumberColumn("Views", format="%d"),
                "completion_rate": st.column_config.NumberColumn("Completion", format="%.1f%%"),
            },
        )
    with right:
        st.subheader("BR9 - User segments")
        ask("BR9", "How many users are Inactive, Casual and Active?")
        st.plotly_chart(
            styled(px.pie(
                segments.reset_index(), names="segment", values="users", color="segment",
                color_discrete_map=SEGMENT_COLORS, title="Users by segment",
            )),
            width="stretch", key="br9_segments",
        )

    st.subheader("BR10 - Trend")
    ask("BR10", "How do daily views change over time?")
    st.plotly_chart(
        styled(px.line(daily, x="day", y="views", title="Views per day", color_discrete_sequence=[ACCENT]), height=300),
        width="stretch", key="br10_daily",
    )


# ------------------------------------------------------------------ Data engineering


def read_or_none(statement: str, params: tuple = ()) -> pd.DataFrame | None:
    """Pipeline metadata may be missing on a stack that has not run the newest batch yet."""
    try:
        return cql(statement, params)
    except Exception as error:
        st.info(f"Not available yet ({type(error).__name__}). Run the Spark batch to create it.")
        return None


@st.fragment(run_every="10s")
def render_stream_freshness() -> None:
    ask("BR2", "Is the streaming path (Kafka, Flink, materializer) delivering fresh windows?")
    live = latest_live_window()
    if live.empty:
        st.warning("No live_trending window yet: the first five-minute window has not closed.")
        return
    started = pd.Timestamp(live["window_start"].max()).tz_localize("UTC")
    lag = pd.Timestamp.now(tz="UTC") - started
    minutes = lag.total_seconds() / 60
    cols = st.columns(3)
    cols[0].metric("Newest window started (UTC)", started.strftime("%d %b %H:%M:%S"), border=True)
    cols[1].metric("Lag since window start", f"{minutes:.1f} min", border=True)
    cols[2].metric("Titles in the window", len(live), border=True)
    if minutes > LIVE_STALE_MINUTES:
        st.error(f"Stale: the newest window is more than {LIVE_STALE_MINUTES} minutes old. Check Flink (port 8081) and the materializer.")
    else:
        st.success(f"Fresh: the newest window is less than {LIVE_STALE_MINUTES} minutes old.")


def pipeline_page() -> None:
    st.header("Data Engineering: pipeline health")
    st.caption("Batch status, data quality, stream freshness and serving-layer row counts.")

    st.subheader("Batch")
    ask("Pipeline", "When did the Spark batch last finish, how long did it take and where does the time go?")
    runs = read_or_none(
        "SELECT finished_at, started_at, status, seconds, rows_written, stage_seconds FROM batch_runs "
        "WHERE bucket = %s LIMIT 5", (BATCH_BUCKET,),
    )
    runner = batch_runner()
    if runs is not None and not runs.empty:
        latest = runs.iloc[0]
        finished = pd.Timestamp(latest["finished_at"]).tz_localize("UTC")
        age = pd.Timestamp.now(tz="UTC") - finished
        cols = st.columns(4)
        cols[0].metric("Last successful run (UTC)", finished.strftime("%d %b %H:%M"), finished.strftime("%Y"), delta_color="off", delta_arrow="off", border=True)
        cols[1].metric("Data age", f"{age.total_seconds() / 3600:.1f} h", border=True)
        cols[2].metric("Duration", f"{latest['seconds']:.0f} s", border=True)
        cols[3].metric("Rows written", whole(latest["rows_written"]), border=True)
        stage_seconds = dict(latest["stage_seconds"] or {})
        order = [name for name in STAGE_ORDER if name in stage_seconds] + [name for name in stage_seconds if name not in STAGE_ORDER]
        stages = pd.DataFrame({"stage": order, "seconds": [stage_seconds[name] for name in order]})
        st.plotly_chart(
            styled(px.bar(stages, x="seconds", y="stage", orientation="h", title="Seconds per stage (latest run)",
                          color_discrete_sequence=[ACCENT]).update_yaxes(categoryorder="array", categoryarray=order[::-1]), height=300),
            width="stretch", key="pipeline_stages",
        )
        with st.expander("Recent runs"):
            st.dataframe(runs.drop(columns="stage_seconds"), hide_index=True, width="stretch")
    elif runs is not None:
        st.warning("No batch run has been recorded yet.")
    process = runner["process"]
    if process is not None and process.poll() is None:
        st.info("A batch started from this dashboard is running now.")
    elif process is not None and process.poll() not in (None, 0):
        st.error(f"The last batch started from this dashboard failed (exit code {process.poll()}):")
        st.code(log_tail(), language="text")
    if st.button("Run the Spark batch now", key="pipeline_run_batch"):
        start_batch()
        st.rerun()

    st.subheader("Data quality")
    ask("Pipeline", "How many raw rows did each cleaning rule reject, per table?")
    quality = read_or_none("SELECT table_name, reason, rows FROM quality_report")
    if quality is not None and not quality.empty:
        pivot = quality.pivot_table(index="table_name", columns="reason", values="rows", aggfunc="sum", fill_value=0)
        pivot = pivot.reindex(columns=["accepted", *REASONS], fill_value=0)  # a rule that rejected nothing is not stored
        rules = list(REASONS)
        pivot["rejected"] = pivot[rules].sum(axis=1)
        pivot["input"] = pivot["rejected"] + pivot["accepted"]
        pivot["rejected_share"] = (pivot["rejected"] / pivot["input"].where(pivot["input"] > 0)).fillna(0)
        cards = st.columns(3)
        cards[0].metric("Raw rows read", whole(pivot["input"].sum()), border=True)
        cards[1].metric("Rows rejected", whole(pivot["rejected"].sum()), border=True)
        cards[2].metric("Rejected share", f"{pivot['rejected'].sum() / max(1, pivot['input'].sum()):.2%}", border=True)
        shown = pivot.rename(columns={"accepted": "kept"})[["input", "kept"] + rules + ["rejected", "rejected_share"]]
        st.dataframe(
            shown.rename_axis("table").reset_index(), hide_index=True, width="stretch",
            column_config={"rejected_share": st.column_config.NumberColumn("Rejected share", format="percent")},
        )
    elif quality is not None:
        st.warning("The quality report is empty.")

    st.subheader("Stream")
    render_stream_freshness()

    st.subheader("Serving layer")
    ask("Pipeline", "Does every Cassandra table hold the rows the batch wrote?")
    written = read_or_none("SELECT table_name, rows FROM batch_table_rows")
    if written is not None and not written.empty:
        written = written.sort_values("table_name").reset_index(drop=True)
        verified = st.session_state.get("verified_counts")
        if st.button("Verify now (COUNT per table)", key="verify_counts"):
            with st.spinner("Counting rows in Cassandra..."):
                verified = table_counts(get_session(), written["table_name"].tolist())
            st.session_state["verified_counts"] = verified
        written = written.rename(columns={"rows": "rows written by last batch"})
        if verified:
            written["rows in Cassandra now"] = written["table_name"].map(verified)
            written["match"] = written["rows in Cassandra now"] == written["rows written by last batch"]
        st.dataframe(written, hide_index=True, width="stretch")


# ------------------------------------------------------------------ Layout

PAGES = {
    "Viewer": ("Discover movies: search, trending, recommendations", viewer_page),
    "Analyst": ("Filter and compare ratings, genres, engagement", analyst_page),
    "Manager": ("Alerts and KPIs for content and retention", manager_page),
    "Data Engineering": ("Pipeline status, data quality, freshness", pipeline_page),
}

st.title("Movie Streaming Analytics Platform")
page = st.sidebar.radio("Workspace", list(PAGES), captions=[description for description, _ in PAGES.values()])
if st.sidebar.button("Refresh batch (Spark)"):
    start_batch()
with st.sidebar:
    render_batch_status()

try:
    PAGES[page][1]()
except Exception as error:  # a failed query must not take the whole dashboard down
    st.error(f"This page could not be rendered: {type(error).__name__}: {error}")
    st.caption("The serving tables may be mid-refresh; wait for the batch to finish and reload.")
