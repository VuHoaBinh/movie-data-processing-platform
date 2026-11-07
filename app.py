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

from src.alerts import activity_alerts, genre_alert
from src.business_rules import (
    ACTIVITY_WINDOW_DAYS, ALL_GENRES, MOVING_AVERAGE_DAYS, PEER_COMPLETION_PERCENTILE, PEER_MIN_VIEWS_FOR_COMPLETION,
    PEER_VIEWS_PERCENTILE, TREND_DAMPING, TREND_MIN_RECENT_VIEWS, TREND_WINDOW_DAYS, WEEKDAY, WEEKEND,
)
from src.cassandra_store import BATCH_BUCKET, CassandraUnavailable, connect, query_frame, table_counts
from src.data_quality import REASONS
from src.trends import dense_series

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None


st.set_page_config(page_title="Movie Analytics Platform", page_icon="🎬", layout="wide")
RAW_DIR = Path("data")
BATCH_LOG = RAW_DIR / "batch_refresh.log"
TOP_SHARE = 0.30  # BR7 shows the top 30% of movies by views.
LIVE_STALE_MINUTES = 10  # BR2 live window older than this is "waiting for Flink".
# Manager alert limits live in src/business_rules.py (ACTIVE_USERS_DROP_ALERT, ACTIVE_TITLES_DROP_ALERT, GENRE_DROP_ALERT).
WEEK_DAYS = MOVING_AVERAGE_DAYS  # the weekly comparison uses the same 7-day window as the moving average.
BR8_LIST_ROWS = 20  # titles listed in the BR8 table.
RISING_ROWS = 10  # titles listed in the BR2 "rising now" table.
PICKER_MATCHES = 50  # a search box narrows a picker to at most this many options before the selectbox appears.
INDEX_TTL = 3600  # seconds the id / title indexes behind the pickers stay cached on the server.
LOG_TAIL_LINES = 25
STAGE_ORDER = ["hive_raw", "hive_curated", "build_tables", "als", "hive_analytics", "cassandra_load"]

# One colour per genre, shared by every chart; segments and plans have fixed colours too.
ACCENT = "#4C78A8"
SEGMENT_COLORS = {"Inactive": "#E4572E", "Casual": "#F3A712", "Active": "#29A375"}
REASON_LABELS = {
    "low_both": "Low views and completion", "both": "Low views and completion",
    "low_views": "Few views", "low_completion": "Abandoned (low completion)",
}
REASON_COLORS = {"Low views and completion": "#E4572E", "Few views": "#F3A712", "Abandoned (low completion)": "#5C80BC"}
PLAN_COLORS = {"Basic": "#9DB4D8", "Standard": "#5C80BC", "Premium": "#2D4A82"}
# The first entry is the default of the radio: ALS measured better than the genre-based lists offline
# (docs/evaluation.md 3.4); genre-based stays as the explainable alternative and as the fallback for users
# without watch history, who have no ALS list.
RECOMMENDERS = {
    "Collaborative filtering (ALS)": (
        "user_recommendations_als",
        "Spark MLlib ALS learns taste factors from who watched what and suggests unwatched titles that similar "
        "viewers watched; it needs at least one watch.",
    ),
    "Genre-based": (
        "user_recommendations",
        "Takes the user's two most-watched genres and lists the best Bayesian-rated titles in them that the user "
        "has not watched; users with no history get the most-viewed titles.",
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


@st.cache_data(ttl=INDEX_TTL, show_spinner=False)
def user_index() -> list[str]:
    """Every user id, kept on the server only: the widgets below never receive more than PICKER_MATCHES of them."""
    return sorted(query_frame(get_session(), "SELECT user_id FROM user_engagement")["user_id"])


@st.cache_data(ttl=INDEX_TTL, show_spinner=False)
def movie_index() -> pd.DataFrame:
    return (
        query_frame(get_session(), "SELECT movie_id, title, genre, duration FROM movie_performance")
        .sort_values("movie_id").reset_index(drop=True)
    )


def pick_user(key: str, label: str = "User") -> str | None:
    """A search box plus a selectbox with at most PICKER_MATCHES matching ids (never the 50,000 of them)."""
    term = st.text_input("Find a user (part of the ID)", key=f"{key}_term", placeholder="U00042").strip().upper()
    matches = [user for user in user_index() if term in user]
    shown = matches[:PICKER_MATCHES]
    if not shown:
        st.info("No user matches this search.")
        return None
    st.caption(f"{len(shown)} of {whole(len(matches))} matching users shown" + ("; type more to narrow" if len(matches) > len(shown) else ""))
    return st.selectbox(label, shown, key=f"{key}_select")


def pick_movie(key: str, label: str = "Title") -> str | None:
    """A search box (title or ID) plus a selectbox with at most PICKER_MATCHES matching titles."""
    term = st.text_input("Find a title (part of the name or ID)", key=f"{key}_term", placeholder="spider").strip()
    movies = movie_index()
    if term:
        hit = movies["title"].str.contains(term, case=False, na=False, regex=False) | movies["movie_id"].str.contains(term, case=False, regex=False)
        movies = movies[hit]
    shown = movies.head(PICKER_MATCHES)
    if shown.empty:
        st.info("No title matches this search.")
        return None
    st.caption(f"{len(shown)} of {whole(len(movies))} matching titles shown" + ("; type more to narrow" if len(movies) > len(shown) else ""))
    names = dict(zip(shown["movie_id"], shown["title"]))
    return st.selectbox(label, list(names), format_func=lambda movie_id: f"{names[movie_id]} ({movie_id})", key=f"{key}_select")


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
    form_user = pick_user("record_user")
    form_movie = pick_movie("record_movie")
    if form_user is None or form_movie is None:
        return
    movie_row = movie_index().set_index("movie_id").loc[form_movie]
    with st.form("record_watch"):
        st.markdown(f"**{form_user}** watches **{movie_row['title']}** ({movie_row['genre']})")
        movie_duration = int(movie_row["duration"])
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


def render_rising() -> None:
    st.subheader("Rising now")
    rising = cql(
        "SELECT title, genre, views_recent, views_previous, growth, reference_date "
        "FROM trending_growth WHERE bucket = %s LIMIT %s",
        (BATCH_BUCKET, RISING_ROWS),
    )
    ask("BR2", "Which titles gained the most views in the latest week compared with the week before? (batch)")
    if rising.empty:
        st.info("No title has enough recent views to be ranked yet.")
        return
    st.caption(
        f"Last {TREND_WINDOW_DAYS} days up to {rising['reference_date'].iloc[0]} against the {TREND_WINDOW_DAYS} days before. "
        f"Growth is the gain divided by the previous views plus {TREND_DAMPING}, so a few extra views on a tiny title do not "
        f"count; titles need at least {TREND_MIN_RECENT_VIEWS} views in the week."
    )
    st.dataframe(
        rising.drop(columns="reference_date"),
        hide_index=True,
        width="stretch",
        column_config={
            "title": "Title", "genre": "Genre",
            "views_recent": st.column_config.NumberColumn(f"Last {TREND_WINDOW_DAYS} days", format="%d"),
            "views_previous": st.column_config.NumberColumn(f"Previous {TREND_WINDOW_DAYS} days", format="%d"),
            "growth": st.column_config.NumberColumn("Growth", format="%.2f"),
        },
    )


def render_recommendations() -> None:
    ask("BR3", "What should this viewer watch next? Titles already watched are never suggested.")
    pick_col, method_col = st.columns([1, 2])
    with pick_col:
        selected_user = pick_user("viewer_user", "Recommendations for")
    with method_col:
        method = st.radio("Recommender", list(RECOMMENDERS), horizontal=True)
        st.caption(RECOMMENDERS[method][1])
    if selected_user is None:
        return
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
        render_rising()
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
        render_recommendations()

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

    # BR9: segments come from quantiles of views per user; the cut-offs are stored with them.
    segments = cql("SELECT segment, users, views FROM user_segments").set_index("segment")
    cuts = cql(
        "SELECT percentile, views_threshold, users FROM segment_thresholds WHERE bucket = %s", (BATCH_BUCKET,)
    ).sort_values("percentile")
    low_cut, high_cut = (int(value) for value in cuts["views_threshold"])
    total_users = int(segments["users"].sum())
    inactive_users = int(segments.loc["Inactive", "users"]) if "Inactive" in segments.index else 0
    segment_labels = {
        "Inactive": f"Inactive (up to {low_cut} views)",
        "Casual": f"Casual ({low_cut + 1} to {high_cut} views)",
        "Active": f"Active (over {high_cut} views)",
    }

    # BR8: titles that are low against their genre peers.
    peers = cql(
        "SELECT genre, movies, views_p10, completion_p10, low_views, low_completion, low_both, flagged "
        "FROM genre_engagement_peers"
    ).sort_values("genre")
    flagged_total, both_total, catalogue = int(peers["flagged"].sum()), int(peers["low_both"].sum()), int(peers["movies"].sum())
    flagged_share = flagged_total / catalogue if catalogue else 0.0

    # BR10: the genre x day grid gives the weekly comparison per genre and for the platform.
    grid = cql("SELECT day, genre, views, views_ma7 FROM genre_daily_views WHERE bucket = %s", (BATCH_BUCKET,))
    grid["day"] = pd.to_datetime(grid["day"].astype(str))
    week_alert, week_changes = genre_alert(grid)
    last_days = sorted(grid["day"].unique())[-2 * WEEK_DAYS:]
    last_week = int(grid[grid["day"].isin(last_days[WEEK_DAYS:])]["views"].sum())
    previous_week = int(grid[grid["day"].isin(last_days[:WEEK_DAYS])]["views"].sum())
    trend = last_week / previous_week - 1 if previous_week else 0.0

    # Alerts compare two windows of an absolute count; the shares of the segment and peer lists are constant by
    # construction, so they are shown as explanations and never alerted on.
    windows = cql("SELECT period, start_date, end_date, views, active_users, active_titles FROM activity_windows WHERE bucket = %s", (BATCH_BUCKET,))
    users_alert, titles_alert = activity_alerts(windows)
    recent = windows[windows["period"] == "recent"]
    active_users = int(recent["active_users"].iloc[0]) if len(recent) else 0
    active_titles = int(recent["active_titles"].iloc[0]) if len(recent) else 0

    def change_text(change) -> str:
        return "no earlier window" if change is None else f"{change:+.1%} vs the {ACTIVITY_WINDOW_DAYS} days before"

    cards = st.columns(4)
    cards[0].metric("Users", whole(total_users), border=True)
    cards[1].metric(f"Active users, last {ACTIVITY_WINDOW_DAYS} days", whole(active_users), change_text(users_alert.change), border=True)
    cards[2].metric(f"Active titles, last {ACTIVITY_WINDOW_DAYS} days", whole(active_titles), change_text(titles_alert.change), border=True)
    cards[3].metric(f"Views, last {WEEK_DAYS} days", whole(last_week), f"{trend:+.1%} vs the {WEEK_DAYS} days before", border=True)

    with st.container(border=True):
        st.markdown("**Alerts**")
        if users_alert.firing:
            st.warning(
                f"Users with a view fell {abs(users_alert.change):.1%} against the previous {ACTIVITY_WINDOW_DAYS} days, past the "
                f"{users_alert.limit:.0%} limit: plan a win-back campaign; {whole(inactive_users)} users are already Inactive (BR9)."
            )
        else:
            st.success(f"Users with a view: {change_text(users_alert.change)}, within the {users_alert.limit:.0%} drop limit (BR9).")
        if titles_alert.firing:
            st.warning(
                f"Titles with a view fell {abs(titles_alert.change):.1%} against the previous {ACTIVITY_WINDOW_DAYS} days, past the "
                f"{titles_alert.limit:.0%} limit: review the catalogue, starting with the {whole(both_total)} titles low on both views and completion (BR8)."
            )
        else:
            st.success(
                f"Titles with a view: {change_text(titles_alert.change)}, within the {titles_alert.limit:.0%} drop limit (BR8); "
                f"{whole(flagged_total)} titles ({flagged_share:.1%}) are low against their genre, {whole(both_total)} on both views and completion."
            )
        if week_alert.firing:
            names = ", ".join(f"{genre} {change:+.1%}" for genre, change in week_changes[week_changes < -week_alert.limit].sort_values().items())
            st.warning(f"Views fell more than {week_alert.limit:.0%} against the previous {WEEK_DAYS} days in: {names} (BR10).")
        else:
            st.success(f"No genre lost more than {week_alert.limit:.0%} of its views against the previous {WEEK_DAYS} days (BR10).")

    st.subheader("BR8 - Titles that are low against their genre")
    ask("BR8", "Which titles get few views or are abandoned compared with other titles of the same genre?")
    st.caption(
        f"A title is flagged when its views are at or below the {PEER_VIEWS_PERCENTILE}th percentile of its genre, or when "
        f"it has at least {PEER_MIN_VIEWS_FOR_COMPLETION} views and its completion is at or below the genre's "
        f"{PEER_COMPLETION_PERCENTILE}th percentile. Titles low on both come first."
    )
    reasons = peers.melt(
        id_vars="genre", value_vars=["low_both", "low_views", "low_completion"], var_name="reason", value_name="titles"
    ).replace({"reason": REASON_LABELS})
    left, right = st.columns([3, 2])
    left.plotly_chart(
        styled(px.bar(
            reasons, x="genre", y="titles", color="reason", color_discrete_map=REASON_COLORS,
            category_orders={"reason": list(REASON_LABELS.values())}, title="Flagged titles by genre and reason",
        )),
        width="stretch", key="br8_reasons",
    )
    with right:
        genre_filter = st.selectbox("Show titles of", ["All genres"] + genres, key="br8_genre")
        if genre_filter == "All genres":
            flagged = cql(
                "SELECT movie_id, title, genre, views, completion_rate, genre_views_p10, genre_completion_p10, reason "
                "FROM low_engagement_movies WHERE bucket = %s LIMIT %s",
                (BATCH_BUCKET, BR8_LIST_ROWS),
            )
        else:
            flagged = cql(
                "SELECT movie_id, title, genre, views, completion_rate, genre_views_p10, genre_completion_p10, reason "
                "FROM low_engagement_by_genre WHERE genre = %s LIMIT %s",
                (genre_filter, BR8_LIST_ROWS),
            )
        flagged["reason"] = flagged["reason"].map(REASON_LABELS)
        st.caption(f"First {len(flagged)} titles, worst first (low on both, then views, then completion)")
    st.dataframe(
        flagged,
        hide_index=True,
        width="stretch",
        column_config={
            "movie_id": "ID", "title": "Title", "genre": "Genre", "reason": "Why flagged",
            "views": st.column_config.NumberColumn("Views", format="%d"),
            "completion_rate": st.column_config.NumberColumn("Completion", format="%.1f%%"),
            "genre_views_p10": st.column_config.NumberColumn("Genre p10 views", format="%d"),
            "genre_completion_p10": st.column_config.NumberColumn("Genre p10 completion", format="%.1f%%"),
        },
    )
    with st.expander("Genre thresholds and counts"):
        st.dataframe(peers, hide_index=True, width="stretch")

    st.subheader("BR9 - User segments")
    ask("BR9", "How many users are Inactive, Casual and Active, and where are the cut-offs?")
    st.caption(
        f"Cut-offs are the {cuts['percentile'].iloc[0]}th and {cuts['percentile'].iloc[1]}th percentile of views per user, "
        f"recomputed by every batch: {low_cut} and {high_cut} views."
    )
    pie_col, matrix_col = st.columns(2)
    shown = segments.reset_index().assign(label=lambda frame: frame["segment"].map(segment_labels))
    pie_col.plotly_chart(
        styled(px.pie(
            shown, names="label", values="users", color="label",
            color_discrete_map={segment_labels[name]: color for name, color in SEGMENT_COLORS.items()},
            title="Users by segment",
        )),
        width="stretch", key="br9_segments",
    )
    agreement = cql(
        "SELECT activity_profile, segment, users FROM segment_agreement WHERE bucket = %s", (BATCH_BUCKET,)
    )
    matrix = agreement.pivot(index="activity_profile", columns="segment", values="users").reindex(
        index=list(SEGMENT_COLORS), columns=list(SEGMENT_COLORS)
    )
    agreement_share = int(agreement.loc[agreement["activity_profile"] == agreement["segment"], "users"].sum()) / max(1, int(agreement["users"].sum()))
    matrix_col.plotly_chart(
        styled(px.imshow(
            matrix, text_auto=",", aspect="auto", color_continuous_scale="Blues",
            labels=dict(x="Segment from views", y="Profile in the data", color="Users"),
            title=f"Segments vs the generator's activity profile: {agreement_share:.1%} agree",
        )),
        width="stretch", key="br9_agreement",
    )

    st.subheader("BR10 - Trends")
    ask("BR10", "How do views change over time, by genre, by title, and between weekdays and weekends?")
    platform_tab, genre_tab, movie_tab, week_tab = st.tabs(["Platform", "By genre", "By title", "Weekdays vs weekends"])
    with platform_tab:
        daily = grid.groupby("day", as_index=False)["views"].sum().sort_values("day")
        daily["Moving average"] = daily["views"].rolling(WEEK_DAYS).mean()
        st.plotly_chart(
            styled(px.line(
                daily, x="day", y=["views", "Moving average"], title="Views per day",
                color_discrete_sequence=[ACCENT, "#E4572E"],
            ), height=300),
            width="stretch", key="br10_daily",
        )
    with genre_tab:
        chosen = st.multiselect("Genres", genres, default=genres[:4], key="br10_genres") or genres
        smooth = st.toggle(f"{WEEK_DAYS}-day moving average", value=True, key="br10_smooth")
        column = "views_ma7" if smooth else "views"
        st.plotly_chart(
            styled(px.line(
                grid[grid["genre"].isin(chosen)].sort_values("day"), x="day", y=column, color="genre",
                color_discrete_map=GENRE_COLORS, title="Views per day by genre" + (f" ({WEEK_DAYS}-day average)" if smooth else ""),
            ), height=340),
            width="stretch", key="br10_genre_lines",
        )
        st.caption(
            f"Last {WEEK_DAYS} days against the {WEEK_DAYS} days before, per genre: "
            + ", ".join(f"{genre} {change:+.1%}" for genre, change in week_changes.items())
        )
    with movie_tab:
        movie_id = pick_movie("br10_movie")
        if movie_id:
            series = cql("SELECT day, views FROM movie_daily_views WHERE movie_id = %s", (movie_id,))
            first_day, last_day = grid["day"].min(), grid["day"].max()
            dense = dense_series(series, first_day, last_day) if not series.empty else None
            if dense is None:
                st.info("This title has no views.")
            else:
                st.plotly_chart(
                    styled(px.line(
                        dense, x="day", y=["views", "views_ma7"], title=f"Views per day for {movie_id}",
                        color_discrete_sequence=[ACCENT, "#E4572E"],
                    ), height=320),
                    width="stretch", key="br10_movie_line",
                )
                st.caption(f"{whole(dense['views'].sum())} views on {len(series)} of {len(dense)} days.")
    with week_tab:
        split = cql(
            "SELECT genre, day_type, days, views, avg_daily_views FROM weekday_weekend_views WHERE bucket = %s",
            (BATCH_BUCKET,),
        )
        overall = split[split["genre"] == ALL_GENRES].set_index("day_type")["avg_daily_views"]
        if {WEEKDAY, WEEKEND} <= set(overall.index):
            difference = overall[WEEKEND] / overall[WEEKDAY] - 1
            st.metric(
                "Views per weekend day against a weekday", f"{difference:+.1%}",
                f"{overall[WEEKEND]:,.0f} vs {overall[WEEKDAY]:,.0f} views per day", delta_color="off", delta_arrow="off", border=True,
            )
        st.plotly_chart(
            styled(px.bar(
                split[split["genre"] != ALL_GENRES], x="genre", y="avg_daily_views", color="day_type", barmode="group",
                color_discrete_sequence=[ACCENT, "#F3A712"], title="Average views per day by genre",
            )),
            width="stretch", key="br10_week",
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
