from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(page_title="Movie Analytics Platform", page_icon="🎬", layout="wide")
RAW_DIR = Path("data")
PROCESSED_DIR = RAW_DIR / "processed"


@st.cache_data
def load_data() -> dict[str, pd.DataFrame]:
    required = [
        "movie_performance",
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
            "Processed data is missing. Run: python -m src.batch_processing"
        )

    data = {name: pd.read_csv(PROCESSED_DIR / f"{name}.csv") for name in required}
    data["movies"] = pd.read_csv(RAW_DIR / "movies.csv")
    data["users"] = pd.read_csv(RAW_DIR / "users.csv", usecols=["user_id"])
    data["daily_views"]["day"] = pd.to_datetime(data["daily_views"]["day"])

    live_path = PROCESSED_DIR / "live_trending.csv"
    data["live_trending"] = pd.read_csv(live_path) if live_path.exists() else pd.DataFrame()
    return data


try:
    data = load_data()
except FileNotFoundError as error:
    st.error(str(error))
    st.code("python -m src.batch_processing\npython -m src.stream_processing")
    st.stop()


movies = data["movies"]
performance = data["movie_performance"]
metrics = data["summary_metrics"].iloc[0]

st.title("Movie Streaming Analytics Platform")
page = st.sidebar.radio("Workspace", ["Viewer", "Analyst", "Manager"])

if page == "Viewer":
    st.subheader("Discover movies")
    search_col, genre_col = st.columns([2, 1])
    query = search_col.text_input("Search title")
    genre = genre_col.selectbox("Genre", ["All"] + sorted(movies["genre"].unique()))

    found = movies[movies["title"].str.contains(query, case=False, na=False)]
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
        st.subheader("Trending movies")
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
        st.subheader("Personalized recommendations")
        recommendations = performance[performance["genre"].isin(favorite_genres)].nlargest(
            8, "avg_rating"
        )
        st.dataframe(
            recommendations[["title", "genre", "avg_rating", "views"]],
            hide_index=True,
            use_container_width=True,
        )

    if not data["live_trending"].empty:
        latest_window = data["live_trending"]["window_start"].max()
        latest = data["live_trending"].query("window_start == @latest_window")
        st.subheader("Live trending - latest 5-minute window")
        st.dataframe(
            latest[["rank", "title", "genre", "live_views", "unique_viewers"]],
            hide_index=True,
            use_container_width=True,
        )

elif page == "Analyst":
    st.subheader("Analyst dashboard")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Users", f"{int(metrics['users']):,}")
    col2.metric("Movies", f"{int(metrics['movies']):,}")
    col3.metric("Views", f"{int(metrics['views']):,}")
    col4.metric("Average rating", f"{metrics['average_rating']:.2f} / 5")

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
    st.write("Low-engagement movies")
    st.dataframe(
        performance.nsmallest(10, "views")[["title", "genre", "views", "completion_rate"]],
        hide_index=True,
        use_container_width=True,
    )

    left, right = st.columns(2)
    left.plotly_chart(
        px.pie(data["user_segments"], names="segment", values="users", title="User segments"),
        use_container_width=True,
    )
    right.plotly_chart(
        px.line(data["daily_views"], x="day", y="views", title="Daily viewing trend"),
        use_container_width=True,
    )
