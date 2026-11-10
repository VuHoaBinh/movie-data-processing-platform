"""Batch layer, alternative solution: Flink SQL in BATCH runtime mode.

Same sources, rules and output tables as the Spark batch job (src/spark_batch_job.py):

* input  = raw CSVs (`users`, `movies`, `ratings`, `watch_history` without the replay day) plus the
           raw-zone archive of live events of closed days (headerless CSV written by src/stream_archiver.py);
           the replay day, batch cutoff and DQ reference time are read from `warehouse/batch_boundary.json`
           written by the last Spark run, so both engines process exactly the same rows;
* rules  = the data-quality rules of src/data_quality.py, first failing rule wins;
* output = the 14 analytics tables + the quality report, as CSV under `warehouse/flink/analytics/<table>/`
           (column names in `_schema.json`), compared with Spark's Hive tables by
           `python -m src.compare_batch_engines`.

Known difference: Spark keeps the *first* row of a duplicated id in file order. A Flink file source has
no row order, so this job keeps the first row ordered by (source: CSV before archive, then the row
values). Both choices agree whenever duplicate rows are identical copies (the generator's case).

    flink run -m flink-jobmanager:8081 -py /opt/flink/usrlib/flink_batch_sql_job.py [--data-dir /opt/flink/data]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment

MIN_VIEWS = int(os.getenv("BR8_MIN_VIEWS", "20"))
LOW_COMPLETION_THRESHOLD = float(os.getenv("BR8_COMPLETION_THRESHOLD", "0.3"))
TOP_N = int(os.getenv("BR3_TOP_N", "8"))

# Output tables: column name -> Flink type (same names and order as the Spark analytics tables).
TABLES: dict[str, list[tuple[str, str]]] = {
    "movie_performance": [
        ("movie_id", "STRING"), ("title", "STRING"), ("genre", "STRING"), ("release_year", "INT"),
        ("duration", "INT"), ("content_type", "STRING"), ("language", "STRING"), ("maturity_rating", "STRING"),
        ("views", "BIGINT"), ("unique_viewers", "BIGINT"), ("avg_rating", "DOUBLE"), ("rating_count", "BIGINT"),
        ("weighted_rating", "DOUBLE"), ("watch_hours", "DOUBLE"), ("completion_rate", "DOUBLE"),
    ],
    "low_engagement_movies": [
        ("movie_id", "STRING"), ("title", "STRING"), ("genre", "STRING"), ("views", "BIGINT"),
        ("watch_minutes", "BIGINT"), ("completion_sum", "DOUBLE"), ("completion_rate", "DOUBLE"),
        ("is_low_engagement", "BOOLEAN"),
    ],
    "genre_popularity": [
        ("genre", "STRING"), ("views", "BIGINT"), ("unique_viewers", "BIGINT"), ("watch_hours", "DOUBLE"),
        ("avg_rating", "DOUBLE"),
    ],
    "subscription_views": [("subscription_plan", "STRING"), ("views", "BIGINT"), ("unique_viewers", "BIGINT")],
    "user_engagement": [
        ("user_id", "STRING"), ("subscription_plan", "STRING"), ("region", "STRING"), ("views", "BIGINT"),
        ("watch_minutes", "BIGINT"), ("active_days", "BIGINT"), ("completion_sum", "DOUBLE"),
        ("avg_completion", "DOUBLE"), ("segment", "STRING"),
    ],
    "user_segments": [("segment", "STRING"), ("users", "BIGINT"), ("views", "BIGINT")],
    "user_preferences": [("user_id", "STRING"), ("favorite_genres", "STRING")],
    "daily_views": [("day", "DATE"), ("views", "BIGINT"), ("unique_viewers", "BIGINT")],
    "hourly_views": [("hour", "INT"), ("views", "BIGINT")],
    "summary_metrics": [
        ("users", "BIGINT"), ("movies", "BIGINT"), ("views", "BIGINT"), ("ratings", "BIGINT"),
        ("average_rating", "DOUBLE"),
    ],
    "user_recommendations": [
        ("user_id", "STRING"), ("rank", "INT"), ("movie_id", "STRING"), ("title", "STRING"), ("genre", "STRING"),
        ("weighted_rating", "DOUBLE"), ("rating_count", "BIGINT"),
    ],
    "rating_distribution": [
        ("movie_id", "STRING"), ("title", "STRING"), ("genre", "STRING"), ("rating_count", "BIGINT"),
        ("avg_rating", "DOUBLE"), ("r1", "BIGINT"), ("r2", "BIGINT"), ("r3", "BIGINT"), ("r4", "BIGINT"),
        ("r5", "BIGINT"),
    ],
    "genre_monthly": [
        ("genre", "STRING"), ("month", "DATE"), ("views", "BIGINT"), ("unique_viewers", "BIGINT"),
        ("watch_hours", "DOUBLE"), ("avg_rating", "DOUBLE"),
    ],
    "view_trends": [
        ("scope", "STRING"), ("key", "STRING"), ("period", "STRING"), ("period_start", "DATE"),
        ("views", "BIGINT"), ("prev_views", "BIGINT"), ("growth", "DOUBLE"),
    ],
    "quality_report": [("table_name", "STRING"), ("reason", "STRING"), ("rows", "BIGINT")],
}
TREND_PERIODS = {"all": ("day", "week", "month"), "genre": ("day", "week", "month"), "movie": ("week", "month")}


def q(name: str) -> str:
    """Quote an identifier (several column names are SQL keywords: rank, key, day, hour, month, ...)."""
    return f"`{name}`"


def parse_ts(column: str) -> str:
    """Timestamp text -> TIMESTAMP(3) in UTC, NULL when unparseable (mirrors Spark `to_timestamp`
    for the formats in the data: naive 'yyyy-MM-dd HH:mm:ss' / 'T' separator, 'Z', '+hh:mm' offsets,
    and a bare date)."""
    base = f"TO_TIMESTAMP(REPLACE(SUBSTRING({column} FROM 1 FOR 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss')"
    offset_minutes = (
        f"(CAST(SUBSTRING({column} FROM 21 FOR 2) AS INT) * 60 + CAST(SUBSTRING({column} FROM 24 FOR 2) AS INT))"
    )
    return f"""(CASE
        WHEN {column} IS NULL THEN CAST(NULL AS TIMESTAMP(3))
        WHEN CHAR_LENGTH({column}) = 19 THEN {base}
        WHEN CHAR_LENGTH({column}) = 20 AND SUBSTRING({column} FROM 20 FOR 1) = 'Z' THEN {base}
        WHEN CHAR_LENGTH({column}) = 25 AND SUBSTRING({column} FROM 20 FOR 1) = '+'
            THEN TIMESTAMPADD(MINUTE, -{offset_minutes}, {base})
        WHEN CHAR_LENGTH({column}) = 25 AND SUBSTRING({column} FROM 20 FOR 1) = '-'
            THEN TIMESTAMPADD(MINUTE, {offset_minutes}, {base})
        WHEN CHAR_LENGTH({column}) = 10 THEN TO_TIMESTAMP({column}, 'yyyy-MM-dd')
        ELSE CAST(NULL AS TIMESTAMP(3)) END)"""


def lit(text: str) -> str:
    """A STRING literal: plain literals of different lengths would be typed CHAR(n) and space-padded."""
    return f"CAST('{text}' AS STRING)"


def nz(column: str) -> str:
    """Empty CSV field -> NULL (Spark reads the raw files with nullValue = '')."""
    return f"NULLIF({column}, '')"


class Job:
    def __init__(self, data_dir: Path, boundary: dict) -> None:
        self.data_dir = data_dir
        self.output_dir = data_dir / "warehouse" / "flink" / "analytics"
        self.archive_dir = data_dir / "warehouse" / "landing" / "watch_events"
        self.replay_day = boundary.get("replay_day")
        self.batch_cutoff = boundary.get("batch_cutoff") or self.replay_day
        self.now = boundary.get("now") or time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        self.env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
        config = self.env.get_config()
        config.set("table.local-time-zone", "UTC")
        config.set("parallelism.default", os.getenv("FLINK_BATCH_PARALLELISM", "2"))

    def sql(self, statement: str) -> None:
        self.env.execute_sql(statement)

    def view(self, name: str, query: str) -> None:
        self.sql(f"CREATE TEMPORARY VIEW {name} AS {query}")

    def csv_source(self, name: str, path: Path, columns: list[str], partition: str | None = None) -> None:
        fields = ", ".join(f"{q(column)} STRING" for column in columns + ([partition] if partition else []))
        partitioned = f"PARTITIONED BY ({q(partition)})" if partition else ""
        self.sql(f"""
            CREATE TEMPORARY TABLE {name} ({fields}) {partitioned} WITH (
                'connector' = 'filesystem', 'path' = 'file://{path.resolve()}', 'format' = 'csv'
            )""")

    # ------------------------------------------------------------------ raw -> curated
    def sources(self) -> None:
        raw = self.data_dir
        self.csv_source("raw_users", raw / "users.csv", [
            "user_id", "age", "gender", "region", "registration_date", "subscription_plan", "activity_profile"])
        self.csv_source("raw_movies", raw / "movies.csv", [
            "movie_id", "title", "genre", "release_year", "duration", "content_type", "language", "maturity_rating"])
        self.csv_source("raw_ratings", raw / "ratings.csv", ["rating_id", "user_id", "movie_id", "rating", "timestamp"])
        watch_columns = ["watch_id", "user_id", "movie_id", "watch_timestamp", "watch_duration", "device"]
        self.csv_source("raw_watches", raw / "watch_history.csv", watch_columns)
        # The CSV header line is read as a data row; drop it by its id value.
        csv_watches = f"""
            SELECT {', '.join(nz(q(c)) + ' AS ' + q(c) for c in watch_columns)}, 0 AS src
            FROM raw_watches WHERE watch_id <> 'watch_id'"""
        if self.replay_day:
            day = f"CAST({parse_ts(nz('watch_timestamp'))} AS DATE)"
            csv_watches += f" AND ({day} IS NULL OR {day} <> DATE '{self.replay_day}')"
        has_archive = (self.archive_dir / "_spark_metadata").is_dir() and any(self.archive_dir.glob("watch_date=*"))
        if has_archive:
            self.csv_source("raw_archive", self.archive_dir, watch_columns, partition="watch_date")
            archived = f"""
                SELECT {', '.join(nz(q(c)) + ' AS ' + q(c) for c in watch_columns)}, 1 AS src FROM raw_archive"""
            if self.batch_cutoff:
                archived += f" WHERE CAST({parse_ts(nz('watch_timestamp'))} AS DATE) < DATE '{self.batch_cutoff}'"
            csv_watches = f"{csv_watches} UNION ALL {archived}"
        self.view("watches_in", csv_watches)

    def curate(self) -> None:
        now = f"TIMESTAMP '{self.now}'"
        # users: duplicate_id, null_value (columns the analytics use)
        self.view("users_tagged", f"""
            SELECT *, CASE
                WHEN rn > 1 THEN {lit('duplicate_id')}
                WHEN user_id IS NULL OR subscription_plan IS NULL OR region IS NULL THEN {lit('null_value')}
            END AS reason
            FROM (
                SELECT {nz('user_id')} AS user_id, {nz('subscription_plan')} AS subscription_plan,
                       {nz('region')} AS region,
                       ROW_NUMBER() OVER (PARTITION BY {nz('user_id')}
                           ORDER BY age, gender, region, registration_date, subscription_plan, activity_profile) AS rn
                FROM raw_users WHERE user_id <> 'user_id'
            )""")
        self.view("users_clean", "SELECT user_id, subscription_plan, region FROM users_tagged WHERE reason IS NULL")
        # movies: duplicate_id, null_value, invalid_value (duration <= 0, non-integer numbers)
        self.view("movies_tagged", f"""
            SELECT *, CASE
                WHEN rn > 1 THEN {lit('duplicate_id')}
                WHEN movie_id IS NULL OR title IS NULL OR genre IS NULL OR release_year IS NULL OR duration IS NULL
                    THEN {lit('null_value')}
                WHEN duration <= 0 OR duration <> FLOOR(duration) OR release_year <> FLOOR(release_year)
                    THEN {lit('invalid_value')}
            END AS reason
            FROM (
                SELECT {nz('movie_id')} AS movie_id, {nz('title')} AS title, {nz('genre')} AS genre,
                       TRY_CAST({nz('release_year')} AS DOUBLE) AS release_year,
                       TRY_CAST({nz('duration')} AS DOUBLE) AS duration,
                       {nz('content_type')} AS content_type, {nz(q('language'))} AS {q('language')},
                       {nz('maturity_rating')} AS maturity_rating,
                       ROW_NUMBER() OVER (PARTITION BY {nz('movie_id')}
                           ORDER BY title, genre, release_year, duration, content_type, {q('language')},
                                    maturity_rating) AS rn
                FROM raw_movies WHERE movie_id <> 'movie_id'
            )""")
        self.view("movies_clean", f"""
            SELECT movie_id, title, genre, CAST(release_year AS INT) AS release_year,
                   CAST(duration AS INT) AS duration, content_type, {q('language')}, maturity_rating
            FROM movies_tagged WHERE reason IS NULL""")
        # ratings: every rule
        self.view("ratings_tagged", f"""
            SELECT r.*, CASE
                WHEN rn > 1 THEN {lit('duplicate_id')}
                WHEN ts_text IS NOT NULL AND ts IS NULL THEN {lit('invalid_timestamp')}
                WHEN rating_id IS NULL OR r.user_id IS NULL OR r.movie_id IS NULL OR rating IS NULL OR ts IS NULL
                    THEN {lit('null_value')}
                WHEN rating < 1 OR rating > 5 OR rating <> FLOOR(rating) THEN {lit('invalid_value')}
                WHEN ts > {now} THEN {lit('future_timestamp')}
                WHEN u.user_id IS NULL THEN {lit('orphan_user')}
                WHEN m.movie_id IS NULL THEN {lit('orphan_movie')}
            END AS reason
            FROM (
                SELECT {nz('rating_id')} AS rating_id, {nz('user_id')} AS user_id, {nz('movie_id')} AS movie_id,
                       TRY_CAST({nz('rating')} AS DOUBLE) AS rating, {nz(q('timestamp'))} AS ts_text,
                       {parse_ts(nz(q('timestamp')))} AS ts,
                       ROW_NUMBER() OVER (PARTITION BY {nz('rating_id')}
                           ORDER BY {q('timestamp')}, user_id, movie_id, rating) AS rn
                FROM raw_ratings WHERE rating_id <> 'rating_id'
            ) r
            LEFT JOIN users_clean u ON r.user_id = u.user_id
            LEFT JOIN movies_clean m ON r.movie_id = m.movie_id""")
        self.view("ratings_clean", """
            SELECT rating_id, user_id, movie_id, CAST(rating AS INT) AS rating, ts FROM ratings_tagged
            WHERE reason IS NULL""")
        # watches: every rule (CSV rows before archived rows for duplicate ids)
        self.view("watches_tagged", f"""
            SELECT w.*, CASE
                WHEN rn > 1 THEN {lit('duplicate_id')}
                WHEN ts_text IS NOT NULL AND ts IS NULL THEN {lit('invalid_timestamp')}
                WHEN watch_id IS NULL OR w.user_id IS NULL OR w.movie_id IS NULL OR ts IS NULL
                     OR watch_duration IS NULL OR device IS NULL THEN {lit('null_value')}
                WHEN watch_duration <= 0 OR watch_duration <> FLOOR(watch_duration) THEN {lit('invalid_value')}
                WHEN ts > {now} THEN {lit('future_timestamp')}
                WHEN u.user_id IS NULL THEN {lit('orphan_user')}
                WHEN m.movie_id IS NULL THEN {lit('orphan_movie')}
            END AS reason
            FROM (
                SELECT watch_id, user_id, movie_id, watch_timestamp AS ts_text, {parse_ts('watch_timestamp')} AS ts,
                       TRY_CAST(watch_duration AS DOUBLE) AS watch_duration, device,
                       ROW_NUMBER() OVER (PARTITION BY watch_id
                           ORDER BY src, watch_timestamp, user_id, movie_id, watch_duration, device) AS rn
                FROM watches_in
            ) w
            LEFT JOIN users_clean u ON w.user_id = u.user_id
            LEFT JOIN movies_clean m ON w.movie_id = m.movie_id""")
        self.view("watches_clean", """
            SELECT watch_id, user_id, movie_id, ts AS watch_timestamp, CAST(watch_duration AS INT) AS watch_duration,
                   device FROM watches_tagged WHERE reason IS NULL""")

    # ------------------------------------------------------------------ curated -> analytics
    def analytics(self) -> dict[str, str]:
        v = self.view
        v("joined", """
            SELECT w.watch_id, w.user_id, w.movie_id, w.watch_timestamp, w.watch_duration, m.title, m.genre,
                   m.duration, u.subscription_plan, u.region,
                   LEAST(CAST(1.0 AS DOUBLE), CAST(w.watch_duration AS DOUBLE) / m.duration) AS completion
            FROM watches_clean w JOIN movies_clean m ON w.movie_id = m.movie_id
            JOIN users_clean u ON w.user_id = u.user_id""")
        # Bayesian average: m = median rating count, C = global mean rating.
        v("rating_counts", """
            SELECT movie_id, AVG(CAST(rating AS DOUBLE)) AS avg_raw, COUNT(rating_id) AS rating_count
            FROM ratings_clean GROUP BY movie_id""")
        # Median: the middle row (odd count) or the mean of the two middle rows (even count).
        v("rating_priors", """
            SELECT g.global_mean, m.min_votes
            FROM (SELECT AVG(CAST(rating AS DOUBLE)) AS global_mean FROM ratings_clean) g,
                 (SELECT AVG(CAST(rating_count AS DOUBLE)) AS min_votes FROM (
                      SELECT rating_count, ROW_NUMBER() OVER (ORDER BY rating_count) AS rn,
                             COUNT(*) OVER () AS n FROM rating_counts)
                  WHERE 2 * rn >= n AND 2 * rn <= n + 2) m""")
        v("rating_stats", """
            SELECT movie_id, rating_count, ROUND(avg_raw, 2) AS avg_rating,
                   ROUND((rating_count * avg_raw + min_votes * global_mean) / (rating_count + min_votes), 2)
                       AS weighted_rating
            FROM rating_counts, rating_priors""")
        v("watch_stats", """
            SELECT movie_id, COUNT(watch_id) AS views, SUM(CAST(watch_duration AS BIGINT)) AS watch_minutes,
                   SUM(completion) AS completion_sum, AVG(completion) AS avg_completion,
                   COUNT(DISTINCT user_id) AS unique_viewers
            FROM joined GROUP BY movie_id""")
        v("movie_stats", f"""
            SELECT m.movie_id, m.title, m.genre, m.release_year, m.duration, m.content_type, m.{q('language')},
                   m.maturity_rating, COALESCE(w.views, 0) AS views, COALESCE(w.unique_viewers, 0) AS unique_viewers,
                   r.avg_rating, r.rating_count, r.weighted_rating,
                   ROUND(CAST(COALESCE(w.watch_minutes, 0) AS DOUBLE) / 60, 1) AS watch_hours,
                   ROUND(w.avg_completion, 3) AS completion_rate, COALESCE(w.watch_minutes, 0) AS watch_minutes,
                   w.completion_sum, w.avg_completion
            FROM movies_clean m LEFT JOIN watch_stats w ON m.movie_id = w.movie_id
            LEFT JOIN rating_stats r ON m.movie_id = r.movie_id""")
        v("movie_performance", f"""
            SELECT movie_id, title, genre, release_year, duration, content_type, {q('language')}, maturity_rating,
                   views, unique_viewers, avg_rating, rating_count, weighted_rating, watch_hours, completion_rate
            FROM movie_stats""")
        v("user_stats", """
            SELECT user_id, COUNT(watch_id) AS views, SUM(CAST(watch_duration AS BIGINT)) AS watch_minutes,
                   COUNT(DISTINCT CAST(watch_timestamp AS DATE)) AS active_days,
                   SUM(completion) AS completion_sum, AVG(completion) AS avg_completion
            FROM joined GROUP BY user_id""")
        v("user_engagement", f"""
            SELECT u.user_id, u.subscription_plan, u.region, COALESCE(s.views, 0) AS views,
                   COALESCE(s.watch_minutes, 0) AS watch_minutes, COALESCE(s.active_days, 0) AS active_days,
                   ROUND(COALESCE(s.completion_sum, CAST(0.0 AS DOUBLE)), 4) AS completion_sum,
                   ROUND(s.avg_completion, 3) AS avg_completion,
                   CASE WHEN COALESCE(s.views, 0) <= 10 THEN {lit('Inactive')}
                        WHEN COALESCE(s.views, 0) <= 35 THEN {lit('Casual')} ELSE {lit('Active')} END AS segment
            FROM users_clean u LEFT JOIN user_stats s ON u.user_id = s.user_id""")
        v("genre_ranked", """
            SELECT user_id, genre, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY genre_views DESC, genre) AS rn
            FROM (SELECT user_id, genre, COUNT(*) AS genre_views FROM joined GROUP BY user_id, genre)""")
        v("favorites", "SELECT user_id, genre FROM genre_ranked WHERE rn <= 2")
        v("favorites_ranked", "SELECT user_id, genre, rn FROM genre_ranked WHERE rn <= 2")
        v("rated_movies", """
            SELECT genre, movie_id, title, weighted_rating, rating_count,
                   ROW_NUMBER() OVER (PARTITION BY genre
                       ORDER BY weighted_rating DESC, rating_count DESC, movie_id) AS genre_rank
            FROM movie_performance WHERE weighted_rating IS NOT NULL""")
        v("watched", "SELECT DISTINCT user_id, movie_id, genre FROM joined")
        v("recommendation_candidates", f"""
            SELECT f.user_id, r.movie_id, r.title, r.genre, r.weighted_rating, r.rating_count
            FROM favorites f
            LEFT JOIN (SELECT user_id, genre, COUNT(*) AS watched FROM watched GROUP BY user_id, genre) g
                ON f.user_id = g.user_id AND f.genre = g.genre
            JOIN rated_movies r ON f.genre = r.genre AND r.genre_rank <= {TOP_N} + COALESCE(g.watched, 0)
            LEFT JOIN watched w ON f.user_id = w.user_id AND r.movie_id = w.movie_id
            WHERE w.movie_id IS NULL""")
        v("genre_views", """
            SELECT genre, COUNT(watch_id) AS views, COUNT(DISTINCT user_id) AS unique_viewers,
                   ROUND(CAST(SUM(CAST(watch_duration AS BIGINT)) AS DOUBLE) / 60, 1) AS watch_hours
            FROM joined GROUP BY genre""")
        v("rated_genre", "SELECT r.rating, r.ts, m.genre FROM ratings_clean r JOIN movies_clean m ON r.movie_id = m.movie_id")
        month = "TO_DATE(DATE_FORMAT({}, 'yyyy-MM-01'))"
        v("genre_month_views", f"""
            SELECT genre, {month.format('watch_timestamp')} AS {q('month')}, COUNT(watch_id) AS views,
                   COUNT(DISTINCT user_id) AS unique_viewers,
                   ROUND(CAST(SUM(CAST(watch_duration AS BIGINT)) AS DOUBLE) / 60, 1) AS watch_hours
            FROM joined GROUP BY genre, {month.format('watch_timestamp')}""")
        v("genre_month_ratings", f"""
            SELECT genre, {month.format('ts')} AS {q('month')}, ROUND(AVG(CAST(rating AS DOUBLE)), 2) AS avg_rating
            FROM rated_genre GROUP BY genre, {month.format('ts')}""")

        queries = {
            "movie_performance": "SELECT * FROM movie_performance",
            "low_engagement_movies": f"""
                SELECT movie_id, title, genre, views, watch_minutes,
                       ROUND(COALESCE(completion_sum, CAST(0.0 AS DOUBLE)), 4), completion_rate,
                       views >= {MIN_VIEWS} AND COALESCE(avg_completion < {LOW_COMPLETION_THRESHOLD}, FALSE)
                FROM movie_stats""",
            "genre_popularity": """
                SELECT v.genre, v.views, v.unique_viewers, v.watch_hours, r.avg_rating FROM genre_views v
                LEFT JOIN (SELECT genre, ROUND(AVG(CAST(rating AS DOUBLE)), 2) AS avg_rating FROM rated_genre
                           GROUP BY genre) r ON v.genre = r.genre""",
            "subscription_views": """
                SELECT subscription_plan, COUNT(watch_id), COUNT(DISTINCT user_id) FROM joined
                GROUP BY subscription_plan""",
            "user_engagement": "SELECT * FROM user_engagement",
            "user_segments": f"""
                SELECT s.segment, COALESCE(e.users, 0), COALESCE(e.views, 0)
                FROM (VALUES ({lit('Inactive')}), ({lit('Casual')}), ({lit('Active')})) AS s(segment)
                LEFT JOIN (SELECT segment, COUNT(user_id) AS users, SUM(views) AS views FROM user_engagement
                           GROUP BY segment) e ON s.segment = e.segment""",
            "user_preferences": """
                SELECT user_id, MAX(CASE WHEN rn = 1 THEN genre END)
                       || COALESCE('|' || MAX(CASE WHEN rn = 2 THEN genre END), '')
                FROM favorites_ranked GROUP BY user_id""",
            "daily_views": """
                SELECT CAST(watch_timestamp AS DATE), COUNT(watch_id), COUNT(DISTINCT user_id) FROM joined
                GROUP BY CAST(watch_timestamp AS DATE)""",
            "hourly_views": """
                SELECT CAST(HOUR(watch_timestamp) AS INT), COUNT(watch_id) FROM joined
                GROUP BY HOUR(watch_timestamp)""",
            "summary_metrics": """
                SELECT u.n, m.n, w.n, r.n, r.average_rating
                FROM (SELECT COUNT(*) AS n FROM users_clean) u, (SELECT COUNT(*) AS n FROM movies_clean) m,
                     (SELECT COUNT(*) AS n FROM watches_clean) w,
                     (SELECT COUNT(*) AS n, ROUND(AVG(CAST(rating AS DOUBLE)), 2) AS average_rating
                      FROM ratings_clean) r""",
            "user_recommendations": f"""
                SELECT user_id, CAST(rn AS INT), movie_id, title, genre, weighted_rating, rating_count FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id
                        ORDER BY weighted_rating DESC, rating_count DESC, movie_id) AS rn
                    FROM recommendation_candidates)
                WHERE rn <= {TOP_N}""",
            "rating_distribution": f"""
                SELECT p.movie_id, p.title, p.genre, p.rating_count, p.avg_rating,
                       {', '.join(f'd.r{star}' for star in range(1, 6))}
                FROM movie_performance p JOIN (
                    SELECT movie_id, {', '.join(f'COUNT(CASE WHEN rating = {star} THEN 1 END) AS r{star}'
                                                for star in range(1, 6))}
                    FROM ratings_clean GROUP BY movie_id) d ON p.movie_id = d.movie_id
                WHERE p.rating_count IS NOT NULL""",
            "genre_monthly": f"""
                SELECT v.genre, v.{q('month')}, v.views, v.unique_viewers, v.watch_hours, r.avg_rating
                FROM genre_month_views v LEFT JOIN genre_month_ratings r
                    ON v.genre = r.genre AND v.{q('month')} = r.{q('month')}""",
            "view_trends": self.view_trends(),
            "quality_report": " UNION ALL ".join(
                f"SELECT {lit(name)}, COALESCE(reason, {lit('accepted')}), COUNT(*) FROM {view}_tagged "
                f"GROUP BY COALESCE(reason, {lit('accepted')})"
                for name, view in (("users", "users"), ("movies", "movies"), ("ratings", "ratings"),
                                   ("watch_history", "watches"))
            ),
        }
        return queries

    def view_trends(self) -> str:
        day = "CAST(watch_timestamp AS DATE)"
        # DAYOFWEEK: 1 = Sunday ... 7 = Saturday; ISO weeks start on Monday.
        starts = {
            "day": day,
            "week": f"CAST(TIMESTAMPADD(DAY, -MOD(DAYOFWEEK({day}) + 5, 7), CAST({day} AS TIMESTAMP(3))) AS DATE)",
            "month": "TO_DATE(DATE_FORMAT(watch_timestamp, 'yyyy-MM-01'))",
        }
        previous = {
            "day": "CAST(TIMESTAMPADD(DAY, -1, CAST(period_start AS TIMESTAMP(3))) AS DATE)",
            "week": "CAST(TIMESTAMPADD(DAY, -7, CAST(period_start AS TIMESTAMP(3))) AS DATE)",
            "month": "CAST(TIMESTAMPADD(MONTH, -1, CAST(period_start AS TIMESTAMP(3))) AS DATE)",
        }
        keys = {"all": lit("all"), "genre": "genre", "movie": "movie_id"}
        parts = []
        for scope, periods in TREND_PERIODS.items():
            for period in periods:
                name = f"trend_{scope}_{period}"
                self.view(name, f"""
                    SELECT {keys[scope]} AS k, {starts[period]} AS period_start, COUNT(watch_id) AS views
                    FROM joined GROUP BY {keys[scope]}, {starts[period]}""")
                parts.append(f"""
                    SELECT {lit(scope)}, c.k, {lit(period)}, c.period_start, c.views, COALESCE(p.views, 0),
                           CASE WHEN p.views > 0
                                THEN ROUND(CAST(c.views - p.views AS DOUBLE) / p.views, 3) END
                    FROM (SELECT k, period_start, views, {previous[period]} AS prev_start FROM {name}) c
                    LEFT JOIN {name} p ON c.k = p.k AND c.prev_start = p.period_start""")
        return " UNION ALL ".join(parts)

    # ------------------------------------------------------------------ run
    def run(self) -> None:
        started = time.perf_counter()
        self.sources()
        self.curate()
        queries = self.analytics()
        shutil.rmtree(self.output_dir, ignore_errors=True)
        self.output_dir.mkdir(parents=True)
        statements = self.env.create_statement_set()
        for table, columns in TABLES.items():
            fields = ", ".join(f"{q(name)} {kind}" for name, kind in columns)
            self.sql(f"""
                CREATE TEMPORARY TABLE sink_{table} ({fields}) WITH (
                    'connector' = 'filesystem', 'path' = 'file://{(self.output_dir / table).resolve()}',
                    'format' = 'csv'
                )""")
            statements.add_insert_sql(f"INSERT INTO sink_{table} {queries[table]}")
        (self.output_dir / "_schema.json").write_text(json.dumps(
            {table: [name for name, _ in columns] for table, columns in TABLES.items()}, indent=1
        ))
        statements.execute().wait()
        print(f"TIMING flink_batch_total {time.perf_counter() - started:.1f}", flush=True)
        print(f"Flink batch tables written to {self.output_dir} "
              f"(replay_day={self.replay_day}, batch_cutoff={self.batch_cutoff}, now={self.now})", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Flink SQL batch job (BR1-BR10), same output as the Spark batch.")
    parser.add_argument("--data-dir", type=Path, default=Path(os.getenv("DATA_DIR", "/opt/flink/data")))
    parser.add_argument("--boundary", type=Path, default=None,
                        help="batch_boundary.json of the Spark run (default: <data-dir>/warehouse/batch_boundary.json)")
    args = parser.parse_args()
    boundary_path = args.boundary or args.data_dir / "warehouse" / "batch_boundary.json"
    boundary = json.loads(boundary_path.read_text()) if boundary_path.is_file() else {}
    Job(args.data_dir, boundary).run()


if __name__ == "__main__":
    main()
