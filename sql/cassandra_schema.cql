-- Serving layer: one table per dashboard query (query-first design).
-- Apply with: cqlsh -f sql/cassandra_schema.cql  (compose runs this in `cassandra-init`).
-- Single-node demo uses RF=1; use NetworkTopologyStrategy with RF=3 in production.
CREATE KEYSPACE IF NOT EXISTS movie_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE movie_analytics;

-- BR1 (catalogue by genre, sorted by views), BR7 (performance per genre).
CREATE TABLE IF NOT EXISTS movie_performance (
  genre text,
  views bigint,
  movie_id text,
  title text,
  release_year int,
  duration int,
  unique_viewers bigint,
  avg_rating double,
  rating_count bigint,
  weighted_rating double,
  watch_hours double,
  completion_rate double,
  PRIMARY KEY ((genre), views, movie_id)
) WITH CLUSTERING ORDER BY (views DESC, movie_id ASC);

-- BR3: top weighted-rated movies inside one favourite genre.
CREATE TABLE IF NOT EXISTS movies_by_genre_rating (
  genre text,
  weighted_rating double,
  movie_id text,
  title text,
  avg_rating double,
  rating_count bigint,
  views bigint,
  PRIMARY KEY ((genre), weighted_rating, movie_id)
) WITH CLUSTERING ORDER BY (weighted_rating DESC, movie_id ASC);

-- BR4: global Bayesian top-rated list (LIMIT 10).
CREATE TABLE IF NOT EXISTS top_rated_movies (
  bucket text,
  weighted_rating double,
  rating_count bigint,
  movie_id text,
  title text,
  genre text,
  avg_rating double,
  views bigint,
  PRIMARY KEY ((bucket), weighted_rating, rating_count, movie_id)
) WITH CLUSTERING ORDER BY (weighted_rating DESC, rating_count DESC, movie_id ASC);

-- BR2 (all-time popular), BR7 (top share of movies by views).
CREATE TABLE IF NOT EXISTS top_viewed_movies (
  bucket text,
  views bigint,
  movie_id text,
  title text,
  genre text,
  unique_viewers bigint,
  avg_rating double,
  completion_rate double,
  watch_hours double,
  PRIMARY KEY ((bucket), views, movie_id)
) WITH CLUSTERING ORDER BY (views DESC, movie_id ASC);

-- BR8: lowest-viewed titles first (ties by title).
CREATE TABLE IF NOT EXISTS low_engagement_movies (
  bucket text,
  views bigint,
  title text,
  movie_id text,
  genre text,
  completion_rate double,
  PRIMARY KEY ((bucket), views, title, movie_id)
) WITH CLUSTERING ORDER BY (views ASC, title ASC, movie_id ASC);

-- BR5
CREATE TABLE IF NOT EXISTS genre_popularity (
  genre text PRIMARY KEY,
  views bigint,
  unique_viewers bigint
);

-- BR6
CREATE TABLE IF NOT EXISTS subscription_views (
  subscription_plan text PRIMARY KEY,
  views bigint,
  unique_viewers bigint
);

CREATE TABLE IF NOT EXISTS user_engagement (
  user_id text PRIMARY KEY,
  subscription_plan text,
  region text,
  views bigint,
  watch_minutes bigint,
  active_days bigint,
  segment text
);

CREATE TABLE IF NOT EXISTS hourly_views (
  hour int PRIMARY KEY,
  views bigint
);

-- BR9
CREATE TABLE IF NOT EXISTS user_segments (
  segment text PRIMARY KEY,
  users bigint,
  views bigint
);

-- BR3: favourite genres of one user ('a|b').
CREATE TABLE IF NOT EXISTS user_preferences (
  user_id text PRIMARY KEY,
  favorite_genres text
);

-- BR3: precomputed top-10 per user (watched titles excluded), one partition per user, rank 1..10.
-- `source` is 'genre' (favourite genres) or 'popular' (filler / cold start) here, 'als' in the ALS table.
CREATE TABLE IF NOT EXISTS user_recommendations (
  user_id text,
  rank int,
  movie_id text,
  title text,
  genre text,
  source text,
  weighted_rating double,
  avg_rating double,
  views bigint,
  PRIMARY KEY ((user_id), rank)
) WITH CLUSTERING ORDER BY (rank ASC);

-- BR3 alternative (Spark MLlib ALS): same shape; only users with watch history have rows.
CREATE TABLE IF NOT EXISTS user_recommendations_als (
  user_id text,
  rank int,
  movie_id text,
  title text,
  genre text,
  source text,
  weighted_rating double,
  avg_rating double,
  views bigint,
  PRIMARY KEY ((user_id), rank)
) WITH CLUSTERING ORDER BY (rank ASC);

-- BR10: time series, one small partition.
CREATE TABLE IF NOT EXISTS daily_views (
  bucket text,
  day date,
  views bigint,
  unique_viewers bigint,
  PRIMARY KEY ((bucket), day)
) WITH CLUSTERING ORDER BY (day ASC);

CREATE TABLE IF NOT EXISTS summary_metrics (
  bucket text PRIMARY KEY,
  users bigint,
  movies bigint,
  views bigint,
  ratings bigint,
  average_rating double
);

-- BR2 live: one partition per UTC day, newest window first, rank 1..10 inside a window.
-- The latest top-10 is `SELECT ... WHERE day = ? LIMIT 10`. TTL expires old windows after 3 days.
CREATE TABLE IF NOT EXISTS live_trending (
  day date,
  window_start timestamp,
  rank int,
  movie_id text,
  title text,
  genre text,
  live_views bigint,
  unique_viewers bigint,
  PRIMARY KEY ((day), window_start, rank)
) WITH CLUSTERING ORDER BY (window_start DESC, rank ASC)
  AND default_time_to_live = 259200;

-- Same shape, written by the Spark Structured Streaming alternative (phase 2).
CREATE TABLE IF NOT EXISTS live_trending_spark (
  day date,
  window_start timestamp,
  rank int,
  movie_id text,
  title text,
  genre text,
  live_views bigint,
  unique_viewers bigint,
  PRIMARY KEY ((day), window_start, rank)
) WITH CLUSTERING ORDER BY (window_start DESC, rank ASC)
  AND default_time_to_live = 259200;

-- Pipeline metadata for the Data Engineering page, written by the Spark batch after the serving tables.
-- Rejected rows per cleaning rule (reason 'accepted' = rows kept), the same numbers as curated.quality_report.
CREATE TABLE IF NOT EXISTS quality_report (
  table_name text,
  reason text,
  rows bigint,
  PRIMARY KEY ((table_name), reason)
);

-- Rows written to each serving table by the latest batch run.
CREATE TABLE IF NOT EXISTS batch_table_rows (
  table_name text PRIMARY KEY,
  rows bigint
);

-- One row per successful batch run, newest first; kept 90 days.
CREATE TABLE IF NOT EXISTS batch_runs (
  bucket text,
  finished_at timestamp,
  started_at timestamp,
  status text,
  seconds double,
  rows_written bigint,
  stage_seconds map<text, double>,
  PRIMARY KEY ((bucket), finished_at)
) WITH CLUSTERING ORDER BY (finished_at DESC)
  AND default_time_to_live = 7776000;
