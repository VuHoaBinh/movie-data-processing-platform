-- Serving layer: one table per dashboard query (query-first design).
-- Apply with: cqlsh -f sql/cassandra_schema.cql  (compose runs this in `cassandra-init`).
-- Single-node demo uses RF=1; use NetworkTopologyStrategy with RF=3 in production.
-- `CREATE TABLE IF NOT EXISTS` never changes an existing table: after a schema change, recreate the
-- serving layer with `docker compose down -v` (everything here is rebuilt by the batch and stream jobs).
CREATE KEYSPACE IF NOT EXISTS movie_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE movie_analytics;

-- Batch versions: every batch run writes its rows under a new version (column `bucket`), then
-- switches `serving_state` to it; the dashboard never sees a half-written or truncated table.
CREATE TABLE IF NOT EXISTS serving_state (
  id text PRIMARY KEY,
  version text,
  previous_version text,
  replay_day date,     -- day replayed into Kafka (owned by the speed layer)
  batch_cutoff date,   -- first day NOT in the batch: live tables supply days >= batch_cutoff
  published_at timestamp
);

-- BR1 (catalogue by genre, sorted by views, filtered by year/language/type/rating), BR7.
CREATE TABLE IF NOT EXISTS movie_performance (
  genre text,
  bucket text,
  views bigint,
  movie_id text,
  title text,
  release_year int,
  duration int,
  content_type text,
  language text,
  maturity_rating text,
  unique_viewers bigint,
  avg_rating double,
  rating_count bigint,
  weighted_rating double,
  watch_hours double,
  completion_rate double,
  PRIMARY KEY ((genre, bucket), views, movie_id)
) WITH CLUSTERING ORDER BY (views DESC, movie_id ASC);

-- BR3: top weighted-rated movies inside one favourite genre.
CREATE TABLE IF NOT EXISTS movies_by_genre_rating (
  genre text,
  bucket text,
  weighted_rating double,
  movie_id text,
  title text,
  avg_rating double,
  rating_count bigint,
  views bigint,
  PRIMARY KEY ((genre, bucket), weighted_rating, movie_id)
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

-- BR8 batch view: every movie with additive sums (views, completion_sum) so the dashboard can add the
-- speed layer's live days; is_low_engagement = views >= min_views AND completion_rate < threshold.
CREATE TABLE IF NOT EXISTS low_engagement_movies (
  bucket text,
  movie_id text,
  title text,
  genre text,
  views bigint,
  watch_minutes bigint,
  completion_sum double,
  completion_rate double,
  is_low_engagement boolean,
  PRIMARY KEY ((bucket), movie_id)
);

-- BR5: per genre, overall and per month.
CREATE TABLE IF NOT EXISTS genre_popularity (
  genre text PRIMARY KEY,
  views bigint,
  unique_viewers bigint,
  watch_hours double,
  avg_rating double
);

CREATE TABLE IF NOT EXISTS genre_monthly (
  genre text,
  month date,
  views bigint,
  unique_viewers bigint,
  watch_hours double,
  avg_rating double,
  PRIMARY KEY ((genre), month)
) WITH CLUSTERING ORDER BY (month ASC);

-- BR3: top-N unwatched movies of the user's favourite genres. `bucket` = batch version that wrote the
-- row; rows of an older version (ranks a newer run no longer produces) are ignored by the dashboard.
CREATE TABLE IF NOT EXISTS user_recommendations (
  user_id text,
  rank int,
  bucket text,
  movie_id text,
  title text,
  genre text,
  weighted_rating double,
  rating_count bigint,
  PRIMARY KEY ((user_id), rank)
) WITH CLUSTERING ORDER BY (rank ASC);

-- BR4: number of 1..5-star ratings per movie.
CREATE TABLE IF NOT EXISTS rating_distribution (
  movie_id text PRIMARY KEY,
  title text,
  genre text,
  rating_count bigint,
  avg_rating double,
  r1 bigint,
  r2 bigint,
  r3 bigint,
  r4 bigint,
  r5 bigint
);

-- BR10: views per day/week/month for the platform (scope 'all'), a genre or a movie, with growth
-- versus the previous period. One partition per (scope, key, period).
CREATE TABLE IF NOT EXISTS view_trends (
  scope text,
  key text,
  period text,
  period_start date,
  views bigint,
  prev_views bigint,
  growth double,
  bucket text,
  PRIMARY KEY ((scope, key, period), period_start)
) WITH CLUSTERING ORDER BY (period_start ASC);

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
  completion_sum double,
  avg_completion double,
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
  last_sent_at timestamp,   -- newest send time in the window (latency = updated_at - last_sent_at)
  updated_at timestamp,
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
  last_sent_at timestamp,   -- newest send time in the window (latency = updated_at - last_sent_at)
  updated_at timestamp,
  PRIMARY KEY ((day), window_start, rank)
) WITH CLUSTERING ORDER BY (window_start DESC, rank ASC)
  AND default_time_to_live = 259200;

-- BR6 live: running totals per (UTC day, user) from the speed layer, one table per engine.
CREATE TABLE IF NOT EXISTS live_user_engagement (
  day date,
  user_id text,
  subscription_plan text,
  views bigint,
  watch_minutes bigint,
  completion_sum double,
  last_sent_at timestamp,
  updated_at timestamp,
  PRIMARY KEY ((day), user_id)
);

CREATE TABLE IF NOT EXISTS live_user_engagement_spark (
  day date,
  user_id text,
  subscription_plan text,
  views bigint,
  watch_minutes bigint,
  completion_sum double,
  last_sent_at timestamp,
  updated_at timestamp,
  PRIMARY KEY ((day), user_id)
);

-- BR8 live: running totals per (UTC day, movie), one table per engine.
CREATE TABLE IF NOT EXISTS live_movie_engagement (
  day date,
  movie_id text,
  title text,
  genre text,
  views bigint,
  watch_minutes bigint,
  completion_sum double,
  last_sent_at timestamp,
  updated_at timestamp,
  PRIMARY KEY ((day), movie_id)
);

CREATE TABLE IF NOT EXISTS live_movie_engagement_spark (
  day date,
  movie_id text,
  title text,
  genre text,
  views bigint,
  watch_minutes bigint,
  completion_sum double,
  last_sent_at timestamp,
  updated_at timestamp,
  PRIMARY KEY ((day), movie_id)
);

-- Simulated clock of the live-day replay (src/replay_clock.py): sim = anchor_sim + (now - anchor_wall) * speed.
CREATE TABLE IF NOT EXISTS replay_clock (
  id text PRIMARY KEY,
  replay_day date,
  anchor_wall timestamp,
  anchor_sim timestamp,
  speed double,
  sent bigint,
  total bigint,
  updated_at timestamp
);
