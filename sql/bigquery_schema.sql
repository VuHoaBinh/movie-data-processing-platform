CREATE TABLE IF NOT EXISTS `movie_analytics.movie_performance` (
  movie_id STRING NOT NULL,
  title STRING NOT NULL,
  genre STRING,
  release_year INT64,
  duration INT64,
  views INT64,
  unique_viewers INT64,
  avg_rating FLOAT64,
  rating_count INT64,
  weighted_rating FLOAT64,
  watch_hours FLOAT64,
  completion_rate FLOAT64
)
CLUSTER BY genre, release_year;

CREATE TABLE IF NOT EXISTS `movie_analytics.daily_views` (
  day DATE NOT NULL,
  views INT64,
  unique_viewers INT64
)
PARTITION BY day;

CREATE TABLE IF NOT EXISTS `movie_analytics.user_engagement` (
  user_id STRING NOT NULL,
  views INT64,
  watch_minutes INT64,
  active_days INT64,
  subscription_plan STRING,
  region STRING,
  segment STRING
)
CLUSTER BY segment, subscription_plan;
