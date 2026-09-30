"""PySpark version of the main movie-performance batch aggregation.

Install PySpark separately and submit with:
    spark-submit src/spark_batch_job.py
"""
from pyspark.sql import SparkSession, functions as F


def main() -> None:
    spark = SparkSession.builder.appName("movie-analytics-batch").getOrCreate()

    users = spark.read.option("header", True).option("inferSchema", True).csv("data/users.csv")
    movies = spark.read.option("header", True).option("inferSchema", True).csv("data/movies.csv")
    ratings = spark.read.option("header", True).option("inferSchema", True).csv("data/ratings.csv")
    watches = spark.read.option("header", True).option("inferSchema", True).csv("data/watch_history.csv")

    joined = watches.join(movies, "movie_id").join(users, "user_id")
    rating_stats = ratings.groupBy("movie_id").agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("rating_id").alias("rating_count"),
    )
    performance = (
        joined.groupBy("movie_id", "title", "genre", "release_year", "duration")
        .agg(
            F.count("watch_id").alias("views"),
            F.round(F.sum("watch_duration") / 60, 1).alias("watch_hours"),
            F.countDistinct("user_id").alias("unique_viewers"),
            F.round(F.avg("watch_duration") / F.first("duration") * 100, 1).alias("completion_rate"),
        )
        .join(rating_stats, "movie_id", "left")
    )
    performance.write.mode("overwrite").option("header", True).csv(
        "data/processed_spark/movie_performance"
    )
    spark.stop()


if __name__ == "__main__":
    main()
