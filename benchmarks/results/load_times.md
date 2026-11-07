# Batch load times (1M watch rows)

| stage | what | seconds |
|---|---|---|
| hive_raw | CSV -> raw Parquet tables (4) | 35.6 |
| hive_curated | raw -> curated Parquet (cleaning, curated.watch_events partitioned by date) | 103.5 |
| build_tables | analytics plan: eager scalars (global rating statistics, recommendation candidate bounds) | 90.7 |
| als | BR3 alternative: ALS training and scoring (Spark MLlib, 1M watch events) | 68.5 |
| hive_analytics | curated -> 21 analytics Parquet tables (20 shared with Pandas + ALS) | 320.9 |
| cassandra_load | analytics + quality report -> 26 Cassandra tables (25 serving + quality report; truncate + insert) + run metadata | 102.3 |
| total | sum of the stages above | 721.5 |

Spark local[8] (`SPARK_LOCAL_CORES`), 1.5 GB driver (`SPARK_DRIVER_MEMORY`), Docker Desktop; Cassandra writes go through spark-cassandra-connector 3.5.1.
