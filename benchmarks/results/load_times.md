# Batch load times (1M watch rows)

| stage | what | seconds |
|---|---|---|
| hive_raw | CSV -> raw Parquet tables (4) | 87.3 |
| hive_curated | raw -> curated Parquet (cleaning, curated.watch_events partitioned by date) | 50.8 |
| build_tables | analytics plan: eager scalars (global rating statistics, recommendation candidate bounds) | 36.8 |
| als | BR3 alternative: ALS training and scoring (Spark MLlib, 1M watch events) | 37.3 |
| hive_analytics | curated -> 12 analytics Parquet tables (11 shared with Pandas + ALS) | 76.5 |
| cassandra_load | analytics + quality report -> 16 Cassandra serving tables (truncate + insert) + run metadata | 48.6 |
| total | sum of the stages above | 337.3 |

Spark local[*], 1.5 GB driver (`SPARK_DRIVER_MEMORY`), Docker Desktop; Cassandra writes go through spark-cassandra-connector 3.5.1.
