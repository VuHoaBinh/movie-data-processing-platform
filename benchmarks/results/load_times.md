# Batch load times (1M watch rows)

| stage | what | seconds |
|---|---|---|
| hive_raw | CSV -> raw Parquet tables (4) | 18.4 |
| hive_curated | raw -> curated Parquet (cleaning, curated.watch_events partitioned by date) | 52.2 |
| build_tables | analytics plan: eager scalars (global rating statistics, recommendation candidate bounds) | 47.4 |
| als | BR3 alternative: ALS training and scoring (Spark MLlib, 1M watch events) | 40.7 |
| hive_analytics | curated -> 20 analytics Parquet tables (19 shared with Pandas + ALS) | 149.8 |
| cassandra_load | analytics + quality report -> 25 Cassandra tables (24 serving + quality report; truncate + insert) + run metadata | 53.5 |
| total | sum of the stages above | 362.0 |

Spark local[8] (`SPARK_LOCAL_CORES`), 1.5 GB driver (`SPARK_DRIVER_MEMORY`), Docker Desktop; Cassandra writes go through spark-cassandra-connector 3.5.1.
