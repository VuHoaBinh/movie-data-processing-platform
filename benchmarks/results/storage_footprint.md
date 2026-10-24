# Storage footprint

| store | size_mb |
|---|---|
| raw CSV (data/*.csv) | 74.3 |
| Hive/Parquet raw.db | 28.8 |
| Hive/Parquet curated.db | 29.7 |
| Hive/Parquet analytics.db | 1.4 |
| Cassandra movie_analytics (13 serving tables, RF=1) | 37.0 |

Measured as file sizes after `nodetool flush`; Cassandra data is LZ4-compressed SSTables.
