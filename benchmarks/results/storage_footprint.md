# Storage footprint

| store | size_mb |
|---|---|
| raw CSV (data/*.csv) | 74.3 |
| Hive/Parquet raw.db | 28.0 |
| Hive/Parquet curated.db | 29.6 |
| Hive/Parquet analytics.db | 7.2 |
| Cassandra movie_analytics keyspace (serving, metadata and live tables, RF=1) | 35.8 |

Measured as file sizes after `nodetool flush`, without truncate snapshots; Cassandra data is LZ4-compressed SSTables.
