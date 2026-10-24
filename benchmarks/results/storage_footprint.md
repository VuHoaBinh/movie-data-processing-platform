# Storage footprint

| store | size_mb |
|---|---|
| raw CSV (data/*.csv) | 74.3 |
| Hive/Parquet raw.db | 28.8 |
| Hive/Parquet curated.db | 29.7 |
| Hive/Parquet analytics.db | 6.2 |
| Cassandra movie_analytics keyspace (serving, metadata and live tables, RF=1) | 31.4 |

Measured as file sizes after `nodetool flush`, without truncate snapshots; Cassandra data is LZ4-compressed SSTables.
