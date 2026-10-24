# Flink vs Spark Structured Streaming (10 s tumbling windows, top-10 per window)

| engine | scenario | events | achieved_events_per_s | windows | windows_exact | p50_latency_s | p95_latency_s | max_latency_s |
|---|---|---|---|---|---|---|---|---|
| flink-python | burst 100,000 events (produced in 13s) | 100000 | 330 | 20 | 20 |  |  | 289.6 |
| flink-sql | burst 100,000 events (produced in 13s) | 100000 | 7297 | 20 | 20 |  |  | 0.6 |
| spark | burst 100,000 events (produced in 13s) | 100000 | 3849 | 20 | 20 |  |  | 12.8 |
| flink-python | 250 events/s x 60s | 14999 | 250 | 3 | 3 | 10.01 | 10.08 | 10.08 |
| flink-sql | 250 events/s x 60s | 14999 | 250 | 3 | 3 | 0.5 | 0.51 | 0.51 |
| spark | 250 events/s x 60s | 14999 | 250 | 3 | 3 | 2.9 | 2.94 | 2.94 |
| flink-python | 500 events/s x 60s | 29999 | 500 | 3 | 3 | 20.68 | 26.09 | 26.09 |
| flink-sql | 500 events/s x 60s | 29999 | 500 | 3 | 3 | 0.53 | 0.77 | 0.77 |
| spark | 500 events/s x 60s | 29999 | 500 | 3 | 3 | 2.65 | 2.76 | 2.76 |
| flink-python | 1000 events/s x 60s | 59987 | 1000 | 3 | 3 | 78.93 | 97.55 | 97.55 |
| flink-sql | 1000 events/s x 60s | 59987 | 1000 | 3 | 3 | 0.55 | 0.75 | 0.75 |
| spark | 1000 events/s x 60s | 59987 | 1000 | 3 | 3 | 2.71 | 2.87 | 2.87 |
| flink-python | 2000 events/s x 60s | 119982 | 1999 | 3 | 3 | 151.95 | 187.53 | 187.53 |
| flink-sql | 2000 events/s x 60s | 119982 | 1999 | 3 | 3 | 0.71 | 1.51 | 1.51 |
| spark | 2000 events/s x 60s | 119982 | 1999 | 3 | 3 | 2.96 | 3.07 | 3.07 |
| flink-python | 4000 events/s x 60s | 239958 | 3995 | 3 | 2 | 219.4 | 278.73 | 278.73 |
| flink-sql | 4000 events/s x 60s | 239958 | 3995 | 3 | 3 | 0.61 | 0.67 | 0.67 |
| spark | 4000 events/s x 60s | 239958 | 3995 | 3 | 3 | 2.69 | 2.73 | 2.73 |
| flink-python | 8000 events/s x 60s | 436713 | 5979 | 5 | 0 |  |  |  |
| flink-sql | 8000 events/s x 60s | 436713 | 5979 | 5 | 5 | 0.94 | 1.38 | 1.38 |
| spark | 8000 events/s x 60s | 436713 | 5979 | 5 | 5 | 2.75 | 3.08 | 3.08 |

Same Kafka topic, same events, both engines running concurrently on one Docker Desktop (WSL2) host; flink-python = the project's PyFlink DataStream job and flink-sql = the Flink SQL tumbling-window job (Flink 2.1, parallelism 1, event time = Kafka record timestamp, monotonous watermarks) -> Kafka -> materializer -> Cassandra; Spark 3.5.3 Structured Streaming (local[*], 1 s trigger, 0 s watermark) -> Cassandra.
Real-time rows: latency = time from the window end until its complete top-10 equals the expected result computed by the producer; `windows_exact` of `windows` checks exactness. Each scenario starts after both engines drained the previous one (a 5 events/s trickle closes open windows); `achieved_events_per_s` is the producer's rate.
Burst row: `achieved_events_per_s` = events / time from the first event until the engine's last window was complete (processing throughput, 0 = not finished within 5 min); `max_latency_s` = time from the end of production until the last window was complete (backlog drain time).
