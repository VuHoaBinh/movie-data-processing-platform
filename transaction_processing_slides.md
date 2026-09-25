# Transaction Processing — Movie Streaming Platform

## 1. Definition
A transaction is one logical unit of work. Transaction processing records operational actions reliably: registering, adding a watchlist item, or submitting a rating.

## 2. Processing model
`Viewer → API → validate → database transaction → COMMIT or ROLLBACK → response/event`.

## 3. ACID
- Atomicity: all rating changes succeed or none do.
- Consistency: ratings stay within 1–5 and foreign keys exist.
- Isolation: simultaneous updates do not corrupt each other.
- Durability: committed data survives a restart.

## 4. Technologies
| Technology | Type | Movie-platform use |
|---|---|---|
| PostgreSQL | relational, ACID | accounts and ratings |
| MySQL | relational, ACID | movie catalogue and watchlists |
| MongoDB | document database | flexible profiles/activity documents |

## 5. Strengths and limits
Correctness, concurrency control, low-latency writes. It is not ideal for large historical aggregation or live window analytics; use Spark/Flink downstream.

## 6. Project example and use
U0001 rates M0001: API validates IDs/score, writes and commits in PostgreSQL, then emits an event. Flink updates live metrics; Spark recomputes daily recommendations. Use transactions for user-facing operational reads/writes requiring consistency.
