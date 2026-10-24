# Pandas vs Spark batch (end to end: read CSV, clean, 10 tables, write CSV)

| watch_rows | ratings_rows | engine | runs | median_s | min_s | max_s | peak_mem_mb | outputs_identical |
|---|---|---|---|---|---|---|---|---|
| 100000 | 50000 | pandas | 3 | 12.2 | 11.5 | 13.2 | 193 | False |
| 100000 | 50000 | spark | 3 | 65.1 | 62.3 | 68.2 | 109 | False |
| 1000000 | 500000 | pandas | 3 | 17.7 | 17.4 | 18.2 | 593 | True |
| 1000000 | 500000 | spark | 3 | 89.0 | 79.5 | 94.0 | 137 | True |
| 5000000 | 2500000 | pandas | 1 | 36.7 | 36.7 | 36.7 | 2251 | True |
| 5000000 | 2500000 | spark | 1 | 135.3 | 135.3 | 135.3 | 138 | True |

Environment: Docker Desktop (WSL2) container, python:3.12 + Java 17, Spark local[*] with 4 GB driver memory (PySpark 3.5.3), Pandas 2.x; users=50,000, movies=20,000, ratings = watches / 2.
Spark time includes JVM start-up. `outputs_identical` compares the ten CSV tables of both engines.
Peak memory = largest resident set among the process and its children (JVM for Spark).
