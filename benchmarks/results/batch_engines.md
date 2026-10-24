# Pandas vs Spark batch (end to end: read CSV, clean, 10 tables, write CSV)

| watch_rows | ratings_rows | engine | runs | median_s | min_s | max_s | peak_mem_mb | outputs_identical |
|---|---|---|---|---|---|---|---|---|
| 100000 | 50000 | pandas | 3 | 9.9 | 7.8 | 12.1 | 196 | True |
| 100000 | 50000 | spark | 3 | 54.9 | 52.9 | 55.2 | 2730 | True |
| 1000000 | 500000 | pandas | 3 | 15.3 | 15.2 | 17.7 | 592 | True |
| 1000000 | 500000 | spark | 3 | 58.5 | 56.5 | 59.2 | 3602 | True |
| 5000000 | 2500000 | pandas | 1 | 31.5 | 31.5 | 31.5 | 2192 | True |
| 5000000 | 2500000 | spark | 1 | 104.7 | 104.7 | 104.7 | 4393 | True |

Environment: Docker Desktop (WSL2) container, python:3.12 + Java 17, Spark local[*] with 4 GB driver memory (PySpark 3.5.3), Pandas 2.x; users=50,000, movies=20,000, ratings = watches / 2.
Spark time includes JVM start-up. `outputs_identical` compares the ten CSV tables of both engines.
Peak memory = highest summed resident set of the process tree (Python + JVM), sampled every 0.5 s from /proc.
