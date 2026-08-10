# 06 — Using Spark Connect (5%)

One idea: the DataFrame API as a thin gRPC client against a remote Spark
server — `SparkSession.builder.remote("sc://...")`, unresolved plans out,
Arrow results back, and the RDD/SparkContext surface as the canonical
incompatibility.

| Notebook | What you practice |
|---|---|
| [`01_spark_connect.py`](01_spark_connect.py) | The architecture diagram, the `.remote()` shape, works/doesn't table, why-it-exists list |

**Study:** if an option touches `sparkContext` or RDDs over Connect, it's
the planted failure. 5% ≈ 2 of 45 questions on
[TestHub](https://testlogichub.com).
