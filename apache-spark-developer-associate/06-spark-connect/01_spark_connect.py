# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Spark Connect: the client-server split
# MAGIC
# MAGIC A 5% section with one idea: since 3.4, Spark's DataFrame API can run as a
# MAGIC **thin client** speaking gRPC to a remote Spark server — no JVM, no
# MAGIC cluster libraries on the client.
# MAGIC
# MAGIC ```
# MAGIC your app (thin client: pyspark-connect, any IDE/process)
# MAGIC     │  unresolved logical plan over gRPC
# MAGIC     ▼
# MAGIC Spark Connect SERVER (the real driver: analyzes, optimizes, executes)
# MAGIC     ▼
# MAGIC executors … results stream back as Arrow batches
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## The API shape
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark = (SparkSession.builder
# MAGIC          .remote("sc://spark-server:15002")     # <- the whole difference
# MAGIC          .getOrCreate())
# MAGIC
# MAGIC df = spark.range(100).filter("id % 2 = 0")     # identical DataFrame API
# MAGIC df.count()                                      # executes on the SERVER
# MAGIC ```
# MAGIC
# MAGIC (Databricks Connect is Spark Connect pointed at a Databricks cluster —
# MAGIC same architecture, `sc://` swapped for workspace auth.)

# COMMAND ----------

# check what this session is
mode = spark.conf.get("spark.api.mode", "classic")  # noqa: F821
is_connect = "connect" in str(type(spark)).lower()  # noqa: F821
print(f"session type: {type(spark).__name__}, connect: {is_connect or mode}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## What changes, what doesn't
# MAGIC
# MAGIC | Works identically | Different / unavailable over Connect |
# MAGIC |---|---|
# MAGIC | DataFrame & Spark SQL APIs | **`sparkContext` / RDD API** — not exposed |
# MAGIC | UDFs, ML on DataFrames | `spark.sparkContext.setLogLevel`, SparkContext-based libs |
# MAGIC | streaming (3.5+) | some low-level listeners/conf APIs |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Code calls `spark.sparkContext...` and fails on Connect" — the RDD /
# MAGIC >   SparkContext surface is the canonical incompatibility.
# MAGIC > - Why it exists: **decoupled client** (any language/process, thin
# MAGIC >   deps), **isolation** (client crashes don't kill the driver),
# MAGIC >   independent upgrades, remote IDE/debugging.
# MAGIC > - The client sends **unresolved plans**, not code or data — the server
# MAGIC >   plans and executes; results return as Arrow.
