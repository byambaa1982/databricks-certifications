# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Spark architecture: the vocabulary and the execution model
# MAGIC
# MAGIC A fifth of the exam is naming parts and predicting execution. Two
# MAGIC diagrams and one glossary cover it; the runnable cells prove the
# MAGIC execution facts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The parts
# MAGIC
# MAGIC ```
# MAGIC DRIVER (your program)                       CLUSTER MANAGER
# MAGIC ├─ SparkSession / SparkContext        <->   (standalone, YARN, k8s)
# MAGIC ├─ builds the DAG, schedules TASKS          allocates EXECUTORS
# MAGIC └─ collects results                              │
# MAGIC                                             EXECUTORS (JVMs on workers)
# MAGIC                                             ├─ run tasks on partitions
# MAGIC                                             ├─ hold cached blocks
# MAGIC                                             └─ report to the driver
# MAGIC ```
# MAGIC
# MAGIC | Term | Is |
# MAGIC |---|---|
# MAGIC | **job** | one action's worth of work |
# MAGIC | **stage** | a run of tasks between **shuffle boundaries** |
# MAGIC | **task** | one stage's work on **one partition** — the unit of parallelism |
# MAGIC | **slot/core** | where a task runs; executors × cores = max parallel tasks |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Number of **tasks in a stage = number of partitions** of that stage's
# MAGIC >   data. More cores than partitions → idle cores.
# MAGIC > - The **driver** plans and coordinates; executors compute. `collect()`
# MAGIC >   brings data to the *driver* — the OOM classic.
# MAGIC > - Fault tolerance = **lineage**: lost partitions are *recomputed* from
# MAGIC >   the DAG, not restored from replicas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lazy evaluation — transformations queue, actions execute

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

df = spark.range(1_000_000).withColumn("x", F.col("id") % 97)  # noqa: F821
filtered = df.filter("x = 3").select("id")   # nothing has run yet
print("no job has executed so far — transformations are lazy")
print(f"count() is an ACTION -> triggers a job: {filtered.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC | Transformations (lazy) | Actions (execute) |
# MAGIC |---|---|
# MAGIC | `select`, `filter`, `withColumn`, `join`, `groupBy().agg`, `orderBy`, `distinct`, `repartition` | `count`, `collect`, `show`, `take/first/head`, `write.*`, `foreach` |
# MAGIC
# MAGIC > **Exam trap:** `printSchema()` and `explain()` are **not** actions —
# MAGIC > they inspect the plan. And an error in a transformation often surfaces
# MAGIC > only at the next action (lazy blame-shifting — a question format).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Narrow vs wide — the shuffle line

# COMMAND ----------

print("narrow (no shuffle): filter/select ->")
df.filter("x = 3").explain()
print("wide (shuffle -> 'Exchange' below): groupBy ->")
df.groupBy("x").count().explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitions and caching — the knobs

# COMMAND ----------

print(f"partitions now: {df.rdd.getNumPartitions()}")
print(f"repartition(8):  {df.repartition(8).rdd.getNumPartitions()}   (full shuffle, can increase)")
print(f"coalesce(2):     {df.coalesce(2).rdd.getNumPartitions()}   (no shuffle, decrease only)")

cached = df.cache()      # lazy! materializes at the next action
cached.count()
print(f"after cache+count -> storageLevel: {cached.storageLevel}")
cached.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `cache()` = `persist(MEMORY_AND_DISK)` (Dataset default) and is
# MAGIC >   **lazy** — nothing is stored until an action runs.
# MAGIC > - `persist()` accepts storage levels (`MEMORY_ONLY`, `DISK_ONLY`,
# MAGIC >   `MEMORY_AND_DISK_SER`, …); `unpersist()` frees it.
# MAGIC > - `repartition(n)` always shuffles (up or down); `coalesce(n)` avoids
# MAGIC >   the shuffle but only reduces.
# MAGIC
# MAGIC Deep dive on the same machinery with Databricks nouns: DE Professional
# MAGIC [`06-cost-and-performance-optimisation`](../../data-engineer-professional/06-cost-and-performance-optimisation/).
