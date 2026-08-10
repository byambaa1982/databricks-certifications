# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Troubleshooting and tuning, OSS edition
# MAGIC
# MAGIC The DE tracks covered this with Databricks nouns; the Spark exam asks it
# MAGIC with OSS nouns — configs, `explain()`, and the Spark UI. Runnable
# MAGIC everywhere.

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## The configs the exam names

# COMMAND ----------

for conf in [
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.shuffle.partitions",
]:
    try:
        print(f"{conf} = {spark.conf.get(conf)}")  # noqa: F821
    except Exception:
        print(f"{conf} = (unset)")

# COMMAND ----------

# MAGIC %md
# MAGIC | Config | Controls | Exam angle |
# MAGIC |---|---|---|
# MAGIC | `spark.sql.shuffle.partitions` | reducers after wide ops (default 200) | tiny data + 200 partitions = overhead; AQE coalescing fixes it at runtime |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | auto-broadcast cutoff (10MB) | `-1` disables broadcasting entirely |
# MAGIC | `spark.sql.adaptive.*` | AQE: coalesce, skew-split, join-strategy switch | on by default in 3.5; the answer to stragglers-after-join |
# MAGIC | `spark.sql.files.maxPartitionBytes` | input split size (128MB) | controls read parallelism |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading plans — broadcast vs sort-merge

# COMMAND ----------

big = spark.range(1_000_000).withColumn("k", F.col("id") % 1000)  # noqa: F821
small = spark.range(1000).withColumnRenamed("id", "k").withColumn("label", F.lit("x"))  # noqa: F821

print("=== hint forces BroadcastHashJoin ===")
big.join(F.broadcast(small), "k").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC In the plan: `BroadcastHashJoin` + `BroadcastExchange` = small side
# MAGIC shipped to every executor, no shuffle of the big side. `SortMergeJoin`
# MAGIC with `Exchange hashpartitioning` on **both** sides = full shuffle. SQL
# MAGIC hint form: `SELECT /*+ BROADCAST(s) */ …`.
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Broadcast the **small** side only — broadcasting big data OOMs
# MAGIC >   executors (and the driver, which assembles the broadcast).
# MAGIC > - `explain(True)` shows parsed → analyzed → optimized → physical;
# MAGIC >   the physical plan is where join strategy and `Exchange` appear.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Symptom → cause — the OSS version
# MAGIC
# MAGIC | Symptom | Likely cause → fix |
# MAGIC |---|---|
# MAGIC | one task far slower than siblings | **skew** → AQE skew join, salt keys (legacy), broadcast the small side |
# MAGIC | executor OOM | huge shuffle partitions / skew / too few partitions → raise parallelism, AQE |
# MAGIC | driver OOM | `collect()`/`toPandas()` on big data, giant broadcast → don't |
# MAGIC | thousands of tiny tasks | over-partitioned → `coalesce`, AQE coalescing |
# MAGIC | spill to disk in a stage | memory pressure per task → more partitions or more memory per executor |
# MAGIC | "Task not serializable" / pickling errors | closures capturing non-serializable objects (connections, loggers) → create them **inside** the function/partition |
# MAGIC | slow job, plan shows repeated computation | reused branch → `cache()` it (then `unpersist`) |
# MAGIC
# MAGIC Spark UI navigation: **Jobs → Stages** (task-time distribution: max ≫
# MAGIC median = skew), **SQL/DataFrame** tab (per-operator rows/time/spill),
# MAGIC **Storage** (what's cached), **Executors** (GC time, memory).
# MAGIC
# MAGIC Databricks-flavored deep dive:
# MAGIC [`06-cost-and-performance-optimisation`](../../data-engineer-professional/06-cost-and-performance-optimisation/)
# MAGIC and [`09-debugging-and-deploying`](../../data-engineer-professional/09-debugging-and-deploying/).
