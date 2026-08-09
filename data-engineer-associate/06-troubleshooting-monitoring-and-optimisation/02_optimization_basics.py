# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Optimization: files, clustering, and the free stuff
# MAGIC
# MAGIC Associate-level optimization is four levers, and the first one — file
# MAGIC layout — we run live: manufacture a small-file problem, measure it,
# MAGIC `OPTIMIZE` it away. Self-contained (builds its own scratch table).

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lever 1: file layout — the small-file problem, live

# COMMAND ----------

(
    spark.range(0, 100_000)  # noqa: F821
    .withColumn("customer_id", (F.col("id") % 500).cast("int"))
    .withColumn("amount", F.round(F.col("id") % 1000 / 3, 2))
    .repartition(60)  # 60 writers -> 60 tiny files
    .write.mode("overwrite").saveAsTable("opt_demo")
)
detail = spark.sql("DESCRIBE DETAIL opt_demo").collect()[0]  # noqa: F821
print(f"before: {detail.numFiles} files, ~{detail.sizeInBytes // max(detail.numFiles, 1)} bytes each")

# COMMAND ----------

result = spark.sql("OPTIMIZE opt_demo").collect()[0]  # noqa: F821
print(f"OPTIMIZE removed {result.metrics.numFilesRemoved}, wrote {result.metrics.numFilesAdded}")
detail = spark.sql("DESCRIBE DETAIL opt_demo").collect()[0]  # noqa: F821
print(f"after:  {detail.numFiles} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lever 2: liquid clustering — co-locate what you filter on

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE opt_demo CLUSTER BY (customer_id);
# MAGIC OPTIMIZE opt_demo

# COMMAND ----------

# MAGIC %md
# MAGIC | | Hive-style partitioning | **Liquid clustering** (`CLUSTER BY`) |
# MAGIC |---|---|---|
# MAGIC | Layout | directory per value | co-located data, no directories |
# MAGIC | Cardinality | must be low | any (skew is fine) |
# MAGIC | Change later | full rewrite | `ALTER TABLE … CLUSTER BY` |
# MAGIC | Verdict | huge tables with a natural date key, legacy | **default for new tables** |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Partitioning by a **high-cardinality** column (user_id) = millions of
# MAGIC >   tiny files — the classic wrong answer.
# MAGIC > - `OPTIMIZE` compacts and applies clustering, but old files stay for time
# MAGIC >   travel until **`VACUUM`** — OPTIMIZE speeds reads, VACUUM reclaims
# MAGIC >   storage. Different jobs.
# MAGIC > - **Predictive optimization** runs OPTIMIZE/VACUUM/stats for you on
# MAGIC >   managed UC tables — the "maintenance without jobs" answer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Levers 3 & 4: the free stuff and the compute
# MAGIC - **AQE** (on by default): coalesces shuffle partitions, fixes join
# MAGIC   strategy at runtime, splits skewed partitions — "a few straggler tasks
# MAGIC   after a join" is AQE's job, not manual salting.
# MAGIC - **Photon**: vectorized engine; big wins for SQL/Delta scans, joins,
# MAGIC   aggregations (not Python UDF loops).
# MAGIC - **Disk cache**: hot Parquet cached on NVMe automatically; distinct from
# MAGIC   `df.cache()` (manual, memory, stale-able — usually the distractor).
# MAGIC - **Broadcast joins**: small dim × big fact = broadcast the *small* side;
# MAGIC   AQE usually does it for you under the threshold.
# MAGIC - Right-size compute: job compute for jobs, serverless for spiky, Photon
# MAGIC   when CPU-bound — the cost half of section 01's money table.

# COMMAND ----------

# check the free stuff is on
for conf in ["spark.sql.adaptive.enabled", "spark.databricks.io.cache.enabled"]:
    try:
        print(f"{conf} = {spark.conf.get(conf)}")  # noqa: F821
    except Exception:
        print(f"{conf} = (not set on this compute)")

# COMMAND ----------

# MAGIC %md
# MAGIC Deep dive (ZORDER, VACUUM mechanics, AQE internals, cost tables):
# MAGIC Professional
# MAGIC [`06-cost-and-performance-optimisation`](../../data-engineer-professional/06-cost-and-performance-optimisation/).
# MAGIC
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS opt_demo
