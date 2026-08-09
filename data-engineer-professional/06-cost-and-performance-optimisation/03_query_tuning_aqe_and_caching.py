# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Query tuning: AQE, joins, and caching
# MAGIC
# MAGIC When file layout is already right, the remaining speed lives in the
# MAGIC **plan**: which join strategy, how many shuffle partitions, whether skew is
# MAGIC handled, and what's cached where. The skill the exam tests is reading a
# MAGIC plan and knowing which knob moves it. Requires section 01's `00_setup`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Broadcast vs shuffle joins — read it off the plan
# MAGIC Small dimension × big fact = **broadcast** the dimension: every executor
# MAGIC gets a copy, **no shuffle of the fact table at all**. Spark does this
# MAGIC automatically under `spark.sql.autoBroadcastJoinThreshold` (10MB default;
# MAGIC AQE can upgrade to broadcast at runtime using *actual* sizes).

# COMMAND ----------

orders = spark.table("silver_orders")  # noqa: F821
customers = spark.table("silver_customers").filter("__END_AT IS NULL")  # noqa: F821

print("=== hinted broadcast ===")
orders.join(F.broadcast(customers), "customer_id").explain()

# COMMAND ----------

# Look for "BroadcastHashJoin" above vs what a shuffle join would show:
# "SortMergeJoin" + an "Exchange hashpartitioning" on BOTH sides.

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - Broadcasting the **big** side is the classic wrong answer — it OOMs the
# MAGIC >   driver/executors. Broadcast the *small* side only.
# MAGIC > - A broadcast join eliminates the shuffle **of both sides**, which is why
# MAGIC >   it's the first lever for star-schema joins (section 10 territory).
# MAGIC > - SQL hint form: `SELECT /*+ BROADCAST(c) */ …`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## AQE — the runtime fixer
# MAGIC Adaptive Query Execution replans **between stages** using real statistics:
# MAGIC
# MAGIC | AQE feature | Fixes |
# MAGIC |---|---|
# MAGIC | coalesce shuffle partitions | 200 tiny reducers for 2MB of data |
# MAGIC | dynamic join strategy | "we guessed sort-merge, it's actually broadcast-sized" |
# MAGIC | **skew join handling** | one giant key making one task run forever |
# MAGIC
# MAGIC AQE is **on by default** — the exam answer for "a few straggler tasks after
# MAGIC a join on a skewed key" is AQE skew handling (it splits oversized
# MAGIC partitions), not salting-by-hand (the pre-AQE answer) and not more nodes.

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
        print(f"{conf} = (not set)")

# COMMAND ----------

# MAGIC %md
# MAGIC > `spark.sql.shuffle.partitions = auto` (Databricks) hands the decision to
# MAGIC > AQE entirely. Hand-tuning it to 200/2000 is the legacy answer; `auto` is
# MAGIC > the current one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caching — two different things share one word
# MAGIC
# MAGIC | | Spark cache (`df.cache()` / `CACHE TABLE`) | Disk cache (io cache) |
# MAGIC |---|---|---|
# MAGIC | Stores | computed **DataFrame results** in executor memory/disk | raw **Parquet data** on local NVMe |
# MAGIC | Scope | must be requested, per query plan | automatic on supported workers |
# MAGIC | Invalidation | **manual** — can serve stale data mid-notebook | automatic on data change |
# MAGIC | Exam guidance | use sparingly; unpersist when done | prefer it; it's free |
# MAGIC
# MAGIC > **Exam trap:** `df.cache()` is lazy — nothing is cached until an action
# MAGIC > runs; and caching a huge DataFrame evicts useful memory (a *slowdown*).
# MAGIC > "Just cache it" is usually the distractor, not the answer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where the time went — after the fact
# MAGIC - `EXPLAIN` / `explain(True)`: parsed → optimized → physical plan; look for
# MAGIC   `Exchange` (shuffle), `BroadcastHashJoin` vs `SortMergeJoin`, `PhotonScan`.
# MAGIC - **Spark UI → SQL/DataFrame tab**: per-operator rows, spill, skew (max vs
# MAGIC   median task duration on a stage).
# MAGIC - `Query Profile` (DBSQL): same idea for warehouse queries.
# MAGIC - Spill to disk in a stage = shuffle partitions too big → let AQE coalesce /
# MAGIC   raise parallelism / more memory per task.

# COMMAND ----------

# MAGIC %md
# MAGIC ## No cleanup needed — nothing was written
