# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Debugging: reading what Spark is telling you
# MAGIC
# MAGIC Debugging questions test whether you know **which failure happens where**
# MAGIC (driver vs executor, analysis vs runtime) and **which page of the Spark UI
# MAGIC answers which symptom**. Requires section 01's `00_setup`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failure class #1: analysis time — before anything runs
# MAGIC Wrong column, missing table, type mismatch in DDL: caught while **planning**,
# MAGIC on the driver, instantly. No cluster work happened.

# COMMAND ----------

try:
    spark.table("silver_orders").select("amuont").collect()  # noqa: F821
except Exception as e:
    msg = str(e).splitlines()[0][:120]
    print(f"{type(e).__name__}: {msg}")
    print("-> instant, driver-side, and usually includes 'Did you mean …' — fix the query")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failure class #2: runtime — on the executors, possibly late
# MAGIC Bad data hitting a UDF, OOM, a file deleted mid-read: these fail **when the
# MAGIC stage runs**, and the Python-level error is wrapped — the real cause is in
# MAGIC the stack trace's `Caused by:` chain (read it bottom-up) or the failed
# MAGIC task's **executor** log, not the driver log.

# COMMAND ----------

@F.udf("int")
def brittle(s):
    return int(s)  # blows up on non-numeric input at RUNTIME


try:
    spark.table("silver_orders").select(brittle(F.col("status"))).collect()  # noqa: F821
except Exception as e:
    print(f"{type(e).__name__} — driver shows the wrapper; the ValueError is down in 'Caused by:'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to look — symptom → page
# MAGIC
# MAGIC | Symptom | Look at |
# MAGIC |---|---|
# MAGIC | job "hangs" | Spark UI → Stages: is one task left running? (skew) or zero running? (waiting on resources) |
# MAGIC | one task far slower than the rest | Stages → task summary: **max ≫ median duration/input = skew** → AQE skew join (section 06) |
# MAGIC | tasks dying with memory errors | executor logs; usual causes below |
# MAGIC | driver dies | `collect()` / `toPandas()` on something huge, or broadcasting a big table |
# MAGIC | "works on sample, OOMs on full data" | same two, at scale |
# MAGIC | slow but not failing | SQL tab → operator times: scan-bound (layout, section 06) vs shuffle-bound (joins) vs spill |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - `collect()` pulls the whole result **to the driver** — the canonical
# MAGIC >   driver-OOM. Answer with `display()`/`limit()`/writes instead.
# MAGIC > - Executor OOM ≠ driver OOM: executor-side is usually skew, huge shuffle
# MAGIC >   partitions, or an exploding join — more driver memory fixes nothing.
# MAGIC > - Lazy evaluation moves the blame: the failing **action** reports an error
# MAGIC >   caused by an earlier transformation. The stack trace names the real op.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming-specific failure modes (recap from sections 01–02, exam-favorite)
# MAGIC
# MAGIC | Failure | Cause / fix |
# MAGIC |---|---|
# MAGIC | `UnknownFieldException` from Auto Loader | new columns arrived; `addNewColumns` stops on purpose → restart picks up schema |
# MAGIC | stream fails after query logic change | aggregation/state changed → checkpoint incompatible → new checkpoint (reprocesses!) |
# MAGIC | "checkpoint doesn't match sink" | someone deleted/recreated the target or reused a checkpoint across queries |
# MAGIC | duplicates after checkpoint deletion | file-discovery state lost → full re-ingest into append sink |
# MAGIC | state growing forever | dedup/aggregation without watermark (section 03) |
# MAGIC
# MAGIC ## Cleanup — nothing was written
