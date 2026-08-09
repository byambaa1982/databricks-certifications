# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — File layout: small files, OPTIMIZE, and clustering
# MAGIC
# MAGIC Most "query is slow" answers on the exam are really **file layout**
# MAGIC answers: too many small files, no co-location for the filter column, or
# MAGIC partitioning used where it hurts. We create the small-file problem on
# MAGIC purpose, then fix it three ways. Requires section 01's `00_setup`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manufacture the small-file problem

# COMMAND ----------

(
    spark.table("silver_orders")  # noqa: F821
    .repartition(50)  # 50 writers -> ~50 tiny files
    .write.mode("overwrite").saveAsTable("orders_smallfiles")
)

detail = spark.sql("DESCRIBE DETAIL orders_smallfiles").collect()[0]  # noqa: F821
print(f"files: {detail.numFiles}, total: {detail.sizeInBytes} bytes "
      f"(~{detail.sizeInBytes // max(detail.numFiles, 1)} bytes/file)")

# COMMAND ----------

# MAGIC %md
# MAGIC Why it hurts: every file is an open/close + a task + a transaction-log
# MAGIC entry; scan cost becomes dominated by **per-file overhead**, and data
# MAGIC skipping stats get too granular to help. Classic causes: over-partitioned
# MAGIC writes, per-minute streaming micro-batches, MERGE churn.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix #1: OPTIMIZE (bin-packing compaction)

# COMMAND ----------

result = spark.sql("OPTIMIZE orders_smallfiles").collect()[0]  # noqa: F821
m = result.metrics
print(f"removed {m.numFilesRemoved} files, wrote {m.numFilesAdded}")

detail = spark.sql("DESCRIBE DETAIL orders_smallfiles").collect()[0]  # noqa: F821
print(f"files now: {detail.numFiles}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `OPTIMIZE` **rewrites** data into ~1GB target files but old files stay
# MAGIC >   on storage (for time travel) until **VACUUM** (notebook 02) — OPTIMIZE
# MAGIC >   alone does not reclaim space.
# MAGIC > - It's idempotent-ish: a second run finds little to do.
# MAGIC > - **Auto compaction** + **optimized writes**
# MAGIC >   (`delta.autoOptimize.autoCompact` / `.optimizeWrite`) prevent the
# MAGIC >   problem at write time — the standing answer for streaming sinks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix #2: co-locate what you filter on — ZORDER (legacy) …
# MAGIC `ZORDER BY (col)` sorts data multi-dimensionally inside files so **data
# MAGIC skipping** (min/max stats per file) can prune aggressively for filters on
# MAGIC those columns.

# COMMAND ----------

display(spark.sql("OPTIMIZE orders_smallfiles ZORDER BY (customer_id)"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## … and Fix #3: liquid clustering (the current answer)
# MAGIC `CLUSTER BY` replaces both partitioning **and** ZORDER for new tables:
# MAGIC incremental (no full rewrite per OPTIMIZE), skew-tolerant, and the
# MAGIC clustering keys can be **changed later** without rewriting the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_clustered
# MAGIC CLUSTER BY (customer_id)
# MAGIC AS SELECT * FROM silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- clustering happens on write and on OPTIMIZE; keys are changeable in place
# MAGIC OPTIMIZE orders_clustered;
# MAGIC ALTER TABLE orders_clustered CLUSTER BY (customer_id, order_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partitioning — know when NOT to
# MAGIC
# MAGIC | | Hive-style `PARTITIONED BY` | Liquid `CLUSTER BY` |
# MAGIC |---|---|---|
# MAGIC | Layout | one **directory per value** | data co-located, no directories |
# MAGIC | Cardinality | must be LOW (date, region) | any, and skew is fine |
# MAGIC | Change layout later | full rewrite | `ALTER TABLE … CLUSTER BY` |
# MAGIC | Small-file risk | high (per-partition files) | low |
# MAGIC | Guidance | only for **very large** tables (TB+) with a natural low-cardinality key, or hard isolation needs | default for new tables |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Partitioning by a **high-cardinality** column (user_id, order_id) is the
# MAGIC >   canonical wrong answer — millions of directories of tiny files.
# MAGIC > - ZORDER and `CLUSTER BY` are **mutually exclusive** with each other and
# MAGIC >   ZORDER can't use partition columns.
# MAGIC > - Data skipping stats are collected on the **first 32 columns** by default
# MAGIC >   (`delta.dataSkippingNumIndexedCols`) — put filterable columns early.
# MAGIC > - **Deletion vectors** make DELETE/UPDATE/MERGE mark rows instead of
# MAGIC >   rewriting whole files (faster writes, reads reconcile); `REORG TABLE …
# MAGIC >   APPLY (PURGE)` materializes them away.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_smallfiles;
# MAGIC DROP TABLE IF EXISTS orders_clustered
