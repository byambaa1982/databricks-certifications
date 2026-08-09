# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Caches, clustering, and table history
# MAGIC
# MAGIC The performance features you *get* (Photon, two caches) and the two you
# MAGIC *use* (liquid clustering, `DESCRIBE HISTORY`). Runnable on the `da_assoc`
# MAGIC tables from section 04's setup.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "da_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The free speed: Photon and two caches
# MAGIC
# MAGIC | Feature | What it is | You do |
# MAGIC |---|---|---|
# MAGIC | **Photon** | vectorized C++ engine | nothing — always on in SQL warehouses |
# MAGIC | **Result cache** | the finished result of an identical query, served instantly | nothing — invalidated automatically when data changes |
# MAGIC | **Disk (IO) cache** | hot Parquet data on the warehouse's local SSDs | nothing — warms with use |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "The dashboard was instant the second time" → **result cache** (same
# MAGIC >   query text, unchanged data). It is *not* stale: data changes invalidate
# MAGIC >   it.
# MAGIC > - Caches don't survive a warehouse **restart** — the first Monday-morning
# MAGIC >   query pays the cold cache (and maybe startup). Serverless shrinks the
# MAGIC >   startup part.
# MAGIC > - Photon speeds SQL operators, not the network or a fan-out join —
# MAGIC >   "enable Photon" is a distractor when the query itself is wrong.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Liquid clustering — make filters cheap

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders CLUSTER BY (customer_id);
# MAGIC OPTIMIZE orders

# COMMAND ----------

# MAGIC %md
# MAGIC Queries filtering on `customer_id` now skip files that can't match
# MAGIC (min/max stats per file do the pruning — visible in the query profile's
# MAGIC "files pruned"). Analyst-level facts: `CLUSTER BY` replaces both
# MAGIC partitioning and ZORDER for new tables; keys can change later without a
# MAGIC rewrite; **predictive optimization** runs the `OPTIMIZE` maintenance for
# MAGIC you on managed tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `DESCRIBE HISTORY` — "did the data change?" answered with SQL
# MAGIC The other half of debugging a dashboard: the query didn't change — the
# MAGIC **table** did. Every write is a version:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, timestamp, operation,
# MAGIC        operationMetrics['numOutputRows'] AS rows_written
# MAGIC FROM (DESCRIBE HISTORY orders)
# MAGIC ORDER BY version DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- and time travel lets you diff "now" vs "then" when a number moved
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM orders)                AS rows_now,
# MAGIC   (SELECT count(*) FROM orders VERSION AS OF 0) AS rows_at_v0

# COMMAND ----------

# MAGIC %md
# MAGIC ## The "why is my dashboard slow/wrong" checklist (the section, compressed)
# MAGIC
# MAGIC 1. **Queued or ran?** history → queue time ⇒ warehouse scaling.
# MAGIC 2. **Where did execution go?** profile → scan-heavy (pruning ~0 ⇒ cluster
# MAGIC    the table), join fan-out (fix keys), spill (bigger warehouse).
# MAGIC 3. **Same query, different number?** `DESCRIBE HISTORY` — the table
# MAGIC    changed; time travel shows the before/after.
# MAGIC 4. **Repeated identical queries?** let the result cache work (don't
# MAGIC    add `now()`-style noise that defeats it) — or precompute with a
# MAGIC    **materialized view**.
# MAGIC
# MAGIC Deep dive on the engine-side levers: engineering
# MAGIC [`06-cost-and-performance-optimisation`](../../data-engineer-professional/06-cost-and-performance-optimisation/).
