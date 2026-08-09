# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — VACUUM: reclaiming storage without breaking time travel
# MAGIC
# MAGIC OPTIMIZE (notebook 01) left the old small files behind on purpose — they
# MAGIC back time travel. VACUUM is what finally deletes them, and the exam tests
# MAGIC the retention window, the safety check, and the blast radius. Requires
# MAGIC section 01's `00_setup`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create garbage: overwrite + compact a scratch table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE vacuum_demo AS SELECT * FROM silver_orders;
# MAGIC UPDATE vacuum_demo SET status = 'refunded' WHERE status = 'cancelled';
# MAGIC OPTIMIZE vacuum_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## DRY RUN — see what would be deleted, delete nothing

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM vacuum_demo RETAIN 0 HOURS DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC ## The safety check is itself exam material
# MAGIC Default retention is **7 days** (`delta.deletedFileRetentionDuration`).
# MAGIC Asking for less trips a guard, because a **currently running** reader or
# MAGIC writer on an older snapshot would break mid-query:

# COMMAND ----------

try:
    spark.sql("VACUUM vacuum_demo RETAIN 0 HOURS")  # noqa: F821
    print("vacuumed with 0-hour retention (check was already disabled in this workspace)")
except Exception as e:
    print(f"refused: {type(e).__name__}")
    print("-> requires spark.databricks.delta.retentionDurationCheck.enabled = false")

# COMMAND ----------

# The study-schema-only demonstration of the override (never do this casually in prod):
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")  # noqa: F821
display(spark.sql("VACUUM vacuum_demo RETAIN 0 HOURS"))  # noqa: F821
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel is now gone (history isn't)

# COMMAND ----------

try:
    spark.sql("SELECT count(*) FROM vacuum_demo VERSION AS OF 0").collect()  # noqa: F821
    print("UNEXPECTED: old version still readable")
except Exception as e:
    print(f"time travel to v0 now fails: {type(e).__name__}")
print(f"history still lists {spark.sql('DESCRIBE HISTORY vacuum_demo').count()} versions")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - VACUUM removes files **outside the retention window** that are no longer
# MAGIC >   referenced — it never touches data reachable by the current version.
# MAGIC > - `DESCRIBE HISTORY` still shows vacuumed versions (the **log** has its
# MAGIC >   own retention, default 30 days) — visible ≠ restorable.
# MAGIC > - VACUUM is **not automatic** on classic tables; predictive optimization
# MAGIC >   (below) is what automates it on managed UC tables.
# MAGIC > - Vacuuming also breaks **CDF reads and streaming sources** that point at
# MAGIC >   removed versions.
# MAGIC > - Cost framing: storage cost ← VACUUM cadence; compute cost ← everything
# MAGIC >   else in this section. Know which lever a question is pulling.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Predictive optimization — stop scheduling maintenance by hand
# MAGIC Enabled at account/catalog/schema level, Databricks decides **when** to run
# MAGIC `OPTIMIZE`, clustering, `VACUUM`, and stats collection on managed UC tables,
# MAGIC using serverless compute, prioritized by observed benefit. The exam phrase
# MAGIC to match: *"maintenance without dedicated jobs or clusters"*. Your OPTIMIZE
# MAGIC / VACUUM jobs remain the answer for external tables and custom cadences.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS vacuum_demo
