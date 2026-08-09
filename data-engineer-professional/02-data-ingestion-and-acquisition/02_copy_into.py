# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — COPY INTO: idempotent batch ingestion
# MAGIC
# MAGIC `COPY INTO` is `spark.read` with a memory: it records **which files it has
# MAGIC already loaded** in the target table's transaction log, so re-running it is
# MAGIC safe — new files load, old files are skipped. That one property (idempotent
# MAGIC retries) is what most of its exam questions test.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The target can start as an empty, schemaless shell
# MAGIC `COPY INTO` needs an existing table, but with `mergeSchema` the table can be
# MAGIC created **without columns** and let the first load define them.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS orders_copy_into

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load — then run this cell again
# MAGIC First run: batch 1's rows. **Second run: `num_inserted_rows = 0`** — the
# MAGIC files are already in the load history, nothing happens. That's the whole
# MAGIC point: schedule it, retry it, re-run it after a failure — no duplicates.

# COMMAND ----------

copy_cmd = f"""
    COPY INTO orders_copy_into
    FROM '{LANDING}/orders'
    FILEFORMAT = JSON
    PATTERN = '*.json'
    FORMAT_OPTIONS ('inferSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')
"""
display(spark.sql(copy_cmd))  # noqa: F821

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS rows_in_table FROM orders_copy_into

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now give it something new
# MAGIC Run `00_setup` with `mode = more`, then re-run the `COPY INTO` cell above.
# MAGIC Only **batch 2** loads (4 rows), and two things happen on the way in:
# MAGIC
# MAGIC 1. The new `channel` column appears on the table — that's
# MAGIC    `COPY_OPTIONS ('mergeSchema' = 'true')` evolving the schema.
# MAGIC 2. Order 5010's `"amount": "N/A"` becomes **NULL** — `COPY INTO` reads
# MAGIC    permissively by default. Compare with Auto Loader (notebook 03), which
# MAGIC    preserves the bad value in `_rescued_data` instead of silently nulling it.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * shows whatever columns exist right now: after the batch-2 load,
# MAGIC -- channel appears (populated for new rows, NULL for batch 1) and order 5010's
# MAGIC -- amount is NULL.
# MAGIC SELECT * FROM orders_copy_into ORDER BY order_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## FORCE — the escape hatch that breaks idempotency on purpose
# MAGIC `COPY_OPTIONS ('force' = 'true')` ignores the load history and re-loads
# MAGIC everything that matches — useful after a bad backfill, but it **will**
# MAGIC duplicate rows in an append-only target.
# MAGIC
# MAGIC | Option | Where | Effect |
# MAGIC |---|---|---|
# MAGIC | `PATTERN = '*.json'` | after FROM | glob filter on file names |
# MAGIC | `FORMAT_OPTIONS(...)` | reader | per-format read options (`inferSchema`, `header`, …) |
# MAGIC | `COPY_OPTIONS('mergeSchema')` | writer | let new columns evolve the target |
# MAGIC | `COPY_OPTIONS('force')` | writer | reload already-loaded files (duplicates!) |
# MAGIC
# MAGIC > **Exam trap:** the load history lives **with the target table**. Drop and
# MAGIC > recreate the table and `COPY INTO` forgets everything — the next run
# MAGIC > reloads all files. A **modified** file (same name, new content) is also
# MAGIC > reloaded, because tracking is by path + version, not name alone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## COPY INTO vs Auto Loader — the decision the exam wants you to make
# MAGIC
# MAGIC | | `COPY INTO` | Auto Loader |
# MAGIC |---|---|---|
# MAGIC | Execution | batch SQL, runs when you run it | Structured Streaming source |
# MAGIC | File tracking | target table's Delta log | RocksDB in the checkpoint |
# MAGIC | Sweet spot | thousands of files, scheduled loads | **millions of files**, continuous or frequent |
# MAGIC | Schema evolution | `mergeSchema` copy option | `addNewColumns` modes + `_rescued_data` |
# MAGIC | Notification mode | no — always lists the directory | optional `cloudFiles.useNotifications` |
# MAGIC
# MAGIC Heuristic: *"SQL-first, scheduled, moderate file counts"* → `COPY INTO`;
# MAGIC *"continuous / huge directories / exactly-once with checkpoints"* → Auto
# MAGIC Loader. Both are idempotent by design — a plain `spark.read` + `INSERT` is
# MAGIC the wrong answer whenever either is offered.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_copy_into
