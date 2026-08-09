# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Loading patterns: full reload vs incremental
# MAGIC
# MAGIC The section's core question, asked a dozen ways: **does this loader
# MAGIC remember what it already loaded?** `spark.read` doesn't; `COPY INTO` and
# MAGIC Auto Loader (notebook 02) do. Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## The three patterns
# MAGIC
# MAGIC | Pattern | Tool | Remembers files? | Re-run behavior |
# MAGIC |---|---|---|---|
# MAGIC | **Full reload** | `spark.read` + CTAS | no | re-reads everything, rebuilds |
# MAGIC | **Incremental batch** | `COPY INTO` | yes — in the target's Delta log | loads only new files |
# MAGIC | **Incremental / continuous** | Auto Loader (`cloudFiles`) | yes — in the checkpoint | processes only new files |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 1: full reload — fine for small reference data

# COMMAND ----------

df = (
    spark.read  # noqa: F821
    .option("header", "true").option("inferSchema", "true")
    .csv(f"{LANDING}/customers_csv")
)
df.write.mode("overwrite").saveAsTable("bronze_orders_full")
print(f"loaded {df.count()} rows — and EVERY run re-reads and rewrites all of it")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps for plain reads:**
# MAGIC > - `inferSchema` / JSON inference cost an **extra scan** — production
# MAGIC >   provides explicit schemas.
# MAGIC > - Re-running CTAS **rebuilds** (fine); re-running `INSERT INTO … SELECT`
# MAGIC >   over the same files **duplicates** (the trap).
# MAGIC > - When a question says *"only new files"* or *"idempotent"*, plain
# MAGIC >   `spark.read` is the wrong answer, always.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 2: `COPY INTO` — run this cell, then run it AGAIN
# MAGIC The target can start as an **empty, schemaless shell**; `mergeSchema` lets
# MAGIC the first load define the columns. Second run: **0 rows** — the files are
# MAGIC in the load history.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS orders_copy_into

# COMMAND ----------

copy_cmd = f"""
    COPY INTO orders_copy_into
    FROM '{LANDING}/orders'
    FILEFORMAT = JSON
    FORMAT_OPTIONS ('inferSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')
"""
display(spark.sql(copy_cmd))  # noqa: F821

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS rows_in_table FROM orders_copy_into

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prove the incremental part
# MAGIC Run `00_setup` with `mode = more`, then re-run the `COPY INTO` cell:
# MAGIC only **batch 2** loads (4 rows), and `mergeSchema` adds its new `channel`
# MAGIC column to the table.
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - The load history lives **with the target table** — drop and recreate the
# MAGIC >   table and everything reloads next run.
# MAGIC > - `COPY_OPTIONS ('force' = 'true')` ignores the history on purpose (and
# MAGIC >   duplicates rows in an append target).
# MAGIC > - `COPY INTO` is **batch SQL** — it runs when you (or a job) run it.
# MAGIC >   "Continuous" or "millions of files" → Auto Loader (notebook 02).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where the other sources fit (know these by name)
# MAGIC
# MAGIC | Source | Tool |
# MAGIC |---|---|
# MAGIC | files in cloud storage | Auto Loader / `COPY INTO` / `read_files()` |
# MAGIC | SaaS apps & databases (Salesforce, SQL Server, …) | **Lakeflow Connect** managed connectors |
# MAGIC | external DB, query-in-place | Lakehouse Federation |
# MAGIC | external DB, one-off pull in code | `spark.read.jdbc` (last resort — no UC governance) |
# MAGIC | message bus | Structured Streaming Kafka/Kinesis sources |
# MAGIC | REST API | call it in Python, land JSON in a **volume**, then ingest normally |
# MAGIC
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_orders_full;
# MAGIC DROP TABLE IF EXISTS orders_copy_into
