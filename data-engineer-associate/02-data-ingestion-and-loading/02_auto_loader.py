# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Auto Loader: schema enforcement and evolution
# MAGIC
# MAGIC The exam's default answer for *"incrementally ingest files as they
# MAGIC arrive"*. Two locations do all the work — `cloudFiles.schemaLocation`
# MAGIC (schema versions) and `checkpointLocation` (which files are done) — and
# MAGIC the schema-evolution behavior is the most-tested part. Run `00_setup`
# MAGIC first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/assoc_autoloader"

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## The ingest cell — you'll run it several times

# COMMAND ----------

try:
    q = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CKPT}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{LANDING}/orders")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", CKPT)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable("orders_autoloader")
    )
    q.awaitTermination()
    print("stream drained cleanly")
except Exception as e:
    print(f"stream stopped: {type(e).__name__}")
    print("If this is the run right after new columns arrived, that's the "
          "DESIGNED behavior (see below) — just re-run this cell.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS rows_ingested FROM orders_autoloader

# COMMAND ----------

# MAGIC %md
# MAGIC ## What just happened, and what happens on re-runs
# MAGIC - **Re-run now** → 0 new rows: the checkpoint remembers processed files.
# MAGIC - **Delete the checkpoint** → full re-ingest (duplicates in an append
# MAGIC   table) — the checkpoint IS the exactly-once guarantee.
# MAGIC - **Schema enforcement**: rows must fit the current schema; anything that
# MAGIC   doesn't fit a column's type lands in `_rescued_data` instead of being
# MAGIC   dropped or nulled.
# MAGIC - **Schema evolution**: run `00_setup` with `mode = more` (adds a `channel`
# MAGIC   column), re-run the cell — it **fails once on purpose**
# MAGIC   (`UnknownFieldException`), writes the new schema version, and the next
# MAGIC   run ingests with the new column. In a scheduled job, the retry does this
# MAGIC   for you.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- after batch 2: order 5010's amount "N/A" is preserved here, not lost
# MAGIC SELECT order_id, amount, _rescued_data, _source_file
# MAGIC FROM orders_autoloader
# MAGIC WHERE _rescued_data IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evolution modes (`cloudFiles.schemaEvolutionMode`)
# MAGIC
# MAGIC | Mode | On new columns |
# MAGIC |---|---|
# MAGIC | `addNewColumns` (default with inference) | fail once, evolve, ingest on restart |
# MAGIC | `rescue` | schema frozen; new columns ride along in `_rescued_data` |
# MAGIC | `failOnNewColumns` | fail every time until you change the schema yourself |
# MAGIC | `none` | new columns silently ignored |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - `schemaLocation` is **required** when inferring (it's where schema
# MAGIC >   versions live). `schemaHints` overrides inference for columns you know
# MAGIC >   better (`"amount DECIMAL(10,2)"`).
# MAGIC > - File discovery is **directory listing** by default;
# MAGIC >   `cloudFiles.useNotifications = true` switches to event-driven for huge
# MAGIC >   directories.
# MAGIC > - Auto Loader **is** Structured Streaming — `trigger(availableNow=True)`
# MAGIC >   turns it into an incremental batch job (run-and-stop), which is how
# MAGIC >   most scheduled pipelines use it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The declarative form — how pipelines write it
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders
# MAGIC AS SELECT *, _metadata.file_path AS _source_file
# MAGIC FROM STREAM read_files('/Volumes/dev_catalog/de_assoc/landing/orders',
# MAGIC                        format => 'json')
# MAGIC ```
# MAGIC
# MAGIC Same engine (Auto Loader + checkpoints), but Lakeflow manages the restarts
# MAGIC and retries. This syntax — `STREAMING TABLE` + `STREAM read_files` — is
# MAGIC exam-recognition material.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_autoloader

# COMMAND ----------

dbutils.fs.rm(CKPT, recurse=True)  # noqa: F821
print("checkpoint removed — the ingest cell starts fresh next time")
