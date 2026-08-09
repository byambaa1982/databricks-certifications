# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Auto Loader: incremental ingestion at scale
# MAGIC
# MAGIC `cloudFiles` is the exam's default right answer for *"continuously ingest
# MAGIC files from cloud storage"*. It's a Structured Streaming source, so
# MAGIC everything from section 01's streaming notebook applies — plus its own
# MAGIC machinery: schema inference with versions, schema **evolution** that stops
# MAGIC the stream on purpose, and `_rescued_data`. Uses `trigger(availableNow=True)`
# MAGIC throughout, so every cell drains what's available and stops.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/ingest_autoloader"

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## The ingest cell — you will run this one several times
# MAGIC Two required things every exam question checks:
# MAGIC - `cloudFiles.schemaLocation` — where inferred schema **versions** are stored
# MAGIC - `checkpointLocation` — where file-discovery state (RocksDB) + offsets live
# MAGIC
# MAGIC `schemaHints` pins the columns you know better than inference does — here
# MAGIC `amount` must be DECIMAL, not the DOUBLE inference would pick.

# COMMAND ----------

try:
    q = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CKPT}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "amount DECIMAL(10,2), order_ts TIMESTAMP")
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
    print("If this is the run right after new columns arrived — that's the "
          "DESIGNED behavior (see below). Just re-run this cell.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS rows_ingested FROM orders_autoloader

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-run the ingest cell: zero new rows
# MAGIC Same as every stream: the checkpoint remembers which files were processed.
# MAGIC Delete the checkpoint → full re-ingest (and with `mergeSchema` on an
# MAGIC append-only target, that means **duplicates** — the checkpoint IS the
# MAGIC exactly-once guarantee).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema evolution — the stream fails *on purpose*
# MAGIC Run `00_setup` with `mode = more`, then the ingest cell again. With the
# MAGIC default mode (`addNewColumns`) the run **fails** with an
# MAGIC `UnknownFieldException`: Auto Loader writes the new schema version (now
# MAGIC including `channel`) to `schemaLocation` and stops, so a restart — in
# MAGIC production, the job's retry — picks it up. Run the ingest cell **once more**
# MAGIC and batch 2 lands with the new column.
# MAGIC
# MAGIC | `cloudFiles.schemaEvolutionMode` | On new columns |
# MAGIC |---|---|
# MAGIC | `addNewColumns` (default with inference) | fail once, evolve schema, ingest on restart |
# MAGIC | `rescue` | schema frozen; new columns go to `_rescued_data` |
# MAGIC | `failOnNewColumns` | fail every time until the schema is updated by hand |
# MAGIC | `none` | new columns silently ignored |

# COMMAND ----------

# MAGIC %md
# MAGIC ## `_rescued_data` — nothing is silently lost
# MAGIC Batch 2's order 5010 has `"amount": "N/A"`, which can't cast to the
# MAGIC DECIMAL our schema hint pinned. `COPY INTO` nulled it (notebook 02); Auto
# MAGIC Loader **keeps the original value** in `_rescued_data`, tagged with the
# MAGIC source file, so you can repair instead of shrug.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, amount, _rescued_data, _source_file
# MAGIC FROM orders_autoloader
# MAGIC WHERE _rescued_data IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** `_rescued_data` catches **type mismatches and
# MAGIC > case-mismatched column names** relative to the *current* schema — it's on
# MAGIC > by default whenever the schema is inferred or provided without it. It is
# MAGIC > not the same thing as `PERMISSIVE`'s `_corrupt_record`: rescued rows still
# MAGIC > load their good columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How Auto Loader finds files: listing vs notifications
# MAGIC - **Directory listing** (default) — lists the path each trigger. Zero extra
# MAGIC   setup; fine into the thousands of files. Incremental listing kicks in
# MAGIC   automatically on lexically-ordered paths (like date partitions).
# MAGIC - **File notification** (`cloudFiles.useNotifications = true`) — subscribes
# MAGIC   to the bucket's event queue (SQS / Event Grid / Pub/Sub, provisioned for
# MAGIC   you). The answer whenever the question says *"millions of files"* or
# MAGIC   *"listing is too slow/expensive"*. `cloudFiles.backfillInterval` schedules
# MAGIC   periodic full listings to guarantee completeness, since notifications can
# MAGIC   be dropped.
# MAGIC
# MAGIC Either way, discovered-file state is in the checkpoint — switching modes
# MAGIC doesn't re-ingest.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production form: a Streaming Table
# MAGIC In Lakeflow / DLT the same thing is declarative — this is the syntax the
# MAGIC exam shows for "SQL-only incremental ingestion with managed infrastructure":
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders
# MAGIC AS SELECT *, _metadata.file_path AS _source_file
# MAGIC FROM STREAM read_files('/Volumes/.../landing/orders', format => 'json')
# MAGIC ```
# MAGIC
# MAGIC Same engine underneath (Auto Loader + checkpoints), but schema evolution
# MAGIC restarts and retries are handled for you.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_autoloader

# COMMAND ----------

dbutils.fs.rm(CKPT, recurse=True)  # noqa: F821
print("checkpoint removed — next run of the ingest cell starts from scratch")
