# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Structured Streaming, Auto Loader, and Change Data Feed
# MAGIC
# MAGIC Every stream here uses `trigger(availableNow=True)`: process everything
# MAGIC available, then stop. The notebook runs end-to-end and never hangs — and
# MAGIC `availableNow` vs `processingTime` vs continuous is itself exam material.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Delta → Delta
# MAGIC Any Delta table is a streaming source. Stream `bronze_orders` into a running
# MAGIC per-status aggregate.
# MAGIC
# MAGIC > **Exam trap:** streaming aggregations (without watermark) require output
# MAGIC > mode `complete` (or `update`) — `append` fails because aggregate rows keep
# MAGIC > changing and can never be finalized.

# COMMAND ----------

import shutil  # noqa: E402

shutil.rmtree(f"{CKPT}/status_counts", ignore_errors=True)  # fresh run each time

q = (
    spark.readStream.table("bronze_orders")  # noqa: F821
    .groupBy("status")
    .agg(F.count("*").alias("orders"), F.sum("amount").alias("amount_total"))
    .writeStream
    .option("checkpointLocation", f"{CKPT}/status_counts")
    .outputMode("complete")
    .trigger(availableNow=True)
    .toTable("stream_status_counts")
)
q.awaitTermination()
display(spark.table("stream_status_counts"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The checkpoint IS the stream's identity
# MAGIC Run the cell below: **zero new rows are processed**, because the checkpoint
# MAGIC remembers the last committed offset. Delete the checkpoint and it reprocesses
# MAGIC everything. Exam: checkpoints store offsets + state, they are per-query, and
# MAGIC changing the query's aggregation logic invalidates them.

# COMMAND ----------

q = (
    spark.readStream.table("bronze_orders")  # noqa: F821
    .groupBy("status")
    .agg(F.count("*").alias("orders"), F.sum("amount").alias("amount_total"))
    .writeStream
    .option("checkpointLocation", f"{CKPT}/status_counts")
    .outputMode("complete")
    .trigger(availableNow=True)
    .toTable("stream_status_counts")
)
q.awaitTermination()
print(f"batches processed this run: {len(q.recentProgress)} (0 or an empty batch = nothing new)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader — incremental file ingestion
# MAGIC This is how the real bronze layer works. Point it at the lakehouse-starter
# MAGIC raw volume (read-only here; our checkpoint tracks which files WE have seen).

# COMMAND ----------

# Auto-locate the raw volume of the deployed lakehouse-starter bundle
raw_path = None
for row in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect():  # noqa: F821
    sch = row.databaseName if hasattr(row, "databaseName") else row.schemaName
    vols = [v.volume_name for v in spark.sql(f"SHOW VOLUMES IN {CATALOG}.{sch}").collect()]  # noqa: F821
    if "raw" in vols:
        raw_path = f"/Volumes/{CATALOG}/{sch}/raw"
        break
assert raw_path, "No 'raw' volume found — deploy lakehouse-starter first"
print(f"reading from {raw_path}/orders")

# COMMAND ----------

shutil.rmtree(f"{CKPT}/autoloader_orders", ignore_errors=True)

q = (
    spark.readStream.format("cloudFiles")  # noqa: F821
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CKPT}/autoloader_orders/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"{raw_path}/orders")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .writeStream
    .option("checkpointLocation", f"{CKPT}/autoloader_orders")
    .trigger(availableNow=True)
    .toTable("my_bronze_orders")
)
q.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which files did Auto Loader pick up?
# MAGIC SELECT _source_file, count(*) AS rows_from_file
# MAGIC FROM my_bronze_orders
# MAGIC GROUP BY _source_file
# MAGIC ORDER BY _source_file

# COMMAND ----------

# MAGIC %md
# MAGIC > Run `databricks bundle run lakehouse_job -t dev` (or the seed task alone)
# MAGIC > and re-run the Auto Loader cell: **only the new files** are ingested.
# MAGIC
# MAGIC **Exam essentials:** `cloudFiles` requires a `schemaLocation`; schema drift
# MAGIC lands in `_rescued_data` by default; file discovery state lives in the
# MAGIC checkpoint (RocksDB), so deleting the checkpoint = full re-ingest.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change Data Feed — read row-level changes
# MAGIC `00_setup` re-enabled `delta.enableChangeDataFeed = true` on the
# MAGIC `silver_orders` copy. Make some changes, then read exactly what changed.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE silver_orders SET status = 'refunded'
# MAGIC WHERE order_id IN (SELECT order_id FROM silver_orders WHERE status = 'cancelled' LIMIT 5)

# COMMAND ----------

last_version = spark.sql("DESCRIBE HISTORY silver_orders LIMIT 1").collect()[0].version  # noqa: F821

changes = (
    spark.read.format("delta")  # noqa: F821
    .option("readChangeFeed", "true")
    .option("startingVersion", last_version)
    .table("silver_orders")
)
display(  # noqa: F821
    changes.select("order_id", "status", "_change_type", "_commit_version").orderBy("order_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC An UPDATE produces **two** CDF rows per changed record: `update_preimage`
# MAGIC (before) and `update_postimage` (after). Inserts produce `insert`, deletes
# MAGIC `delete`. Downstream consumers read increments instead of re-scanning the
# MAGIC table — CDF also works with `readStream` for continuous propagation.
# MAGIC
# MAGIC > **Exam trap:** CDF must be enabled **before** the changes you want to read,
# MAGIC > and `table_changes()` / `readChangeFeed` on versions older than the
# MAGIC > enablement (or vacuumed away) errors out.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger cheat sheet (memorize)
# MAGIC | Trigger | Behavior | Use case |
# MAGIC |---|---|---|
# MAGIC | `availableNow=True` | drain all available data in batches, then stop | scheduled jobs, this notebook |
# MAGIC | `processingTime='1 minute'` | micro-batch every interval, runs forever | always-on pipelines |
# MAGIC | (default) | micro-batch as fast as possible, runs forever | low-latency |
# MAGIC | `once=True` | deprecated single-batch version of availableNow | legacy answers only |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS stream_status_counts;
# MAGIC DROP TABLE IF EXISTS my_bronze_orders
