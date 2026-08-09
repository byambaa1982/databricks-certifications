# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Streaming metrics: reading a query's vital signs
# MAGIC
# MAGIC Every streaming question in this section reduces to: **where do the numbers
# MAGIC live and which number answers which symptom?** The answers are
# MAGIC `lastProgress` / `recentProgress` on the query, the `StreamingQueryListener`
# MAGIC for push-based capture, and (for Auto Loader) the backlog metrics. Requires
# MAGIC section 01's `00_setup` (the `de_pro` clones).

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
# MAGIC ## Attach a listener, then run a stream
# MAGIC A `StreamingQueryListener` gets a callback per batch — this is how teams
# MAGIC push metrics to their observability stack instead of polling.

# COMMAND ----------

from pyspark.sql.streaming import StreamingQueryListener  # noqa: E402

captured = []


class MetricsListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        captured.append(("started", event.name or event.id))

    def onQueryProgress(self, event):
        p = event.progress
        captured.append(("progress", f"batch={p.batchId} rows={p.numInputRows}"))

    def onQueryTerminated(self, event):
        captured.append(("terminated", str(event.id)))


listener = MetricsListener()
spark.streams.addListener(listener)  # noqa: F821

# COMMAND ----------

import shutil  # noqa: E402

shutil.rmtree(f"{CKPT}/monitor_stream", ignore_errors=True)

q = (
    spark.readStream.table("bronze_orders")  # noqa: F821
    .groupBy("status")
    .agg(F.count("*").alias("orders"))
    .writeStream
    .queryName("orders_by_status_monitor")
    .option("checkpointLocation", f"{CKPT}/monitor_stream")
    .outputMode("complete")
    .trigger(availableNow=True)
    .toTable("monitor_status_counts")
)
q.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull-based: `lastProgress` — know these fields cold
# MAGIC
# MAGIC | Field | Question it answers |
# MAGIC |---|---|
# MAGIC | `numInputRows` | how much did this batch process? |
# MAGIC | `inputRowsPerSecond` vs `processedRowsPerSecond` | arriving faster than we process? → **falling behind** |
# MAGIC | `durationMs` (`triggerExecution`, `addBatch`, …) | where the batch time went |
# MAGIC | `stateOperators[].numRowsTotal` | how big is aggregation/dedup state? → unbounded-state bugs |
# MAGIC | `sources[].startOffset/endOffset` | what slice of the source this batch covered |

# COMMAND ----------

import json  # noqa: E402

p = q.lastProgress
print(json.dumps(
    {k: p[k] for k in ("batchId", "numInputRows", "durationMs", "stateOperators") if k in p},
    indent=2, default=str,
))

# COMMAND ----------

# `recentProgress` keeps the last ~100 batches — the poor man's dashboard
for prog in q.recentProgress:
    print(f"batch {prog['batchId']}: {prog['numInputRows']} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the listener captured (async — may lag a moment)

# COMMAND ----------

import time  # noqa: E402

time.sleep(5)  # listener callbacks are asynchronous
for kind, detail in captured:
    print(f"{kind:11s} {detail}")
spark.streams.removeListener(listener)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader backlog — "is ingestion keeping up?"
# MAGIC In an Auto Loader stream's progress, `sources[0].metrics` reports:
# MAGIC - `numFilesOutstanding` / `numBytesOutstanding` — the **backlog** waiting to
# MAGIC   be processed.
# MAGIC
# MAGIC Rising backlog = under-provisioned stream. These are also the numbers behind
# MAGIC the jobs UI's **streaming backlog** graphs and backlog-based alerting on the
# MAGIC job. (Run section 02's `03_auto_loader.py` stream and inspect
# MAGIC `q.lastProgress["sources"]` to see them live.)
# MAGIC
# MAGIC > **Exam trap:** metrics live in the **query progress / listener / event
# MAGIC > log** — not in the target table. `DESCRIBE HISTORY` on the sink shows
# MAGIC > per-commit write metrics (notebook 02), but rates, state size, and backlog
# MAGIC > only exist in streaming progress.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS monitor_status_counts
