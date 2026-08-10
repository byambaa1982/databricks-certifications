# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Structured Streaming essentials
# MAGIC
# MAGIC The OSS streaming model: an unbounded table processed in micro-batches,
# MAGIC with **output modes**, **triggers**, **watermarks**, and **checkpoints**
# MAGIC as the four levers the exam tests. Runnable with the rate source — no
# MAGIC external systems needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The model and the levers
# MAGIC
# MAGIC | Lever | Options | Decides |
# MAGIC |---|---|---|
# MAGIC | **output mode** | `append` / `complete` / `update` | which result rows go to the sink each batch |
# MAGIC | **trigger** | default / `processingTime` / `availableNow` / (`once` deprecated) | when batches fire |
# MAGIC | **watermark** | `withWatermark(col, delay)` | how late is too late + when state is freed |
# MAGIC | **checkpoint** | `checkpointLocation` | offsets + state → exactly-once recovery |

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402
import tempfile  # noqa: E402

ckpt = tempfile.mkdtemp()
out = tempfile.mkdtemp() + "/rate_out"

stream = (
    spark.readStream.format("rate").option("rowsPerSecond", 100).load()  # noqa: F821
    .withColumn("bucket", F.col("value") % 5)
)
q = (
    stream.writeStream.format("parquet")
    .option("path", out)
    .option("checkpointLocation", ckpt)
    .trigger(availableNow=True)      # drain what's available, then stop
    .outputMode("append")
    .start()
)
q.awaitTermination()
print(f"batch ran; wrote {spark.read.parquet(out).count()} rows; "  # noqa: F821
      f"lastProgress batchId = {q.lastProgress['batchId'] if q.lastProgress else 'n/a'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Output-mode legality — the most-tested grid
# MAGIC
# MAGIC | Query shape | append | update | complete |
# MAGIC |---|---|---|---|
# MAGIC | plain select/filter (no aggregation) | ✅ | ✅ | ❌ |
# MAGIC | aggregation **without** watermark | ❌ | ✅ | ✅ |
# MAGIC | aggregation **with** watermark | ✅ (rows emit when window closes) | ✅ | ✅ |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Aggregation + `append` + **no watermark** throws — append needs
# MAGIC >   finalized rows, and without a watermark aggregates never finalize.
# MAGIC > - `complete` rewrites the **entire result table** every batch — only
# MAGIC >   sane for small aggregates.
# MAGIC > - File sinks support **append only**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Windows + watermark — the shape to recognize
# MAGIC
# MAGIC ```python
# MAGIC (events
# MAGIC  .withWatermark("event_time", "10 minutes")
# MAGIC  .groupBy(F.window("event_time", "5 minutes"), "key")
# MAGIC  .count())
# MAGIC ```
# MAGIC
# MAGIC In `append` mode a window's row is emitted once the watermark passes the
# MAGIC window end; events later than the watermark are **dropped**. The
# MAGIC watermark bounds **state** — without it, windowed/dedup state grows
# MAGIC forever (the streaming-job-slowly-dying question).
# MAGIC
# MAGIC ## Checkpoints and exactly-once
# MAGIC - Checkpoint stores **source offsets + operator state**, per query. Same
# MAGIC   checkpoint → resume; deleted checkpoint → reprocess from scratch.
# MAGIC - End-to-end exactly-once needs a **replayable source** (Kafka, files) +
# MAGIC   **idempotent/transactional sink** (Delta, foreachBatch-with-merge) +
# MAGIC   the checkpoint. All three, or it's at-least-once.
# MAGIC - Changing the query's aggregation/schema invalidates checkpoint state —
# MAGIC   the "stream fails after code change" question.
# MAGIC
# MAGIC `foreachBatch` = run arbitrary batch logic (MERGE, multi-sink writes) on
# MAGIC each micro-batch's DataFrame — the escape hatch the exam names for sinks
# MAGIC without native streaming support.
# MAGIC
# MAGIC Databricks-flavored practice with real tables: DE Professional
# MAGIC [`05_structured_streaming`](../../data-engineer-professional/01-developing-code-for-data-processing/05_structured_streaming.py).
