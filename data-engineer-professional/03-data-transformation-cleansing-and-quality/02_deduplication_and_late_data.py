# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Deduplication and late-arriving data
# MAGIC
# MAGIC Duplicates come from three places — retried writes, replayed sources, and
# MAGIC genuinely repeated events — and the fix is different depending on whether
# MAGIC you're deduping **a batch**, **an append stream**, or **against what's
# MAGIC already in the target**. The exam tests all three. Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch dedup #1: exact duplicates
# MAGIC `distinct()` / `dropDuplicates()` (no args) compare **entire rows** — safe
# MAGIC for the "same row loaded twice" case.

# COMMAND ----------

d = spark.table("bronze_orders_dirty")  # noqa: F821
print(f"rows:            {d.count()}")
print(f"distinct rows:   {d.distinct().count()}")
print(f"distinct ids:    {d.select('order_id').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch dedup #2: one row per key — order matters
# MAGIC > **Exam trap (again, because the exam repeats it):**
# MAGIC > `dropDuplicates(["order_id"])` keeps an **arbitrary** row per key. When
# MAGIC > "keep the latest" is required, only `row_number()` over a descending sort
# MAGIC > (or `max_by`) is correct.

# COMMAND ----------

latest = Window.partitionBy("order_id").orderBy(F.desc("order_ts"))

deduped = (
    d.withColumn("rn", F.row_number().over(latest))
    .filter("rn = 1")
    .drop("rn")
)
print(f"after latest-per-key dedup: {deduped.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dedup #3: against the target — the insert-only MERGE
# MAGIC A batch that *retries* must not re-insert rows the last attempt already
# MAGIC landed. `MERGE ... WHEN NOT MATCHED THEN INSERT` makes the append
# MAGIC idempotent: matched keys are simply left alone.

# COMMAND ----------

deduped.createOrReplaceTempView("incoming_batch")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_dedup_target AS
# MAGIC SELECT * FROM incoming_batch LIMIT 5;   -- pretend a previous attempt landed 5 rows
# MAGIC
# MAGIC MERGE INTO orders_dedup_target t
# MAGIC USING incoming_batch s
# MAGIC   ON t.order_id = s.order_id
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC The MERGE metrics above show it: 5 keys matched and were skipped, the rest
# MAGIC inserted. Re-run the MERGE cell — `num_inserted_rows` drops to **0**.
# MAGIC
# MAGIC > **Exam trap:** insert-only MERGE assumes the **source** has one row per
# MAGIC > key (dedupe it first, as we did) — otherwise the same batch can still
# MAGIC > insert duplicates of itself: `WHEN NOT MATCHED` checks the target, not
# MAGIC > the other source rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming dedup — state must be bounded
# MAGIC `dropDuplicates` on a stream keeps every seen key **in state, forever**,
# MAGIC unless a watermark bounds it. The watermark says: "I will never see events
# MAGIC older than X behind the max seen so far — feel free to forget older keys."

# COMMAND ----------

import shutil  # noqa: E402

shutil.rmtree(f"{CKPT}/dedup_stream", ignore_errors=True)

q = (
    spark.readStream.table("bronze_orders_dirty")  # noqa: F821
    .withColumn("order_ts", F.to_timestamp("order_ts"))
    .filter("order_ts IS NOT NULL")
    .withWatermark("order_ts", "1 day")
    .dropDuplicates(["order_id", "order_ts"])
    .writeStream
    .option("checkpointLocation", f"{CKPT}/dedup_stream")
    .trigger(availableNow=True)
    .toTable("orders_stream_deduped")
)
q.awaitTermination()
print(f"streamed + deduped: {spark.table('orders_stream_deduped').count()} rows")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps, rapid fire:**
# MAGIC > - Stream `dropDuplicates` **without** a watermark = unbounded state growth
# MAGIC >   (the job eventually dies) — the exam's favorite wrong answer.
# MAGIC > - The watermark column **must be one of the dedup keys** for state to be
# MAGIC >   cleaned (hence `["order_id", "order_ts"]` above).
# MAGIC > - `dropDuplicatesWithinWatermark` (DBR 13+) relaxes that: dedup on
# MAGIC >   `order_id` alone, expiring keys once they age past the watermark —
# MAGIC >   the right tool when retries carry **different** timestamps.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Late data — what a watermark actually does
# MAGIC One knob, two effects, and the exam asks about both:
# MAGIC
# MAGIC | Effect | Meaning for `withWatermark(ts, "1 day")` |
# MAGIC |---|---|
# MAGIC | **Admission** | events up to 1 day later than the max seen `ts` are still processed |
# MAGIC | **State cleanup** | windows / keys older than that are finalized and freed |
# MAGIC
# MAGIC In **append** output mode, an aggregation emits a window's row only once
# MAGIC the watermark passes the window's end — later-arriving events for it are
# MAGIC **dropped**. `update` mode emits corrections instead. Pick lateness
# MAGIC tolerance vs. latency: a bigger watermark loses less data but holds more
# MAGIC state and emits later.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_dedup_target;
# MAGIC DROP TABLE IF EXISTS orders_stream_deduped
