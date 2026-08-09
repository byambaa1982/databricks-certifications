# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Delta DML: MERGE, UPDATE, DELETE
# MAGIC
# MAGIC This notebook **mutates** tables — that's the point. You're working on the
# MAGIC `de_pro` clones, so nothing here touches the real pipeline. Re-run `00_setup`
# MAGIC to reset after experimenting.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a customer snapshot table to merge into
# MAGIC A Type 1 (overwrite-in-place) target, built from the current SCD2 rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_customers AS
# MAGIC SELECT customer_id, name, email, tier
# MAGIC FROM silver_customers
# MAGIC WHERE __END_AT IS NULL;
# MAGIC
# MAGIC SELECT count(*) AS current_customers FROM dim_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## The CDC-to-MERGE pattern (the exam's favorite)
# MAGIC Two steps, always in this order:
# MAGIC 1. **Dedupe the source to one row per key** — MERGE throws
# MAGIC    `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW` if two source rows hit the
# MAGIC    same target row.
# MAGIC 2. MERGE with INSERT / UPDATE / DELETE branches driven by the `op` column.

# COMMAND ----------

from pyspark.sql.window import Window  # noqa: E402

latest = Window.partitionBy("customer_id").orderBy(F.desc("event_ts"))

(
    spark.table("bronze_customers_cdc")  # noqa: F821
    .withColumn("rn", F.row_number().over(latest))
    .filter("rn = 1")
    .drop("rn")
    .createOrReplaceTempView("cdc_latest")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dim_customers t
# MAGIC USING cdc_latest s
# MAGIC   ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
# MAGIC WHEN MATCHED AND s.op != 'DELETE' THEN
# MAGIC   UPDATE SET t.name = s.name, t.email = s.email, t.tier = s.tier
# MAGIC WHEN NOT MATCHED AND s.op != 'DELETE' THEN
# MAGIC   INSERT (customer_id, name, email, tier)
# MAGIC   VALUES (s.customer_id, s.name, s.email, s.tier)

# COMMAND ----------

# MAGIC %md
# MAGIC The result row above (`num_affected_rows`, `num_updated_rows`,
# MAGIC `num_deleted_rows`, `num_inserted_rows`) is itself exam material — know what
# MAGIC each branch contributes.
# MAGIC
# MAGIC > **Exam trap:** a `WHEN NOT MATCHED` branch can only INSERT; a
# MAGIC > `WHEN MATCHED` branch can only UPDATE or DELETE. `WHEN NOT MATCHED BY
# MAGIC > SOURCE` (target rows absent from source) can UPDATE or DELETE — that's the
# MAGIC > branch for "remove customers no longer in the feed".

# COMMAND ----------

# MAGIC %md
# MAGIC ## UPDATE / DELETE are transactional table operations

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE dim_customers SET tier = 'platinum' WHERE tier = 'gold';
# MAGIC
# MAGIC DELETE FROM dim_customers WHERE email IS NULL;
# MAGIC
# MAGIC SELECT tier, count(*) FROM dim_customers GROUP BY tier

# COMMAND ----------

# MAGIC %md
# MAGIC ## Every write is a version: history, time travel, RESTORE

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY dim_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The table as it was before the UPDATE/DELETE (version numbers from HISTORY above)
# MAGIC SELECT tier, count(*) FROM dim_customers VERSION AS OF 1 GROUP BY tier

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Undo everything back to the freshly created snapshot
# MAGIC RESTORE TABLE dim_customers TO VERSION AS OF 0;
# MAGIC SELECT count(*) AS after_restore FROM dim_customers

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** `RESTORE` is itself a new commit — history is preserved, so
# MAGIC > you can restore a restore. `VACUUM` is what permanently removes old files
# MAGIC > and breaks time travel beyond the retention window (default 7 days).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema evolution on MERGE
# MAGIC With `withSchemaEvolution()` (or `spark.databricks.delta.schema.autoMerge.enabled`),
# MAGIC MERGE can add source-only columns to the target.

# COMMAND ----------

from delta.tables import DeltaTable  # noqa: E402

source = spark.sql(  # noqa: F821
    "SELECT customer_id, tier, current_timestamp() AS last_seen_at FROM cdc_latest WHERE op != 'DELETE'"
)

(
    DeltaTable.forName(spark, "dim_customers")  # noqa: F821
    .merge(source, "dim_customers.customer_id = source.customer_id")
    .withSchemaEvolution()
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# last_seen_at now exists on the target — added by the merge, not by ALTER TABLE
display(spark.table("dim_customers").limit(5))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## When NOT to hand-roll MERGE
# MAGIC The real pipeline builds `silver_customers` with DLT's
# MAGIC `apply_changes(stored_as_scd_type=2)` — it handles ordering, dedup, and SCD2
# MAGIC bookkeeping declaratively. Exam heuristic: **inside a DLT pipeline, prefer
# MAGIC `apply_changes` (AUTO CDC); MERGE is for jobs and ad-hoc engineering.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dim_customers
