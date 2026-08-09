# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Slowly changing dimensions, hand-rolled and declarative
# MAGIC
# MAGIC The one modelling pattern the exam always includes: a customer changes
# MAGIC tier — what happens to the dimension row? We run the **SCD2 MERGE** live
# MAGIC (the trickiest SQL in the whole repo, worth predicting cell by cell), then
# MAGIC map it to DLT's `apply_changes`. Requires section 01's `00_setup`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The type ladder
# MAGIC
# MAGIC | Type | On change | History | Use when |
# MAGIC |---|---|---|---|
# MAGIC | **0** | ignore it | original only | immutable attributes (birth date) |
# MAGIC | **1** | overwrite | none | corrections; history worthless |
# MAGIC | **2** | close old row, insert new | full | the default for analytics |
# MAGIC | 3 | extra "previous_x" column | one step | rare; exam distractor |
# MAGIC
# MAGIC The SCD2 row lifecycle: `(customer 101, tier=silver, start=Jan, end=NULL,
# MAGIC current=true)` → tier changes in March → old row gets `end=Mar,
# MAGIC current=false`; new row `(tier=gold, start=Mar, end=NULL, current=true)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a small SCD2 dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_customer_scd2 AS
# MAGIC SELECT
# MAGIC   customer_id, name, tier,
# MAGIC   to_timestamp(__START_AT)  AS start_at,
# MAGIC   cast(NULL AS timestamp)   AS end_at,
# MAGIC   true                      AS is_current
# MAGIC FROM silver_customers
# MAGIC WHERE __END_AT IS NULL;
# MAGIC
# MAGIC SELECT count(*) AS current_rows FROM dim_customer_scd2

# COMMAND ----------

# MAGIC %md
# MAGIC ## A change batch arrives
# MAGIC Two customers with a **new tier**, one brand-new customer.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tier_changes AS
# MAGIC SELECT min(customer_id) AS customer_id, 'UPGRADED-1' AS name, 'platinum' AS tier
# MAGIC FROM dim_customer_scd2
# MAGIC UNION ALL
# MAGIC SELECT max(customer_id), 'UPGRADED-2', 'platinum' FROM dim_customer_scd2
# MAGIC UNION ALL
# MAGIC SELECT cast(999999 AS BIGINT), 'Brand New Customer', 'bronze'

# COMMAND ----------

# MAGIC %md
# MAGIC ## The SCD2 MERGE — the union-staging trick
# MAGIC One MERGE must do two things to a changed key: **close** the old row *and*
# MAGIC **insert** the new one. The trick: stage a **second copy** of each
# MAGIC genuinely-changed record with `merge_key = NULL` — a NULL key can never
# MAGIC match, so that copy always falls through to INSERT, while the keyed copy
# MAGIC matches the current row and closes it.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dim_customer_scd2 t
# MAGIC USING (
# MAGIC   -- copy 1: new customers fall through to INSERT; changed customers MATCH
# MAGIC   --         their current row and close it
# MAGIC   SELECT customer_id AS merge_key, * FROM tier_changes
# MAGIC   UNION ALL
# MAGIC   -- copy 2: ONLY for records whose tier actually changed; NULL key never
# MAGIC   --         matches -> always inserts the new version
# MAGIC   SELECT cast(NULL AS BIGINT) AS merge_key, c.*
# MAGIC   FROM tier_changes c
# MAGIC   JOIN dim_customer_scd2 d
# MAGIC     ON c.customer_id = d.customer_id AND d.is_current AND d.tier <> c.tier
# MAGIC ) s
# MAGIC ON t.customer_id = s.merge_key AND t.is_current = true
# MAGIC WHEN MATCHED AND t.tier <> s.tier THEN
# MAGIC   UPDATE SET t.end_at = current_timestamp(), t.is_current = false
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (customer_id, name, tier, start_at, end_at, is_current)
# MAGIC   VALUES (s.customer_id, s.name, s.tier, current_timestamp(), NULL, true)

# COMMAND ----------

# MAGIC %md
# MAGIC Predict before running the next cell: the two upgraded customers should
# MAGIC have **two rows each** (one closed, one current), the new customer **one**.
# MAGIC
# MAGIC Walk the metrics above: 2 updates (copy-1 of the changed customers closing
# MAGIC their rows), 3 inserts (copy-2 of both changes + copy-1 of the brand-new
# MAGIC customer, which matched nothing). An **unchanged** customer in the batch
# MAGIC would produce neither — copy-1 matches but `t.tier <> s.tier` blocks the
# MAGIC update, and no copy-2 exists for it. That reasoning walk *is* the exam
# MAGIC question.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, tier, start_at, end_at, is_current
# MAGIC FROM dim_customer_scd2
# MAGIC WHERE customer_id IN (SELECT customer_id FROM tier_changes)
# MAGIC ORDER BY customer_id, start_at

# COMMAND ----------

# MAGIC %md
# MAGIC ## The declarative version — what the real pipeline uses
# MAGIC The `silver_customers` table this repo clones was built by exactly this:
# MAGIC
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="silver_customers",
# MAGIC     source="bronze_customers_cdc",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by=col("event_ts"),        # ordering column — REQUIRED
# MAGIC     apply_as_deletes=expr("op = 'DELETE'"),
# MAGIC     stored_as_scd_type=2,               # 1 = overwrite, 2 = history
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC It produces the `__START_AT` / `__END_AT` columns you've been filtering on
# MAGIC since section 01, and handles ordering, dedup, out-of-order late events,
# MAGIC and deletes — all the things the hand-rolled MERGE above does not.
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - `sequence_by` is what makes out-of-order CDC safe — hand-rolled MERGE
# MAGIC >   has no equivalent unless you dedupe + order yourself (section 01's
# MAGIC >   `row_number` pattern).
# MAGIC > - Querying SCD2: current rows = `__END_AT IS NULL` (or `is_current`);
# MAGIC >   as-of rows = the point-in-time range join (section 01, notebook 01).
# MAGIC > - Joining facts to SCD2 **without** the current/as-of filter fans out —
# MAGIC >   the bug you demonstrated live in section 01.
# MAGIC > - Inside DLT pipelines prefer `apply_changes`; MERGE is for jobs — same
# MAGIC >   heuristic as section 01, notebook 04.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dim_customer_scd2
