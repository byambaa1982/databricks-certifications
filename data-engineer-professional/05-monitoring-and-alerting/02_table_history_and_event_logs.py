# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Table history and pipeline event logs
# MAGIC
# MAGIC Two audit trails, different scopes: `DESCRIBE HISTORY` tells you **what
# MAGIC happened to a table** (per commit, with metrics); the DLT **event log**
# MAGIC tells you **what happened in a pipeline** (per update, with expectation
# MAGIC metrics and lineage). Requires section 01's `00_setup`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate some history worth reading

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE history_demo AS SELECT * FROM silver_orders;
# MAGIC UPDATE history_demo SET status = 'refunded' WHERE status = 'cancelled';
# MAGIC DELETE FROM history_demo WHERE amount < 20;
# MAGIC OPTIMIZE history_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY history_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## `operationMetrics` — the numbers inside each commit
# MAGIC `DESCRIBE HISTORY` is queryable like any DataFrame; `operationMetrics` is a
# MAGIC map. This is where "how many rows did that DELETE remove?" lives.

# COMMAND ----------

display(  # noqa: F821
    spark.sql("DESCRIBE HISTORY history_demo")  # noqa: F821
    .selectExpr(
        "version",
        "operation",
        "operationMetrics['numOutputRows']   AS out_rows",
        "operationMetrics['numUpdatedRows']  AS updated",
        "operationMetrics['numDeletedRows']  AS deleted",
        "operationMetrics['numRemovedFiles'] AS files_removed",
        "operationMetrics['numAddedFiles']   AS files_added",
    )
    .orderBy("version")
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `DESCRIBE HISTORY` reads the **transaction log**, so it's cheap — no
# MAGIC >   table scan.
# MAGIC > - History retention is governed by
# MAGIC >   `delta.logRetentionDuration` (default 30 days) — independent from
# MAGIC >   `delta.deletedFileRetentionDuration` (VACUUM's window, default 7 days).
# MAGIC >   You can *see* a version in history yet fail to *time travel* to it
# MAGIC >   because VACUUM already removed its files.
# MAGIC > - Each MERGE/UPDATE/DELETE is **one commit** — per-branch counts come from
# MAGIC >   `operationMetrics`, not extra versions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The DLT / Lakeflow event log
# MAGIC Every pipeline update appends structured events (JSON `details` column) to
# MAGIC its event log. Query it with the `event_log` TVF (needs a pipeline, hence
# MAGIC reference):
# MAGIC
# MAGIC ```sql
# MAGIC -- by pipeline id
# MAGIC SELECT * FROM event_log('<pipeline-id>');
# MAGIC
# MAGIC -- or via a table the pipeline produces
# MAGIC SELECT * FROM event_log(TABLE(dev_catalog.lakehouse.silver_orders));
# MAGIC ```
# MAGIC
# MAGIC The two queries the exam expects you to recognize:
# MAGIC
# MAGIC ```sql
# MAGIC -- data quality: expectation pass/fail counts per table
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name,
# MAGIC   row_expectations.passed_records,
# MAGIC   row_expectations.failed_records
# MAGIC FROM (
# MAGIC   SELECT timestamp,
# MAGIC          explode(from_json(details:flow_progress:data_quality:expectations,
# MAGIC                  'array<struct<dataset:string,name:string,
# MAGIC                                passed_records:long,failed_records:long>>'))
# MAGIC          AS row_expectations
# MAGIC   FROM event_log('<pipeline-id>')
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC | `event_type` | What it carries |
# MAGIC |---|---|
# MAGIC | `flow_progress` | rows written, **expectation metrics**, backlog |
# MAGIC | `update_progress` | update lifecycle (WAITING → RUNNING → COMPLETED/FAILED) |
# MAGIC | `flow_definition` | lineage: inputs/outputs of each flow |
# MAGIC
# MAGIC > **Exam trap:** expectation metrics are **only** in the event log — not in
# MAGIC > the target table, not in DESCRIBE HISTORY. "Where do you check how many
# MAGIC > rows violated an expectation?" → the event log.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS history_demo
