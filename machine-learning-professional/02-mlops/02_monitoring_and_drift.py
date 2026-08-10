# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Lakehouse Monitoring: profiles, drift, and the loop
# MAGIC
# MAGIC The MLOps section's second half: watching data and models **after**
# MAGIC deployment. Lakehouse Monitoring attaches to a Delta table, maintains two
# MAGIC metric tables and a dashboard, and its **inference profile** is the
# MAGIC purpose-built one for models. Run section 01's `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## The three profile types — pick by what the table holds
# MAGIC
# MAGIC | Profile | For a table of… | Adds |
# MAGIC |---|---|---|
# MAGIC | **Snapshot** | anything | full-table stats per refresh |
# MAGIC | **Time series** | rows with a timestamp | stats **per window** + drift vs previous windows |
# MAGIC | **Inference** | model requests: features + `prediction` (+ optional `label`, `model_id`) | everything time series has **plus model quality metrics** and per-model-version slicing |
# MAGIC
# MAGIC The output: `<table>_profile_metrics` and `<table>_drift_metrics` Delta
# MAGIC tables + an auto-generated dashboard. Alert on the metric tables like any
# MAGIC table (SQL alerts / jobs — the DE monitoring story, reused).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build an inference log worth monitoring

# COMMAND ----------

# score both feature snapshots with a simple rule to simulate two "months"
# of logged inferences — Feb's drifted features shift the prediction mix
inference_log = (
    spark.table("customer_features_history")  # noqa: F821
    .withColumn("prediction",
                (F.col("support_tickets") * 9 + (70 - F.col("tenure_months")) * 0.8 > 60)
                .cast("int"))
    .withColumn("model_id", F.lit("churn_model_v1"))
    .selectExpr("customer_id", "feature_ts AS inference_ts",
                "tenure_months", "support_tickets", "monthly_spend",
                "prediction", "model_id")
)
inference_log.write.mode("overwrite").saveAsTable("churn_inference_log")

display(  # noqa: F821
    inference_log.groupBy("inference_ts")
    .agg(F.avg("prediction").alias("positive_rate"),
         F.avg("support_tickets").alias("avg_tickets"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC The positive rate moved between January and February — that shift in the
# MAGIC **prediction distribution** is exactly what an inference monitor's drift
# MAGIC metrics surface, *without needing labels*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating the monitor — the API shape
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service.catalog import (
# MAGIC     MonitorInferenceLog, MonitorInferenceLogProblemType,
# MAGIC )
# MAGIC w = WorkspaceClient()
# MAGIC w.quality_monitors.create(
# MAGIC     table_name=f"{CATALOG}.{SCHEMA}.churn_inference_log",
# MAGIC     assets_dir="/Workspace/Users/me/monitors",
# MAGIC     output_schema_name=f"{CATALOG}.{SCHEMA}",
# MAGIC     inference_log=MonitorInferenceLog(
# MAGIC         timestamp_col="inference_ts",
# MAGIC         model_id_col="model_id",
# MAGIC         prediction_col="prediction",
# MAGIC         problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION,
# MAGIC         granularities=["1 day"],
# MAGIC         # label_col="churned",   # add when ground truth arrives -> quality metrics
# MAGIC     ),
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## What drift looks like in the metric tables
# MAGIC
# MAGIC | Signal | Metric family | Meaning |
# MAGIC |---|---|---|
# MAGIC | feature drift | distribution distances (JS divergence, chi-square, KS) vs prior window / baseline | inputs changed — the world moved |
# MAGIC | prediction drift | same stats on the `prediction` column | output mix changed — visible **before labels arrive** |
# MAGIC | label drift | stats on `label_col` | the target itself changed |
# MAGIC | quality decay | accuracy/F1/etc. once labels are joined | the model is actually wrong now |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Labels arrive **late** in production — prediction/feature drift are the
# MAGIC >   early-warning signals; quality metrics come when `label_col` fills in.
# MAGIC > - **Custom metrics** (business KPIs, e.g. approval rate by segment) can
# MAGIC >   be added to a monitor's computation — the named answer for "track a
# MAGIC >   domain metric alongside drift".
# MAGIC > - Monitor output is **just Delta tables** — alerting = SQL alerts / jobs
# MAGIC >   on them; drift alert → triggers the retraining job (notebook 01's
# MAGIC >   loop, closed).
# MAGIC > - Endpoint health (latency, QPS, error rate) is **serving metrics**, a
# MAGIC >   different layer than data/model quality — know which layer a symptom
# MAGIC >   belongs to.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS churn_inference_log
