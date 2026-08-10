# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Deployment: batch, streaming, real-time
# MAGIC
# MAGIC The smallest section is one decision — **how fresh must predictions
# MAGIC be?** — and the API shape for each answer. The batch path runs live if
# MAGIC section 01's notebook 03 registered the model.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The decision table (most of the section's questions)
# MAGIC
# MAGIC | Mode | Freshness | Compute | Typical ask |
# MAGIC |---|---|---|---|
# MAGIC | **Batch** | scheduled (nightly scores into a Delta table) | a job; cheapest | "score all customers every morning" |
# MAGIC | **Streaming** | continuous over arriving data | Structured Streaming / DLT | "score events as they land" |
# MAGIC | **Real-time** | per-request, milliseconds | **Mosaic AI Model Serving** endpoint | "score inside the checkout flow" |
# MAGIC
# MAGIC Batch covers the vast majority of real use cases — reach for real-time
# MAGIC only when a *user is waiting on the answer*. Answering "REST endpoint"
# MAGIC for a nightly-scores scenario is the section's canonical wrong answer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch — `mlflow.pyfunc.spark_udf` scores in parallel

# COMMAND ----------

try:
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    MODEL_URI = f"models:/{CATALOG}.{SCHEMA}.churn_model@champion"

    predict = mlflow.pyfunc.spark_udf(spark, MODEL_URI, result_type="double")  # noqa: F821
    scored = (
        spark.table("customers_churn")  # noqa: F821
        .na.drop(subset=["monthly_spend"])
        .withColumn("churn_pred",
                    predict("age", "tenure_months", "support_tickets"))
    )
    scored.select("customer_id", "churn_pred").write.mode("overwrite") \
        .saveAsTable("churn_scores")
    print(f"scored {scored.count()} customers into churn_scores")  # noqa: F821
except Exception as e:
    print(f"needs the registered model from section 01 nb 03 ({type(e).__name__}) — "
          "the spark_udf pattern above is the exam content")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `mlflow.pyfunc.spark_udf` wraps **any** logged model as a Spark UDF —
# MAGIC >   distributed scoring without rewriting the model. This is *the* batch
# MAGIC >   pattern.
# MAGIC > - Loading by **alias** (`@champion`) means promotion (section 01) flows
# MAGIC >   to inference with zero code changes.
# MAGIC > - Feature-store-logged models (`fe.score_batch`) need only **keys** —
# MAGIC >   lookups happen inside.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming — same UDF, streaming DataFrame
# MAGIC
# MAGIC ```python
# MAGIC (spark.readStream.table("events")
# MAGIC    .withColumn("score", predict(*feature_cols))
# MAGIC    .writeStream
# MAGIC    .option("checkpointLocation", ckpt)
# MAGIC    .trigger(availableNow=True)
# MAGIC    .toTable("scored_events"))
# MAGIC ```
# MAGIC
# MAGIC All the streaming rules from the engineering tracks apply (checkpoint =
# MAGIC exactly-once; availableNow = drain-and-stop).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-time — Mosaic AI Model Serving
# MAGIC A **serving endpoint** hosts one or more UC-registered models behind a
# MAGIC REST API — serverless, autoscaling (including **scale-to-zero**), with
# MAGIC built-in auth and request logging.
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC w.serving_endpoints.create(
# MAGIC     name="churn-endpoint",
# MAGIC     config={
# MAGIC         "served_entities": [
# MAGIC             {"entity_name": "dev_catalog.ml_assoc.churn_model",
# MAGIC              "entity_version": "1", "workload_size": "Small",
# MAGIC              "scale_to_zero_enabled": True},
# MAGIC         ],
# MAGIC         # canary: split traffic between versions
# MAGIC         "traffic_config": {"routes": [
# MAGIC             {"served_model_name": "churn_model-1", "traffic_percentage": 90},
# MAGIC             {"served_model_name": "churn_model-2", "traffic_percentage": 10},
# MAGIC         ]},
# MAGIC     },
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - **Traffic splitting** between served versions = canary / A-B rollout —
# MAGIC >   the named feature for "test v2 on 10% of requests".
# MAGIC > - **Scale-to-zero** trades cold-start latency for idle cost — wrong for
# MAGIC >   strict-latency endpoints, right for spiky internal tools.
# MAGIC > - Endpoints serve **UC-registered** models — registration (section 01)
# MAGIC >   is the prerequisite, and endpoint permissions are UC grants.
# MAGIC > - Query it like any REST API (`/serving-endpoints/<name>/invocations`)
# MAGIC >   with a JSON payload of records.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS churn_scores
