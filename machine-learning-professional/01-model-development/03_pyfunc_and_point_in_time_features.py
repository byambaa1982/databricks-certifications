# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Custom PyFunc models and point-in-time features
# MAGIC
# MAGIC Two professional signatures: wrapping arbitrary logic as a **PyFunc** so
# MAGIC it deploys like any model, and joining features **as of the label's
# MAGIC timestamp** so training never sees the future. The setup planted the
# MAGIC leakage trap on purpose. Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom PyFunc — model + pre/post-processing as ONE artifact

# COMMAND ----------

try:
    import mlflow
    import pandas as pd

    class ChurnScorer(mlflow.pyfunc.PythonModel):
        """Wraps a model with business post-processing: score -> tier."""

        def load_context(self, context):
            import joblib
            self.model = joblib.load(context.artifacts["sk_model"])

        def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
            proba = self.model.predict_proba(model_input)[:, 1]
            return pd.DataFrame({
                "churn_probability": proba,
                "risk_tier": pd.cut(proba, [0, 0.3, 0.7, 1.0],
                                    labels=["low", "medium", "high"]),
            })

    # train a base model, save it as an artifact, wrap it
    import joblib
    from sklearn.ensemble import RandomForestClassifier

    pdf = (
        spark.table("customer_features_history")  # noqa: F821
        .filter("feature_ts = '2024-01-01'")
        .join(spark.table("churn_labels"), "customer_id")  # noqa: F821
        .select("tenure_months", "support_tickets", "monthly_spend", "churned")
        .toPandas()
    )
    base = RandomForestClassifier(n_estimators=50, random_state=42).fit(
        pdf.drop(columns=["churned"]), pdf["churned"])
    joblib.dump(base, "/tmp/base_model.joblib")

    with mlflow.start_run(run_name="pyfunc_churn_scorer"):
        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=ChurnScorer(),
            artifacts={"sk_model": "/tmp/base_model.joblib"},
            input_example=pdf.drop(columns=["churned"]).head(2),
        )
    print("PyFunc logged — it now loads/serves exactly like any flavor model")
except ImportError as e:
    print(f"needs ML Runtime ({e}) — the class shape is the exam content")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - Subclass `mlflow.pyfunc.PythonModel`; heavy loading goes in
# MAGIC >   **`load_context`** (runs once per worker/endpoint start), logic in
# MAGIC >   **`predict`**. Loading the artifact inside `predict` is the
# MAGIC >   performance anti-pattern.
# MAGIC > - `artifacts={...}` bundles files with the model; `input_example`
# MAGIC >   captures a **signature** — serving validates against it.
# MAGIC > - Why PyFunc at all: ensembles, pre/post-processing, business rules, or
# MAGIC >   any library without a built-in flavor — one deployable unit instead
# MAGIC >   of glue code at inference time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point-in-time features — join *as of*, never *latest*
# MAGIC Labels observed **Jan 15**; the feature table has Jan 1 and Feb 1
# MAGIC snapshots. The correct join takes each key's **latest row at or before**
# MAGIC the observation time — Jan 1. Naively joining the latest snapshot trains
# MAGIC on **February** data to predict a January outcome: future leakage.

# COMMAND ----------

# the point-in-time join, spelled out in SQL (an AS OF join by hand)
pit_sql = """
    SELECT l.customer_id, l.observation_ts, l.churned,
           f.tenure_months, f.support_tickets, f.monthly_spend, f.feature_ts
    FROM churn_labels l
    JOIN customer_features_history f
      ON f.customer_id = l.customer_id AND f.feature_ts <= l.observation_ts
    QUALIFY row_number() OVER (
        PARTITION BY l.customer_id ORDER BY f.feature_ts DESC) = 1
"""
pit = spark.sql(pit_sql)  # noqa: F821
display(pit.groupBy("feature_ts").count())  # noqa: F821  -- every row should use the Jan snapshot

# COMMAND ----------

# MAGIC %md
# MAGIC The FeatureEngineeringClient does this declaratively — the exam wants the
# MAGIC parameter names:
# MAGIC
# MAGIC ```python
# MAGIC fe.create_table(name=f"{CATALOG}.{SCHEMA}.customer_features_history",
# MAGIC                 primary_keys=["customer_id"],
# MAGIC                 timeseries_columns=["feature_ts"], df=...)     # <- timeseries col
# MAGIC
# MAGIC training_set = fe.create_training_set(
# MAGIC     df=labels_df,                                # has observation_ts
# MAGIC     feature_lookups=[FeatureLookup(
# MAGIC         table_name=f"{CATALOG}.{SCHEMA}.customer_features_history",
# MAGIC         lookup_key="customer_id",
# MAGIC         timestamp_lookup_key="observation_ts",   # <- point-in-time correctness
# MAGIC     )],
# MAGIC     label="churned",
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Online tables and feature serving — the low-latency tail
# MAGIC - An **online table** is a read-optimized, automatically-synced replica of
# MAGIC   a Delta feature table, built for millisecond lookups.
# MAGIC - A **feature serving endpoint** exposes features (via a FeatureSpec) over
# MAGIC   REST — for models that compute elsewhere but need governed features.
# MAGIC - Models logged with `fe.log_model` + online tables do the lookup
# MAGIC   **inside model serving** — the caller sends only keys.
# MAGIC
# MAGIC > **Exam trap:** batch scoring reads the offline Delta table; real-time
# MAGIC > serving needs the **online** table (or precomputed features in the
# MAGIC > request). "Point model serving at the Delta table for millisecond
# MAGIC > lookups" is the wrong option.

# COMMAND ----------

import os
if os.path.exists("/tmp/base_model.joblib"):
    os.remove("/tmp/base_model.joblib")
print("cleanup done")
