# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Distributed training patterns: pick the right shape
# MAGIC
# MAGIC The professional exam's first discrimination: **is the data big, or is
# MAGIC the *number of models* big?** Different answers, different tools. The
# MAGIC many-models pattern (`applyInPandas`) runs live. Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## The decision table
# MAGIC
# MAGIC | Situation | Tool |
# MAGIC |---|---|
# MAGIC | one model, data **fits on one node** | single-node sklearn/XGBoost (simplest wins); parallelize *tuning* instead |
# MAGIC | one model, data **doesn't fit** | **Spark ML** (distributed estimators) |
# MAGIC | **many small models** (per store / region / SKU) | `groupBy().applyInPandas()` — each group trains on one worker |
# MAGIC | row-wise inference with a single-node model | `pandas_udf` / `mapInPandas` batches |
# MAGIC | deep learning across GPUs | Ray / TorchDistributor (know the names) |
# MAGIC
# MAGIC > **Exam trap:** "data fits in memory but training is slow" → the answer
# MAGIC > is parallel **tuning** (notebook 02) or a bigger single node — not
# MAGIC > rewriting to Spark ML. Distributed training pays coordination costs
# MAGIC > that only pay off when data *can't* fit.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Many models at once — `applyInPandas`, one model per region

# COMMAND ----------

features = (
    spark.table("customer_features_history")  # noqa: F821
    .filter("feature_ts = '2024-01-01'")
    .join(spark.table("churn_labels"), "customer_id")  # noqa: F821
    .join(spark.table("customers").select("customer_id", "region"), "customer_id")  # noqa: F821
)

result_schema = "region string, n_rows long, auc double"

def train_per_region(pdf):
    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import train_test_split

    X = pdf[["tenure_months", "support_tickets", "monthly_spend"]]
    y = pdf["churned"]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.3, random_state=42)
    model = RandomForestClassifier(n_estimators=50, random_state=42).fit(X_tr, y_tr)
    auc = roc_auc_score(y_te, model.predict_proba(X_te)[:, 1])
    return pd.DataFrame([{"region": pdf["region"].iloc[0],
                          "n_rows": len(pdf), "auc": float(auc)}])

try:
    display(  # noqa: F821
        features.groupBy("region").applyInPandas(train_per_region, schema=result_schema)
    )
except Exception as e:
    print(f"needs sklearn on the workers / ML Runtime ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `applyInPandas` requires the **output schema declared up front**; each
# MAGIC >   group must fit in one worker's memory (grouped, not distributed,
# MAGIC >   training).
# MAGIC > - Inside the function you're in **plain pandas/sklearn land** — no Spark
# MAGIC >   objects. In real jobs, log each group's model to MLflow as a
# MAGIC >   **nested run** (notebook 02) or into one artifact per group.
# MAGIC > - `pandas_udf` = column(s) in → column out (vectorized functions);
# MAGIC >   `applyInPandas` = whole **group** in → DataFrame out;
# MAGIC >   `mapInPandas` = iterator of batches (schema can change, memory-safe
# MAGIC >   streaming through partitions). Picking among the three is a question
# MAGIC >   format.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark ML when the data itself is big — the shapes to recognize
# MAGIC The Associate covered the mechanics
# MAGIC ([pipelines](../../machine-learning-associate/03-model-development/01_pipelines_and_validation.py));
# MAGIC professional adds the judgment:
# MAGIC
# MAGIC - Spark ML estimators (`LogisticRegression`, `RandomForestClassifier`,
# MAGIC   GBTs, ALS) train on **distributed DataFrames** — no `toPandas()`.
# MAGIC - `toPandas()` on big data is the **driver-OOM** classic; sample first or
# MAGIC   stay in Spark.
# MAGIC - Single-node libraries inside Spark only via the pandas function APIs
# MAGIC   above — importing sklearn does not make it distributed.
