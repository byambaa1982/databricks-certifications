# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Feature engineering in Unity Catalog
# MAGIC
# MAGIC A feature table is a UC Delta table with a **primary key** — that
# MAGIC constraint is what lets training jobs *look features up* by key instead
# MAGIC of re-deriving them, and what lets serving reuse the exact same values.
# MAGIC Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the feature table — PK is the defining property

# COMMAND ----------

features = (
    spark.table("customers_churn")  # noqa: F821
    .select(
        "customer_id",
        "age", "tenure_months", "support_tickets",
        F.coalesce("monthly_spend", F.lit(0)).alias("monthly_spend"),
        (F.col("support_tickets") / F.greatest(F.col("tenure_months"), F.lit(1)))
            .alias("tickets_per_month"),
    )
)
features.write.mode("overwrite").saveAsTable("customer_features")

spark.sql("ALTER TABLE customer_features ALTER COLUMN customer_id SET NOT NULL")  # noqa: F821
try:
    spark.sql("ALTER TABLE customer_features ADD CONSTRAINT cf_pk PRIMARY KEY (customer_id)")  # noqa: F821
    print("primary key set — this is now a feature table UC tooling recognizes")
except Exception as e:
    print(f"PK already set or not permitted ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The FeatureEngineeringClient — the API shapes to recognize
# MAGIC
# MAGIC ```python
# MAGIC from databricks.feature_engineering import (
# MAGIC     FeatureEngineeringClient, FeatureLookup,
# MAGIC )
# MAGIC fe = FeatureEngineeringClient()
# MAGIC
# MAGIC # create (instead of the SQL above) — schema + PK from the DataFrame
# MAGIC fe.create_table(name="dev_catalog.ml_assoc.customer_features",
# MAGIC                 primary_keys=["customer_id"], df=features)
# MAGIC
# MAGIC # training set: label DataFrame + features joined BY KEY
# MAGIC training_set = fe.create_training_set(
# MAGIC     df=spark.table("customers_churn").select("customer_id", "churned"),
# MAGIC     feature_lookups=[FeatureLookup(
# MAGIC         table_name="dev_catalog.ml_assoc.customer_features",
# MAGIC         lookup_key="customer_id",
# MAGIC     )],
# MAGIC     label="churned",
# MAGIC )
# MAGIC model_df = training_set.load_df()
# MAGIC
# MAGIC # log the model WITH its feature metadata -> inference can look up
# MAGIC # features itself, callers pass only the keys
# MAGIC fe.log_model(model=model, artifact_path="model",
# MAGIC              flavor=mlflow.sklearn, training_set=training_set,
# MAGIC              registered_model_name="dev_catalog.ml_assoc.churn_model")
# MAGIC ```

# COMMAND ----------

try:
    from databricks.feature_engineering import FeatureEngineeringClient  # noqa: F401
    print("databricks-feature-engineering available — the cells above are runnable here")
except ImportError:
    print("client not installed (needs ML Runtime / %pip install databricks-feature-engineering) — "
          "the API shapes above are the exam material either way")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - A feature table = UC Delta table **with a primary key** (add a
# MAGIC >   timeseries column for point-in-time lookups). It's not a separate
# MAGIC >   storage system — governance, lineage, and grants are plain UC.
# MAGIC > - `create_training_set` joins features to your label DataFrame **by
# MAGIC >   key** — the point is *reuse without re-deriving* (and no
# MAGIC >   training/serving skew).
# MAGIC > - `fe.log_model(...)` packages the lookups with the model, so batch
# MAGIC >   scoring needs **only keys**, not feature values.
# MAGIC > - Why bother: **consistency** across teams and across train/serve —
# MAGIC >   "two teams compute tenure differently" is the symptom that points
# MAGIC >   here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup — keep `customer_features` (sections 03–04 reuse it)
