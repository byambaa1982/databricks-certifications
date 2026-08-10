# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Feature prep: encoding, transforms, and the split
# MAGIC
# MAGIC Turning cleaned columns into model food: when one-hot encoding is right
# MAGIC (and when it isn't), log transforms for skew, scaling, and the
# MAGIC train/test split done leakage-free. Run section 01's `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

df = spark.table("customers_churn").na.drop(subset=["monthly_spend"])  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Categorical encoding: StringIndexer → OneHotEncoder

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder, StringIndexer  # noqa: E402

indexer = StringIndexer(inputCol="plan", outputCol="plan_idx", handleInvalid="keep")
encoder = OneHotEncoder(inputCols=["plan_idx"], outputCols=["plan_ohe"])

indexed = indexer.fit(df).transform(df)
encoded = encoder.fit(indexed).transform(indexed)
display(encoded.select("plan", "plan_idx", "plan_ohe").distinct())  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps — when one-hot is the WRONG answer:**
# MAGIC > - **High-cardinality** categoricals (zip codes, user ids) → thousands of
# MAGIC >   sparse columns; prefer target/hash encoding or embeddings.
# MAGIC > - **Tree-based models** (random forest, XGBoost) handle indexed
# MAGIC >   categoricals; OHE mostly helps **linear** models, which would
# MAGIC >   otherwise read `plan_idx = 2 > 1` as an *order* that isn't there.
# MAGIC > - `handleInvalid="keep"` reserves a bucket for **unseen categories at
# MAGIC >   inference** — without it, new categories crash the pipeline.
# MAGIC > - `StringIndexer` orders labels by **frequency** (most frequent = 0.0).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log transform — for right-skewed positives

# COMMAND ----------

display(  # noqa: F821
    df.select(
        F.col("monthly_spend"),
        F.log1p("monthly_spend").alias("log_spend"),  # log1p handles 0 safely
    ).summary("min", "25%", "50%", "75%", "max")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Skewed spend compresses into a near-symmetric range — linear models and
# MAGIC distance-based methods behave better. Remember to **invert** (`expm1`)
# MAGIC when a *regression target* was log-transformed and you report predictions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scaling and the leakage-free split

# COMMAND ----------

from pyspark.ml.feature import StandardScaler, VectorAssembler  # noqa: E402

train, test = df.randomSplit([0.75, 0.25], seed=42)
print(f"train {train.count()} / test {test.count()}")

assembler = VectorAssembler(
    inputCols=["age", "tenure_months", "support_tickets", "monthly_spend"],
    outputCol="features_raw",
)
scaler = StandardScaler(inputCol="features_raw", outputCol="features")

# fit the scaler on TRAIN only; transform both — that's the leakage rule
scaler_model = scaler.fit(assembler.transform(train))
train_ready = scaler_model.transform(assembler.transform(train))
test_ready = scaler_model.transform(assembler.transform(test))
display(train_ready.select("features_raw", "features").limit(3))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - Fit scalers/imputers/encoders on **train**, apply to test — fitting on
# MAGIC >   the full data is leakage (the recurring theme). A `Pipeline` (section
# MAGIC >   03) makes this automatic.
# MAGIC > - Scaling matters for **distance/gradient** methods (k-NN, SVM, linear
# MAGIC >   with regularization); trees don't care — "scale features for random
# MAGIC >   forest" is a distractor.
# MAGIC > - `randomSplit` with a **seed** is how you make the split reproducible;
# MAGIC >   exact proportions are approximate on distributed data.
# MAGIC > - Spark ML wants features as a single **vector column** — that's
# MAGIC >   `VectorAssembler`'s whole job.
