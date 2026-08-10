# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Spark ML pipelines, cross-validation, and imbalance
# MAGIC
# MAGIC The Spark ML mental model is two words — **estimator** (has `.fit()`,
# MAGIC returns a transformer) and **transformer** (has `.transform()`) — and a
# MAGIC `Pipeline` chains them so the leakage rule from section 02 enforces
# MAGIC itself. Run section 01's `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

df = spark.table("customers_churn").na.drop(subset=["monthly_spend"])  # noqa: F821
train, test = df.randomSplit([0.75, 0.25], seed=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The full pipeline — every prep step inside

# COMMAND ----------

from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.classification import RandomForestClassifier  # noqa: E402
from pyspark.ml.feature import (  # noqa: E402
    OneHotEncoder, StringIndexer, VectorAssembler,
)

pipeline = Pipeline(stages=[
    StringIndexer(inputCol="plan", outputCol="plan_idx", handleInvalid="keep"),
    OneHotEncoder(inputCols=["plan_idx"], outputCols=["plan_ohe"]),
    VectorAssembler(
        inputCols=["age", "tenure_months", "support_tickets", "monthly_spend", "plan_ohe"],
        outputCol="features",
    ),
    RandomForestClassifier(labelCol="churned", featuresCol="features",
                           numTrees=100, maxDepth=5, seed=42),
])

model = pipeline.fit(train)          # estimator -> PipelineModel (a transformer)
predictions = model.transform(test)  # every stage applied, train-fitted
display(predictions.select("churned", "prediction", "probability").limit(5))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `Pipeline` is an **estimator**; `pipeline.fit()` returns a
# MAGIC >   `PipelineModel` (a **transformer**). Prep stages fit on exactly the
# MAGIC >   data the pipeline fits on — leakage handled by construction.
# MAGIC > - `RandomForestClassifier` = estimator; the fitted
# MAGIC >   `RandomForestClassificationModel` = transformer. Naming the category
# MAGIC >   of a given class IS a question format.
# MAGIC > - The whole `PipelineModel` is what you log to MLflow — inference then
# MAGIC >   takes raw columns, not pre-engineered vectors.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-validation — and its cheaper sibling

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator  # noqa: E402
from pyspark.ml.tuning import (  # noqa: E402
    CrossValidator, ParamGridBuilder, TrainValidationSplit,
)

rf = pipeline.getStages()[-1]
grid = (ParamGridBuilder()
        .addGrid(rf.maxDepth, [3, 5, 8])
        .addGrid(rf.numTrees, [50, 100])
        .build())
evaluator = BinaryClassificationEvaluator(labelCol="churned", metricName="areaUnderROC")

cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid,
                    evaluator=evaluator, numFolds=3, parallelism=2, seed=42)
cv_model = cv.fit(train)
print(f"best AUC (3-fold avg): {max(cv_model.avgMetrics):.3f}")
print(f"held-out test AUC:     {evaluator.evaluate(cv_model.transform(test)):.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `CrossValidator` trains **k × |grid|** models (here 3 × 6 = 18) —
# MAGIC >   robust but expensive. `TrainValidationSplit` trains |grid| models on
# MAGIC >   one split — cheaper, noisier: the right answer when compute is the
# MAGIC >   constraint or data is large.
# MAGIC > - CV folds validate on **training data**; the untouched **test** set is
# MAGIC >   still needed for the final honest number.
# MAGIC > - `parallelism` fits candidate models concurrently across the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Class imbalance — our label is ~2:1 on purpose
# MAGIC
# MAGIC | Approach | How | Note |
# MAGIC |---|---|---|
# MAGIC | **class weights** | `weightCol` with higher weight on the minority | no data change; first thing to try |
# MAGIC | resample | oversample minority / undersample majority (train only!) | resampling the *test* set fakes the metric |
# MAGIC | threshold tuning | move the 0.5 cutoff using the PR curve | free, post-training |
# MAGIC | better metric | stop optimizing accuracy (next notebook) | always |

# COMMAND ----------

weighted = train.withColumn(
    "w", F.when(F.col("churned") == 1, 2.0).otherwise(1.0)
)
rf_w = RandomForestClassifier(labelCol="churned", featuresCol="features",
                              weightCol="w", numTrees=100, seed=42)
# (swap into the pipeline exactly like the unweighted version)
print("weighted estimator ready — weightCol is the exam's named lever")
