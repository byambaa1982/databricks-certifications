# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — EDA and cleaning: stats, outliers, missing values
# MAGIC
# MAGIC The dataset from `00_setup` has planted problems — NULL spend and two
# MAGIC absurd outliers — so every technique here visibly changes the numbers.
# MAGIC Run section 01's `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

df = spark.table("customers_churn")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary statistics — three tools, know their differences

# COMMAND ----------

# summary() adds percentiles over describe() — that's the exam-level difference
display(df.select("age", "tenure_months", "monthly_spend", "support_tickets").summary())  # noqa: F821

# COMMAND ----------

# dbutils.data.summarize renders an interactive profile (distributions per column)
try:
    dbutils.data.summarize(df)  # noqa: F821
except Exception as e:
    print(f"summarize unavailable here ({type(e).__name__}) — it renders an "
          "interactive column profile in Databricks notebooks")

# COMMAND ----------

# MAGIC %md
# MAGIC > Spot the damage in `summary()`: max `monthly_spend` is 99999 (outliers)
# MAGIC > and its count is lower than the others (NULLs don't count).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outliers — the two textbook filters

# COMMAND ----------

stats = df.select(
    F.mean("monthly_spend").alias("mu"),
    F.stddev("monthly_spend").alias("sigma"),
    F.expr("percentile(monthly_spend, 0.25)").alias("q1"),
    F.expr("percentile(monthly_spend, 0.75)").alias("q3"),
).collect()[0]

iqr = stats.q3 - stats.q1
sd_kept = df.filter(F.col("monthly_spend").between(stats.mu - 3 * stats.sigma,
                                                   stats.mu + 3 * stats.sigma))
iqr_kept = df.filter(F.col("monthly_spend").between(stats.q1 - 1.5 * iqr,
                                                    stats.q3 + 1.5 * iqr))

print(f"rows with spend:   {df.filter('monthly_spend IS NOT NULL').count()}")
print(f"±3 stddev keeps:   {sd_kept.count()}")
print(f"1.5×IQR keeps:     {iqr_kept.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** the stddev rule uses μ and σ that the outliers themselves
# MAGIC > inflate — with extreme outliers it can keep them. The **IQR rule is
# MAGIC > robust** (quartiles barely move). "Which filter for heavily skewed data
# MAGIC > with extreme values?" → IQR.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Missing values — the decision, then the tool

# COMMAND ----------

# the decision table, executed three ways on the NULL spend column
n = df.count()
print(f"drop rows:    keeps {df.na.drop(subset=['monthly_spend']).count()} of {n}")

median_spend = df.approxQuantile("monthly_spend", [0.5], 0.01)[0]
print(f"impute median {median_spend:.2f}: keeps all {n} rows, robust to the outliers")
imputed = df.na.fill({"monthly_spend": median_spend})
print(f"NULLs after impute: {imputed.filter('monthly_spend IS NULL').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC | Situation | Move |
# MAGIC |---|---|
# MAGIC | few rows affected, data plentiful | drop (`na.drop`) |
# MAGIC | numeric, roughly symmetric | impute **mean** |
# MAGIC | numeric, skewed / outliers present | impute **median** (our case) |
# MAGIC | categorical | impute **mode** or an explicit `"unknown"` |
# MAGIC | missingness itself is signal | add a `was_missing` indicator column too |
# MAGIC
# MAGIC The pipeline version is `pyspark.ml.feature.Imputer` (strategy = mean /
# MAGIC median / mode) — as a pipeline stage it learns the fill value on
# MAGIC **training data only** and applies it everywhere, which is the leakage-safe
# MAGIC answer (section 03 assembles it).
# MAGIC
# MAGIC > **Exam trap:** computing the fill value on the *whole* dataset before
# MAGIC > splitting leaks test information into training. Fit imputers (and
# MAGIC > scalers) on the training split — i.e., inside the Pipeline.
