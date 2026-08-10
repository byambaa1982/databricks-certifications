# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — a deterministic churn dataset
# MAGIC
# MAGIC Builds `dev_catalog.ml_assoc.customers_churn`: 1,000 synthetic customers
# MAGIC with numeric + categorical features, a few NULLs and outliers (section 02
# MAGIC needs them), and a `churned` label whose signal is real enough for models
# MAGIC to learn. Fully deterministic — values derive from `hash(id)`, so every
# MAGIC run rebuilds the identical table.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# hash-derived pseudo-randomness: deterministic, no seeds to manage
h = lambda col, mod: F.abs(F.hash(F.col(col).cast("string"))) % mod  # noqa: E731

base = (
    spark.range(1, 1001).withColumnRenamed("id", "customer_id")  # noqa: F821
    .withColumn("age", (h("customer_id", 45) + 21).cast("int"))
    .withColumn("tenure_months", (h("customer_id", 70) + F.lit(1)).cast("int"))
    .withColumn("monthly_spend",
                F.round(30 + h("customer_id", 4000) / 22.0, 2))
    .withColumn("support_tickets", (h("customer_id", 8)).cast("int"))
    .withColumn("plan",
                F.element_at(F.array(F.lit("basic"), F.lit("standard"), F.lit("premium")),
                             (h("customer_id", 3) + 1).cast("int")))
)

# label with real signal: short tenure + many tickets + basic plan -> churn-y
score = (
    (70 - F.col("tenure_months")) * 0.8
    + F.col("support_tickets") * 9
    + F.when(F.col("plan") == "basic", 18).otherwise(0)
    + (h("customer_id", 30))          # noise
)
labeled = base.withColumn("churned", (score > 75).cast("int"))

# blemishes for section 02: NULL spend rows and two absurd outliers
final = (
    labeled
    .withColumn("monthly_spend",
                F.when(h("customer_id", 25) == 0, None)         # ~4% NULLs
                 .otherwise(F.col("monthly_spend")))
    .withColumn("monthly_spend",
                F.when(F.col("customer_id").isin(77, 501), 99999.0)  # outliers
                 .otherwise(F.col("monthly_spend")))
)
final.write.mode("overwrite").saveAsTable("customers_churn")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

df = spark.table("customers_churn")  # noqa: F821
print(f"rows: {df.count()}")
print(f"churn rate: {df.agg(F.avg('churned')).collect()[0][0]:.2%}")
print(f"NULL spend: {df.filter('monthly_spend IS NULL').count()} rows")
print(f"outliers:   {df.filter('monthly_spend > 10000').count()} rows")
display(df.limit(5))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Ready. The class imbalance (~2:1) is intentional — section 03 addresses
# MAGIC it. Open `01_ml_runtime_and_automl` next.
