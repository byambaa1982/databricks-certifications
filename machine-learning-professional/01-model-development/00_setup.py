# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — churn data with timestamps
# MAGIC
# MAGIC Professional topics need what the Associate dataset lacked: **event
# MAGIC time**. Builds `dev_catalog.ml_pro` with a customer table, a
# MAGIC **time-versioned feature table** (for point-in-time lookups), and a
# MAGIC label table with observation dates. Hash-deterministic; re-run to reset.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

h = lambda col, mod: F.abs(F.hash(F.col(col).cast("string"))) % mod  # noqa: E731

# COMMAND ----------

# customers: static attributes + a region for per-group model training
customers = (
    spark.range(1, 801).withColumnRenamed("id", "customer_id")  # noqa: F821
    .withColumn("region",
                F.element_at(F.array(F.lit("north"), F.lit("south"),
                                     F.lit("east"), F.lit("west")),
                             (h("customer_id", 4) + 1).cast("int")))
    .withColumn("plan",
                F.element_at(F.array(F.lit("basic"), F.lit("standard"), F.lit("premium")),
                             (h("customer_id", 3) + 1).cast("int")))
)
customers.write.mode("overwrite").saveAsTable("customers")

# feature history: TWO snapshots per customer (Jan + Feb) -> point-in-time matters
snap = lambda ts, drift: (  # noqa: E731
    customers
    .withColumn("feature_ts", F.lit(ts).cast("timestamp"))
    .withColumn("tenure_months", (h("customer_id", 70) + F.lit(1) + F.lit(drift)).cast("int"))
    .withColumn("support_tickets", (h("customer_id", 8) + F.lit(drift)).cast("int"))
    .withColumn("monthly_spend", F.round(30 + h("customer_id", 4000) / 22.0 + drift * 5, 2))
    .select("customer_id", "feature_ts", "tenure_months", "support_tickets", "monthly_spend")
)
snap("2024-01-01", 0).unionByName(snap("2024-02-01", 1)) \
    .write.mode("overwrite").saveAsTable("customer_features_history")

spark.sql("ALTER TABLE customer_features_history ALTER COLUMN customer_id SET NOT NULL")  # noqa: F821
spark.sql("ALTER TABLE customer_features_history ALTER COLUMN feature_ts SET NOT NULL")  # noqa: F821

# labels observed mid-January: correct lookups must use the JAN snapshot
score = (70 - h("customer_id", 70)) * 0.8 + h("customer_id", 8) * 9 + h("customer_id", 30)
labels = (
    customers.select("customer_id")
    .withColumn("observation_ts", F.lit("2024-01-15").cast("timestamp"))
    .withColumn("churned", (score > 75).cast("int"))
)
labels.write.mode("overwrite").saveAsTable("churn_labels")

print("customers:                 800 rows (4 regions)")
print("customer_features_history: 1600 rows (2 timestamped snapshots)")
print("churn_labels:              800 rows observed 2024-01-15")

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Ready. The trap is baked in: labels observed **Jan 15** must join the
# MAGIC **Jan 1** features — joining the latest (Feb) snapshot is *future
# MAGIC leakage*, and notebook 03 makes the point-in-time lookup do it right.
