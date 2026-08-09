# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — build a deliberately dirty bronze table
# MAGIC
# MAGIC Cleansing needs something to cleanse. This setup takes the `bronze_orders`
# MAGIC clone from **section 01's `00_setup`** (run that first if you haven't) and
# MAGIC produces `bronze_orders_dirty`: the same data plus **exact duplicates**,
# MAGIC **NULL keys**, and **impossible amounts** — the three failure modes every
# MAGIC notebook in this folder attacks. Re-run anytime to reset.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preconditions

# COMMAND ----------

existing = {t.tableName for t in spark.sql("SHOW TABLES").collect()}  # noqa: F821
assert "bronze_orders" in existing, (
    f"{CATALOG}.{SCHEMA}.bronze_orders not found — run section 01's 00_setup "
    "first (it clones the medallion tables into this schema)."
)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inject the dirt
# MAGIC Everything is derived from real rows (schema stays identical, whatever the
# MAGIC clone's exact columns are), and every injection is deterministic:
# MAGIC - 3 lowest `order_id` rows appended again → **exact duplicates**
# MAGIC - 2 rows with `customer_id` nulled → **missing keys**
# MAGIC - 2 rows with negative / absurd `amount` → **out-of-range values**

# COMMAND ----------

base = spark.table("bronze_orders")  # noqa: F821
types = dict(base.dtypes)

dupes = base.orderBy("order_id").limit(3)

null_keys = (
    base.orderBy(F.desc("order_id")).limit(2)
    .withColumn("customer_id", F.lit(None).cast(types["customer_id"]))
)

bad_amounts = (
    base.orderBy("order_id").limit(2)
    .withColumn("order_id", F.col("order_id") + 900000)  # new ids: bad values, not duplicates
    .withColumn(
        "amount",
        F.when(F.monotonically_increasing_id() % 2 == 0, F.lit(-49.99))
         .otherwise(F.lit(9999999.0))
         .cast(types["amount"]),
    )
)

dirty = base.unionByName(dupes).unionByName(null_keys).unionByName(bad_amounts)
dirty.write.mode("overwrite").saveAsTable("bronze_orders_dirty")

print(f"bronze_orders:       {base.count()} rows")
print(f"bronze_orders_dirty: {spark.table('bronze_orders_dirty').count()} rows (+7 injected)")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — see the dirt you just made

# COMMAND ----------

d = spark.table("bronze_orders_dirty")  # noqa: F821
display(  # noqa: F821
    spark.createDataFrame(  # noqa: F821
        [
            ("exact duplicate rows", d.count() - d.distinct().count()),
            ("NULL customer_id", d.filter("customer_id IS NULL").count()),
            ("amount <= 0 or > 1M", d.filter("amount <= 0 OR amount > 1000000").count()),
        ],
        ["problem", "rows"],
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Ready. Open `01_constraints_and_enforcement` next.
