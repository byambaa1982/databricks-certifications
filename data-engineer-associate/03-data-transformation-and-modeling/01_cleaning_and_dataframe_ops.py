# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze → silver: cleaning and DataFrame operations
# MAGIC
# MAGIC The heart of the section: casts that fail quietly, conditional logic,
# MAGIC dedup done right, and every join type with the row counts to prove them.
# MAGIC **Predict each cell's output before running it** — that's the exam skill.
# MAGIC Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Casting — bad values become NULL, not errors

# COMMAND ----------

orders = (
    spark.table("bronze_orders_assoc")  # noqa: F821
    .withColumn("amount", F.col("amount").cast("decimal(10,2)"))  # "N/A" -> NULL
    .withColumn("order_ts", F.to_timestamp("order_ts"))           # "not-a-date" -> NULL
    .withColumn("status", F.lower(F.trim("status")))              # "Completed " -> "completed"
)
display(orders.filter("amount IS NULL OR order_ts IS NULL"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** `cast()` / `to_timestamp()` return **NULL** on bad input —
# MAGIC > no exception (unless ANSI mode is on). `try_cast` is the explicit
# MAGIC > NULL-on-failure version. Cleaning = cast, then **decide** what to do with
# MAGIC > the NULLs: `dropna`/`filter` to remove, `fillna`/`coalesce` to default.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conditional columns: `when` / `otherwise`

# COMMAND ----------

display(  # noqa: F821
    orders.withColumn(
        "order_size",
        F.when(F.col("amount") >= 500, "large")
         .when(F.col("amount") >= 100, "medium")
         .otherwise("small"),   # omit this and unmatched rows get NULL
    ).groupBy("order_size").count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication — three tools, one trap

# COMMAND ----------

d = orders
print(f"rows:                        {d.count()}")
print(f"distinct():                  {d.distinct().count()}   <- exact duplicates removed")
print(f"dropDuplicates(['order_id']): {d.dropDuplicates(['order_id']).count()}   <- one row per key, ARBITRARY row")

# COMMAND ----------

# latest-per-key, deterministically — the pattern that's actually correct
from pyspark.sql.window import Window  # noqa: E402

latest = Window.partitionBy("order_id").orderBy(F.desc("order_ts"))
deduped = d.withColumn("rn", F.row_number().over(latest)).filter("rn = 1").drop("rn")
print(f"row_number dedup: {deduped.count()} rows, keeping the LATEST per key")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The clean silver table

# COMMAND ----------

silver = (
    deduped
    .filter("customer_id IS NOT NULL AND amount IS NOT NULL AND order_ts IS NOT NULL")
    .withColumn("order_date", F.to_date("order_ts"))
)
silver.write.mode("overwrite").saveAsTable("silver_orders_assoc")
print(f"silver_orders_assoc: {silver.count()} rows (from 14 bronze)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins — counts prove the semantics
# MAGIC Orders reference customer **106** (not in the dim); customer **107** has
# MAGIC no orders. Watch where each one appears:

# COMMAND ----------

customers = spark.table("bronze_customers_assoc")  # noqa: F821

for how in ["inner", "left", "full", "left_semi", "left_anti"]:
    n = silver.join(customers, "customer_id", how).count()
    print(f"{how:10s} -> {n} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `left_semi` = filter, keeps **left columns only** (orders whose customer
# MAGIC >   exists); `left_anti` = the complement (orders with **no** matching
# MAGIC >   customer — the orphan-finding tool).
# MAGIC > - `full` keeps everything from both sides, NULL-padded (customer 107
# MAGIC >   appears with NULL order columns).
# MAGIC > - Joining on duplicate keys **fans out** (m×n rows) — dedupe first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unions — position vs name

# COMMAND ----------

jan = silver.filter("order_date < '2024-02-01'")
feb = silver.filter("order_date >= '2024-02-01'").select(*reversed(silver.columns))  # scrambled column order!

print(f"union() would silently mis-align the scrambled columns (positional)")
print(f"unionByName(): {jan.unionByName(feb.select(silver.columns)).count()} rows, aligned by NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** `union()` is **positional** — same column *count* is
# MAGIC > enough, and mismatched orders produce garbage, not errors.
# MAGIC > `unionByName()` aligns by name (and `allowMissingColumns=True` fills
# MAGIC > gaps with NULL). SQL's `UNION` deduplicates; `UNION ALL` doesn't —
# MAGIC > DataFrame `union()` behaves like `UNION ALL`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup — keep `silver_orders_assoc` (notebook 02 uses it)
