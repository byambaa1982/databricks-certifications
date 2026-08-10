# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Spark SQL: the same engine, two syntaxes
# MAGIC
# MAGIC The exam loves showing a DataFrame snippet and a SQL snippet and asking
# MAGIC if they're equivalent (usually yes — same optimizer, same plan). This
# MAGIC notebook builds tiny tables and does everything both ways.

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402

orders = spark.createDataFrame(  # noqa: F821
    [(1, 101, 120.5, "done"), (2, 102, 39.9, "done"), (3, 101, 640.0, "open"),
     (4, 103, 89.0, "done"), (5, None, 55.2, "done")],
    "order_id INT, customer_id INT, amount DOUBLE, status STRING",
)
customers = spark.createDataFrame(  # noqa: F821
    [(101, "Bat", "gold"), (102, "Naran", "silver"), (104, "Tuya", "bronze")],
    "customer_id INT, name STRING, tier STRING",
)
orders.createOrReplaceTempView("orders_v")
customers.createOrReplaceTempView("customers_v")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views: the bridge between the two worlds
# MAGIC
# MAGIC | | scope | referenced as |
# MAGIC |---|---|---|
# MAGIC | `createOrReplaceTempView` | this SparkSession | `orders_v` |
# MAGIC | `createOrReplaceGlobalTempView` | all sessions on the app | `global_temp.orders_v` |
# MAGIC | `saveAsTable` | catalog — persists | `catalog.schema.orders` |
# MAGIC
# MAGIC > **Exam trap:** global temp views live in the **`global_temp`** database
# MAGIC > and MUST be qualified with it — forgetting the prefix is the planted
# MAGIC > error in "why does this fail".

# COMMAND ----------

# same query, both syntaxes — identical plans
sql_way = spark.sql("""
    SELECT c.tier, sum(o.amount) AS revenue
    FROM orders_v o JOIN customers_v c ON o.customer_id = c.customer_id
    WHERE o.status = 'done'
    GROUP BY c.tier
""")  # noqa: F821
df_way = (
    orders.filter(F.col("status") == "done")
    .join(customers, "customer_id")
    .groupBy("tier")
    .agg(F.sum("amount").alias("revenue"))
)
print("results match:", sorted(sql_way.collect()) == sorted(df_way.collect()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The function families the exam samples from
# MAGIC
# MAGIC | Family | Names to recognize |
# MAGIC |---|---|
# MAGIC | string | `concat`, `substring`, `split`, `trim`, `upper/lower`, `regexp_replace`, `regexp_extract` |
# MAGIC | date/time | `to_date`, `to_timestamp`, `date_add`, `months_between`, `date_trunc`, `year/month/dayofmonth`, `unix_timestamp` |
# MAGIC | conditional | `when/otherwise`, `coalesce`, `nullif`, `ifnull`, CASE WHEN |
# MAGIC | aggregate | `count/sum/avg/min/max`, `countDistinct`, `collect_list/collect_set`, `first/last` |
# MAGIC | array/map | `explode`, `array_contains`, `size`, `transform`, `filter` |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - `collect_list` keeps duplicates, `collect_set` doesn't — and neither
# MAGIC >   guarantees order.
# MAGIC > - NULL rules ride again: `count(col)` skips NULLs, `concat` with a NULL
# MAGIC >   returns NULL (`concat_ws` doesn't).
# MAGIC > - SQL `UNION` dedupes, `UNION ALL` doesn't; DataFrame `union()` behaves
# MAGIC >   like `UNION ALL`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registering a Python UDF for SQL

# COMMAND ----------

@F.udf("string")
def tier_label(tier):
    return None if tier is None else f"{tier}-member"

spark.udf.register("tier_label_sql", tier_label)  # noqa: F821
display(spark.sql("SELECT name, tier_label_sql(tier) AS label FROM customers_v"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `spark.udf.register("name", fn)` is what makes a Python UDF callable
# MAGIC >   **from SQL**; the `@udf` decorator alone only covers DataFrame use.
# MAGIC > - Declare the **return type**; default is string.
# MAGIC > - UDFs are opaque to the optimizer and serialize per row — built-ins
# MAGIC >   first (the ranking from the DE tracks applies verbatim).
