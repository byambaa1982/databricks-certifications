# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — The DataFrame API, drilled
# MAGIC
# MAGIC The heaviest section is pure API precision: which call, which argument,
# MAGIC what comes back. Everything here is a mini-drill — **predict each cell**.

# COMMAND ----------

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, IntegerType, StringType, StructField, StructType,
)

schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
])
orders = spark.createDataFrame(  # noqa: F821
    [(1, 101, 120.5, "done"), (2, 102, None, "done"), (3, 101, 640.0, "open"),
     (4, 103, 89.0, "DONE "), (2, 102, None, "done")],
    schema,
)
orders.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column selection & creation — the five spellings

# COMMAND ----------

a = orders.select("order_id", "amount")
b = orders.select(F.col("order_id"), F.col("amount"))
c = orders.select(orders.order_id, orders["amount"])
d = orders.selectExpr("order_id", "amount * 1.1 AS with_tax")   # SQL expressions
e = orders.withColumn("with_tax", F.col("amount") * 1.1)        # add/replace ONE column
print("a/b/c identical; selectExpr takes SQL strings; withColumn keeps all columns")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `withColumn("x", ...)` **replaces** x if it exists; `withColumnRenamed`
# MAGIC >   renames; `drop("x")` removes (and dropping a non-existent column is a
# MAGIC >   silent no-op, not an error).
# MAGIC > - Mixing a string and an expression — `select("amount * 1.1")` — fails:
# MAGIC >   strings are column *names*; expressions need `expr()`/`selectExpr` or
# MAGIC >   Column objects.
# MAGIC > - `filter` and `where` are aliases; both accept a Column
# MAGIC >   (`F.col("x") > 5`, with `&`/`|`/`~` and parentheses) or a SQL string.

# COMMAND ----------

# rows: dedupe, nulls, sort
print(f"rows:                        {orders.count()}")
print(f"dropDuplicates():            {orders.dropDuplicates().count()}")
print(f"dropDuplicates(['order_id']): {orders.dropDuplicates(['order_id']).count()}")
print(f"na.drop (any null col):      {orders.na.drop().count()}")
print(f"na.fill(0.0) nulls left:     {orders.na.fill(0.0).filter('amount IS NULL').count()}")

display(orders.orderBy(F.desc_nulls_last("amount")).limit(3))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `na.fill(0.0)` fills only columns of a **matching type** (here the
# MAGIC >   double column); `na.fill({"amount": 0.0})` targets by name.
# MAGIC > - `orderBy`/`sort` are aliases; `desc_nulls_last` / `asc_nulls_first`
# MAGIC >   control NULL placement — default: NULLs first ascending, last
# MAGIC >   descending.
# MAGIC > - `limit(n)` is a transformation; `take(n)` is an action returning Rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## groupBy, joins, union — the count-prediction machine

# COMMAND ----------

customers = spark.createDataFrame(  # noqa: F821
    [(101, "Bat"), (102, "Naran"), (104, "Tuya")], "customer_id INT, name STRING")

clean = (
    orders.dropDuplicates(["order_id"])
    .withColumn("status", F.lower(F.trim("status")))
)
display(  # noqa: F821
    clean.groupBy("status").agg(
        F.count("*").alias("rows"),
        F.count("amount").alias("non_null_amounts"),
        F.sum("amount").alias("revenue"),
    )
)
for how in ["inner", "left", "left_anti"]:
    print(f"{how:10s}: {clean.join(customers, 'customer_id', how).count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC > Join argument forms: `on="customer_id"` (string — one output key
# MAGIC > column) vs `on=a.customer_id == b.customer_id` (expression — BOTH key
# MAGIC > columns survive, ambiguous-name errors follow). The string form is the
# MAGIC > tidy default; the expression form is required for differing names.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and write — modes are the tested part

# COMMAND ----------

import tempfile  # noqa: E402

path = tempfile.mkdtemp() + "/orders_parquet"
clean.write.mode("overwrite").partitionBy("status").parquet(path)
back = spark.read.parquet(path)  # noqa: F821
print(f"round trip: {back.count()} rows; partition column readable: "
      f"{sorted(r.status for r in back.select('status').distinct().collect())}")

# COMMAND ----------

# MAGIC %md
# MAGIC | Write mode | On existing data |
# MAGIC |---|---|
# MAGIC | `error`/`errorifexists` (default) | fails |
# MAGIC | `overwrite` | replaces |
# MAGIC | `append` | adds (duplicates possible) |
# MAGIC | `ignore` | silently does nothing |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - The **default** write mode errors on existing paths — "why did the
# MAGIC >   second run fail" is usually this.
# MAGIC > - Reader options quiz: CSV `header`/`inferSchema`/`sep`, JSON
# MAGIC >   `multiLine`, everywhere `mode` (PERMISSIVE default) and `.schema()`
# MAGIC >   beats inference (one fewer pass).
# MAGIC > - `partitionBy` on write creates directory partitions; it is unrelated
# MAGIC >   to `repartition` (in-memory distribution).
