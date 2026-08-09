# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — UDFs and higher-order functions
# MAGIC
# MAGIC The exam tests two things here: **when a UDF is the wrong answer** (usually)
# MAGIC and the mechanics of each UDF flavor when you genuinely need one.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 1: built-ins beat UDFs
# MAGIC Python UDFs serialize every row Python↔JVM and are invisible to the optimizer.
# MAGIC Same logic, three ways — always prefer the first:

# COMMAND ----------

# 1. Built-in (optimizer sees it, no serialization) — the right answer
masked = spark.table("gold_customer_revenue").withColumn(  # noqa: F821
    "masked_email",
    F.concat(F.substring("email", 1, 2), F.lit("***@"), F.substring_index("email", "@", -1)),
)
display(masked.select("email", "masked_email").limit(5))  # noqa: F821

# COMMAND ----------

# 2. Python UDF — same result, row-at-a-time serialization
@F.udf("string")
def mask_email_udf(email):
    if email is None or "@" not in email:
        return None
    local, domain = email.split("@", 1)
    return f"{local[:2]}***@{domain}"

display(  # noqa: F821
    spark.table("gold_customer_revenue")  # noqa: F821
    .select("email", mask_email_udf("email").alias("masked_email"))
    .limit(5)
)

# COMMAND ----------

# 3. pandas UDF — vectorized with Arrow: batches, not rows.
#    Still opaque to the optimizer, but far less overhead than a plain UDF.
import pandas as pd  # noqa: E402
from pyspark.sql.functions import pandas_udf  # noqa: E402


@pandas_udf("string")
def mask_email_pandas(emails: pd.Series) -> pd.Series:
    parts = emails.str.split("@", n=1, expand=True)
    return parts[0].str[:2] + "***@" + parts[1]


display(  # noqa: F821
    spark.table("gold_customer_revenue")  # noqa: F821
    .select("email", mask_email_pandas("email").alias("masked_email"))
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam ranking (fastest → slowest):** built-in > pandas UDF > Python UDF.
# MAGIC > Scala UDFs avoid the serialization penalty but still block optimization.
# MAGIC > Also: Python UDFs receive **None** for NULL inputs — handle it or crash.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL UDFs — reusable, governed by Unity Catalog, optimizer-friendly
# MAGIC Unlike Python UDFs, SQL UDFs are inlined into the plan. They live in the
# MAGIC catalog, so they're `GRANT`-able and show up in `information_schema`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION order_size(amount DECIMAL(10,2))
# MAGIC RETURNS STRING
# MAGIC RETURN CASE WHEN amount >= 500 THEN 'large'
# MAGIC             WHEN amount >= 100 THEN 'medium'
# MAGIC             ELSE 'small' END;
# MAGIC
# MAGIC SELECT order_size(amount) AS size, count(*) AS orders, sum(amount) AS revenue
# MAGIC FROM silver_orders
# MAGIC GROUP BY ALL
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Higher-order functions — loop over ARRAY columns without exploding
# MAGIC Build an array per customer (their order amounts), then `transform`,
# MAGIC `filter`, and `aggregate` it. These run inside the engine — no UDF needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_orders AS (
# MAGIC   SELECT customer_id, collect_list(amount) AS amounts
# MAGIC   FROM silver_orders
# MAGIC   GROUP BY customer_id
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   size(amounts)                                                   AS order_count,
# MAGIC   transform(amounts, a -> round(a * 1.1, 2))                      AS with_10pct_tax,
# MAGIC   filter(amounts, a -> a >= 100)                                  AS big_orders,
# MAGIC   aggregate(amounts, cast(0 AS decimal(20,2)), (acc, a) -> acc + a) AS total,
# MAGIC   exists(amounts, a -> a > 500)                                   AS has_giant_order
# MAGIC FROM customer_orders
# MAGIC ORDER BY total DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** `aggregate()`'s start value sets the result type. Starting
# MAGIC > from `0` (an INT) with DECIMAL elements is a type-mismatch error — cast the
# MAGIC > initial value, exactly as above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS order_size
