# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Aggregations: the NULL rules and the reshapes
# MAGIC
# MAGIC Aggregation questions are mostly about **what NULL does to each function**
# MAGIC and which construct filters where. Requires `silver_orders_assoc` from
# MAGIC notebook 01 (run `00_setup` then notebook 01 first).

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## The NULL rules — memorize this cell's output
# MAGIC `count(*)` counts rows; `count(col)` skips NULLs; `sum`/`avg` skip NULLs;
# MAGIC `sum` over **only** NULLs is NULL, not 0. We aggregate the raw bronze
# MAGIC (which still has its NULL amounts) to see it:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   status,
# MAGIC   count(*)                              AS all_rows,
# MAGIC   count(try_cast(amount AS decimal(10,2))) AS non_null_amounts,
# MAGIC   sum(try_cast(amount AS decimal(10,2)))   AS revenue,
# MAGIC   round(avg(try_cast(amount AS decimal(10,2))), 2) AS avg_amount
# MAGIC FROM bronze_orders_assoc
# MAGIC GROUP BY status

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `count(*) <> count(col)` whenever the column has NULLs — questions show
# MAGIC >   both and ask why they differ.
# MAGIC > - `avg` ignores NULLs in **both** numerator and denominator — it is *not*
# MAGIC >   `sum/count(*)`.
# MAGIC > - `coalesce(sum(x), 0)` is the standard "make empty groups say 0" move.
# MAGIC > - `count(DISTINCT col)` also skips NULLs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## groupBy / agg — the DataFrame form, plus GROUP BY ALL

# COMMAND ----------

silver = spark.table("silver_orders_assoc")  # noqa: F821
display(  # noqa: F821
    silver.groupBy("order_date").agg(
        F.count("*").alias("orders"),
        F.sum("amount").alias("revenue"),
        F.countDistinct("customer_id").alias("customers"),
        F.max("amount").alias("biggest_order"),
    ).orderBy("order_date")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GROUP BY ALL: group by every non-aggregate column in the SELECT
# MAGIC SELECT order_date, status, count(*) AS orders, sum(amount) AS revenue
# MAGIC FROM silver_orders_assoc
# MAGIC GROUP BY ALL
# MAGIC ORDER BY order_date, status

# COMMAND ----------

# MAGIC %md
# MAGIC ## WHERE vs HAVING — filter rows vs filter groups

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, sum(amount) AS lifetime_value
# MAGIC FROM silver_orders_assoc
# MAGIC WHERE status = 'completed'          -- filters ROWS, before grouping
# MAGIC GROUP BY customer_id
# MAGIC HAVING sum(amount) > 100            -- filters GROUPS, after aggregating
# MAGIC ORDER BY lifetime_value DESC

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** `HAVING` without `GROUP BY` is legal (treats the whole
# MAGIC > result as one group), and a `WHERE` referencing an aggregate is an
# MAGIC > error — the "which query is valid" questions live on this line.

# COMMAND ----------

# MAGIC %md
# MAGIC ## pivot — categories become columns

# COMMAND ----------

display(  # noqa: F821
    silver.groupBy("order_date")
    .pivot("status", ["completed", "pending", "cancelled"])
    .agg(F.sum("amount"))
    .orderBy("order_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC > Passing the value list to `pivot()` (as above) skips the extra job Spark
# MAGIC > otherwise runs to discover the distinct values — the small-print
# MAGIC > performance fact the exam likes.
# MAGIC
# MAGIC ## A window, briefly — when you need the group value on every row
# MAGIC `groupBy` collapses; a window keeps all rows and adds context:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, amount,
# MAGIC        round(100 * amount / sum(amount) OVER (PARTITION BY customer_id), 1)
# MAGIC          AS pct_of_customer_total
# MAGIC FROM silver_orders_assoc
# MAGIC ORDER BY customer_id, order_id

# COMMAND ----------

# MAGIC %md
# MAGIC Deep dive on windows (frames, `lag`/`lead`, rankings): the Professional
# MAGIC folder's
# MAGIC [`02_window_functions`](../../data-engineer-professional/01-developing-code-for-data-processing/02_window_functions.py).
# MAGIC
# MAGIC ## Cleanup — nothing new was written
