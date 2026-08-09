# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — DataFrame transformations
# MAGIC
# MAGIC Bronze-to-silver is where most transformation questions live: casting
# MAGIC strings, handling bad rows, conditional logic, joins, and aggregation.
# MAGIC Run `00_setup` first; then **predict every cell's output before running it**
# MAGIC — that's exactly the skill the exam tests.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Casting and cleaning (what silver actually does to bronze)
# MAGIC `bronze_orders` stores `order_ts` as STRING and `amount` as DOUBLE.
# MAGIC
# MAGIC > **Exam trap:** `cast()` and `to_timestamp()` return **NULL** on bad input —
# MAGIC > they never throw. Unless ANSI mode (`spark.sql.ansi.enabled = true`) is on,
# MAGIC > in which case invalid casts DO raise errors.

# COMMAND ----------

orders = (
    spark.table("bronze_orders")  # noqa: F821
    .withColumn("order_ts", F.to_timestamp("order_ts"))           # bad strings -> NULL, no error
    .withColumn("amount", F.col("amount").cast("decimal(10,2)"))  # money is DECIMAL, never DOUBLE
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("status", F.lower(F.trim("status")))
)
display(orders.select("order_id", "order_ts", "amount", "status").limit(5))  # noqa: F821

# COMMAND ----------

# How many bronze rows fail the timestamp cast? These are the rows silver drops.
display(  # noqa: F821
    spark.table("bronze_orders")  # noqa: F821
    .withColumn("parsed_ts", F.to_timestamp("order_ts"))
    .groupBy(F.col("parsed_ts").isNull().alias("bad_timestamp"))
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conditional logic: `when` / `otherwise`
# MAGIC Without `.otherwise()`, unmatched rows get **NULL** — a favorite exam question.

# COMMAND ----------

display(  # noqa: F821
    orders.withColumn(
        "order_size",
        F.when(F.col("amount") >= 500, "large")
         .when(F.col("amount") >= 100, "medium")
         .otherwise("small"),
    ).groupBy("order_size").count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `selectExpr` — the same logic as SQL expressions
# MAGIC The exam loves showing the same transformation in both syntaxes.

# COMMAND ----------

display(  # noqa: F821
    spark.table("bronze_orders").selectExpr(  # noqa: F821
        "order_id",
        "cast(amount as decimal(10,2)) as amount",
        "to_timestamp(order_ts) as order_ts",
        "case when status = 'completed' then amount else 0 end as recognized_revenue",
    ).limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins — enriching orders with the customer tier
# MAGIC `silver_customers` is SCD Type 2.
# MAGIC
# MAGIC > **Exam trap:** joining a fact table to an SCD2 dimension without filtering
# MAGIC > `__END_AT IS NULL` (or range-joining on the validity window) **duplicates
# MAGIC > fact rows**. Both the bug and both fixes show up in questions.

# COMMAND ----------

current_customers = (
    spark.table("silver_customers")  # noqa: F821
    .filter(F.col("__END_AT").isNull())  # current version only
    .select("customer_id", "name", "tier")
)

enriched = spark.table("silver_orders").join(current_customers, "customer_id", "left")  # noqa: F821
display(  # noqa: F821
    enriched.groupBy("tier").agg(
        F.sum("amount").alias("revenue"),
        F.countDistinct("customer_id").alias("customers"),
    )
)

# COMMAND ----------

# Demonstrate the fan-out bug: same join WITHOUT the current-row filter.
# If any customer has history, row counts differ.
fanned = spark.table("silver_orders").join(  # noqa: F821
    spark.table("silver_customers").select("customer_id", "tier"),  # noqa: F821
    "customer_id",
    "left",
)
print(f"orders:                {spark.table('silver_orders').count()}")  # noqa: F821
print(f"joined to ALL history: {fanned.count()}   <- fan-out!")
print(f"joined to current:     {enriched.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point-in-time join — the tier the customer had *when they ordered*

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o.order_id, o.order_ts, o.amount, c.tier AS tier_at_order_time
# MAGIC FROM silver_orders o
# MAGIC JOIN silver_customers c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC  AND o.order_ts >= to_timestamp(c.__START_AT)
# MAGIC  AND (c.__END_AT IS NULL OR o.order_ts < to_timestamp(c.__END_AT))
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregations and the NULL rules
# MAGIC `count(*)` counts NULLs; `count(col)`, `sum`, `avg` skip them; `sum` of only
# MAGIC NULLs is NULL (not 0).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   status,
# MAGIC   count(*)      AS all_rows,
# MAGIC   count(amount) AS non_null_amts,
# MAGIC   sum(amount)   AS revenue,
# MAGIC   avg(amount)   AS avg_amount
# MAGIC FROM bronze_orders
# MAGIC GROUP BY status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify your logic against the real pipeline
# MAGIC Recompute `gold_daily_revenue` from `silver_orders` and diff it. **Zero rows
# MAGIC returned = your aggregation matches `transforms.daily_revenue` exactly.**
# MAGIC Note the `coalesce(..., 0)`: `sum(CASE ...)` over only-NULL inputs returns
# MAGIC NULL, and the pipeline coalesces that to 0 — itself a recurring exam question.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH mine AS (
# MAGIC   SELECT order_date,
# MAGIC          coalesce(sum(CASE WHEN status = 'completed' THEN amount END),
# MAGIC                   cast(0 AS decimal(18,2)))                    AS revenue,
# MAGIC          count(*)                                              AS order_count,
# MAGIC          sum(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count
# MAGIC   FROM silver_orders GROUP BY order_date
# MAGIC )
# MAGIC SELECT * FROM mine
# MAGIC EXCEPT
# MAGIC SELECT order_date, revenue, order_count, cancelled_count FROM gold_daily_revenue

# COMMAND ----------

# MAGIC %md
# MAGIC ## Narrow vs wide — check the plan, not your memory
# MAGIC - **Narrow** (no shuffle): `select`, `filter`, `withColumn`, `union`
# MAGIC - **Wide** (shuffle): `groupBy`, `join`, `distinct`, `orderBy`, `repartition`
# MAGIC - `coalesce(n)` reduces partitions **without** shuffle; `repartition(n)`
# MAGIC   always shuffles (and unlike `coalesce` can also increase partitions).
# MAGIC
# MAGIC `Exchange` in the plan below = shuffle.

# COMMAND ----------

spark.table("silver_orders").groupBy("status").count().explain()  # noqa: F821
