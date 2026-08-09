# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Window functions
# MAGIC
# MAGIC Rankings, running totals, lag/lead, and the single most-tested pattern on
# MAGIC the exam: **latest row per key** with `row_number()`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ranking: top spender per tier
# MAGIC Know the difference cold:
# MAGIC - `row_number()` — unique 1,2,3 (ties broken arbitrarily)
# MAGIC - `rank()` — ties share a rank, then **gaps** (1,1,3)
# MAGIC - `dense_rank()` — ties share a rank, **no gaps** (1,1,2)

# COMMAND ----------

w = Window.partitionBy("tier").orderBy(F.desc("lifetime_value"))

display(  # noqa: F821
    spark.table("gold_customer_revenue")  # noqa: F821
    .withColumn("row_number", F.row_number().over(w))
    .withColumn("rank", F.rank().over(w))
    .withColumn("dense_rank", F.dense_rank().over(w))
    .filter(F.col("row_number") <= 3)
    .select("tier", "name", "lifetime_value", "row_number", "rank", "dense_rank")
    .orderBy("tier", "row_number")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running totals and moving averages
# MAGIC > **Exam trap:** with `orderBy` and no explicit frame, the default frame is
# MAGIC > `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — a running total.
# MAGIC > Without `orderBy`, it's the whole partition.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   revenue,
# MAGIC   sum(revenue) OVER (ORDER BY order_date)                                        AS running_revenue,
# MAGIC   avg(revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_7d_avg
# MAGIC FROM gold_daily_revenue
# MAGIC ORDER BY order_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## lag / lead: day-over-day change

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   revenue,
# MAGIC   lag(revenue) OVER (ORDER BY order_date)             AS prev_day,
# MAGIC   revenue - lag(revenue) OVER (ORDER BY order_date)   AS change,
# MAGIC   round(100.0 * (revenue - lag(revenue) OVER (ORDER BY order_date))
# MAGIC         / nullif(lag(revenue) OVER (ORDER BY order_date), 0), 1) AS pct_change
# MAGIC FROM gold_daily_revenue
# MAGIC ORDER BY order_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## THE exam pattern: latest event per key
# MAGIC `bronze_customers_cdc` has many events per customer. Collapse to the latest —
# MAGIC this is how you dedupe CDC before a MERGE (notebook 04 uses exactly this).

# COMMAND ----------

latest = Window.partitionBy("customer_id").orderBy(F.desc("event_ts"))

latest_events = (
    spark.table("bronze_customers_cdc")  # noqa: F821
    .withColumn("rn", F.row_number().over(latest))
    .filter("rn = 1")
    .drop("rn")
)
print(f"CDC events:        {spark.table('bronze_customers_cdc').count()}")  # noqa: F821
print(f"distinct customers: {latest_events.count()}")
display(latest_events.select("customer_id", "op", "event_ts", "tier").limit(10))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** `dropDuplicates(["customer_id"])` also returns one row per
# MAGIC > key, but an **arbitrary** one — not the latest. If ordering matters, only
# MAGIC > `row_number()` (or `max_by`) is correct.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Windows vs groupBy — know when each applies
# MAGIC `groupBy` collapses rows; a window keeps every row and adds context. Share of
# MAGIC tier revenue per customer needs both values on one row → window.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   name,
# MAGIC   tier,
# MAGIC   lifetime_value,
# MAGIC   round(100 * lifetime_value / sum(lifetime_value) OVER (PARTITION BY tier), 1) AS pct_of_tier
# MAGIC FROM gold_customer_revenue
# MAGIC WHERE lifetime_value > 0
# MAGIC ORDER BY tier, pct_of_tier DESC
