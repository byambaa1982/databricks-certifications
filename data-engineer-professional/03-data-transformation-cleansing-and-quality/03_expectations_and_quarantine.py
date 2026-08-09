# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Expectations and the quarantine pattern
# MAGIC
# MAGIC Constraints (notebook 01) fail the whole write; sometimes you want the
# MAGIC opposite: **load the good rows, set the bad ones aside, and keep count of
# MAGIC both.** In Lakeflow / DLT that's *expectations*; outside a pipeline it's
# MAGIC the quarantine pattern built from plain `when`/`filter` — which is what we
# MAGIC run live here. Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rules as data
# MAGIC One dict, three uses: tag rows, split the flow, report per-rule counts.
# MAGIC (This is exactly the shape DLT's `@dlt.expect_all` takes.)

# COMMAND ----------

RULES = {
    "valid_customer": "customer_id IS NOT NULL",
    "valid_amount": "amount > 0 AND amount <= 1000000",
    "valid_timestamp": "to_timestamp(order_ts) IS NOT NULL",
}

failed = F.array_compact(F.array(*[
    F.when(~F.expr(cond), F.lit(name)) for name, cond in RULES.items()
]))

tagged = spark.table("bronze_orders_dirty").withColumn("failed_rules", failed)  # noqa: F821
display(  # noqa: F821
    tagged.filter(F.size("failed_rules") > 0)
    .select("order_id", "customer_id", "amount", "order_ts", "failed_rules")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split: silver gets the good, quarantine gets the bad **with reasons**

# COMMAND ----------

(
    tagged.filter(F.size("failed_rules") == 0)
    .drop("failed_rules")
    .dropDuplicates()
    .write.mode("overwrite").saveAsTable("orders_quality_silver")
)
(
    tagged.filter(F.size("failed_rules") > 0)
    .withColumn("_quarantined_at", F.current_timestamp())
    .write.mode("overwrite").saveAsTable("orders_quarantine")
)
print(f"silver:     {spark.table('orders_quality_silver').count()} rows")  # noqa: F821
print(f"quarantine: {spark.table('orders_quarantine').count()} rows")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The quality report — which rules fail, how often

# COMMAND ----------

display(  # noqa: F821
    tagged.select(F.explode_outer("failed_rules").alias("rule"))
    .groupBy("rule").count()
    .withColumn("rule", F.coalesce("rule", F.lit("(passed all rules)")))
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The same thing, declaratively: DLT / Lakeflow expectations
# MAGIC Inside a pipeline you declare the rule and the **violation policy**;
# MAGIC metrics land in the pipeline event log automatically.
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC @dlt.table
# MAGIC @dlt.expect("valid_customer", "customer_id IS NOT NULL")          # warn: keep row, count it
# MAGIC @dlt.expect_or_drop("valid_amount", "amount > 0")                 # drop: row removed, counted
# MAGIC @dlt.expect_or_fail("valid_timestamp", "order_ts IS NOT NULL")    # fail: pipeline stops
# MAGIC def orders_silver():
# MAGIC     return dlt.read_stream("orders_bronze")
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_silver (
# MAGIC   CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
# MAGIC ) AS SELECT * FROM STREAM(LIVE.orders_bronze)
# MAGIC ```
# MAGIC
# MAGIC | Policy | Bad row | Pipeline | Where you see it |
# MAGIC |---|---|---|---|
# MAGIC | `expect` (warn — default) | **kept** | continues | event log metrics |
# MAGIC | `expect_or_drop` | dropped | continues | event log metrics |
# MAGIC | `expect_or_fail` | rolls back the update | **stops** | failed update |

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - Plain `expect` does **not** drop anything — "the row is still loaded" is
# MAGIC >   the answer they're fishing for.
# MAGIC > - `expect_all*` variants take a **dict of rules** (like our `RULES`).
# MAGIC > - Expectation metrics are read from the **event log**, not from the table.
# MAGIC > - Expectations can't quarantine on their own — the standard answer for
# MAGIC >   "route bad rows to another table" is **two tables with opposite
# MAGIC >   filters** (exactly the split above).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_quality_silver;
# MAGIC DROP TABLE IF EXISTS orders_quarantine
