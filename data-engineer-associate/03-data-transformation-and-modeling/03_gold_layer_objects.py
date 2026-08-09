# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — The gold layer: which object to build
# MAGIC
# MAGIC Gold is where consumers meet the data, and the exam's question shape is:
# MAGIC *given this consumption pattern, which object?* — table, view,
# MAGIC **materialized view**, or **streaming table**. Requires notebook 01's
# MAGIC `silver_orders_assoc`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medallion, one line per layer
# MAGIC **Bronze** = raw, append-only, re-processable. **Silver** = validated,
# MAGIC deduplicated entities (notebook 01 built one). **Gold** = aggregated /
# MAGIC shaped for consumption — what this notebook builds. Cleansing lives
# MAGIC bronze→silver; business aggregation lives silver→gold — the exam loves
# MAGIC putting the right step in the wrong hop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The four objects

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1) TABLE: materialized data, refreshed only when YOUR job rewrites it
# MAGIC CREATE OR REPLACE TABLE gold_daily_revenue_assoc AS
# MAGIC SELECT order_date,
# MAGIC        coalesce(sum(CASE WHEN status = 'completed' THEN amount END), 0) AS revenue,
# MAGIC        count(*) AS order_count
# MAGIC FROM silver_orders_assoc
# MAGIC GROUP BY order_date;
# MAGIC
# MAGIC -- 2) VIEW: just a saved query — always current, recomputed EVERY read
# MAGIC CREATE OR REPLACE VIEW gold_daily_revenue_v AS
# MAGIC SELECT order_date, sum(amount) AS revenue
# MAGIC FROM silver_orders_assoc GROUP BY order_date;
# MAGIC
# MAGIC SELECT * FROM gold_daily_revenue_assoc ORDER BY order_date

# COMMAND ----------

# 3) MATERIALIZED VIEW and 4) STREAMING TABLE need serverless/DBSQL or a
#    pipeline — created here if the compute supports it, reference otherwise.
for label, stmt in {
    "materialized view": """
        CREATE OR REPLACE MATERIALIZED VIEW gold_daily_revenue_mv AS
        SELECT order_date, sum(amount) AS revenue
        FROM silver_orders_assoc GROUP BY order_date
    """,
    "streaming table": """
        CREATE OR REFRESH STREAMING TABLE gold_orders_stream_assoc AS
        SELECT * FROM STREAM silver_orders_assoc
    """,
}.items():
    try:
        spark.sql(stmt)  # noqa: F821
        print(f"created {label}")
    except Exception as e:
        print(f"{label}: not supported on this compute ({type(e).__name__}) — "
              "works on serverless SQL warehouses and in Lakeflow pipelines")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The decision table — this IS the exam content
# MAGIC
# MAGIC | Object | Data stored? | Freshness | Recompute cost | Reach for it when |
# MAGIC |---|---|---|---|---|
# MAGIC | **Table** (CTAS + job) | yes | as of last job run | you schedule it | full control, any compute |
# MAGIC | **View** | no | always current | **every query pays** | cheap logic, small data, security layer |
# MAGIC | **Materialized view** | yes | as of last refresh — **incremental** where possible | managed | BI queries repeated often; "precompute but stay declarative" |
# MAGIC | **Streaming table** | yes | continuous / per trigger | incremental by definition | append-heavy sources, ingestion + light transform |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - A view **stores no data** and recomputes per query — "dashboard is slow,
# MAGIC >   queries the same view constantly" → materialized view.
# MAGIC > - An MV refreshes **incrementally** when it can (append-only sources,
# MAGIC >   supported expressions) and falls back to full recompute when it can't.
# MAGIC > - A streaming table processes each input row **once** (checkpointed) —
# MAGIC >   it's the declarative face of Structured Streaming.
# MAGIC > - Temp views die with the session; global temp views with the cluster —
# MAGIC >   neither belongs in gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality gates on the way to gold
# MAGIC Two enforcement styles, and knowing which is which is a recurring question:
# MAGIC
# MAGIC ```sql
# MAGIC -- Delta constraints: bad WRITES FAIL (the whole transaction)
# MAGIC ALTER TABLE gold_daily_revenue_assoc ADD CONSTRAINT positive_revenue
# MAGIC   CHECK (revenue >= 0);
# MAGIC
# MAGIC -- Lakeflow expectations: bad ROWS are counted/dropped, pipeline continues
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_orders (
# MAGIC   CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
# MAGIC ) AS SELECT * FROM STREAM read_files(...)
# MAGIC ```
# MAGIC
# MAGIC Constraint = protect the table, fail the writer. Expectation = let the
# MAGIC pipeline flow, quarantine/count the bad rows. Deep dive: Professional
# MAGIC [`03-data-transformation-cleansing-and-quality`](../../data-engineer-professional/03-data-transformation-cleansing-and-quality/).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

for stmt in [
    "DROP VIEW IF EXISTS gold_daily_revenue_v",
    "DROP TABLE IF EXISTS gold_daily_revenue_assoc",
    "DROP MATERIALIZED VIEW IF EXISTS gold_daily_revenue_mv",
    "DROP TABLE IF EXISTS gold_orders_stream_assoc",
    "DROP TABLE IF EXISTS silver_orders_assoc",
    "DROP TABLE IF EXISTS bronze_orders_assoc",
    "DROP TABLE IF EXISTS bronze_customers_assoc",
]:
    try:
        spark.sql(stmt)  # noqa: F821
    except Exception:
        pass
print("section 03 scratch objects removed (re-run 00_setup to rebuild)")
