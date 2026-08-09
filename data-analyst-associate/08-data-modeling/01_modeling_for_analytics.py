# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Data modeling for analysts
# MAGIC
# MAGIC A small section with one skill: **recognize the model** in front of you
# MAGIC (star, snowflake, data vault, one big table) and know where each lives in
# MAGIC the medallion. The star gets built live on the `da_assoc` tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "da_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The four shapes
# MAGIC
# MAGIC | Model | Shape | Trade |
# MAGIC |---|---|---|
# MAGIC | **Star** | fact + denormalized dimensions, one join hop | the BI default — simple, fast (dims broadcast) |
# MAGIC | **Snowflake** | star with normalized dims (dim → sub-dim) | less redundancy, more joins — rarely worth it on the lakehouse |
# MAGIC | **Data vault** | hubs / links / satellites | integration-layer flexibility; feeds stars, isn't queried by BI directly |
# MAGIC | **One big table (OBT)** | everything pre-joined wide | zero joins for consumers; attribute changes are expensive |
# MAGIC
# MAGIC Medallion mapping: raw → **bronze**; cleaned entities (and data vault, if
# MAGIC used) → **silver**; **stars and OBTs → gold**, built *for* consumption.
# MAGIC Analyst-level verdict the exam expects: star for gold BI, snowflake mostly
# MAGIC legacy, vault is engineering's middle layer, OBT for a single hot use case.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Our schema IS a star — say it out loud
# MAGIC `orders` (fact: events, measures `quantity`, FKs) joined to `customers`
# MAGIC and `products` (dimensions: descriptive attributes). The **grain** of the
# MAGIC fact = one row per order line. Every metric question is "aggregate the
# MAGIC fact, slice by dimension attributes":

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.category, c.tier,
# MAGIC        sum(o.quantity * p.unit_price) AS revenue
# MAGIC FROM orders o                       -- fact
# MAGIC JOIN customers c USING (customer_id)  -- dim
# MAGIC JOIN products  p USING (product_id)   -- dim
# MAGIC WHERE o.status = 'completed'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## And the gold OBT, when one use case is hot

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_order_lines AS
# MAGIC SELECT o.order_id, o.order_ts, o.status, o.quantity,
# MAGIC        c.name AS customer_name, c.tier,
# MAGIC        p.product_name, p.category, p.unit_price,
# MAGIC        o.quantity * p.unit_price AS line_value
# MAGIC FROM orders o
# MAGIC LEFT JOIN customers c USING (customer_id)
# MAGIC LEFT JOIN products  p USING (product_id);
# MAGIC
# MAGIC SELECT count(*) AS rows, count(DISTINCT order_id) AS orders FROM gold_order_lines

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - **Grain first.** Mixing grains (order-level and line-level measures in
# MAGIC >   one fact) double-counts — the "revenue is exactly 2× reality" bug.
# MAGIC > - Facts hold **measures + foreign keys**; dims hold **attributes**. A
# MAGIC >   "dimension" with amounts in it is mis-modeled.
# MAGIC > - Left-joining a fact to an **SCD2** dimension without the current-row
# MAGIC >   filter fans out — the deep dive lives in engineering
# MAGIC >   [`10-data-modelling`](../../data-engineer-professional/10-data-modelling/).
# MAGIC > - PK/FK constraints on the lakehouse are **informational** — declare
# MAGIC >   them for BI tools and humans; enforce uniqueness upstream.
# MAGIC
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold_order_lines
