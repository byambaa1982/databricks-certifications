# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — SQL essentials: the constructs the exam actually tests
# MAGIC
# MAGIC Joins with the blemished rows visible, aggregation NULL rules, CTEs and
# MAGIC subqueries, and the object zoo (table / view / temp view). **Predict every
# MAGIC result before running** — the exam shows you queries and asks what comes
# MAGIC back. Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "da_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins — the blemishes make the answer visible
# MAGIC Order 5009 references customer **106** (doesn't exist); customer **107**
# MAGIC never ordered. Predict each count:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'inner' AS join_type, count(*) AS rows FROM orders o JOIN      customers c USING (customer_id)
# MAGIC UNION ALL
# MAGIC SELECT 'left',            count(*) FROM orders o LEFT JOIN customers c USING (customer_id)
# MAGIC UNION ALL
# MAGIC SELECT 'right',           count(*) FROM orders o RIGHT JOIN customers c USING (customer_id)
# MAGIC UNION ALL
# MAGIC SELECT 'full',            count(*) FROM orders o FULL JOIN customers c USING (customer_id)
# MAGIC UNION ALL
# MAGIC SELECT 'anti (orphans)',  count(*) FROM orders o ANTI JOIN customers c USING (customer_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 11 / 12 / 12 / 13 / 1 — inner drops the orphan order 5009, left keeps it
# MAGIC (NULL customer columns), right swaps it for orderless 107, full keeps
# MAGIC both, anti finds exactly the orphan. If you predicted those five you've
# MAGIC answered most join questions on the exam.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation and the NULL rules
# MAGIC Order 5010 has NULL quantity — watch every function treat it differently:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   status,
# MAGIC   count(*)            AS all_rows,        -- counts NULLs
# MAGIC   count(quantity)     AS with_quantity,   -- skips NULLs
# MAGIC   sum(quantity)       AS units,           -- skips NULLs
# MAGIC   avg(quantity)       AS avg_units,       -- skips NULLs (not sum/count(*))
# MAGIC   count(DISTINCT customer_id) AS customers
# MAGIC FROM orders
# MAGIC GROUP BY status
# MAGIC ORDER BY status

# COMMAND ----------

# MAGIC %md
# MAGIC ## CTEs and subqueries — revenue per customer, tiered

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_values AS (
# MAGIC   SELECT o.order_id, o.customer_id,
# MAGIC          o.quantity * p.unit_price AS order_value
# MAGIC   FROM orders o
# MAGIC   JOIN products p USING (product_id)
# MAGIC   WHERE o.status = 'completed'
# MAGIC ),
# MAGIC customer_revenue AS (
# MAGIC   SELECT customer_id, sum(order_value) AS revenue, count(*) AS orders
# MAGIC   FROM order_values GROUP BY customer_id
# MAGIC )
# MAGIC SELECT c.name, c.tier, r.revenue, r.orders,
# MAGIC        CASE WHEN r.revenue >= 400 THEN 'high value'
# MAGIC             WHEN r.revenue >= 100 THEN 'mid value'
# MAGIC             ELSE 'low value' END AS segment
# MAGIC FROM customer_revenue r
# MAGIC JOIN customers c USING (customer_id)
# MAGIC ORDER BY r.revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - A CTE exists **only for that one query** — it's readable naming, not a
# MAGIC >   stored object (the "which construct persists?" question).
# MAGIC > - Correlated subqueries (`WHERE x > (SELECT avg(...) WHERE inner.id =
# MAGIC >   outer.id)`) work but read worse than a window — and the exam accepts
# MAGIC >   both, so learn to *recognize* equivalence.
# MAGIC > - `WHERE` filters rows before grouping; `HAVING` filters groups after;
# MAGIC >   `QUALIFY` filters window results — the three-way version of the
# MAGIC >   classic pair.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A window, because dashboards always need one

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, order_ts, quantity,
# MAGIC        row_number() OVER (PARTITION BY customer_id ORDER BY order_ts DESC) AS recency_rank
# MAGIC FROM orders
# MAGIC QUALIFY recency_rank = 1        -- latest order per customer
# MAGIC ORDER BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## The object zoo — what persists, what doesn't
# MAGIC
# MAGIC | Object | Stores data? | Lives | Use |
# MAGIC |---|---|---|---|
# MAGIC | `TABLE` | yes | until dropped | facts, dims, results worth keeping |
# MAGIC | `VIEW` | no — recomputes per query | until dropped | reusable logic, security layer |
# MAGIC | `TEMP VIEW` | no | **this session only** | scratch steps |
# MAGIC | `MATERIALIZED VIEW` | yes — refreshed | until dropped | precomputed BI aggregates (section 05 side) |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW completed_orders_v AS
# MAGIC SELECT * FROM orders WHERE status = 'completed';
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW scratch_v AS
# MAGIC SELECT * FROM orders WHERE quantity IS NULL;
# MAGIC
# MAGIC SELECT (SELECT count(*) FROM completed_orders_v) AS completed,
# MAGIC        (SELECT count(*) FROM scratch_v)          AS null_quantity_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel — the analyst's undo

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE orders SET status = 'refunded' WHERE order_id = 5004;
# MAGIC SELECT version, operation FROM (DESCRIBE HISTORY orders) ORDER BY version DESC LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- read the table as it was before; RESTORE would rewind it for real
# MAGIC SELECT status FROM orders VERSION AS OF 0 WHERE order_id = 5004

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** `VERSION AS OF` / `TIMESTAMP AS OF` **read** the past;
# MAGIC > `RESTORE TABLE … TO VERSION AS OF n` **rewrites** the present. And time
# MAGIC > travel reaches only as far back as VACUUM has left the files.
# MAGIC
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS completed_orders_v
