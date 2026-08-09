# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Constraints: what Delta actually enforces
# MAGIC
# MAGIC Three kinds of "constraint" exist on Databricks and the exam loves mixing
# MAGIC them up: **enforced** ones (`NOT NULL`, `CHECK`) that make bad writes
# MAGIC *fail*, **informational** ones (`PRIMARY KEY`, `FOREIGN KEY`) that enforce
# MAGIC *nothing*, and DLT **expectations** (notebook 03) that can *drop rows
# MAGIC instead of failing*. Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build a table worth protecting
# MAGIC Standard cleanse of the dirty bronze — cast, filter the garbage, dedupe.
# MAGIC The WHERE clause is exactly the rule set we're about to turn into
# MAGIC constraints.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_checked AS
# MAGIC SELECT DISTINCT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   cast(amount AS decimal(10,2)) AS amount,
# MAGIC   lower(trim(status))           AS status,
# MAGIC   to_timestamp(order_ts)        AS order_ts
# MAGIC FROM bronze_orders_dirty
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC   AND amount > 0 AND amount <= 1000000
# MAGIC   AND to_timestamp(order_ts) IS NOT NULL;
# MAGIC
# MAGIC SELECT count(*) AS clean_rows FROM orders_checked

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enforced #1: NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_checked ALTER COLUMN customer_id SET NOT NULL;
# MAGIC ALTER TABLE orders_checked ALTER COLUMN order_id SET NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enforced #2: CHECK
# MAGIC > **Exam trap:** `ADD CONSTRAINT` **scans the whole table first** and fails
# MAGIC > if any existing row violates the condition. That's why the cleanse came
# MAGIC > before the constraint — try adding `amount > 0` to
# MAGIC > `bronze_orders_dirty` and it refuses.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_checked ADD CONSTRAINT valid_amount
# MAGIC   CHECK (amount > 0 AND amount <= 1000000);
# MAGIC ALTER TABLE orders_checked ADD CONSTRAINT valid_status
# MAGIC   CHECK (status IN ('completed', 'pending', 'cancelled', 'refunded'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Violations fail the WRITE — nothing is dropped, nothing is partial

# COMMAND ----------

for bad_insert, why in [
    ("INSERT INTO orders_checked VALUES (1, NULL, 10.00, 'completed', current_timestamp())",
     "NULL customer_id"),
    ("INSERT INTO orders_checked VALUES (2, 101, -5.00, 'completed', current_timestamp())",
     "negative amount"),
    ("INSERT INTO orders_checked VALUES (3, 101, 10.00, 'SHIPPED?!', current_timestamp())",
     "status not in the allowed list"),
]:
    try:
        spark.sql(bad_insert)  # noqa: F821
        print(f"UNEXPECTED: {why} was accepted")
    except Exception as e:
        print(f"rejected ({why}): {type(e).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** a constraint violation fails the **entire transaction** —
# MAGIC > a 1M-row MERGE with one bad row writes *zero* rows. Constraints protect
# MAGIC > the table; they are not a row-by-row filter. If you want "load the good,
# MAGIC > quarantine the bad", that's DLT expectations or the quarantine pattern
# MAGIC > (notebook 03).

# COMMAND ----------

# MAGIC %md
# MAGIC ## NOT enforced: PRIMARY KEY and FOREIGN KEY
# MAGIC Unity Catalog accepts PK/FK declarations — for tools, lineage, and the
# MAGIC optimizer — but **does not enforce them**. Watch a duplicate primary key
# MAGIC sail right in:

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_checked ADD CONSTRAINT orders_pk PRIMARY KEY (order_id);
# MAGIC
# MAGIC -- order_id 900001 twice — an enforced PK would reject the second insert
# MAGIC INSERT INTO orders_checked VALUES (900001, 101, 10.00, 'completed', current_timestamp());
# MAGIC INSERT INTO orders_checked VALUES (900001, 102, 20.00, 'completed', current_timestamp());
# MAGIC
# MAGIC SELECT order_id, count(*) AS copies FROM orders_checked
# MAGIC WHERE order_id = 900001 GROUP BY order_id

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** PK/FK are **informational**. Uniqueness is *your* job —
# MAGIC > dedupe before MERGE (notebook 02). Adding `RELY` to a PK tells the
# MAGIC > optimizer to *trust* it (enabling eliminations that assume uniqueness) —
# MAGIC > if you lied, queries can silently return wrong results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where constraints live

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CHECK constraints appear as delta.constraints.* table properties
# MAGIC SHOW TBLPROPERTIES orders_checked

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_checked
