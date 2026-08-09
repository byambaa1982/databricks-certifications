# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — build dirty bronze tables to transform
# MAGIC
# MAGIC Self-contained: creates `dev_catalog.de_assoc` (if section 02's setup
# MAGIC hasn't already) and two synthetic **bronze** tables with exactly the
# MAGIC problems the notebooks fix — string-typed numbers and dates, mixed-case
# MAGIC statuses, NULL keys, duplicates, and a customer table to join. Re-run
# MAGIC anytime to reset.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

orders_rows = [
    # (order_id, customer_id, amount STRING, status STRING, order_ts STRING)
    (5001, 101, "120.50", "Completed ", "2024-01-05T10:15:00"),
    (5002, 102, "39.99", "completed", "2024-01-05T11:02:00"),
    (5003, 103, "640.00", "PENDING", "2024-01-05T12:30:00"),
    (5004, 101, "89.00", "cancelled", "2024-01-05T13:45:00"),
    (5005, 104, "250.00", "completed", "2024-01-12T09:20:00"),
    (5006, 102, "15.75", "completed", "2024-01-12T10:05:00"),
    (5007, 105, "480.25", "pending", "2024-01-12T14:11:00"),
    (5008, 103, "75.00", "completed", "2024-02-02T08:30:00"),
    (5009, 106, "310.40", "completed", "2024-02-02T09:15:00"),
    (5010, 104, "N/A", "pending", "2024-02-02T11:00:00"),       # bad amount
    (5011, None, "55.20", "completed", "2024-02-02T12:40:00"),  # NULL key
    (5012, 101, "42.00", "completed", "not-a-date"),            # bad timestamp
    (5005, 104, "250.00", "completed", "2024-01-12T09:20:00"),  # exact duplicate
    (5006, 102, "15.75", "completed", "2024-01-12T10:05:00"),   # exact duplicate
]
spark.createDataFrame(  # noqa: F821
    orders_rows, "order_id INT, customer_id INT, amount STRING, status STRING, order_ts STRING"
).write.mode("overwrite").saveAsTable("bronze_orders_assoc")

customers_rows = [
    # (customer_id, name, email, tier)  — 106 is missing on purpose (join demos)
    (101, "Bat", "bat@example.com", "gold"),
    (102, "Naran", "naran@example.com", "silver"),
    (103, "Sara Lee", "sara.lee@example.com", "gold"),
    (104, "Dulguun", None, "bronze"),                            # NULL email
    (105, "Tuya", "tuya@example.com", "bronze"),
    (107, "Zolboo", "zolboo@example.com", "silver"),             # customer with no orders
]
spark.createDataFrame(  # noqa: F821
    customers_rows, "customer_id INT, name STRING, email STRING, tier STRING"
).write.mode("overwrite").saveAsTable("bronze_customers_assoc")

print("bronze_orders_assoc:    14 rows (2 duplicates, 1 bad amount, 1 NULL key, 1 bad ts)")
print("bronze_customers_assoc:  6 rows (1 NULL email; customer 106 missing, 107 orderless)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.table("bronze_orders_assoc").orderBy("order_id"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Ready. Open `01_cleaning_and_dataframe_ops` next.
