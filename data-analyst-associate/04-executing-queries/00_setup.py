# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — a small retail schema to query
# MAGIC
# MAGIC Builds `dev_catalog.da_assoc`: three tables shaped like every analytics
# MAGIC exam question — `customers`, `products`, `orders` — with the usual
# MAGIC real-world blemishes (a customer with no orders, an order for an unknown
# MAGIC customer, NULLs where they hurt). Deterministic; re-run to reset.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "da_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

customers = [
    (101, "Bat", "bat@example.com", "gold", "2023-03-15"),
    (102, "Naran", "naran@example.com", "silver", "2023-06-02"),
    (103, "Sara Lee", "sara.lee@example.com", "gold", "2023-08-21"),
    (104, "Dulguun", None, "bronze", "2024-01-10"),            # NULL email
    (105, "Tuya", "tuya@example.com", "bronze", "2024-02-28"),
    (107, "Zolboo", "zolboo@example.com", "silver", "2024-03-05"),  # no orders
]
spark.createDataFrame(  # noqa: F821
    customers, "customer_id INT, name STRING, email STRING, tier STRING, signup_date STRING"
).selectExpr("customer_id", "name", "email", "tier", "cast(signup_date AS date) AS signup_date") \
 .write.mode("overwrite").saveAsTable("customers")

products = [
    (1, "Laptop Stand", "accessories", 49.99),
    (2, "USB-C Hub", "accessories", 89.00),
    (3, "Mechanical Keyboard", "peripherals", 159.00),
    (4, "4K Monitor", "displays", 429.00),
    (5, "Webcam", "peripherals", 79.50),
]
spark.createDataFrame(  # noqa: F821
    products, "product_id INT, product_name STRING, category STRING, unit_price DOUBLE"
).selectExpr("product_id", "product_name", "category",
             "cast(unit_price AS decimal(10,2)) AS unit_price") \
 .write.mode("overwrite").saveAsTable("products")

orders = [
    # (order_id, customer_id, product_id, quantity, status, order_ts)
    (5001, 101, 4, 1, "completed", "2024-01-05 10:15:00"),
    (5002, 102, 1, 2, "completed", "2024-01-05 11:02:00"),
    (5003, 103, 3, 1, "pending",   "2024-01-08 12:30:00"),
    (5004, 101, 2, 1, "cancelled", "2024-01-09 13:45:00"),
    (5005, 104, 5, 3, "completed", "2024-01-12 09:20:00"),
    (5006, 102, 2, 1, "completed", "2024-01-15 10:05:00"),
    (5007, 105, 3, 2, "pending",   "2024-01-18 14:11:00"),
    (5008, 103, 1, 1, "completed", "2024-02-02 08:30:00"),
    (5009, 106, 4, 1, "completed", "2024-02-03 09:15:00"),   # unknown customer!
    (5010, 104, 5, None, "pending", "2024-02-05 11:00:00"),  # NULL quantity
    (5011, 101, 3, 1, "completed", "2024-02-08 12:40:00"),
    (5012, 105, 1, 4, "completed", "2024-02-11 16:22:00"),
]
spark.createDataFrame(  # noqa: F821
    orders, "order_id INT, customer_id INT, product_id INT, quantity INT, status STRING, order_ts STRING"
).selectExpr("order_id", "customer_id", "product_id", "quantity", "status",
             "cast(order_ts AS timestamp) AS order_ts") \
 .write.mode("overwrite").saveAsTable("orders")

print("customers: 6 rows (104 has NULL email; 107 has no orders)")
print("products:  5 rows")
print("orders:    12 rows (5009 -> unknown customer 106; 5010 has NULL quantity)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o.order_id, c.name, p.product_name, o.quantity, o.status
# MAGIC FROM orders o
# MAGIC LEFT JOIN customers c USING (customer_id)
# MAGIC LEFT JOIN products p USING (product_id)
# MAGIC ORDER BY order_id LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Ready. Open `01_sql_essentials` next.
