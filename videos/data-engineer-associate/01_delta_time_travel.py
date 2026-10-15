# Databricks notebook source
# MAGIC %md
# MAGIC # Delta time travel
# MAGIC
# MAGIC Companion notebook for the Emma on Data video.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless compute is enough. Outputs shown in the video were captured on 10/15/2026.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta time travel
# MAGIC Every write is a **version**. `VERSION AS OF` reads an old one.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE demo.orders (
# MAGIC   id INT, customer STRING, amount DOUBLE
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO demo.orders VALUES
# MAGIC   (1, 'Alice', 120.0), (2, 'Bob', 75.5), (3, 'Chen', 300.0);

# COMMAND ----------

display(spark.table("demo.orders"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS rows_before
# MAGIC FROM demo.orders VERSION AS OF 0;

# COMMAND ----------

spark.table("demo.orders").printSchema()
