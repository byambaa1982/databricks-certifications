# Databricks notebook source
# MAGIC %md
# MAGIC # Q60: CREATE TABLE IF NOT EXISTS vs CREATE OR REPLACE TABLE
# MAGIC
# MAGIC Companion notebook for the Emma on Data video.
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless compute is enough. Outputs shown in the video were captured on 10/15/2026.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS product_catalog (product_id STRING, name STRING)
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS product_catalog (product_id STRING, name STRING)
# MAGIC USING DELTA;  -- second run
# MAGIC DESCRIBE HISTORY product_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO product_catalog VALUES ('p1', 'Desk lamp'), ('p2', 'Mouse pad');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE product_catalog (product_id BIGINT, product_name STRING, category STRING)
# MAGIC USING DELTA;
# MAGIC DESCRIBE TABLE product_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY product_catalog;
