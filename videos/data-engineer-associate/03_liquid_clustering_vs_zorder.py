# Databricks notebook source
# MAGIC %md
# MAGIC # Q46: Liquid Clustering vs Z-ORDER
# MAGIC
# MAGIC Companion notebook for the Emma on Data video: https://www.youtube.com/watch?v=6CESMQ9tl0I
# MAGIC
# MAGIC Import this file into a Databricks workspace (Workspace > Import) and run it top to bottom. Serverless compute is enough. Outputs shown in the video were captured on 10/15/2026.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sensor_events (device_id STRING, event_type STRING, region STRING, ts TIMESTAMP)
# MAGIC USING DELTA CLUSTER BY (device_id, event_type);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sensor_events VALUES
# MAGIC   ('d1', 'temp', 'us-east', TIMESTAMP'2026-10-15 09:00:00'),
# MAGIC   ('d2', 'temp', 'eu-west', TIMESTAMP'2026-10-15 09:00:05'),
# MAGIC   ('d1', 'vibration', 'us-east', TIMESTAMP'2026-10-15 09:00:09'),
# MAGIC   ('d3', 'temp', 'ap-south', TIMESTAMP'2026-10-15 09:00:12');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sensor_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sensor_events CLUSTER BY (device_id, event_type, region, ts);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sensor_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sensor_events;  -- same 4 files

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sensor_events;
