# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Troubleshooting and monitoring
# MAGIC
# MAGIC Symptom → place-to-look → fix. The exam gives you a sick job or cluster
# MAGIC and four diagnoses; classifying the failure first makes the right one
# MAGIC obvious. The failure demos run live.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failure class 1: before anything runs (analysis time)
# MAGIC Wrong column/table, bad SQL: caught while planning, on the driver,
# MAGIC instantly — the error names the problem (often with "Did you mean").

# COMMAND ----------

df = spark.range(5).withColumn("amount", F.col("id") * 10)  # noqa: F821
try:
    df.select("amuont").collect()
except Exception as e:
    print(f"{type(e).__name__}: {str(e).splitlines()[0][:100]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failure class 2: at runtime (on executors)
# MAGIC Bad data in a UDF, OOM, skew: fails **when the stage runs**; the Python
# MAGIC error is wrapped — read the `Caused by:` chain bottom-up, and check the
# MAGIC **failed task's** logs, not just the driver's.

# COMMAND ----------

@F.udf("int")
def brittle(s):
    return int(s)


try:
    df.withColumn("s", F.lit("not-a-number")).select(brittle("s")).collect()
except Exception as e:
    print(f"{type(e).__name__} — the real ValueError is inside 'Caused by:'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The symptom table
# MAGIC
# MAGIC | Symptom | Look at | Usual cause → fix |
# MAGIC |---|---|---|
# MAGIC | cluster won't start | cluster event log | instance availability (try another type/zone), failing **init script**, permissions |
# MAGIC | driver dies / notebook detaches | driver log | `collect()`/`toPandas()` on big data, broadcasting a huge table |
# MAGIC | tasks die with memory errors | Spark UI → failed stage → executor logs | skew, giant shuffle partitions → AQE, more/beefier workers |
# MAGIC | one task runs forever | Spark UI → Stages → task summary (**max ≫ median**) | data skew → AQE skew join handles it |
# MAGIC | job slower every week | job run history / duration trend | growing input + full reloads → incremental (section 02), compaction (notebook 02) |
# MAGIC | stream falling behind | streaming tab / progress metrics | under-provisioned, tiny files, see backlog metrics |
# MAGIC | pipeline task failed | the pipeline's DAG view + event log | the failing flow's error; expectations metrics for quality failures |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - **Driver** OOM and **executor** OOM have different causes — "add more
# MAGIC >   workers" does nothing for a driver-side `collect()`.
# MAGIC > - Lazy evaluation: the error surfaces at the **action** but was caused by
# MAGIC >   an earlier transformation — the stack trace names the real operator.
# MAGIC > - Shuffles (`Exchange` in the plan) are where slow queries hide; the
# MAGIC >   Spark UI's SQL tab shows per-operator time, spill, and rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where monitoring lives (names to match on the exam)
# MAGIC - **Job run history** — per-job durations and outcomes; trends live here.
# MAGIC - **Spark UI** — jobs/stages/tasks, SQL plans, storage, executors.
# MAGIC - **Query history / query profile** — the SQL-warehouse equivalent.
# MAGIC - **Pipeline event log** — Lakeflow updates, expectations, backlog.
# MAGIC - **System tables** (`system.lakeflow.*`, `system.billing.usage`) —
# MAGIC   account-wide, queryable with SQL, alertable.
# MAGIC
# MAGIC Deep dive: Professional
# MAGIC [`05-monitoring-and-alerting`](../../data-engineer-professional/05-monitoring-and-alerting/)
# MAGIC and [`09-debugging-and-deploying`](../../data-engineer-professional/09-debugging-and-deploying/).
