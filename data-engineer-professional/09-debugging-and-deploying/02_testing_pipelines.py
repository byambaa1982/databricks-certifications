# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Testing pipeline code
# MAGIC
# MAGIC Testable pipelines share one design move: **transformations are pure
# MAGIC functions** — DataFrame in, DataFrame out, no reads or writes inside. Then
# MAGIC unit tests build tiny inputs and assert on outputs, and the same function
# MAGIC runs unchanged in the production job. Fully runnable.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## The unit under test — logic only, no I/O

# COMMAND ----------

def cleanse_orders(df):
    """bronze -> silver rules from section 03, as a pure function."""
    return (
        df.withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
        .withColumn("status", F.lower(F.trim("status")))
        .filter("customer_id IS NOT NULL AND amount > 0 AND order_ts IS NOT NULL")
        .dropDuplicates(["order_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unit test: tiny input, exact expected output
# MAGIC `pyspark.testing.assertDataFrameEqual` compares data **and** schema, with
# MAGIC readable diffs — this is the assertion the exam expects you to reach for.

# COMMAND ----------

from pyspark.testing import assertDataFrameEqual  # noqa: E402

input_df = spark.createDataFrame(  # noqa: F821
    [
        (1, 101, "120.50", "Completed ", "2024-01-05T10:15:00"),  # messy but valid
        (2, None, "50.00", "completed", "2024-01-05T11:00:00"),   # NULL key -> dropped
        (3, 102, "-5.00", "completed", "2024-01-05T12:00:00"),    # bad amount -> dropped
        (1, 101, "120.50", "Completed ", "2024-01-05T10:15:00"),  # duplicate -> dropped
    ],
    "order_id INT, customer_id INT, amount STRING, status STRING, order_ts STRING",
)

expected = spark.createDataFrame(  # noqa: F821
    [(1, 101, "120.50", "completed", "2024-01-05T10:15:00")],
    "order_id INT, customer_id INT, amount STRING, status STRING, order_ts STRING",
).select(
    "order_id", "customer_id",
    F.col("amount").cast("decimal(10,2)").alias("amount"),
    "status",
    F.to_timestamp("order_ts").alias("order_ts"),
)

assertDataFrameEqual(cleanse_orders(input_df), expected)
print("cleanse_orders: PASS")

# COMMAND ----------

# See what a failure looks like — the diff names the row and column
try:
    assertDataFrameEqual(
        cleanse_orders(input_df),
        expected.withColumn("status", F.lit("pending")),
    )
except Exception as e:
    print(f"{type(e).__name__} (diff excerpt):")
    print(str(e)[:400])

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `assertDataFrameEqual` ignores **row order** by default (set
# MAGIC >   `checkRowOrder=True` to enforce it) — DataFrames have no order anyway.
# MAGIC > - `assertSchemaEqual` exists for schema-only checks.
# MAGIC > - Tests that read production tables are **integration** tests; unit tests
# MAGIC >   build inputs with `createDataFrame` so they run anywhere, fast, without
# MAGIC >   data access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How this scales into a real repo
# MAGIC
# MAGIC ```
# MAGIC my_pipeline/
# MAGIC ├── src/my_pipeline/transforms.py    <- cleanse_orders() lives here
# MAGIC ├── tests/test_transforms.py         <- pytest + a local/serverless SparkSession
# MAGIC ├── notebooks/silver_orders.py       <- thin: read -> cleanse_orders() -> write
# MAGIC └── databricks.yml                   <- the bundle (notebook 03)
# MAGIC ```
# MAGIC
# MAGIC - Notebooks stay **thin wrappers** (read, call functions, write); logic
# MAGIC   lives in the package where pytest can reach it.
# MAGIC - CI runs pytest on every PR (Spark Connect / databricks-connect or local
# MAGIC   Spark); merges deploy the bundle to staging, run an **integration** job
# MAGIC   on real-but-small data, then promote to prod (notebook 03).
# MAGIC - DLT expectations (section 03) are the *runtime* safety net; tests are the
# MAGIC   *pre-deploy* one. The exam likes asking which catches what: a logic bug →
# MAGIC   tests; a bad upstream batch → expectations.
