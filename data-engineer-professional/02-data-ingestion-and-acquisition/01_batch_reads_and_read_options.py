# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Batch reads and read options
# MAGIC
# MAGIC `spark.read` is the one-shot tool: it reads whatever matches the path,
# MAGIC every time, with no memory of previous runs. The exam's ingestion questions
# MAGIC usually hinge on **which loader remembers what** — this notebook covers the
# MAGIC one that remembers nothing; `COPY INTO` (notebook 02) and Auto Loader
# MAGIC (notebook 03) are the ones that track files. Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

from pyspark.sql import functions as F  # noqa: E402, F401
from pyspark.sql.types import (  # noqa: E402
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV — the options do the work
# MAGIC Without `header` the column names become `_c0, _c1, …`; without a schema
# MAGIC every column is STRING.
# MAGIC
# MAGIC > **Exam trap:** `inferSchema=true` costs an **extra full pass over the
# MAGIC > files** just to guess types. In production you provide an explicit schema —
# MAGIC > it's faster and it's the only way to catch drift instead of absorbing it.

# COMMAND ----------

display(  # noqa: F821
    spark.read.option("header", "true")  # noqa: F821
    .option("inferSchema", "true")
    .csv(f"{LANDING}/customers")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse modes: PERMISSIVE vs DROPMALFORMED vs FAILFAST
# MAGIC `customers.csv` has one row with a **missing** column and one with an
# MAGIC **extra** column. What happens to them is entirely decided by `mode`:
# MAGIC
# MAGIC | mode | malformed rows become |
# MAGIC |---|---|
# MAGIC | `PERMISSIVE` (default) | NULLs, and the raw line lands in `_corrupt_record` |
# MAGIC | `DROPMALFORMED` | silently dropped |
# MAGIC | `FAILFAST` | an exception — the read dies |

# COMMAND ----------

customers_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("tier", StringType()),
    StructField("_corrupt_record", StringType()),  # PERMISSIVE parks bad lines here
])

permissive = (
    spark.read.schema(customers_schema)  # noqa: F821
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .csv(f"{LANDING}/customers")
)
display(permissive)  # noqa: F821

# COMMAND ----------

dropped = (
    spark.read.schema(customers_schema)  # noqa: F821
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .csv(f"{LANDING}/customers")
)
print(f"PERMISSIVE keeps {permissive.count()} rows, DROPMALFORMED keeps {dropped.count()}")

# COMMAND ----------

try:
    (
        spark.read.schema(customers_schema)  # noqa: F821
        .option("header", "true")
        .option("mode", "FAILFAST")
        .csv(f"{LANDING}/customers")
        .count()
    )
except Exception as e:
    print(f"FAILFAST raised {type(e).__name__} — exactly what you want in a pipeline "
          "that must never load bad data silently")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** `_corrupt_record` only exists if it's **in the schema you
# MAGIC > provide** (or via `columnNameOfCorruptRecord`). And a query that selects
# MAGIC > *only* `_corrupt_record` errors out — Spark needs at least one real column
# MAGIC > in the projection (or a cached DataFrame) to know which rows failed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## JSON — one object per line, unless you say otherwise
# MAGIC Spark's JSON reader expects **JSON Lines**. A pretty-printed file (one array
# MAGIC spread over many lines) reads as garbage unless `multiLine=true`. Schema
# MAGIC inference on JSON also scans the data — same production advice as CSV.

# COMMAND ----------

orders_raw = spark.read.json(f"{LANDING}/orders")  # noqa: F821
orders_raw.printSchema()
display(orders_raw.orderBy("order_id"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## `read_files()` — the SQL twin
# MAGIC Same engine, SQL syntax, and it adds a `_rescued_data` column **by default**
# MAGIC to catch anything that didn't fit the inferred schema. This is also what
# MAGIC `CREATE STREAMING TABLE … AS SELECT … FROM STREAM read_files(…)` builds on.

# COMMAND ----------

query = f"SELECT * FROM read_files('{LANDING}/orders', format => 'json') ORDER BY order_id"
display(spark.sql(query))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## CTAS — from raw files straight to a Delta table
# MAGIC The classic "just get it into bronze" move. Note what it is **not**:
# MAGIC incremental. Every run re-reads *all* files and rebuilds the table.

# COMMAND ----------

ctas = f"""
    CREATE OR REPLACE TABLE bronze_orders_ctas AS
    SELECT
      order_id,
      customer_id,
      cast(amount AS decimal(10,2)) AS amount,
      status,
      to_timestamp(order_ts)        AS order_ts,
      current_timestamp()           AS _ingested_at,
      _metadata.file_path           AS _source_file
    FROM json.`{LANDING}/orders`
"""
spark.sql(ctas)  # noqa: F821
display(spark.sql("SELECT count(*) AS rows_loaded FROM bronze_orders_ctas"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam trap:** `json.\`/path\`` / `read_files` / `spark.read` all re-read
# MAGIC > **everything, every run** — run it twice on a growing folder and CTAS
# MAGIC > rebuilds while `INSERT INTO … SELECT` **duplicates**. The moment a question
# MAGIC > says "only new files" or "incremental", the answer is `COPY INTO` or Auto
# MAGIC > Loader, never a plain read. Also: `_metadata.file_path` (used above) is how
# MAGIC > you keep file lineage — it's a hidden column, present on every file source.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_orders_ctas
