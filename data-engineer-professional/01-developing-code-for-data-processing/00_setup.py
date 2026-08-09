# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — build the `dev_catalog.de_pro` practice schema
# MAGIC
# MAGIC Clones the tables deployed by
# MAGIC [lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter) into a
# MAGIC dedicated study schema. Work happens on the **clones**, so you can freely
# MAGIC `MERGE`, `UPDATE`, and `DELETE` in the other notebooks without touching the
# MAGIC real pipeline outputs — re-run this notebook anytime to reset everything.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("study_schema", "de_pro")  # noqa: F821
dbutils.widgets.text("source_schema", "")  # noqa: F821 - blank = auto-detect

CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("study_schema")  # noqa: F821
SOURCE_SCHEMA = dbutils.widgets.get("source_schema")  # noqa: F821

TABLES = [
    "bronze_orders",
    "bronze_customers_cdc",
    "silver_orders",
    "silver_customers",
    "gold_daily_revenue",
    "gold_customer_revenue",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the source schema
# MAGIC Dev-mode bundle deployments prefix the schema (`dev_<user>_lakehouse_starter`),
# MAGIC so instead of hardcoding it we look for whichever schema holds the medallion tables.

# COMMAND ----------

if not SOURCE_SCHEMA:
    for row in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect():  # noqa: F821
        candidate = row.databaseName if hasattr(row, "databaseName") else row.schemaName
        tables = {t.tableName for t in spark.sql(f"SHOW TABLES IN {CATALOG}.{candidate}").collect()}  # noqa: F821
        if set(TABLES).issubset(tables):
            SOURCE_SCHEMA = candidate
            break
    else:
        raise ValueError(
            f"No schema in {CATALOG} contains all medallion tables. "
            "Deploy lakehouse-starter first, or set the source_schema widget."
        )
print(f"Cloning from {CATALOG}.{SOURCE_SCHEMA} -> {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the study schema and clone the tables
# MAGIC `DEEP CLONE` copies data **and** table properties, but it is not allowed on
# MAGIC Streaming Tables / Materialized Views (which is what the Lakeflow pipeline
# MAGIC produces) — those fall back to `CREATE TABLE AS SELECT`, and we re-enable
# MAGIC Change Data Feed on `silver_orders` explicitly since notebook 05 relies on it.

# COMMAND ----------

from pyspark.errors import AnalysisException  # noqa: E402

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(  # noqa: F821
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints"
)  # streaming notebooks need somewhere to store checkpoints

for table in TABLES:
    try:
        spark.sql(  # noqa: F821
            f"CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{table} "
            f"DEEP CLONE {CATALOG}.{SOURCE_SCHEMA}.{table}"
        )
        print(f"cloned {table}")
    except AnalysisException:
        spark.sql(  # noqa: F821
            f"CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.{table} "
            f"AS SELECT * FROM {CATALOG}.{SOURCE_SCHEMA}.{table}"
        )
        print(f"copied {table} (CTAS — source is a streaming table / MV)")

# CDF must survive the copy: notebook 05 reads the change feed of silver_orders
spark.sql(  # noqa: F821
    f"ALTER TABLE {CATALOG}.{SCHEMA}.silver_orders "
    "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

import pandas as pd  # noqa: E402

counts = [
    (t, spark.table(f"{CATALOG}.{SCHEMA}.{t}").count())  # noqa: F821
    for t in TABLES
]
display(spark.createDataFrame(pd.DataFrame(counts, columns=["table", "rows"])))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Ready. Open `01_dataframe_transformations` next — every notebook in this
# MAGIC folder starts with `USE {catalog}.{study_schema}`, so they all run against
# MAGIC these clones.
