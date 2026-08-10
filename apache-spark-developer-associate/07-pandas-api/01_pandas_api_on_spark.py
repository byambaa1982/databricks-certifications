# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Pandas API on Spark
# MAGIC
# MAGIC `pyspark.pandas` gives pandas syntax over distributed Spark execution —
# MAGIC the migration path for pandas codebases that outgrew one machine. The
# MAGIC exam tests the conversions and the places where pandas habits bite.

# COMMAND ----------

import pyspark.pandas as ps  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402, F401

psdf = ps.DataFrame({
    "customer_id": [101, 102, 103, 104],
    "amount": [120.5, 39.9, 640.0, 89.0],
    "tier": ["gold", "silver", "gold", "bronze"],
})
# pandas idioms, distributed execution:
psdf["with_tax"] = psdf["amount"] * 1.1
print(psdf.groupby("tier")["amount"].sum().sort_index().to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## The three DataFrames and the conversions
# MAGIC
# MAGIC | | pandas | **pandas-on-Spark** | Spark DataFrame |
# MAGIC |---|---|---|---|
# MAGIC | lives | driver memory | cluster | cluster |
# MAGIC | API | pandas | pandas | Spark |
# MAGIC | eager/lazy | eager | mostly lazy underneath | lazy |
# MAGIC
# MAGIC ```python
# MAGIC sdf  = psdf.to_spark()          # pandas-on-Spark -> Spark DF
# MAGIC psdf = sdf.pandas_api()         # Spark DF        -> pandas-on-Spark
# MAGIC pdf  = psdf.to_pandas()         # -> DRIVER memory (small data only!)
# MAGIC psdf = ps.from_pandas(pdf)      # pandas          -> distributed
# MAGIC ```

# COMMAND ----------

sdf = psdf.to_spark()
print(f"round trip: {type(sdf).__name__} -> {type(sdf.pandas_api()).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `to_pandas()` collects to the **driver** — same OOM as `collect()`;
# MAGIC >   the "why did converting crash the driver" question.
# MAGIC > - Operations needing a **global row order** (`iloc` by position,
# MAGIC >   `shift`, cumulative ops) are expensive or restricted — distributed
# MAGIC >   data has no free total order. Sorting is explicit.
# MAGIC > - The default index can be costly: `ps.set_option(
# MAGIC >   "compute.default_index_type", "distributed")` avoids the
# MAGIC >   sequence-index shuffle — index handling is THE performance gotcha.
# MAGIC > - Don't confuse with **`pandas_udf`** — that's a Spark DataFrame tool
# MAGIC >   applying vectorized pandas functions; `pyspark.pandas` is a whole
# MAGIC >   pandas-style API. (And plain `spark.createDataFrame(pdf)` accepts a
# MAGIC >   pandas DataFrame directly.)
# MAGIC
# MAGIC ## When to choose what
# MAGIC
# MAGIC | Situation | Use |
# MAGIC |---|---|
# MAGIC | pandas code, data no longer fits | pandas API on Spark (minimal rewrite) |
# MAGIC | new pipeline code | Spark DataFrame API (full control, clearest plans) |
# MAGIC | small result needs a pandas-only library | `.to_pandas()` at the *end* |
# MAGIC | per-group pandas logic inside Spark | `applyInPandas` (ML Professional's pattern) |
