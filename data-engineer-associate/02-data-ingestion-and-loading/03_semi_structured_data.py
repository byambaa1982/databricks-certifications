# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Semi-structured and unstructured data
# MAGIC
# MAGIC Real feeds are nested. The exam wants three skills: reading nested JSON
# MAGIC into **structs and arrays** (and navigating them), parsing JSON that
# MAGIC arrives as **strings** inside a column, and knowing where **unstructured**
# MAGIC files live (volumes). Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

from pyspark.sql import functions as F  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Nested JSON → structs and arrays, automatically

# COMMAND ----------

customers = spark.read.json(f"{LANDING}/customers_nested")  # noqa: F821
customers.printSchema()  # contact is a STRUCT, order_ids an ARRAY

# COMMAND ----------

# MAGIC %md
# MAGIC ## Navigate: dot notation for structs, `explode` for arrays

# COMMAND ----------

display(  # noqa: F821
    customers.select(
        "customer_id",
        "name",
        F.col("contact.email").alias("email"),      # struct field -> dot path
        F.size("order_ids").alias("n_orders"),
        F.explode("order_ids").alias("order_id"),   # one row per array element
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `explode` **multiplies rows** (one per element) — aggregate counts
# MAGIC >   change after exploding, a favorite "what does this return" setup.
# MAGIC > - `explode` **drops** rows with NULL/empty arrays; `explode_outer` keeps
# MAGIC >   them (with NULL).
# MAGIC > - Dot paths work in SQL too: `SELECT contact.email FROM …` — and `*`
# MAGIC >   flattening: `SELECT contact.* FROM …`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## JSON that arrives as a STRING column
# MAGIC Common bronze shape: the whole payload in one string column. Two tools —
# MAGIC the `:` path operator (no schema needed, returns strings) and
# MAGIC `from_json` (typed structs, needs a schema).

# COMMAND ----------

# build that shape: one raw JSON string per row
customers.select(
    F.col("customer_id"),
    F.to_json(F.struct("name", "tier", "contact", "order_ids")).alias("raw"),
).createOrReplaceTempView("customers_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   raw:name                AS name,          -- ':' extracts by path, as STRING
# MAGIC   raw:contact.email       AS email,
# MAGIC   raw:order_ids[0]        AS first_order
# MAGIC FROM customers_raw

# COMMAND ----------

# from_json gives real types back — schema_of_json can bootstrap the schema
sample = spark.table("customers_raw").select("raw").first()["raw"]  # noqa: F821
schema = spark.sql(  # noqa: F821
    "SELECT schema_of_json('" + sample.replace("'", "\\'") + "') AS s"
).first()["s"]
print(f"inferred: {schema}")

display(  # noqa: F821
    spark.table("customers_raw")  # noqa: F821
    .withColumn("parsed", F.from_json("raw", schema))
    .select("customer_id", "parsed.name", "parsed.contact.email")
)

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `:` returns **STRING** (cast if you need numbers); `from_json` returns
# MAGIC >   typed structs but needs a schema.
# MAGIC > - The **VARIANT** type (`parse_json(raw)`) is the newer answer for "store
# MAGIC >   JSON whose shape keeps changing, query it fast without a schema".
# MAGIC > - CSV/JSON readers put unparseable *rows* in `_corrupt_record`
# MAGIC >   (PERMISSIVE mode); Auto Loader puts unfittable *values* in
# MAGIC >   `_rescued_data`. Different mechanisms — keep the names straight.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unstructured files — volumes are the governed home
# MAGIC PDFs, images, models, checkpoints: they live in **UC volumes**
# MAGIC (`/Volumes/catalog/schema/volume/…`), governed by `READ VOLUME` /
# MAGIC `WRITE VOLUME` — not in DBFS root, not behind mounts (both legacy).
# MAGIC `spark.read.format("binaryFile")` ingests them as (path, bytes) rows when
# MAGIC you need them in a table.

# COMMAND ----------

files = spark.read.format("binaryFile").load(f"{LANDING}/customers_csv")  # noqa: F821
display(files.select("path", "length", "modificationTime"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup — only a temp view was created; nothing to drop
