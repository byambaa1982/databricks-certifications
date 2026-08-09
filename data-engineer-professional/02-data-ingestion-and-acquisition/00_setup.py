# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup — seed the `landing` volume with raw files
# MAGIC
# MAGIC Section 02 is about **getting files into Delta**, so this setup builds the
# MAGIC one thing every ingestion notebook needs: a landing zone full of raw JSON
# MAGIC and CSV files. Everything is synthetic and self-contained — no dependency
# MAGIC on lakehouse-starter (it happily shares the `de_pro` schema if section 01's
# MAGIC setup already created it).
# MAGIC
# MAGIC **Widgets:**
# MAGIC - `mode = reset` (default) — wipe the landing zone, write **batch 1**, drop the section's scratch tables and checkpoints
# MAGIC - `mode = more` — add **batch 2**: a new file that carries a **new column**
# MAGIC   (`channel`) and one unparseable `amount`. Run this between passes to give
# MAGIC   `COPY INTO` and Auto Loader something new to find.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
dbutils.widgets.dropdown("mode", "reset", ["reset", "more"])  # noqa: F821

CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
MODE = dbutils.widgets.get("mode")  # noqa: F821

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

SCRATCH_TABLES = ["bronze_orders_ctas", "orders_copy_into", "orders_autoloader"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema and volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")  # noqa: F821
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The raw files
# MAGIC Batch 1 is what a normal week looks like. Batch 2 is what production actually
# MAGIC sends you eventually: a **new column** the producer added without telling
# MAGIC anyone, and a value (`"amount": "N/A"`) that doesn't match the inferred type.

# COMMAND ----------

BATCH_1 = {
    "orders/orders_2024_01_05.json": "\n".join([
        '{"order_id": 5001, "customer_id": 101, "amount": 120.50, "status": "completed", "order_ts": "2024-01-05T10:15:00"}',
        '{"order_id": 5002, "customer_id": 102, "amount": 39.99, "status": "completed", "order_ts": "2024-01-05T11:02:00"}',
        '{"order_id": 5003, "customer_id": 103, "amount": 640.00, "status": "pending", "order_ts": "2024-01-05T12:30:00"}',
        '{"order_id": 5004, "customer_id": 101, "amount": 89.00, "status": "cancelled", "order_ts": "2024-01-05T13:45:00"}',
    ]),
    "orders/orders_2024_01_12.json": "\n".join([
        '{"order_id": 5005, "customer_id": 104, "amount": 250.00, "status": "completed", "order_ts": "2024-01-12T09:20:00"}',
        '{"order_id": 5006, "customer_id": 102, "amount": 15.75, "status": "completed", "order_ts": "2024-01-12T10:05:00"}',
        '{"order_id": 5007, "customer_id": 105, "amount": 480.25, "status": "pending", "order_ts": "2024-01-12T14:11:00"}',
    ]),
    # customers.csv: row 103 has a quoted comma (fine), row 104 is missing a
    # column, row 105 has an extra one — notebook 01 uses these for parse modes.
    "customers/customers.csv": "\n".join([
        "customer_id,name,email,tier",
        "101,Bat,bat@example.com,gold",
        "102,Naran,naran@example.com,silver",
        '103,"Lee, Sara",sara.lee@example.com,gold',
        "104,Dulguun,dulguun@example.com",
        "105,Tuya,tuya@example.com,bronze,extra_field",
    ]),
}

BATCH_2 = {
    "orders/orders_2024_02_02.json": "\n".join([
        '{"order_id": 5008, "customer_id": 103, "amount": 75.00, "status": "completed", "order_ts": "2024-02-02T08:30:00", "channel": "web"}',
        '{"order_id": 5009, "customer_id": 106, "amount": 310.40, "status": "completed", "order_ts": "2024-02-02T09:15:00", "channel": "mobile"}',
        '{"order_id": 5010, "customer_id": 104, "amount": "N/A", "status": "pending", "order_ts": "2024-02-02T11:00:00", "channel": "web"}',
        '{"order_id": 5011, "customer_id": 101, "amount": 55.20, "status": "completed", "order_ts": "2024-02-02T12:40:00", "channel": "store"}',
    ]),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the batch

# COMMAND ----------

if MODE == "reset":
    dbutils.fs.rm(LANDING, recurse=True)  # noqa: F821
    for sub in ("ingest_autoloader", "ingest_autoloader_schema"):
        dbutils.fs.rm(f"{CKPT}/{sub}", recurse=True)  # noqa: F821
    for table in SCRATCH_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {table}")  # noqa: F821
    files = BATCH_1
    print("reset: landing zone wiped, scratch tables dropped, writing batch 1")
else:
    files = BATCH_2
    print("more: adding batch 2 on top of whatever is already there")

for rel_path, content in files.items():
    dbutils.fs.put(f"{LANDING}/{rel_path}", content, overwrite=True)  # noqa: F821
    print(f"wrote {rel_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

for d in dbutils.fs.ls(LANDING):  # noqa: F821
    for f in dbutils.fs.ls(d.path):  # noqa: F821
        print(f"{f.path}  ({f.size} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Ready. Open `01_batch_reads_and_read_options` next. When a notebook says
# MAGIC *"add new files"*, come back here and run with `mode = more`.
