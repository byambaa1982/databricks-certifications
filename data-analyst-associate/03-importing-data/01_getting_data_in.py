# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Importing data, the analyst's five doors
# MAGIC
# MAGIC The smallest section: know which door fits which situation. Only one of
# MAGIC the five needs engineering help — recognizing that boundary **is** the
# MAGIC exam content.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The five doors
# MAGIC
# MAGIC | Situation | Door |
# MAGIC |---|---|
# MAGIC | "I have a CSV/Excel on my laptop" | **UI upload** — Catalog Explorer → *Create table* (≤ ~2GB, creates a UC table with inferred types you can edit) |
# MAGIC | files already in cloud storage / a volume | `read_files()` for a look, **CTAS** to land it, `COPY INTO` when it keeps arriving |
# MAGIC | a vendor/partner shares data | **Delta Sharing** — their share becomes a read-only catalog, always current, no copies |
# MAGIC | public/commercial datasets | **Marketplace** (Delta Sharing underneath) |
# MAGIC | continuous feeds, APIs, operational databases | **hand it to engineering** — Auto Loader / Lakeflow Connect (their sections, not yours) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The SQL you do own — `read_files` and CTAS

# COMMAND ----------

# a peek at files without creating anything (works on any volume path you can read)
query = f"""
    SELECT * FROM read_files(
      '/Volumes/{CATALOG}/{SCHEMA}/landing/orders',
      format => 'json'
    ) LIMIT 5
"""
try:
    display(spark.sql(query))  # noqa: F821
except Exception as e:
    print(f"no landing volume here ({type(e).__name__}) — run "
          "data-engineer-associate/02's 00_setup to create one, or point the "
          "widgets at a volume you have")

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- land it as a table (one-shot)
# MAGIC CREATE OR REPLACE TABLE bronze_orders AS
# MAGIC SELECT * FROM read_files('/Volumes/.../landing/orders', format => 'json');
# MAGIC
# MAGIC -- keep landing new files idempotently (incremental batch)
# MAGIC COPY INTO bronze_orders
# MAGIC FROM '/Volumes/.../landing/orders'
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ('inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - `read_files` / CTAS re-read **everything, every run**; `COPY INTO`
# MAGIC >   loads **only new files** (it tracks them in the target table). The
# MAGIC >   word "incremental" in a question is pointing at `COPY INTO` (or Auto
# MAGIC >   Loader, engineering's tool).
# MAGIC > - The UI upload creates a **managed UC table** — governable, shareable —
# MAGIC >   not a file on a cluster.
# MAGIC > - Delta Sharing data is **read-only and live**: no refresh jobs, no
# MAGIC >   copies; if you need to transform it, CTAS *your* copy into your schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Receiving a share (the consumer side, reference)
# MAGIC
# MAGIC ```sql
# MAGIC SHOW PROVIDERS;                                   -- who shares with us
# MAGIC CREATE CATALOG partner_data USING SHARE `acme_provider`.orders_share;
# MAGIC SELECT * FROM partner_data.sales.orders LIMIT 10; -- live, read-only
# MAGIC ```
# MAGIC
# MAGIC Marketplace does this for you — "Get instant access" mounts the listing
# MAGIC as a catalog the same way.
# MAGIC
# MAGIC Deep dive: engineering
# MAGIC [`04-data-sharing-and-federation`](../../data-engineer-professional/04-data-sharing-and-federation/)
# MAGIC (provider side, tokens, D2D vs open) and
# MAGIC [`02-data-ingestion-and-loading`](../../data-engineer-associate/02-data-ingestion-and-loading/)
# MAGIC (Auto Loader, the continuous door).
