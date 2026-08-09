# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — The Unity Catalog object model
# MAGIC
# MAGIC Governance questions start with *where things live*: one *metastore* per
# MAGIC region, catalogs → schemas → securables inside it, and the managed-vs-
# MAGIC external split that decides **what DROP actually deletes**. Requires
# MAGIC section 01's `00_setup` for the live cells.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The hierarchy
# MAGIC
# MAGIC ```
# MAGIC metastore (one per region, attached to workspaces)
# MAGIC └── catalog                     ← environment/domain boundary (dev, prod, finance)
# MAGIC     └── schema                  ← project/team boundary
# MAGIC         ├── tables (managed | external)   ├── views / MVs
# MAGIC         ├── volumes (managed | external)  ├── functions
# MAGIC         └── models
# MAGIC metastore-level plumbing: storage credentials, external locations,
# MAGIC connections, shares/recipients
# MAGIC ```
# MAGIC
# MAGIC Three-level names everywhere: `catalog.schema.object`. Workspaces are
# MAGIC **access points**, not boundaries — the same catalog can be reachable from
# MAGIC many workspaces (binding is configurable).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Managed vs external — the DROP question

# COMMAND ----------

t = spark.sql("DESCRIBE EXTENDED silver_orders").collect()  # noqa: F821
for row in t:
    if row.col_name in ("Type", "Location", "Provider"):
        print(f"{row.col_name:10s} {row.data_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC | | Managed table | External table |
# MAGIC |---|---|---|
# MAGIC | Data lives | metastore/catalog/schema's managed storage | a path **you** chose (external location) |
# MAGIC | `DROP TABLE` | deletes metadata **and (after 30 days) the data** | deletes **metadata only** — files stay |
# MAGIC | `UNDROP TABLE` | ✅ within 30 days | n/a (data never left) |
# MAGIC | Predictive optimization, some perf features | ✅ | limited |
# MAGIC | When | default — use unless you have a reason | existing data lakes, external writers/readers |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "We dropped the external table — how do we recover the data?" → the
# MAGIC >   **files were never deleted**; re-create the table on the same location.
# MAGIC > - `UNDROP TABLE` works for **managed** tables within the retention window.
# MAGIC > - Volumes follow the same managed/external logic, but for **files**
# MAGIC >   (checkpoints, uploads, models) — they are the governed replacement for
# MAGIC >   mounting storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The storage plumbing under external things
# MAGIC
# MAGIC ```
# MAGIC STORAGE CREDENTIAL  (an IAM role / managed identity UC can assume)
# MAGIC └── EXTERNAL LOCATION  (that credential + a cloud path prefix, GRANT-able)
# MAGIC     └── external tables / external volumes under that prefix
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC CREATE EXTERNAL LOCATION landing_zone
# MAGIC URL 's3://corp-landing/prod'
# MAGIC WITH (STORAGE CREDENTIAL corp_ingest_role);
# MAGIC
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION landing_zone TO `engineers`;
# MAGIC ```
# MAGIC
# MAGIC > **Exam trap:** direct-path access (`s3://…` in code) bypasses none of
# MAGIC > this — UC checks external-location grants for path reads too. Legacy
# MAGIC > DBFS mounts are the *un*-governed thing being replaced by volumes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `information_schema` — governance as queryable tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, table_type, data_source_format
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = current_catalog() AND table_schema = 'de_pro'
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC Every catalog also carries its own `information_schema`
# MAGIC (`dev_catalog.information_schema.…`) scoped to that catalog;
# MAGIC `system.information_schema` spans the metastore. Columns, grants, tags,
# MAGIC constraints — all appear here, which makes governance auditable with SQL.
