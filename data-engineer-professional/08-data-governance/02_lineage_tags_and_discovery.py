# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Lineage, tags, and discovery
# MAGIC
# MAGIC Governance beyond access control: **where data came from** (lineage,
# MAGIC captured automatically), **what it is** (tags, comments), and **how people
# MAGIC find it** (Catalog Explorer / `information_schema`). Requires section 01's
# MAGIC `00_setup` for the live cells.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage — you don't build it, you query it
# MAGIC Unity Catalog records lineage **automatically** for anything that runs
# MAGIC through it — SQL, Python, jobs, DLT — at **table and column level**,
# MAGIC across workspaces. UI: Catalog Explorer → table → Lineage. SQL:

# COMMAND ----------

try:
    display(  # noqa: F821
        spark.sql(f"""
            SELECT source_table_full_name, target_table_full_name,
                   entity_type, event_time
            FROM system.access.table_lineage
            WHERE target_table_full_name LIKE '{CATALOG}.{SCHEMA}.%'
               OR source_table_full_name LIKE '{CATALOG}.{SCHEMA}.%'
            ORDER BY event_time DESC
            LIMIT 20
        """)
    )
except Exception as e:
    print(f"system.access.table_lineage not accessible here ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - Lineage capture requires the compute to be **UC-enabled** and the code
# MAGIC >   to address tables by name — reading raw files by path breaks the chain.
# MAGIC > - `system.access.table_lineage` / `column_lineage` hold **events** (with
# MAGIC >   retention ~1 year), not a static graph — dedupe by entity when counting.
# MAGIC > - Column-level lineage exists (which output column came from which input
# MAGIC >   column) — the answer to "impact analysis before dropping a column".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tags — classification you can query and act on

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver_customers SET TAGS ('quality' = 'silver', 'domain' = 'customers');
# MAGIC ALTER TABLE silver_customers ALTER COLUMN email SET TAGS ('pii' = 'email');
# MAGIC
# MAGIC SELECT * FROM system.information_schema.table_tags
# MAGIC WHERE schema_name = 'de_pro'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- find ALL columns tagged as PII across the schema — the compliance sweep
# MAGIC SELECT table_name, column_name, tag_name, tag_value
# MAGIC FROM system.information_schema.column_tags
# MAGIC WHERE schema_name = 'de_pro'

# COMMAND ----------

# MAGIC %md
# MAGIC Tags are key(-value) labels on any securable, inherited nowhere but
# MAGIC queryable everywhere (`information_schema.*_tags`). Standard uses: PII
# MAGIC classification (drives "find every PII column" audits, pairs with masks
# MAGIC from section 07), cost attribution, medallion quality level, certification
# MAGIC status. Comments (`COMMENT ON …`) carry the human description; Catalog
# MAGIC Explorer can draft them with AI, and **certified** badges mark canonical
# MAGIC tables for discovery.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup — remove the demo tags

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver_customers UNSET TAGS ('quality', 'domain');
# MAGIC ALTER TABLE silver_customers ALTER COLUMN email UNSET TAGS ('pii')

# COMMAND ----------

# MAGIC %md
# MAGIC ## The section in one table
# MAGIC
# MAGIC | Need | Tool |
# MAGIC |---|---|
# MAGIC | "what feeds this table / what breaks if I drop it" | lineage (UI or `system.access.*_lineage`) |
# MAGIC | "find every PII column" | column tags + `information_schema.column_tags` |
# MAGIC | "which is the *official* revenue table" | certification + comments in Catalog Explorer |
# MAGIC | "who owns this / who can read it" | ownership + grants (section 07) |
# MAGIC | "external files under governance" | volumes + external locations (notebook 01) |
