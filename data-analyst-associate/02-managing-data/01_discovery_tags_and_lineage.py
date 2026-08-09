# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Managing data: find it, label it, trust it
# MAGIC
# MAGIC The analyst's governance section: discovering tables you didn't build,
# MAGIC labeling the ones you curate, and answering "can I trust this column?"
# MAGIC with metadata instead of Slack archaeology. Live cells use whatever
# MAGIC catalog/schema your widgets point at.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discovery — Catalog Explorer for browsing, `information_schema` for SQL
# MAGIC Catalog Explorer shows a table's schema, sample data, details, lineage,
# MAGIC and permissions in one place. The same metadata is queryable:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, table_type, comment
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = current_catalog() AND table_schema = current_schema()
# MAGIC ORDER BY table_name
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Label what you curate: comments and tags

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE curated_demo AS
# MAGIC SELECT 101 AS customer_id, 'bat@example.com' AS email, 'gold' AS tier;
# MAGIC
# MAGIC COMMENT ON TABLE curated_demo IS 'Demo: current customer tiers, one row per customer';
# MAGIC ALTER TABLE curated_demo ALTER COLUMN email COMMENT 'Contact email — PII';
# MAGIC
# MAGIC ALTER TABLE curated_demo SET TAGS ('quality' = 'gold', 'domain' = 'customers');
# MAGIC ALTER TABLE curated_demo ALTER COLUMN email SET TAGS ('pii' = 'email')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- tags are queryable inventory — the "find all PII" sweep
# MAGIC SELECT table_name, column_name, tag_name, tag_value
# MAGIC FROM system.information_schema.column_tags
# MAGIC WHERE schema_name = current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - **Comments** are free-text docs; **tags** are key-value classification
# MAGIC >   you can query and build policy on; the **certified** badge is set by
# MAGIC >   privileged users to mark the canonical table. Three different tools.
# MAGIC > - Tagging a column `pii` doesn't *protect* it — masks/grants do (section
# MAGIC >   09). Tags make the column *findable* (and drive ABAC where enabled).
# MAGIC > - Catalog Explorer can draft AI-generated comments — a human approves.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage — automatic, two directions
# MAGIC Captured for anything that runs through UC compute, table- and
# MAGIC column-level. Upstream = "where did this come from" (audit a number);
# MAGIC downstream = "who reads this" (impact analysis before changing it).

# COMMAND ----------

try:
    display(  # noqa: F821
        spark.sql("""
            SELECT source_table_full_name, target_table_full_name, event_time
            FROM system.access.table_lineage
            ORDER BY event_time DESC
            LIMIT 10
        """)
    )
except Exception as e:
    print(f"system.access.table_lineage not accessible here ({type(e).__name__}) — "
          "use the Lineage tab in Catalog Explorer instead")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning data with SQL — the analyst's short list
# MAGIC You'll clean *inside queries* more than you'll build pipelines. The moves
# MAGIC that show up: `trim`/`lower` for messy strings, `coalesce` for defaults,
# MAGIC `try_cast` for bad values (NULL instead of error), `CASE WHEN` for
# MAGIC bucketing, and `DISTINCT` / `GROUP BY` for dedup — all drilled with real
# MAGIC data in section [`04`](../04-executing-queries/).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS curated_demo
