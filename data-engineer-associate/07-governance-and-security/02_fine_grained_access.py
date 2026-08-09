# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Fine-grained access: masks, filters, and ABAC
# MAGIC
# MAGIC Grants decide *whether* you reach a table; this notebook decides *what you
# MAGIC see once you do* — run live so you watch your own email get redacted.
# MAGIC Self-contained (builds its own table).

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers_fga AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (101, 'Bat',     'bat@example.com',      'gold'),
# MAGIC   (102, 'Naran',   'naran@example.com',    'silver'),
# MAGIC   (103, 'Sara',    'sara.lee@example.com', 'gold'),
# MAGIC   (104, 'Dulguun', 'dulguun@example.com',  'bronze')
# MAGIC AS t(customer_id, name, email, tier)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column mask — non-admins see redacted emails
# MAGIC A SQL UDF taking the column's type, returning the same type, bound with
# MAGIC `SET MASK`. `is_account_group_member('admins')` is false for most of us,
# MAGIC so the mask visibly fires:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION mask_email(email STRING)
# MAGIC RETURN CASE WHEN is_account_group_member('admins') THEN email
# MAGIC             ELSE concat('***@', split_part(email, '@', 2)) END;
# MAGIC
# MAGIC ALTER TABLE customers_fga ALTER COLUMN email SET MASK mask_email;
# MAGIC
# MAGIC SELECT * FROM customers_fga

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row filter — non-admins see only gold-tier rows

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION gold_rows_only(tier STRING)
# MAGIC RETURN is_account_group_member('admins') OR tier = 'gold';
# MAGIC
# MAGIC ALTER TABLE customers_fga SET ROW FILTER gold_rows_only ON (tier);
# MAGIC
# MAGIC SELECT count(*) AS visible_rows FROM customers_fga

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - Filters and masks bind to the **table** and apply to **every reader,
# MAGIC >   owner included** — exemptions live *inside* the UDF (the group-member
# MAGIC >   branch), not in grants.
# MAGIC > - The legacy tool is a **dynamic view** (`CASE WHEN
# MAGIC >   is_account_group_member(...)` in the view body) with users granted the
# MAGIC >   view only — still on the exam as the contrast option.
# MAGIC > - The functions to recognize: `current_user()`,
# MAGIC >   `is_account_group_member('…')`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ABAC — govern by tag, not by table
# MAGIC Attribute-based access control scales the same idea: tag columns/tables
# MAGIC (e.g. `pii = 'email'`), then write **policies against the tags** —
# MAGIC "mask every column tagged `pii` for everyone outside
# MAGIC `privacy-cleared`" — instead of binding a mask to each table by hand.
# MAGIC
# MAGIC | | Manual filters/masks | ABAC |
# MAGIC |---|---|---|
# MAGIC | Bound to | one table / column at a time | **every** object carrying the tag |
# MAGIC | New PII column appears | someone must remember to mask it | tag it and the policy already applies |
# MAGIC | Exam phrase | "mask this column" | "enforce masking **across all** tables containing PII" |
# MAGIC
# MAGIC Tags are also queryable inventory — the PII sweep:
# MAGIC `SELECT * FROM system.information_schema.column_tags WHERE tag_name = 'pii'`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_fga;
# MAGIC DROP FUNCTION IF EXISTS mask_email;
# MAGIC DROP FUNCTION IF EXISTS gold_rows_only
