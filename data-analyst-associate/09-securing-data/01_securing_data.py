# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Securing data, the analyst's share of it
# MAGIC
# MAGIC You don't administer the metastore, but you own tables and share
# MAGIC dashboards — so the exam tests the three-grant path, ownership, and what
# MAGIC protects PII when your work reaches other people. The mask/filter demo
# MAGIC runs live.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "da_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The three-grant path (yes, again — it's on every Databricks exam)
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG dev_catalog          TO `marketing-analysts`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  dev_catalog.da_assoc TO `marketing-analysts`;
# MAGIC GRANT SELECT      ON TABLE   dev_catalog.da_assoc.orders TO `marketing-analysts`;
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Has SELECT, still can't query" → missing `USE CATALOG`/`USE SCHEMA`.
# MAGIC > - Grants inherit **downward** — SELECT on the schema covers future
# MAGIC >   tables too.
# MAGIC > - Grant to **groups**; as the table's **owner** you can grant on your own
# MAGIC >   objects without being an admin (ownership follows the creator).

# COMMAND ----------

print(f"current_user: {spark.sql('SELECT current_user()').collect()[0][0]}")  # noqa: F821
try:
    rows = spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.{SCHEMA}").collect()  # noqa: F821
    for r in rows[:10]:
        print(f"   {list(r)}")
    if not rows:
        print("   (no explicit grants — owner/admin only)")
except Exception as e:
    print(f"SHOW GRANTS not permitted here ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PII controls — watch your own email get masked

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers_secured_da AS
# MAGIC SELECT customer_id, name, email, tier FROM customers;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION mask_email_da(email STRING)
# MAGIC RETURN CASE WHEN is_account_group_member('admins') THEN email
# MAGIC             ELSE concat('***@', split_part(email, '@', 2)) END;
# MAGIC
# MAGIC ALTER TABLE customers_secured_da ALTER COLUMN email SET MASK mask_email_da;
# MAGIC
# MAGIC SELECT * FROM customers_secured_da ORDER BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC Row filters are the row-wise sibling (`SET ROW FILTER fn ON (col)`), and
# MAGIC **dynamic views** are the legacy pattern (CASE on
# MAGIC `is_account_group_member` inside a view, users granted the view only).
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Masks/filters bind to the **table** and apply to every reader — owner
# MAGIC >   included; exemptions live inside the function.
# MAGIC > - Tagging a column `pii` (section 02) makes it *findable*, not
# MAGIC >   *protected* — the mask is the protection.
# MAGIC > - The dashboard interaction from section 06: **embedded credentials
# MAGIC >   bypass per-viewer filtering** (everyone sees the publisher's view);
# MAGIC >   per-viewer security needs viewer credentials.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The analyst's security checklist
# MAGIC 1. Query with the **least-privileged** account that works; ask for grants
# MAGIC    at the group level.
# MAGIC 2. Before sharing anything (dashboard, Genie space, table), ask: *whose
# MAGIC    credentials will the readers use, and do filters/masks survive that?*
# MAGIC 3. Keep PII in few, tagged, masked columns; never copy it into scratch
# MAGIC    tables that escape governance.
# MAGIC 4. Own what you create: set comments, tags, and sane grants on your gold
# MAGIC    tables — you are that table's owner.
# MAGIC
# MAGIC Deep dives: engineering
# MAGIC [`07-data-security-and-compliance`](../../data-engineer-professional/07-data-security-and-compliance/)
# MAGIC and associate [`07-governance-and-security`](../../data-engineer-associate/07-governance-and-security/).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customers_secured_da;
# MAGIC DROP FUNCTION IF EXISTS mask_email_da
