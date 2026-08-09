# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Row filters, column masks, and dynamic views
# MAGIC
# MAGIC Fine-grained access control, **run live**: a SQL UDF decides per row / per
# MAGIC value what each querying user sees. Attached to the table, the policy
# MAGIC follows the data into every query — no copies, no "secure view" sprawl.
# MAGIC Works on a scratch clone; requires section 01's `00_setup`.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers_secured AS
# MAGIC SELECT customer_id, name, email, tier
# MAGIC FROM silver_customers WHERE __END_AT IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column mask: non-admins see redacted emails
# MAGIC A mask is a UDF taking the column's type and returning the same type. Bind
# MAGIC it with `SET MASK`. (`is_account_group_member('admins')` is false for most
# MAGIC of us here, so the mask *visibly* fires.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION email_mask(email STRING)
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('admins') THEN email
# MAGIC   ELSE concat('***@', split_part(email, '@', 2))
# MAGIC END;
# MAGIC
# MAGIC ALTER TABLE customers_secured
# MAGIC   ALTER COLUMN email SET MASK email_mask

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_secured LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row filter: only gold-tier rows for non-admins
# MAGIC A filter is a **boolean** UDF over the columns you bind with `ON (…)` —
# MAGIC rows where it returns false simply don't exist for the query.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION gold_only(tier STRING)
# MAGIC RETURN is_account_group_member('admins') OR tier = 'gold';
# MAGIC
# MAGIC ALTER TABLE customers_secured SET ROW FILTER gold_only ON (tier);
# MAGIC
# MAGIC SELECT tier, count(*) AS visible_rows FROM customers_secured GROUP BY tier

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - Filters and masks apply to **every** reader — including the table owner.
# MAGIC >   Exemptions are expressed *inside* the UDF (like the
# MAGIC >   `is_account_group_member` branch), not by privilege.
# MAGIC > - The policy travels with the **table**: ad-hoc SQL, jobs, dashboards all
# MAGIC >   see filtered data. A separate "clean view for the analysts" can drift;
# MAGIC >   this can't.
# MAGIC > - Limitations worth knowing: tables with filters/masks can't be shared
# MAGIC >   via **Delta Sharing** (share a view instead), and MERGE **into** them is
# MAGIC >   restricted.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The older tool: dynamic views
# MAGIC Pre-filter/mask questions still appear. Same functions, but baked into a
# MAGIC view over the (unprotected) table — and users get access to the **view
# MAGIC only**, never the base table:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW customers_secure_view AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   CASE WHEN is_account_group_member('admins')
# MAGIC        THEN email ELSE '***REDACTED***' END AS email,
# MAGIC   tier
# MAGIC FROM customers_secured
# MAGIC WHERE is_account_group_member('admins') OR tier = 'gold';
# MAGIC
# MAGIC SELECT * FROM customers_secure_view LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC | | Dynamic view | Row filter / column mask |
# MAGIC |---|---|---|
# MAGIC | Protects | the view (base table must be locked down separately) | the table itself, everywhere |
# MAGIC | Extra objects | one view per policy combo | none |
# MAGIC | Exam-era status | legacy but still asked | the current answer |
# MAGIC
# MAGIC The functions to recognize on sight: `current_user()`,
# MAGIC `is_account_group_member('…')`, `is_member('…')` (workspace-local, legacy).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS customers_secure_view;
# MAGIC DROP TABLE IF EXISTS customers_secured;
# MAGIC DROP FUNCTION IF EXISTS email_mask;
# MAGIC DROP FUNCTION IF EXISTS gold_only
