# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Unity Catalog: objects and access
# MAGIC
# MAGIC Governance is 15% of this exam — the two most-asked facts are the
# MAGIC **managed vs external DROP semantics** and the **three-grant path** to a
# MAGIC table. Both are here, with live introspection.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The hierarchy: metastore → catalog → schema → securable
# MAGIC Three-level names (`catalog.schema.table`) everywhere; volumes govern
# MAGIC files the same way tables govern rows. Workspaces are access points, not
# MAGIC boundaries — one metastore per region serves many workspaces.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Managed vs external — the DROP question

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gov_demo AS SELECT 1 AS id;
# MAGIC DESCRIBE EXTENDED gov_demo

# COMMAND ----------

# MAGIC %md
# MAGIC | | Managed | External |
# MAGIC |---|---|---|
# MAGIC | Data lives | UC-managed storage | a path you chose (external location) |
# MAGIC | `DROP TABLE` | metadata **and** data go (recoverable ~30 days) | **metadata only** — files stay |
# MAGIC | `UNDROP TABLE` | ✅ within the window | unnecessary (data never left) |
# MAGIC | Default? | **yes — use unless you have a reason** | pre-existing lakes, outside readers |
# MAGIC
# MAGIC > **Exam trap:** "we dropped the external table, how do we get the data
# MAGIC > back?" — you never lost it; re-create the table at the same location.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access: the three-grant path
# MAGIC Reaching a table needs the **whole path**:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG dev_catalog          TO `analysts`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  dev_catalog.de_assoc TO `analysts`;
# MAGIC GRANT SELECT      ON TABLE   dev_catalog.de_assoc.gov_demo TO `analysts`;
# MAGIC
# MAGIC REVOKE SELECT ON TABLE dev_catalog.de_assoc.gov_demo FROM `analysts`;
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "User has SELECT but can't query" → missing `USE CATALOG` / `USE
# MAGIC >   SCHEMA`. The single most reliable governance question.
# MAGIC > - Grants **inherit downward**: SELECT on a schema covers every table in
# MAGIC >   it, now and future.
# MAGIC > - Grant to **groups**, not individuals; production jobs authenticate as
# MAGIC >   **service principals** with their own grants.
# MAGIC > - `MODIFY` = INSERT/UPDATE/DELETE; `ALL PRIVILEGES` = the bundle.
# MAGIC > - UC privileges are **additive** (grant/revoke). Explicit `DENY` is the
# MAGIC >   legacy workspace-ACL model — in UC you deny by *not granting* (or with
# MAGIC >   ABAC policies, notebook 02).

# COMMAND ----------

print(f"current_user: {spark.sql('SELECT current_user()').collect()[0][0]}")  # noqa: F821
for stmt in [f"SHOW GRANTS ON SCHEMA {CATALOG}.{SCHEMA}"]:
    try:
        rows = spark.sql(stmt).collect()  # noqa: F821
        print(f"{stmt}:")
        for r in rows[:10]:
            print(f"   {list(r)}")
        if not rows:
            print("   (no explicit grants — owner/admin only)")
    except Exception as e:
        print(f"{stmt}: not permitted ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gov_demo
