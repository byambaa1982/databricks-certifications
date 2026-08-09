# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — The Unity Catalog privilege model
# MAGIC
# MAGIC Access control questions are mechanical once you hold two rules: privileges
# MAGIC sit on a **hierarchy** (metastore → catalog → schema → securable) and
# MAGIC flow **downward** when granted high, and reaching any object needs the
# MAGIC **whole path** enabled (`USE CATALOG` + `USE SCHEMA` + the object
# MAGIC privilege). Most wrong answers violate one of the two.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The two rules, concretely
# MAGIC
# MAGIC ```sql
# MAGIC -- the minimum for an analyst to SELECT one table: THREE grants
# MAGIC GRANT USE CATALOG ON CATALOG dev_catalog        TO `analysts`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA  dev_catalog.de_pro TO `analysts`;
# MAGIC GRANT SELECT      ON TABLE   dev_catalog.de_pro.silver_orders TO `analysts`;
# MAGIC
# MAGIC -- or grant high and let it inherit to everything inside, present and future
# MAGIC GRANT SELECT ON CATALOG dev_catalog TO `analysts`;   -- includes USE-like reach? NO:
# MAGIC -- inheritance carries SELECT to every table, but USE CATALOG/USE SCHEMA are
# MAGIC -- still separate privileges — grant them too.
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - `USE CATALOG` / `USE SCHEMA` **gate the path**; `SELECT` on the table
# MAGIC >   without them = access denied. The "user was granted SELECT but still
# MAGIC >   can't query" question is always this.
# MAGIC > - Grants go to **groups** (best practice) or users/service principals;
# MAGIC >   grants on a securable are inherited by everything below it.
# MAGIC > - **Ownership** is special: owners (and metastore admins) can grant; mere
# MAGIC >   SELECT holders cannot pass it on (no WITH GRANT OPTION in UC — granting
# MAGIC >   rights come from ownership or MANAGE).
# MAGIC > - `ALL PRIVILEGES` expands to the individual privileges (and picks up new
# MAGIC >   ones over time).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introspection — runnable wherever you have access

# COMMAND ----------

print(f"current_user(): {spark.sql('SELECT current_user()').collect()[0][0]}")  # noqa: F821

for stmt in [
    f"SHOW GRANTS ON SCHEMA {CATALOG}.{SCHEMA}",
    f"SHOW GRANTS ON TABLE {CATALOG}.{SCHEMA}.silver_orders",
]:
    try:
        rows = spark.sql(stmt).collect()  # noqa: F821
        print(f"\n-- {stmt}")
        for r in rows[:10]:
            print(f"   {list(r)}")
        if not rows:
            print("   (no explicit grants — owner/admin access only)")
    except Exception as e:
        print(f"\n-- {stmt}: not permitted ({type(e).__name__})")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- information_schema mirrors grants as queryable tables
# MAGIC SELECT grantee, privilege_type, table_name
# MAGIC FROM system.information_schema.table_privileges
# MAGIC WHERE table_schema = 'de_pro'
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Privilege cheat sheet — the ones the exam asks by name
# MAGIC
# MAGIC | Privilege | Lets you |
# MAGIC |---|---|
# MAGIC | `USE CATALOG` / `USE SCHEMA` | traverse the path (no data access by itself) |
# MAGIC | `SELECT` | read tables/views |
# MAGIC | `MODIFY` | INSERT / UPDATE / DELETE / MERGE |
# MAGIC | `CREATE TABLE` (on schema) | create tables there |
# MAGIC | `READ VOLUME` / `WRITE VOLUME` | file access through volumes |
# MAGIC | `EXECUTE` | run functions / models |
# MAGIC | `CREATE EXTERNAL LOCATION` etc. | storage plumbing (metastore level) |
# MAGIC | `MANAGE` | grant on the object without owning it |
# MAGIC
# MAGIC Service principals (jobs' identities) hold privileges exactly like users —
# MAGIC production jobs should run as a service principal, not as a person
# MAGIC (section 09 picks this up in deployment).
