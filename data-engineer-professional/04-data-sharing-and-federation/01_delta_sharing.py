# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Delta Sharing: giving your data out
# MAGIC
# MAGIC Delta Sharing is an **open protocol** for sharing live data without copying
# MAGIC it. The exam tests the object model (provider → share → recipient), the
# MAGIC difference between the two sharing flavors, and what can/can't be shared.
# MAGIC Creating shares needs metastore-admin-ish privileges, so the DDL here is
# MAGIC reference; the introspection cells run wherever you have permission.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The object model — memorize this chain
# MAGIC
# MAGIC ```
# MAGIC provider (you)                                recipient (them)
# MAGIC ┌─────────────────────────────┐               ┌──────────────────────────┐
# MAGIC │ Unity Catalog metastore     │               │ Databricks: a catalog    │
# MAGIC │   SHARE  ─ tables           │ ── protocol ─▶│   created FROM the share │
# MAGIC │          ─ schemas          │               │ Non-Databricks: pandas / │
# MAGIC │          ─ volumes          │               │   Spark connector +      │
# MAGIC │          ─ models, notebooks│               │   credential file        │
# MAGIC │   RECIPIENT ─ granted SELECT│               └──────────────────────────┘
# MAGIC └─────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC - A **share** is a named collection of objects from your metastore.
# MAGIC - A **recipient** is who you grant it to.
# MAGIC - Data is read **live from your cloud storage** — no copies, no sync jobs;
# MAGIC   the provider pays storage, the recipient's reads hit signed short-lived
# MAGIC   URLs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The DDL (reference — needs `CREATE SHARE` / `CREATE RECIPIENT` privileges)
# MAGIC
# MAGIC ```sql
# MAGIC CREATE SHARE orders_share COMMENT 'Curated orders for partner X';
# MAGIC
# MAGIC ALTER SHARE orders_share ADD TABLE dev_catalog.de_pro.silver_orders;
# MAGIC
# MAGIC -- share a whole schema, and share CDF so recipients can read increments
# MAGIC ALTER SHARE orders_share ADD SCHEMA dev_catalog.de_pro;
# MAGIC ALTER SHARE orders_share ADD TABLE dev_catalog.de_pro.silver_orders
# MAGIC   WITH CHANGE DATA FEED;
# MAGIC
# MAGIC -- Databricks-to-Databricks: identified by their metastore's sharing id
# MAGIC CREATE RECIPIENT partner_x
# MAGIC   USING ID 'aws:us-west-2:19a84bee-...';
# MAGIC
# MAGIC -- open sharing (recipient has no Databricks): token-based
# MAGIC CREATE RECIPIENT partner_open;   -- returns an activation link for the credential file
# MAGIC
# MAGIC GRANT SELECT ON SHARE orders_share TO RECIPIENT partner_x;
# MAGIC ```
# MAGIC
# MAGIC On the receiving side (Databricks-to-Databricks):
# MAGIC
# MAGIC ```sql
# MAGIC CREATE CATALOG partner_data USING SHARE `provider_name`.orders_share;
# MAGIC SELECT * FROM partner_data.de_pro.silver_orders;   -- reads provider's storage, live
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introspection — safe to run, degrades gracefully without privileges

# COMMAND ----------

for stmt in ["SHOW SHARES", "SHOW RECIPIENTS", "SHOW PROVIDERS"]:
    try:
        rows = spark.sql(stmt).collect()  # noqa: F821
        names = [r[0] for r in rows] or ["(none)"]
        print(f"{stmt:16s} -> {', '.join(str(n) for n in names)}")
    except Exception as e:
        print(f"{stmt:16s} -> not permitted here ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks-to-Databricks vs open sharing — the exam's favorite contrast
# MAGIC
# MAGIC | | D2D (both on Unity Catalog) | Open sharing |
# MAGIC |---|---|---|
# MAGIC | Recipient identified by | metastore **sharing identifier** | activation link → downloaded **credential file** |
# MAGIC | Auth | UC-managed, no tokens to rotate | bearer token in the credential file (expirable) |
# MAGIC | Recipient reads with | a catalog mounted from the share | pandas / Spark `delta-sharing` connector, Power BI, etc. |
# MAGIC | Can share | tables, views, volumes, models, notebooks | tables (and CDF) |
# MAGIC | Streaming reads | ✅ supported | limited — batch-oriented connectors |
# MAGIC | Governance on recipient side | recipient's UC applies | whatever their tool does |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Delta Sharing shares **live data without copying** — any answer involving
# MAGIC >   replication jobs or exports is wrong.
# MAGIC > - Views can be shared; the provider's row filters / column masks apply.
# MAGIC > - Recipients **cannot write** — shares are read-only by definition.
# MAGIC > - Cross-cloud and cross-platform is the point: the recipient does not need
# MAGIC >   Databricks at all (that's what the open protocol is for).

# COMMAND ----------

# MAGIC %md
# MAGIC ## When the question says… → answer
# MAGIC
# MAGIC | Question says | Answer |
# MAGIC |---|---|
# MAGIC | "share with a partner who doesn't use Databricks" | open sharing + credential file |
# MAGIC | "share with another workspace / metastore" | D2D via sharing identifier |
# MAGIC | "partner should only see current rows / masked columns" | share a **view** |
# MAGIC | "partner wants incremental changes, not full re-reads" | share **with CDF** |
# MAGIC | "avoid egress/duplication, keep one copy" | that's Delta Sharing's core property |
