# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Lakehouse Federation: querying data you *don't* move
# MAGIC
# MAGIC Federation is the mirror image of sharing: instead of giving your data out,
# MAGIC you reach into **external systems** (Postgres, Snowflake, BigQuery, …) and
# MAGIC query them through Unity Catalog as if they were local catalogs — no
# MAGIC ingestion pipeline, no copy. Needs a reachable external database to fully
# MAGIC run, so DDL is reference; introspection cells run anywhere.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Two objects, one chain
# MAGIC
# MAGIC ```
# MAGIC CONNECTION  (how to reach it: type, host, credentials)
# MAGIC     └── FOREIGN CATALOG  (what it looks like in UC: catalog → schemas → tables)
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC CREATE CONNECTION pg_prod TYPE postgresql
# MAGIC OPTIONS (
# MAGIC   host 'pg.internal.example.com',
# MAGIC   port '5432',
# MAGIC   user secret('prod-scope', 'pg-user'),        -- secrets, never literals
# MAGIC   password secret('prod-scope', 'pg-password')
# MAGIC );
# MAGIC
# MAGIC CREATE FOREIGN CATALOG pg_sales USING CONNECTION pg_prod
# MAGIC OPTIONS (database 'sales');
# MAGIC
# MAGIC -- from here it's just three-level namespace like any other catalog:
# MAGIC SELECT region, sum(total) FROM pg_sales.public.invoices GROUP BY region;
# MAGIC ```
# MAGIC
# MAGIC Supported sources (know the family, not the fine print): PostgreSQL, MySQL,
# MAGIC SQL Server, Redshift, Snowflake, BigQuery, Synapse, Databricks itself, and
# MAGIC catalogs via Hive Metastore / Glue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introspection — run anywhere

# COMMAND ----------

for stmt in ["SHOW CONNECTIONS", "SHOW CATALOGS"]:
    try:
        rows = spark.sql(stmt).collect()  # noqa: F821
        names = [r[0] for r in rows][:20] or ["(none)"]
        print(f"{stmt:17s} -> {', '.join(str(n) for n in names)}")
    except Exception as e:
        print(f"{stmt:17s} -> not permitted here ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What actually happens at query time
# MAGIC - UC translates and **pushes down** filters, projections, joins and (source
# MAGIC   permitting) aggregates to the remote engine — you pay the remote system's
# MAGIC   compute for what it can do, and Spark finishes what it can't.
# MAGIC - Foreign catalogs are **read-only** (writes fail; a couple of sources allow
# MAGIC   them, but "federation = reading" is the exam-level answer).
# MAGIC - UC governance applies on top: GRANTs on the foreign catalog, lineage,
# MAGIC   audit — one permission model over data that never moved.
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Federation is for **query-in-place, exploration, and small/medium
# MAGIC >   lookups** — not for repeatedly hammering a production OLTP database with
# MAGIC >   heavy analytics. The remedy the exam wants: **materialize** hot foreign
# MAGIC >   data locally (CTAS or a materialized view on a schedule) and query the
# MAGIC >   copy.
# MAGIC > - Credentials live in the **connection**, and access to the connection is
# MAGIC >   itself governable — analysts never see the password.
# MAGIC > - A foreign catalog is a **live window**, not a snapshot: the remote
# MAGIC >   system's current state is what you query.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Moving-data decision table — sharing vs federation vs ingestion
# MAGIC
# MAGIC | You need to… | Reach for |
# MAGIC |---|---|
# MAGIC | let *others* read *your* Delta tables, live, no copy | **Delta Sharing** (notebook 01) |
# MAGIC | query an *external* database from Databricks, no copy | **Lakehouse Federation** |
# MAGIC | run heavy, repeated analytics on external data | **ingest it** (Auto Loader / LakeFlow Connect / CTAS from federation) |
# MAGIC | one-off read of a JDBC source in code | `spark.read.jdbc` (no UC governance — last resort) |
# MAGIC
# MAGIC The single sentence to remember: **sharing exports access, federation
# MAGIC imports access, ingestion imports data.**
