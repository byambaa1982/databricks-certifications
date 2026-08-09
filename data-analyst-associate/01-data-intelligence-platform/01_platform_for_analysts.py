# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — The platform, seen from the analyst's chair
# MAGIC
# MAGIC Same platform as the engineering tracks, different entry points: you live
# MAGIC in **Databricks SQL** (SQL editor + warehouses), find data through
# MAGIC **Catalog Explorer** and **Marketplace**, and trust it through Unity
# MAGIC Catalog's metadata — certification badges, tags, and lineage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The analyst's map of the platform
# MAGIC
# MAGIC | You need to… | Go to |
# MAGIC |---|---|
# MAGIC | write and run SQL | **SQL editor** (on a SQL warehouse) |
# MAGIC | find and understand tables | **Catalog Explorer** (schema, sample data, lineage, tags) |
# MAGIC | get external datasets | **Marketplace** (free + paid, delivered via Delta Sharing) |
# MAGIC | build charts and reports | **AI/BI Dashboards** (section 06) |
# MAGIC | let stakeholders self-serve | **Genie spaces** (section 07) |
# MAGIC | ask what a table means | column comments, certification badge, owner — all in Catalog Explorer |
# MAGIC
# MAGIC The compute difference from engineering: analysts use **SQL warehouses**,
# MAGIC not clusters. A warehouse only speaks SQL, starts fast, scales by cluster
# MAGIC count, and stops itself.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL warehouses — the one table to memorize
# MAGIC
# MAGIC | | Serverless | Pro | Classic |
# MAGIC |---|---|---|---|
# MAGIC | Startup | **seconds** | minutes | minutes |
# MAGIC | Runs in | Databricks' account | yours | yours |
# MAGIC | Features | all (Photon, predictive IO, instant scale) | most | basic |
# MAGIC | Exam guidance | **default choice** | when serverless isn't available | legacy |
# MAGIC
# MAGIC - **Size** (2X-Small … 4X-Large) = per-cluster horsepower → speeds up a
# MAGIC   single big query.
# MAGIC - **Scaling** (min/max clusters) = concurrency → absorbs many users at
# MAGIC   once; queries queue when maxed.
# MAGIC - **Auto-stop** ends idle spend — serverless makes stop/start painless.
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Dashboard is slow for one huge query" → bigger **size**. "Dashboard is
# MAGIC >   slow at 9am when everyone opens it" → more **clusters** (scaling). Size
# MAGIC >   vs scale-out is the classic pair.
# MAGIC > - Warehouses run **SQL only** — Python notebooks need a cluster or
# MAGIC >   serverless notebook compute.
# MAGIC > - Photon is **always on** in SQL warehouses — you don't configure it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog, analyst edition
# MAGIC Three-level names — `catalog.schema.table` — and everything else is
# MAGIC metadata *about* those objects: **comments** (what it means),
# MAGIC **tags** (how it's classified), **certified** badge (the official one),
# MAGIC **lineage** (where it came from), **owner** (who to ask).

# COMMAND ----------

for probe in ["SELECT current_metastore()", "SELECT current_catalog()", "SELECT current_user()"]:
    try:
        print(f"{probe:35s} -> {spark.sql(probe).collect()[0][0]}")  # noqa: F821
    except Exception as e:
        print(f"{probe:35s} -> n/a ({type(e).__name__})")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Marketplace and certified data — trust signals
# MAGIC - **Marketplace** listings deliver data via **Delta Sharing**: you get a
# MAGIC   read-only catalog, always current, no copies (section 03 imports one).
# MAGIC - A **certified** (✔) table is one your organization marked as canonical —
# MAGIC   prefer it over look-alikes. Certification is a *signal*, not a control:
# MAGIC   it doesn't change permissions.
# MAGIC - **Lineage** answers both directions: "where did this number come from"
# MAGIC   (upstream) and "what breaks if this table changes" (downstream) —
# MAGIC   captured automatically, table- and column-level.
# MAGIC
# MAGIC Deep dives: engineering tracks'
# MAGIC [`08-data-governance`](../../data-engineer-professional/08-data-governance/)
# MAGIC (lineage, tags) and
# MAGIC [`04-data-sharing-and-federation`](../../data-engineer-professional/04-data-sharing-and-federation/)
# MAGIC (Delta Sharing mechanics).
