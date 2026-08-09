# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Platform architecture: what runs where, who pays for what
# MAGIC
# MAGIC The smallest exam section, and pure orientation: the two planes, what Delta
# MAGIC Lake actually is, and which compute you reach for. Nearly every question
# MAGIC here is answered by one of the three tables below. The introspection cells
# MAGIC run on any UC-enabled compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The two planes
# MAGIC
# MAGIC ```
# MAGIC CONTROL PLANE (Databricks' account)          COMPUTE ("data") PLANE (your account*)
# MAGIC ┌───────────────────────────────┐            ┌──────────────────────────────────┐
# MAGIC │ web app / notebooks UI        │            │ clusters & SQL warehouses        │
# MAGIC │ job scheduler, Unity Catalog  │──manages──▶│ (Spark runs HERE)                │
# MAGIC │ metadata, access control      │            │ your cloud storage (S3/ADLS/GCS) │
# MAGIC │ (notebook code, configs)      │            │ (your DATA stays HERE)           │
# MAGIC └───────────────────────────────┘            └──────────────────────────────────┘
# MAGIC                                    * serverless compute runs in DATABRICKS' account,
# MAGIC                                      still reading data from YOUR storage
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Your **data never lives in the control plane** — it stays in your cloud
# MAGIC >   storage; the control plane holds metadata, code, and configs.
# MAGIC > - **Serverless** moves compute into Databricks' account (fast start, no
# MAGIC >   cluster management) — the *data* still stays in your storage.
# MAGIC > - Query results / notebook outputs can pass through the control plane —
# MAGIC >   that's the nuance behind "where does my data go" questions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake in one table
# MAGIC Delta = **Parquet files + a transaction log** (`_delta_log/` of ordered JSON
# MAGIC commits + checkpoints). Everything the exam asks follows from the log:
# MAGIC
# MAGIC | Property | Because the log… |
# MAGIC |---|---|
# MAGIC | ACID transactions | commits are atomic entries — readers see a version or they don't |
# MAGIC | Time travel / RESTORE | old versions = old file lists, still on storage |
# MAGIC | Schema enforcement | the schema lives in the log; bad writes are rejected |
# MAGIC | Streaming + batch on one table | both read the same log |
# MAGIC | UPDATE / DELETE / MERGE | new files + log entry, never in-place edits |
# MAGIC
# MAGIC Delta is the **default table format** on Databricks — `CREATE TABLE` gives
# MAGIC you Delta unless you say otherwise.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821

print(f"runtime: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion', 'n/a')}")  # noqa: F821
for probe in ["SELECT current_metastore()", "SELECT current_user()", "SELECT current_version().dbsql_version"]:
    try:
        print(f"{probe:42s} -> {spark.sql(probe).collect()[0][0]}")  # noqa: F821
    except Exception as e:
        print(f"{probe:42s} -> n/a ({type(e).__name__})")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the three-level namespace at a glance (Unity Catalog fundamentals —
# MAGIC -- deep dive in section 07)
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute selection — the money table
# MAGIC
# MAGIC | Workload | Compute | Why |
# MAGIC |---|---|---|
# MAGIC | scheduled pipelines | **job compute** (or serverless jobs) | cheaper DBU rate, exists only while the job runs |
# MAGIC | interactive notebooks | all-purpose compute + auto-terminate | shareable, restartable — but ~2× job DBU rate |
# MAGIC | SQL / BI dashboards | SQL warehouse (**serverless** preferred) | instant start, auto-stop, Photon always on |
# MAGIC | Lakeflow / DLT pipelines | pipeline compute | managed by the pipeline itself |
# MAGIC
# MAGIC DBU = the billing unit (rate varies by compute type + tier). The cost
# MAGIC questions at Associate level are the first row's trap in disguise: **don't
# MAGIC run scheduled production work on all-purpose clusters.**
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Photon: vectorized engine, on by default in SQL warehouses, opt-in on
# MAGIC >   clusters — speeds up SQL/DataFrame work, not Python UDF loops.
# MAGIC > - Auto-termination exists for **clusters**; auto-**stop** for warehouses.
# MAGIC >   Serverless makes both someone else's problem.
# MAGIC > - Runtime picks (LTS vs latest) show up: production jobs pin an **LTS**
# MAGIC >   runtime.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The platform vocabulary the exam assumes
# MAGIC
# MAGIC | Term | Is |
# MAGIC |---|---|
# MAGIC | Workspace | the environment (UI + compute + files) your team works in |
# MAGIC | Unity Catalog | the governance layer: catalogs → schemas → tables/volumes (section 07) |
# MAGIC | Lakeflow | the data engineering umbrella: Connect (ingestion), Declarative Pipelines (DLT), Jobs (orchestration — section 04) |
# MAGIC | DBFS / volumes | legacy path-based storage vs UC-governed file storage — new work uses **volumes** |
# MAGIC | Repos / Git folders | git-backed folders in the workspace (section 05) |
# MAGIC | Medallion | bronze → silver → gold layering (section 03 puts it to work) |
# MAGIC
# MAGIC Deep-dive pointer: the Professional folder's
# MAGIC [`08-data-governance`](../../data-engineer-professional/08-data-governance/)
# MAGIC covers the object model this section only sketches.
