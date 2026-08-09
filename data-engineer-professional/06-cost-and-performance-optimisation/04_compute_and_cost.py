# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Compute choices: where the money actually goes
# MAGIC
# MAGIC File layout and plans tune the numerator; this notebook is the
# MAGIC denominator — **what compute you run and what it costs**. Almost every
# MAGIC cost question on the exam is one of five levers: compute type, Photon,
# MAGIC autoscaling, spot, and turning things off.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lever 1: the right compute type for the workload
# MAGIC
# MAGIC | Workload | Right compute | The trap answer |
# MAGIC |---|---|---|
# MAGIC | Scheduled ETL | **job compute** (or serverless jobs) | all-purpose cluster — ~2× the DBU rate for the same nodes |
# MAGIC | Ad-hoc notebooks | all-purpose, **auto-terminate set** | leaving it running 24/7 |
# MAGIC | BI / SQL | SQL warehouse (serverless: fast start/stop) | classic warehouse idling |
# MAGIC | DLT / Lakeflow | pipeline compute (its own SKU) | — |
# MAGIC
# MAGIC > **Exam trap #1 of the whole section:** running scheduled production jobs
# MAGIC > on an **all-purpose** cluster. Job compute is cheaper per DBU and dies
# MAGIC > with the job. Sharing one all-purpose cluster across many scheduled jobs
# MAGIC > compounds it (contention + top rate).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Levers 2–5, rapid fire
# MAGIC - **Photon** — vectorized C++ engine; big speedups for SQL/DataFrame scans,
# MAGIC   joins, aggregations, and Delta writes. Costs more per DBU but usually
# MAGIC   **less per job** because it finishes sooner. Not for RDDs/UDF-heavy code.
# MAGIC - **Autoscaling** — set min/max workers; for streaming use **enhanced
# MAGIC   autoscaling** (DLT) which can scale *down* safely by managing state.
# MAGIC - **Spot / preemptible workers** — big discount, can vanish; fine for
# MAGIC   workers on retryable batch, never for the driver, avoid for tight-SLA.
# MAGIC - **Auto-termination** — all-purpose clusters idle at full price;
# MAGIC   auto-terminate (and pools to soften the cold-start penalty it causes).
# MAGIC - **Serverless everywhere it fits** — pay only while running, no idle,
# MAGIC   instant start; the standing answer for spiky/interactive load.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watching the spend — the same system tables as section 05

# COMMAND ----------

try:
    display(  # noqa: F821
        spark.sql("""
            SELECT u.sku_name,
                   round(sum(u.usage_quantity), 1)                    AS dbus,
                   round(sum(u.usage_quantity * p.pricing.default), 2) AS approx_usd
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
              ON u.sku_name = p.sku_name
             AND u.usage_start_time >= p.price_start_time
             AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
            WHERE u.usage_start_time > current_timestamp() - INTERVAL 30 DAYS
            GROUP BY u.sku_name
            ORDER BY approx_usd DESC
        """)
    )
except Exception as e:
    print(f"system.billing not accessible here ({type(e).__name__}) — "
          "grantable via GRANT SELECT ON system tables by an account admin")

# COMMAND ----------

# MAGIC %md
# MAGIC Tag clusters and jobs (`custom_tags`) and the tags flow into
# MAGIC `system.billing.usage.custom_tags` — that's how "which team spent what"
# MAGIC questions are answered. Budgets + budget alerts sit on top of the same data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The decision drill — cost question → first lever to pull
# MAGIC
# MAGIC | Question says | Pull |
# MAGIC |---|---|
# MAGIC | "nightly job on an interactive cluster" | job compute |
# MAGIC | "cluster busy 2h/day, billed 24" | auto-terminate / serverless |
# MAGIC | "SQL dashboards slow to start, idle cost high" | serverless SQL warehouse |
# MAGIC | "batch job cost, some interruption OK" | spot workers (driver on-demand) |
# MAGIC | "CPU-bound SQL/Delta job too slow" | Photon |
# MAGIC | "which team is spending" | tags + `system.billing.usage` |
# MAGIC | "storage bill growing forever" | VACUUM cadence + retention (notebook 02) |
# MAGIC | "same query re-scans same data" | disk cache / warehouse result cache (notebook 03) |
# MAGIC
# MAGIC One framing to keep: **cluster levers change $/hour; layout and plan levers
# MAGIC change hours.** The cheapest DBU is the one you don't burn.
