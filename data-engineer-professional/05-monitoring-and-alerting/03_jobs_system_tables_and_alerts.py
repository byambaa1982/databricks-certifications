# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Job monitoring, system tables, and alerts
# MAGIC
# MAGIC The operational layer: what tells you a **job** is sick, what the
# MAGIC **system tables** record account-wide, and which **alerting** mechanism
# MAGIC fits which symptom. System-table cells run if your workspace grants you
# MAGIC access and degrade gracefully if not.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job-level monitoring — the checklist
# MAGIC - **Notifications** on start / success / failure / **duration warning**
# MAGIC   (`health` rules, e.g. `RUN_DURATION_SECONDS > threshold`) → email, Slack,
# MAGIC   webhooks, PagerDuty via notification destinations.
# MAGIC - **Retries** per task; **`max_concurrent_runs`** to stop schedule pile-ups.
# MAGIC - **Repair run** — re-run only failed tasks *and their dependents*, keeping
# MAGIC   successful results. The exam's answer to "one task of a 10-task job
# MAGIC   failed; cheapest fix?"
# MAGIC - **Streaming backlog alerts** — jobs can alert on Auto Loader/Kafka backlog
# MAGIC   (the metrics from notebook 01).
# MAGIC
# MAGIC > **Exam trap:** a repair run re-executes the failed task with the **same
# MAGIC > run parameters** — it is not a new scheduled run, and successful tasks are
# MAGIC > not re-run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## System tables — account-wide, read-only, in `system.`
# MAGIC
# MAGIC | Table | What it answers |
# MAGIC |---|---|
# MAGIC | `system.billing.usage` | DBUs by workspace/SKU/tag — **cost monitoring** |
# MAGIC | `system.billing.list_prices` | $ per DBU by SKU — join to usage for spend |
# MAGIC | `system.access.audit` | who did what (auditing, notebook 07's territory) |
# MAGIC | `system.lakeflow.jobs` / `job_run_timeline` / `job_task_run_timeline` | job & task run history across all workspaces |
# MAGIC | `system.compute.clusters` / `node_timeline` | cluster configs and utilization |
# MAGIC
# MAGIC They're Delta tables — alert on them with scheduled queries like any other.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821

probes = {
    "job runs, last 7 days": """
        SELECT result_state, count(*) AS runs
        FROM system.lakeflow.job_run_timeline
        WHERE period_start_time > current_timestamp() - INTERVAL 7 DAYS
        GROUP BY result_state ORDER BY runs DESC
    """,
    "DBU usage by SKU, last 7 days": """
        SELECT sku_name, round(sum(usage_quantity), 2) AS dbus
        FROM system.billing.usage
        WHERE usage_start_time > current_timestamp() - INTERVAL 7 DAYS
        GROUP BY sku_name ORDER BY dbus DESC
    """,
}
for label, sql in probes.items():
    try:
        rows = spark.sql(sql).collect()  # noqa: F821
        print(f"-- {label}")
        for r in rows[:10]:
            print(f"   {list(r)}")
        if not rows:
            print("   (no rows)")
    except Exception as e:
        print(f"-- {label}: not accessible here ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL alerts — turn any query into a pager
# MAGIC An alert = **query + condition + schedule + notification destination**.
# MAGIC The query runs on a SQL warehouse; when the condition on its first-row
# MAGIC value flips to true, notifications fire (once, or at a re-notify interval).
# MAGIC
# MAGIC The freshness alert every team ends up writing:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT timestampdiff(HOUR, max(order_ts), current_timestamp()) AS hours_stale
# MAGIC FROM dev_catalog.de_pro.silver_orders
# MAGIC -- alert condition: hours_stale > 24
# MAGIC ```
# MAGIC
# MAGIC > **Exam trap:** SQL alerts evaluate on a **schedule**, not continuously —
# MAGIC > worst-case detection latency is the schedule interval. For instant
# MAGIC > reaction to a failing *job*, use job notifications; for row-level
# MAGIC > enforcement, constraints/expectations (section 03). Alerts are for **data
# MAGIC > conditions** (freshness, volume, quality counts) checked periodically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakehouse Monitoring — statistics over time, managed for you
# MAGIC Attach a monitor to a table and Databricks maintains two metric tables
# MAGIC (`_profile_metrics`, `_drift_metrics`) plus an auto-generated dashboard.
# MAGIC
# MAGIC | Profile type | For | Adds |
# MAGIC |---|---|---|
# MAGIC | **Snapshot** | any Delta table | full-table stats per refresh |
# MAGIC | **Time series** | tables with a timestamp column | stats **per time window** |
# MAGIC | **Inference** | ML request/prediction logs | model quality + **drift** vs baseline |
# MAGIC
# MAGIC Drift metrics compare consecutive windows (or vs a baseline table) — the
# MAGIC exam phrase to match is *"detect statistical changes in data over time
# MAGIC without writing custom checks"*.
# MAGIC
# MAGIC ## Which tool for which symptom (the section in one table)
# MAGIC
# MAGIC | Symptom | Tool |
# MAGIC |---|---|
# MAGIC | job failed / ran too long | job notifications + repair run |
# MAGIC | stream falling behind | progress metrics / backlog alert (notebook 01) |
# MAGIC | how many rows failed quality checks | DLT event log (notebook 02) |
# MAGIC | table hasn't updated in N hours | SQL alert on freshness |
# MAGIC | distribution shifted week-over-week | Lakehouse Monitoring drift metrics |
# MAGIC | spend is creeping up | `system.billing.usage` + a scheduled query/alert |
