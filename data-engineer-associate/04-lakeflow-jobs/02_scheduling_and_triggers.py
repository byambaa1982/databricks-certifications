# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Scheduling and triggers: time-based vs data-driven
# MAGIC
# MAGIC 16% of the exam is jobs, and half of that is picking the right **trigger**.
# MAGIC The decision has exactly two questions: *does work arrive on a clock or as
# MAGIC data?* and *how fresh is fresh enough?*

# COMMAND ----------

# MAGIC %md
# MAGIC ## The trigger types
# MAGIC
# MAGIC | Trigger | Fires when | Reach for it when |
# MAGIC |---|---|---|
# MAGIC | **Manual** | you click / API call | ad-hoc, backfills |
# MAGIC | **Scheduled (cron)** | a time expression (with timezone) | steady batch cadence — "every night at 2" |
# MAGIC | **File arrival** | new files land in a UC volume / external location | "process uploads as they come" without a 24/7 cluster |
# MAGIC | **Continuous** | always running; restarts on completion/failure | lowest latency without managing your own streaming loop |
# MAGIC | **Table update** | source Delta table(s) get new commits | downstream job chained to upstream *data*, not upstream *clock* |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - File arrival checks roughly **every minute** and needs no running
# MAGIC >   compute between arrivals — that's its whole pitch vs continuous.
# MAGIC > - "Job B must run after job A's table has new data" → **table update
# MAGIC >   trigger** (or a Run Job task in A) — not two cron schedules you *hope*
# MAGIC >   line up. Clock-chaining is the canonical wrong answer.
# MAGIC > - Continuous + queueing: a failing continuous job backs off, it doesn't
# MAGIC >   pile up runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-based vs data-driven — the decision in one table
# MAGIC
# MAGIC | Question says | Answer |
# MAGIC |---|---|
# MAGIC | "every morning before 8am" | cron schedule |
# MAGIC | "as soon as the vendor drops files" | file arrival |
# MAGIC | "within a minute or two of new events, all day" | continuous (or a streaming pipeline) |
# MAGIC | "whenever the upstream silver table refreshes" | table update trigger |
# MAGIC | "rerun just the failed part of last night's run" | repair run (notebook 01) |
# MAGIC
# MAGIC ## Schedules meet reality
# MAGIC - **Pause/resume** a schedule without deleting it (bundles set
# MAGIC   `schedule.pause_status` per target — dev deploys arrive paused).
# MAGIC - Cron expressions carry a **timezone**; DST surprises are a support-ticket
# MAGIC   classic the exam nods at.
# MAGIC - Notifications (start/success/failure/duration) per job or per task —
# MAGIC   wire them to email/Slack/webhooks via notification destinations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading a job definition — bundle YAML form
# MAGIC The same job the UI shows is this in a bundle (section 05 deploys it):
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     nightly_orders:
# MAGIC       trigger:
# MAGIC         file_arrival:
# MAGIC           url: /Volumes/dev_catalog/de_assoc/landing/orders/
# MAGIC       email_notifications:
# MAGIC         on_failure: [data-eng@example.com]
# MAGIC       tasks:
# MAGIC         - task_key: ingest
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../02-data-ingestion-and-loading/02_auto_loader.py
# MAGIC         - task_key: transform
# MAGIC           depends_on: [{task_key: ingest}]
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../03-data-transformation-and-modeling/01_cleaning_and_dataframe_ops.py
# MAGIC ```
# MAGIC
# MAGIC Recognizing `trigger.file_arrival`, `depends_on`, and `email_notifications`
# MAGIC in YAML is exam material — you don't write it from memory, you **read** it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Introspect your own runs (works if system tables are granted)

# COMMAND ----------

try:
    display(  # noqa: F821
        spark.sql("""
            SELECT result_state, count(*) AS runs
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time > current_timestamp() - INTERVAL 30 DAYS
            GROUP BY result_state ORDER BY runs DESC
        """)
    )
except Exception as e:
    print(f"system.lakeflow not accessible here ({type(e).__name__}) — "
          "the Jobs UI's run history shows the same data per job")
