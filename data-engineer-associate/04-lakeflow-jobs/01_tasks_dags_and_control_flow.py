# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Tasks, DAGs, and control flow
# MAGIC
# MAGIC A Lakeflow Job is a **DAG of tasks**; the exam tests whether you can wire
# MAGIC one correctly: dependencies, conditions, loops, retries, and passing values
# MAGIC between tasks. Jobs are defined in the UI/bundles, so this is
# MAGIC reference-first — with the one runnable piece (task values) live.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task types — recognize them by name
# MAGIC
# MAGIC | Task type | Runs |
# MAGIC |---|---|
# MAGIC | Notebook | a workspace/Git notebook (most common) |
# MAGIC | Python script / wheel | a file or packaged entry point |
# MAGIC | SQL | a query, dashboard refresh, or file on a SQL warehouse |
# MAGIC | Pipeline | a Lakeflow Declarative (DLT) pipeline update |
# MAGIC | dbt | dbt models |
# MAGIC | Run Job | **another job** (composition) |
# MAGIC | **If/else condition** | branches the DAG on an expression |
# MAGIC | **For each** | loops a task over a list, with per-iteration parameters |
# MAGIC
# MAGIC ## Dependencies and run conditions
# MAGIC Every task lists `depends_on` — that's the DAG. The **Run if** setting
# MAGIC decides what dependency outcome lets a task start:
# MAGIC
# MAGIC | Run if | Task starts when… |
# MAGIC |---|---|
# MAGIC | All succeeded (default) | every dependency succeeded |
# MAGIC | At least one succeeded / failed | mixed outcomes allowed |
# MAGIC | **All done** | dependencies finished, success or not — the "always run cleanup/notify" setting |
# MAGIC | None failed | success or skipped |
# MAGIC
# MAGIC > **Exam trap:** "send an alert task even when upstream fails" → `All done`
# MAGIC > (not a retry, not a separate job).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retries, timeouts, and repair
# MAGIC - **Retries are per task** (count + interval) — transient failures heal
# MAGIC   without re-running the whole job.
# MAGIC - **Timeouts** per task and per job; **duration warnings** notify without
# MAGIC   killing.
# MAGIC - **Repair run** (after a failure): re-runs only failed tasks **and their
# MAGIC   dependents**, with the same parameters — successful tasks keep their
# MAGIC   results. The cheapest-fix answer, every time it appears.
# MAGIC - `max_concurrent_runs` (default 1) — whether a schedule can overlap a
# MAGIC   still-running instance; with queueing, extra triggers **queue** instead
# MAGIC   of being skipped.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Passing values between tasks — runnable
# MAGIC `dbutils.jobs.taskValues` is the official channel: upstream `set`s,
# MAGIC downstream `get`s by task name. (Outside a job run, `get` returns the
# MAGIC `default` — which is why this cell works interactively.)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="row_count", value=1234)  # noqa: F821
got = dbutils.jobs.taskValues.get(  # noqa: F821
    taskKey="ingest_task", key="row_count", default=-1, debugValue=1234
)
print(f"downstream task would read: {got}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - Task values are for **small** control-flow values (counts, flags, paths)
# MAGIC >   — data goes through **tables**, never task values.
# MAGIC > - The if/else condition task reads them:
# MAGIC >   `{{tasks.ingest_task.values.row_count}} > 0` → branch.
# MAGIC > - Job **parameters** (`{{job.parameters.env}}`) flow *down* into every
# MAGIC >   task; task values flow *between* tasks.
# MAGIC
# MAGIC ## For-each — the loop the exam names
# MAGIC A `for_each_task` takes an `inputs` list (static JSON or a task value) and
# MAGIC runs its inner task once per element, `{{input}}` as the parameter, with a
# MAGIC `concurrency` cap. "Run the same notebook for 30 tables" → for-each, not 30
# MAGIC copy-pasted tasks and not a driver-side Python loop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute for jobs
# MAGIC - **Job compute**: created for the run, dies after — the default, cheapest.
# MAGIC - **One cluster shared across tasks** of a run (per-task or shared — shared
# MAGIC   saves startup per task).
# MAGIC - **Serverless jobs**: no cluster config at all; fastest start.
# MAGIC - All-purpose clusters for scheduled jobs = the recurring wrong answer
# MAGIC   (2× DBU rate, idle cost).
# MAGIC
# MAGIC Deep dive on the monitoring half (run history, alerts, repair):
# MAGIC section [`06`](../06-troubleshooting-monitoring-and-optimisation/) and the
# MAGIC Professional folder's
# MAGIC [`05-monitoring-and-alerting`](../../data-engineer-professional/05-monitoring-and-alerting/).
