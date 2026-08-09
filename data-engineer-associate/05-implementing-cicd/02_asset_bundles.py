# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Asset Bundles: dev → test → prod
# MAGIC
# MAGIC Bundles ("Declarative Automation Bundles" in the exam guide) put jobs,
# MAGIC pipelines, and their configs **in the repo as YAML**, parameterized per
# MAGIC environment. The exam wants you to read a `databricks.yml`, know what
# MAGIC `mode: development` changes, and place the four commands in a CI flow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `databricks.yml` — variables and targets
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: orders_pipeline
# MAGIC
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     description: Target catalog
# MAGIC     default: dev_catalog
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     nightly_orders:
# MAGIC       tasks:
# MAGIC         - task_key: ingest
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/ingest.py
# MAGIC             base_parameters: { catalog: ${var.catalog} }
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     default: true
# MAGIC   test:
# MAGIC     workspace: { host: https://test-workspace... }
# MAGIC     variables: { catalog: test_catalog }
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     workspace: { host: https://prod-workspace... }
# MAGIC     variables: { catalog: prod_catalog }
# MAGIC     run_as: { service_principal_name: sp-prod-pipelines }
# MAGIC ```
# MAGIC
# MAGIC One definition, three environments — `${var.…}` substitution per target is
# MAGIC the "environment configuration" the guide names.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `mode: development` vs `mode: production`
# MAGIC
# MAGIC | | development | production |
# MAGIC |---|---|---|
# MAGIC | Resource names | **prefixed** `[dev yourname]` — everyone gets an isolated copy | canonical names |
# MAGIC | Schedules/triggers | **paused** on deploy | live |
# MAGIC | Runs as | you | the configured **service principal** |
# MAGIC | Meant for | iterating safely | CI-driven releases |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Deploy is **declarative**: the bundle is the source of truth, and
# MAGIC >   hand-edits in the UI are overwritten next deploy — that's the feature,
# MAGIC >   not a bug ("job definitions drifted between environments" → bundles).
# MAGIC > - Prod runs as a **service principal** so nothing breaks when a person
# MAGIC >   leaves — the "job failed after Alice's account was disabled" question.
# MAGIC > - Rollback = redeploy the previous git tag. If changes were UI clicks,
# MAGIC >   there's nothing to roll back to.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The CI/CD flow to recognize
# MAGIC
# MAGIC ```
# MAGIC feature branch ── PR ─▶ main ─────▶ tag/release
# MAGIC      │                   │               │
# MAGIC  unit tests          bundle deploy    bundle deploy -t prod
# MAGIC  bundle validate     -t test          (service principal)
# MAGIC                          │
# MAGIC                      bundle run smoke_job -t test
# MAGIC ```
# MAGIC
# MAGIC | Command | Job in the flow |
# MAGIC |---|---|
# MAGIC | `databricks bundle validate` | PR gate — YAML/vars sanity, no deploy |
# MAGIC | `databricks bundle deploy -t <target>` | create/update that environment |
# MAGIC | `databricks bundle run <job> -t <target>` | integration/smoke test |
# MAGIC | `databricks bundle destroy -t <target>` | tear down (dev/test hygiene) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity-check a bundle without deploying (works where the CLI exists)
# MAGIC
# MAGIC ```bash
# MAGIC git clone https://github.com/byambaa1982/lakehouse-starter && cd lakehouse-starter
# MAGIC databricks bundle validate          # resolves variables, prints the plan
# MAGIC databricks bundle deploy -t dev     # everything arrives prefixed + paused
# MAGIC ```
# MAGIC
# MAGIC The sister repo lakehouse-starter is a working example of everything on
# MAGIC this page — deploying it once teaches this section better than rereading
# MAGIC it. Deep dive: Professional
# MAGIC [`09-debugging-and-deploying`](../../data-engineer-professional/09-debugging-and-deploying/).
