# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Databricks Asset Bundles: deploying like software
# MAGIC
# MAGIC Bundles are the exam's answer to every "how do we promote this to prod"
# MAGIC question: the job/pipeline/cluster definitions live **in the repo as YAML**,
# MAGIC parameterized per target, deployed by CLI — the same mechanism this study
# MAGIC repo's lakehouse-starter uses (`databricks bundle deploy -t dev`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## The anatomy of `databricks.yml`
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: lakehouse_starter
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     lakehouse_job:
# MAGIC       tasks:
# MAGIC         - task_key: seed
# MAGIC           notebook_task: { notebook_path: ../notebooks/seed.py }
# MAGIC         - task_key: pipeline
# MAGIC           depends_on: [{ task_key: seed }]
# MAGIC           pipeline_task: { pipeline_id: ${resources.pipelines.lakehouse_dlt.id} }
# MAGIC   pipelines:
# MAGIC     lakehouse_dlt:
# MAGIC       libraries:
# MAGIC         - notebook: { path: ../dlt/transforms.py }
# MAGIC       catalog: ${var.catalog}
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development          # <- the exam keyword
# MAGIC     default: true
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     run_as: { service_principal_name: "sp-prod-pipelines" }
# MAGIC ```
# MAGIC
# MAGIC | `mode: development` does | `mode: production` does |
# MAGIC |---|---|
# MAGIC | **prefixes** every resource with `dev_<user>_` (so each engineer gets an isolated copy — exactly why section 01's setup had to auto-detect the schema name) | deploys with canonical names |
# MAGIC | **pauses schedules/triggers** | schedules run |
# MAGIC | allows deploy of a dirty working tree | expects CI, locks source to the deploy |
# MAGIC | runs as **you** | runs as the configured **service principal** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The four commands
# MAGIC
# MAGIC ```bash
# MAGIC databricks bundle validate            # schema-check the YAML, resolve variables
# MAGIC databricks bundle deploy -t dev       # create/update the resources in the target
# MAGIC databricks bundle run lakehouse_job -t dev
# MAGIC databricks bundle destroy -t dev      # tear the target's resources down
# MAGIC ```
# MAGIC
# MAGIC Deploy is **declarative**: the bundle is the source of truth, and drift
# MAGIC (someone hand-edited the job in the UI) is overwritten on the next deploy.
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Job definitions differ between dev and prod and nobody knows why" →
# MAGIC >   resources defined **in the workspace UI** instead of in a bundle. The
# MAGIC >   fix is bundles (or Terraform), not discipline.
# MAGIC > - Production jobs **run as a service principal**, never a departed
# MAGIC >   engineer's user — the "job broke when Alice left" question.
# MAGIC > - `mode: development` prefixing is a *feature* (isolation), and it's why
# MAGIC >   dev artifacts must never be hardcoded by name.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The CI/CD flow the exam expects
# MAGIC
# MAGIC ```
# MAGIC feature branch ── PR ──▶ main ──▶ release
# MAGIC      │                    │           │
# MAGIC   pytest (unit,        bundle      bundle deploy -t prod
# MAGIC   notebook 02)         deploy -t staging     (run_as service principal)
# MAGIC      │                    │
# MAGIC   validate bundle      integration job on
# MAGIC                        small real data
# MAGIC ```
# MAGIC
# MAGIC - **Git folders (Repos)** put the workspace under version control for
# MAGIC   *development*; **bundles** are the unit of *deployment*. They compose:
# MAGIC   develop in a git folder, deploy through CI.
# MAGIC - Rollback = redeploy the previous git tag — because everything (jobs,
# MAGIC   pipelines, cluster specs, permissions) is in the repo, the old state is
# MAGIC   reproducible. If deploys were manual UI edits, there is nothing to roll
# MAGIC   back *to* — that asymmetry is a recurring question.
# MAGIC - DLT has its own dev/prod switch (development mode reuses a cluster and
# MAGIC   relaxes retries; production mode enforces retries + fresh compute) —
# MAGIC   orthogonal to bundle targets, and a favorite confusion pair.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify your CLI wiring (safe anywhere)

# COMMAND ----------

import subprocess  # noqa: E402

try:
    out = subprocess.run(
        ["databricks", "--version"], capture_output=True, text=True, timeout=30
    )
    print(out.stdout.strip() or out.stderr.strip())
except FileNotFoundError:
    print("databricks CLI not on this cluster's PATH — normal for a shared cluster; "
          "bundles are usually driven from a laptop or CI runner, not from notebooks")
