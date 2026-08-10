# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Deploy-code, testing, and bundles for ML
# MAGIC
# MAGIC MLOps on Databricks has one central doctrine the exam returns to
# MAGIC repeatedly: **deploy code, not models.** The training *pipeline* is what
# MAGIC promotes through dev → staging → prod; each environment retrains and
# MAGIC registers its own model in its own catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy-code vs deploy-model
# MAGIC
# MAGIC | | **Deploy code** (recommended) | Deploy model |
# MAGIC |---|---|---|
# MAGIC | What moves between envs | the training pipeline (via git + bundles) | a model artifact |
# MAGIC | Model trained on prod data? | ✅ — each env trains on its own data | ❌ — trained wherever it was built |
# MAGIC | Reproducibility | full (code + data + env per stage) | artifact-only |
# MAGIC | When the other one wins | — | training is enormous/one-off, or artifacts must be byte-identical across envs |
# MAGIC
# MAGIC The environment layout that goes with it: **one catalog per environment**
# MAGIC (`dev`, `staging`, `prod`), same schema/model names inside each. Promotion
# MAGIC = the *code* moving through CI, models retrained per env, and the
# MAGIC **`@champion` alias** updated after validation — consumers load
# MAGIC `models:/prod.ml.churn_model@champion` and never change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing the pipeline like software
# MAGIC The DE tracks built the machinery
# MAGIC ([testing](../../data-engineer-professional/09-debugging-and-deploying/02_testing_pipelines.py));
# MAGIC ML adds its own layers:
# MAGIC
# MAGIC | Layer | Checks | Example |
# MAGIC |---|---|---|
# MAGIC | **unit** | pure feature/transform functions | `assertDataFrameEqual` on a tiny frame |
# MAGIC | **model validation** | new candidate beats a bar *before* promotion | min AUC, beats `@champion` on holdout, latency budget |
# MAGIC | **integration** | the whole train→register→score path on small data | staging catalog run in CI |
# MAGIC
# MAGIC ```python
# MAGIC # the promotion gate the exam describes, as code
# MAGIC candidate_auc = evaluate(model_uri=f"models:/{name}@challenger")
# MAGIC champion_auc  = evaluate(model_uri=f"models:/{name}@champion")
# MAGIC assert candidate_auc >= champion_auc - 0.01, "candidate loses; do not promote"
# MAGIC client.set_registered_model_alias(name, "champion", candidate_version)
# MAGIC ```
# MAGIC
# MAGIC > **Exam trap:** validation gates run **before** the alias moves — an
# MAGIC > alias flip is the *last* step, not the test itself. And
# MAGIC > `mlflow.evaluate()` exists precisely to produce those candidate
# MAGIC > metrics uniformly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bundles carry the whole ML system
# MAGIC Same Asset Bundles as the DE tracks
# MAGIC ([bundles](../../data-engineer-professional/09-debugging-and-deploying/03_bundles_and_cicd.py)),
# MAGIC with ML-shaped resources: the training **job**, the **experiment**, the
# MAGIC registered-model reference, monitoring config.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     churn_training:
# MAGIC       tasks:
# MAGIC         - task_key: train
# MAGIC           notebook_task: { notebook_path: ../training/train.py }
# MAGIC         - task_key: validate            # the gate, as a task
# MAGIC           depends_on: [{ task_key: train }]
# MAGIC           notebook_task: { notebook_path: ../training/validate_and_promote.py }
# MAGIC targets:
# MAGIC   dev:     { mode: development, default: true }
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     run_as: { service_principal_name: sp-ml-prod }
# MAGIC     variables: { catalog: prod }
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Prod training jobs run as a **service principal**; the model's owner
# MAGIC >   is that principal, not a person.
# MAGIC > - `mode: development` prefixes + pauses — dev experiments don't collide
# MAGIC >   or fire schedules (same facts as DE, asked with ML nouns).
# MAGIC > - "Notebook was edited in prod and nobody knows what trained the model"
# MAGIC >   → the answer is bundles/git as source of truth, never workspace
# MAGIC >   permissions alone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The retraining triggers
# MAGIC
# MAGIC | Trigger | Mechanism |
# MAGIC |---|---|
# MAGIC | schedule ("weekly") | job schedule — simplest, often enough |
# MAGIC | new data arrived | file-arrival / table-update trigger on the training job |
# MAGIC | **drift detected** | Lakehouse Monitoring metrics (notebook 02) + an alert or a job condition that launches retraining |
# MAGIC
# MAGIC Drift-triggered retraining is the professional answer to "retrain only
# MAGIC when needed" — schedule-only answers waste compute; never-retrain answers
# MAGIC rot.
