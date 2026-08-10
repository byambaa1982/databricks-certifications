# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — ML Runtime and AutoML
# MAGIC
# MAGIC Two platform facts open the heaviest section: which **compute** ML work
# MAGIC runs on, and what **AutoML** actually produces (hint: notebooks you keep,
# MAGIC not a black box).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ML Runtime — the cluster choice
# MAGIC The **Databricks Runtime for Machine Learning** is the standard runtime
# MAGIC plus preinstalled, version-matched ML libraries: sklearn, XGBoost,
# MAGIC MLflow, Hyperopt, PyTorch/TensorFlow — and GPU variants.
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Team wastes time pip-installing matching libraries on every cluster"
# MAGIC >   → use the **ML Runtime**, not init scripts.
# MAGIC > - Extra packages: **notebook-scoped** `%pip install` (isolated per
# MAGIC >   notebook, gone with the session) beats cluster-wide installs for
# MAGIC >   experimentation.
# MAGIC > - SQL warehouses can't train models — ML work needs a cluster (or
# MAGIC >   serverless notebook compute).

# COMMAND ----------

import importlib

for lib in ["mlflow", "sklearn", "hyperopt", "xgboost"]:
    try:
        m = importlib.import_module(lib)
        print(f"{lib:10s} {getattr(m, '__version__', '?')}   (ML Runtime has these preinstalled)")
    except ImportError:
        print(f"{lib:10s} MISSING -> this is not an ML Runtime cluster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoML — a glass box, not a black box
# MAGIC Point AutoML at a table + target and it: explores algorithms, engineers
# MAGIC baseline features, tunes, and — the exam's favorite fact — **generates an
# MAGIC editable notebook for every trial** plus a data-exploration notebook.
# MAGIC You get the best model *and* the code that made it.
# MAGIC
# MAGIC | AutoML supports | Notes |
# MAGIC |---|---|
# MAGIC | classification / regression / **forecasting** | the three problem types |
# MAGIC | UI or Python API | `databricks.automl.classify(...)` |
# MAGIC | outputs | best run, per-trial notebooks, EDA notebook, MLflow experiment with every trial logged |
# MAGIC
# MAGIC ```python
# MAGIC from databricks import automl
# MAGIC summary = automl.classify(
# MAGIC     dataset=spark.table("dev_catalog.ml_assoc.customers_churn"),
# MAGIC     target_col="churned",
# MAGIC     timeout_minutes=15,
# MAGIC )
# MAGIC summary.best_trial.model_path      # ready for registration / inference
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - AutoML's role: **baseline fast** + feature/algorithm *selection
# MAGIC >   insight* — you open the generated notebook and keep engineering.
# MAGIC >   "AutoML replaces data scientists" is always the wrong option.
# MAGIC > - Every trial lands in an **MLflow experiment** — comparison and
# MAGIC >   reproducibility come free (notebook 03's machinery).
# MAGIC > - Forecasting is included; clustering/recommendation are **not** AutoML
# MAGIC >   problem types.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where everything lives — the ML side of the workspace
# MAGIC
# MAGIC | Asset | Home |
# MAGIC |---|---|
# MAGIC | experiments & runs | **MLflow Tracking** (Experiments UI) |
# MAGIC | registered models | **Unity Catalog** (`catalog.schema.model`) |
# MAGIC | feature tables | Unity Catalog tables with primary keys (notebook 02) |
# MAGIC | serving endpoints | **Mosaic AI Model Serving** (section 04) |
# MAGIC
# MAGIC One namespace, one permission model — the same UC story as every other
# MAGIC track, now covering models.
