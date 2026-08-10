# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Distributed tuning (Optuna) and nested MLflow runs
# MAGIC
# MAGIC Professional-level tuning is two upgrades over the Associate's Hyperopt:
# MAGIC **Optuna** as the current framework (with the same "parallelize trials,
# MAGIC not the model" logic), and **nested runs** so a tuning session logs as
# MAGIC one parent with a child per trial. Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optuna — define-by-run search, TPE under the hood

# COMMAND ----------

try:
    import mlflow
    import optuna
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import cross_val_score

    pdf = (
        spark.table("customer_features_history")  # noqa: F821
        .filter("feature_ts = '2024-01-01'")
        .join(spark.table("churn_labels"), "customer_id")  # noqa: F821
        .select("tenure_months", "support_tickets", "monthly_spend", "churned")
        .toPandas()
    )
    X, y = pdf.drop(columns=["churned"]), pdf["churned"]

    def objective(trial):
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 50, 200, step=25),
            "max_depth": trial.suggest_int("max_depth", 3, 10),
            "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 10),
        }
        model = RandomForestClassifier(**params, random_state=42)
        return cross_val_score(model, X, y, cv=3, scoring="roc_auc").mean()

    # parent run wraps the study; every trial logs as a CHILD run
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    with mlflow.start_run(run_name="optuna_tuning") as parent:
        study = optuna.create_study(direction="maximize")
        for _ in range(10):
            trial = study.ask()
            with mlflow.start_run(run_name=f"trial_{trial.number}", nested=True):
                value = objective(trial)
                mlflow.log_params(trial.params)
                mlflow.log_metric("cv_auc", value)
            study.tell(trial, value)
        mlflow.log_metric("best_cv_auc", study.best_value)
        mlflow.log_params({f"best_{k}": v for k, v in study.best_params.items()})
    print(f"best AUC {study.best_value:.3f} with {study.best_params}")
except ImportError as e:
    print(f"needs ML Runtime ({e}) — the structure above is the exam content")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - Optuna's space is **define-by-run** — `trial.suggest_*` calls inside
# MAGIC >   the objective (vs Hyperopt's up-front `hp.*` dict). `direction=
# MAGIC >   "maximize"` removes Hyperopt's negate-the-metric dance.
# MAGIC > - `nested=True` is the whole nested-run API: parent = the tuning
# MAGIC >   session, children = trials; the experiment UI groups them, and
# MAGIC >   `search_runs` can filter to the parent's children. One flat run per
# MAGIC >   trial with no parent is the anti-pattern being tested.
# MAGIC > - Scaling out: give the study shared storage (or use Optuna's Spark/Ray
# MAGIC >   integrations) and run workers in parallel — same principle as
# MAGIC >   `SparkTrials`: **parallelize single-node trials across the cluster**.
# MAGIC > - **Ray on Databricks** is the name for scaling Python-native ML
# MAGIC >   (tuning, RL, distributed data-parallel training) beyond what Spark's
# MAGIC >   APIs cover — recognize when an option says "Ray cluster on
# MAGIC >   Databricks".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading a tuning experiment afterwards

# COMMAND ----------

try:
    import mlflow
    runs = mlflow.search_runs(
        filter_string="attributes.run_name LIKE 'trial_%'",
        order_by=["metrics.cv_auc DESC"],
        max_results=5,
    )
    print(runs[["run_id", "metrics.cv_auc"]].to_string(index=False))
except Exception as e:
    print(f"(no runs to search here: {type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which tuner when — the one-line answers
# MAGIC
# MAGIC | Question says | Answer |
# MAGIC |---|---|
# MAGIC | "modern framework, define-by-run, pruning" | Optuna |
# MAGIC | "`fmin`, `hp.choice`, `SparkTrials`" | Hyperopt (legacy but still on the exam) |
# MAGIC | "tune a *Spark ML* pipeline" | `CrossValidator`/`TrainValidationSplit` with a param grid |
# MAGIC | "no code at all, baseline fast" | AutoML |
# MAGIC | "distribute deep-learning trials over GPUs" | Ray Tune on Databricks |
