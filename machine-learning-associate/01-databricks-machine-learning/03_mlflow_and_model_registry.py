# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — MLflow tracking and the Unity Catalog model registry
# MAGIC
# MAGIC The section's core machinery: every training run **logged** (params,
# MAGIC metrics, artifacts), the best one **registered** in UC, and promotion done
# MAGIC with **aliases** (`@champion` / `@challenger`). Trains a real model where
# MAGIC sklearn is available. Run `00_setup` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Track a run — params, metrics, model, all reproducible

# COMMAND ----------

try:
    import mlflow
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import f1_score, roc_auc_score
    from sklearn.model_selection import train_test_split

    pdf = (
        spark.table("customers_churn")  # noqa: F821
        .select("age", "tenure_months", "support_tickets", "churned")
        .toPandas()
    )
    X, y = pdf.drop(columns=["churned"]), pdf["churned"]
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=42)

    mlflow.autolog(disable=True)  # explicit logging below, for teaching
    with mlflow.start_run(run_name="rf_baseline") as run:
        params = {"n_estimators": 100, "max_depth": 5, "random_state": 42}
        model = RandomForestClassifier(**params).fit(X_tr, y_tr)

        mlflow.log_params(params)
        mlflow.log_metric("f1", f1_score(y_te, model.predict(X_te)))
        mlflow.log_metric("roc_auc", roc_auc_score(y_te, model.predict_proba(X_te)[:, 1]))
        mlflow.sklearn.log_model(model, "model")
        RUN_ID = run.info.run_id
    print(f"logged run {RUN_ID}")
except ImportError as e:
    RUN_ID = None
    print(f"needs an ML Runtime cluster ({e}) — read on; the API is the exam content")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `mlflow.autolog()` (or flavor-specific `mlflow.sklearn.autolog()`)
# MAGIC >   logs params/metrics/models **automatically** — the answer to "log
# MAGIC >   without changing training code". ML Runtime enables it by default for
# MAGIC >   many flavors.
# MAGIC > - A **run** belongs to an **experiment**; artifacts (plots, the model)
# MAGIC >   attach to the run. Compare runs in the UI or with
# MAGIC >   `mlflow.search_runs(order_by=["metrics.f1 DESC"])`.
# MAGIC > - Reproducibility = the run's params + code version + environment
# MAGIC >   (conda/requirements are logged with the model).

# COMMAND ----------

if RUN_ID:
    import mlflow
    best = mlflow.search_runs(order_by=["metrics.f1 DESC"], max_results=3)
    print(best[["run_id", "metrics.f1", "metrics.roc_auc"]].to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register in Unity Catalog, promote with aliases

# COMMAND ----------

if RUN_ID:
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    MODEL_NAME = f"{CATALOG}.{SCHEMA}.churn_model"
    try:
        mv = mlflow.register_model(f"runs:/{RUN_ID}/model", MODEL_NAME)
        client = mlflow.MlflowClient()
        client.set_registered_model_alias(MODEL_NAME, "champion", mv.version)
        print(f"registered {MODEL_NAME} v{mv.version}, alias @champion set")

        loaded = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
        print(f"loaded by alias — predicts: {loaded.predict(X_te.head(3)).tolist()}")
    except Exception as e:
        print(f"registration needs UC model permissions here ({type(e).__name__}) — "
              "the flow is the exam content")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The promotion story the exam wants
# MAGIC
# MAGIC ```
# MAGIC train -> log run -> register (new VERSION of catalog.schema.model)
# MAGIC       -> evaluate -> set alias @challenger
# MAGIC       -> beats @champion? -> move @champion alias to the new version
# MAGIC consumers ALWAYS load models:/<name>@champion  (never a hardcoded version)
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - UC registry replaces the old workspace registry — model names are
# MAGIC >   **three-level** (`catalog.schema.model`) and *aliases* replace the old
# MAGIC >   *stages* (Staging/Production). Stage-transition answers are legacy.
# MAGIC > - An alias points at exactly **one version**; moving it is atomic
# MAGIC >   promotion/rollback — consumers change nothing.
# MAGIC > - Registered models inherit UC governance: grants (`EXECUTE`), lineage
# MAGIC >   to their training data, cross-workspace sharing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup — keep the registered model (section 04 serves it)
