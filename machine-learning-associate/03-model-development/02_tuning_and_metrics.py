# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Hyperopt tuning and the metrics that matter
# MAGIC
# MAGIC Two halves the exam pairs: **searching** hyperparameter space
# MAGIC intelligently (Hyperopt's Bayesian TPE beats grid/random) and **scoring**
# MAGIC the result with the right metric for the problem. Runs where ML Runtime
# MAGIC libraries exist; every table is exam material regardless.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "ml_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperopt — the API the exam quotes
# MAGIC `fmin` minimizes an objective over a search space with an algorithm:

# COMMAND ----------

try:
    from hyperopt import STATUS_OK, Trials, fmin, hp, tpe
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import cross_val_score

    pdf = (
        spark.table("customers_churn")  # noqa: F821
        .select("age", "tenure_months", "support_tickets", "churned")
        .toPandas()
    )
    X, y = pdf.drop(columns=["churned"]), pdf["churned"]

    space = {
        "max_depth": hp.quniform("max_depth", 3, 10, 1),       # quantized uniform
        "n_estimators": hp.quniform("n_estimators", 50, 200, 25),
        "min_samples_leaf": hp.quniform("min_samples_leaf", 1, 10, 1),
    }

    def objective(params):
        model = RandomForestClassifier(
            max_depth=int(params["max_depth"]),
            n_estimators=int(params["n_estimators"]),
            min_samples_leaf=int(params["min_samples_leaf"]),
            random_state=42,
        )
        f1 = cross_val_score(model, X, y, cv=3, scoring="f1").mean()
        return {"loss": -f1, "status": STATUS_OK}   # fmin MINIMIZES -> negate

    trials = Trials()
    best = fmin(fn=objective, space=space, algo=tpe.suggest,
                max_evals=15, trials=trials, rstate=None, verbose=False)
    print(f"best params: {best}")
    print(f"best f1:     {-min(t['result']['loss'] for t in trials.trials):.3f}")
except ImportError as e:
    print(f"needs ML Runtime ({e}) — the API shape above is the exam content")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `fmin` **minimizes** — maximize a metric by returning its **negative**.
# MAGIC > - `tpe.suggest` = Bayesian (learns from past trials); `rand.suggest` =
# MAGIC >   random. Bayesian finds good regions in fewer evals than grid/random —
# MAGIC >   that comparison is a straight question.
# MAGIC > - Search space vocab: `hp.choice` (categorical), `hp.uniform` /
# MAGIC >   `hp.quniform` (continuous / quantized), `hp.loguniform` (learning
# MAGIC >   rates).
# MAGIC > - **`SparkTrials`** parallelizes *single-machine* models (sklearn)
# MAGIC >   across the cluster; for *distributed* Spark ML models use plain
# MAGIC >   `Trials` (the model already uses the cluster). Swapping those two is
# MAGIC >   the classic wrong pair.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classification metrics — pick by cost of error
# MAGIC
# MAGIC | Metric | Answers | Reach for it when |
# MAGIC |---|---|---|
# MAGIC | Accuracy | share correct | classes balanced, errors equal cost — rarer than it sounds |
# MAGIC | Precision | of predicted positives, how many real? | false positives costly (spam, fraud flags) |
# MAGIC | Recall | of real positives, how many caught? | false negatives costly (churn, disease) |
# MAGIC | **F1** | harmonic mean of P and R | imbalance + both errors matter |
# MAGIC | ROC-AUC | ranking quality across all thresholds | compare models before picking a threshold |
# MAGIC | Log loss | quality of the *probabilities* | calibrated scores matter (pricing risk) |
# MAGIC
# MAGIC > **Exam trap:** 95% accuracy on a 95:5 imbalanced set = the
# MAGIC > predict-all-majority baseline. Imbalance questions want F1 / recall /
# MAGIC > PR-AUC, never accuracy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regression metrics
# MAGIC
# MAGIC | Metric | Property |
# MAGIC |---|---|
# MAGIC | **RMSE** | same units as target; **squares errors → punishes big misses** |
# MAGIC | **MAE** | same units; linear → robust to outliers |
# MAGIC | R² | share of variance explained (0–1, can go negative for bad fits) |
# MAGIC | MAPE | percentage terms; undefined at zero targets |
# MAGIC
# MAGIC RMSE vs MAE is the tested pair: outliers you must not ignore → RMSE;
# MAGIC outliers you must not be dominated by → MAE.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bias–variance — the diagnosis table
# MAGIC
# MAGIC | Train score | Test score | Diagnosis | Moves |
# MAGIC |---|---|---|---|
# MAGIC | low | low | **underfit** (high bias) | more capacity, more features, less regularization |
# MAGIC | high | low | **overfit** (high variance) | more data, regularization, shallower trees, early stopping |
# MAGIC | high | high | healthy | ship it (then monitor — section 04) |
# MAGIC
# MAGIC Cross-validation doesn't *fix* variance — it *reveals* it honestly.
# MAGIC That distinction appears in question form.
