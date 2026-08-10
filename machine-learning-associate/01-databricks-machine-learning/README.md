# 01 — Databricks Machine Learning (38%)

The heaviest section by far: the platform's ML tooling. Four notebooks — a
deterministic churn dataset everything else reuses, ML Runtime + AutoML
(glass-box: it generates notebooks you keep), feature tables in Unity
Catalog (a Delta table with a primary key, looked up by key at train and
serve time), and the MLflow → UC-registry → alias-promotion flow with a real
model trained and registered where the runtime allows.

## Run order

| Notebook | What you practice |
|---|---|
| [`00_setup.py`](00_setup.py) | Builds `dev_catalog.ml_assoc.customers_churn`: 1,000 hash-deterministic rows, real label signal, planted NULLs/outliers, intentional class imbalance |
| [`01_ml_runtime_and_automl.py`](01_ml_runtime_and_automl.py) | ML Runtime vs pip-installing by hand, notebook-scoped `%pip`, AutoML's three problem types and its per-trial generated notebooks, where ML assets live in UC |
| [`02_feature_engineering_in_uc.py`](02_feature_engineering_in_uc.py) | A feature table built live (PK constraint), `FeatureEngineeringClient` / `FeatureLookup` / `create_training_set` / `fe.log_model` shapes, why lookups beat re-deriving |
| [`03_mlflow_and_model_registry.py`](03_mlflow_and_model_registry.py) | Explicit logging vs `autolog`, `search_runs`, UC registration (three-level names), `@champion`/`@challenger` aliases as atomic promotion, loading by alias |

## Setup

Run `00_setup.py` on (ideally) an ML Runtime cluster. Widgets default to
`dev_catalog` / `ml_assoc`.

## How to study

1. The three flows to know cold: AutoML → notebooks + experiment; features →
   PK table + lookup by key; models → run → version → alias. Most questions
   are one arrow of one flow.
2. Aliases replaced stages — any answer promoting to "Production stage" is
   the legacy distractor.
3. Then drill this section on [TestHub](https://testlogichub.com) — 38% ≈ 17
   of 45 questions, nearly double any other section.
