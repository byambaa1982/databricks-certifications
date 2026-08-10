# Databricks Machine Learning Associate

Study notes organized by the official exam sections (the **March 2025
guide**), one folder per section, ordered by section number. The weight next
to each section is its share of the exam — spend your study time accordingly.
~45 scored questions, 90 minutes, 70% to pass.

| # | Section | Weight |
|---|---------|--------|
| 01 | [Databricks Machine Learning](01-databricks-machine-learning/) | 38% |
| 02 | [Data Processing](02-data-processing/) | 19% |
| 03 | [Model Development](03-model-development/) | 31% |
| 04 | [Model Deployment](04-model-deployment/) | 12% |

Sections 01 + 03 are **69% of the exam** — the platform's ML tooling
(AutoML, feature engineering in Unity Catalog, MLflow, the model registry)
and classical model development (Spark ML, tuning, metrics). Data processing
is largely section 03 of the analyst/engineer tracks wearing an ML hat, and
deployment is one decision table plus API shapes.

## Setup

Most notebooks want a **Databricks ML Runtime** cluster (mlflow, sklearn,
hyperopt preinstalled) — cells that need it degrade gracefully elsewhere.
Section 01's `00_setup.py` builds a deterministic synthetic churn dataset in
`dev_catalog.ml_assoc` that every other notebook uses.

## Practice

- 🎯 [Timed practice exams on TestHub](https://testlogichub.com)
- 🏗️ [lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter) — the gold tables make realistic feature sources
- 📖 [Official exam page](https://www.databricks.com/learn/certification/machine-learning-associate)
