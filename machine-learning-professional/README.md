# Databricks Machine Learning Professional

Study notes organized by the official exam sections (the **September 2025
guide** — restructured to three sections), one folder per section. The weight
next to each section is its share of the exam. ~60 questions, 120 minutes,
70% to pass.

| # | Section | Weight |
|---|---------|--------|
| 01 | [Model Development Using Spark ML](01-model-development/) | 44% |
| 02 | [MLOps](02-mlops/) | 44% |
| 03 | [Model Deployment](03-model-deployment/) | 12% |

Two sections **are** the exam: distributed model development (Spark ML,
`applyInPandas`, Optuna/Ray, custom PyFunc, point-in-time features) and MLOps
(deploy-code with UC aliases, testing, bundles, Lakehouse Monitoring,
drift-triggered retraining). Deployment is one strategies table plus API
shapes. The guide's own advice: prefer **modern Unity Catalog constructs**
over legacy answers everywhere — stages, workspace registry, and
hand-managed environments are the standing distractors.

## Prerequisites

Assumes the [ML Associate](../machine-learning-associate/) material —
especially MLflow tracking, the UC registry with aliases, and feature tables.
Where a topic starts there, the notebooks link back instead of repeating.

## Setup

Section 01's `00_setup.py` builds `dev_catalog.ml_pro` with a deterministic
dataset that includes **event timestamps** (point-in-time lookups and
monitoring need them). ML Runtime recommended; cells degrade gracefully.

## Practice

- 🎯 [Timed practice exams on TestHub](https://testlogichub.com)
- 📖 [Official exam page](https://www.databricks.com/learn/certification/machine-learning-professional)
