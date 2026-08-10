# 01 — Model Development Using Spark ML (44%)

Half the exam. Three notebooks on the professional-grade judgments: pick the
right distributed shape (big data vs many models vs neither —
`applyInPandas` trains one model per region live), tune with Optuna under a
parent run with nested children, and package/feed models properly — custom
PyFunc as one deployable artifact, and point-in-time feature lookups proved
against a deliberately planted future-leakage trap.

## Run order

| Notebook | What you practice |
|---|---|
| [`00_setup.py`](00_setup.py) | `dev_catalog.ml_pro`: customers with regions, a **two-snapshot timestamped** feature history, labels observed between the snapshots (the leakage trap) |
| [`01_distributed_training_patterns.py`](01_distributed_training_patterns.py) | The decision table (big data vs many models), per-region training with `applyInPandas` live, `pandas_udf` vs `applyInPandas` vs `mapInPandas`, the `toPandas()` driver-OOM classic |
| [`02_tuning_and_nested_runs.py`](02_tuning_and_nested_runs.py) | Optuna define-by-run with `direction="maximize"`, every trial logged as a `nested=True` child run, `search_runs` over trials, the which-tuner-when table (Optuna/Hyperopt/CV/AutoML/Ray) |
| [`03_pyfunc_and_point_in_time_features.py`](03_pyfunc_and_point_in_time_features.py) | `PythonModel` with `load_context`/`predict`/`artifacts`/signature, the hand-written AS-OF join proving every row uses the Jan snapshot, `timeseries_columns` + `timestamp_lookup_key`, online tables + feature serving |

## Setup

Run `00_setup.py` on an ML Runtime cluster. Widgets default to
`dev_catalog` / `ml_pro`.

## How to study

1. The section's discriminations are all "which shape": data-big → Spark ML;
   models-many → `applyInPandas`; neither → single node + parallel tuning.
   State the shape before reading the options.
2. Point-in-time is the professional signature topic: run the AS-OF join,
   then say why "join the latest features" is leakage in one sentence.
3. Then drill this section on [TestHub](https://testlogichub.com) — 44% ≈ 26
   of 60 questions.
