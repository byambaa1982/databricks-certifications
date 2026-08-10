# 02 — MLOps (44%)

The other half of the exam. One doctrine — **deploy code, not models** —
and the machinery around it: one catalog per environment with `@champion`
aliases as the promotion mechanism, validation gates before the alias moves,
bundles carrying the training job with a validate-and-promote task, and then
the post-deployment loop: Lakehouse Monitoring's three profiles, drift
metrics that fire before labels arrive, and drift-triggered retraining.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_deploy_code_testing_and_bundles.py`](01_deploy_code_testing_and_bundles.py) | Deploy-code vs deploy-model (and when the latter wins), per-env catalogs + alias promotion, the three test layers with the promotion-gate code, ML-shaped bundle YAML, the three retraining triggers |
| [`02_monitoring_and_drift.py`](02_monitoring_and_drift.py) | The three monitor profiles, a simulated two-month inference log whose prediction mix visibly drifts, the `quality_monitors.create` inference-log shape, the drift-signal table, custom metrics, alerting on metric tables |

## Setup

Run section 01's `00_setup.py` first — notebook 02 builds its inference log
from the timestamped feature history.

## How to study

1. Recite the doctrine and its two corollaries: code promotes (models
   retrain per env), and consumers always load by alias (promotion = alias
   flip, after the gate passes).
2. Sort monitoring symptoms by layer: inputs moved → feature drift; output
   mix moved → prediction drift; model wrong → quality (needs labels);
   endpoint slow → serving metrics. Wrong-layer options are the distractors.
3. Then drill this section on [TestHub](https://testlogichub.com) — 44% ≈ 26
   of 60 questions.
