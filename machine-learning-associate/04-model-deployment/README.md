# 04 — Model Deployment (12%)

One decision — how fresh must predictions be — and three API shapes: batch
via `mlflow.pyfunc.spark_udf` (run live against the registered champion),
streaming as the same UDF on a streaming DataFrame, and real-time via Mosaic
AI Model Serving endpoints with traffic splitting and scale-to-zero.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_inference_and_serving.py`](01_inference_and_serving.py) | The freshness decision table (and the REST-for-nightly-scores trap), `spark_udf` batch scoring by `@champion` alias run live, the streaming variant, serving-endpoint config with traffic split and scale-to-zero |

## Setup

Run section 01's `00_setup.py` and its notebook 03 (registers
`churn_model`) first for the live batch cell; everything else is
self-contained.

## How to study

1. Classify the scenario by who's waiting: nobody → batch; a pipeline →
   streaming; a user → real-time endpoint. Then match the named features
   (traffic split = canary, scale-to-zero = idle cost vs cold start).
2. Notice how much of this section is section 01 again: aliases, UC
   registration, and feature-store lookups are what make each mode
   promotable without code changes.
3. Then drill this section on [TestHub](https://testlogichub.com) — 12% ≈ 5
   of 45 questions. That's all four sections — full timed mock next.
