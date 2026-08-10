# 03 — Model Deployment (12%)

The smallest section: blue-green vs canary vs shadow as risk postures, how a
serving endpoint's traffic config implements two of the three, inference
tables as the feedback data source, and the two query shapes (MLflow
Deployments SDK and plain REST with signature-validated payloads).

## Run order

| Notebook | What you practice |
|---|---|
| [`01_serving_strategies.py`](01_serving_strategies.py) | The three-strategies table with the without-affecting-users → shadow trap, the `update_config` traffic-split shape, inference tables feeding section 02's monitors, both query APIs, the full loop recapped |

## Setup

Reference-first; builds on the Associate deployment notebook and this
track's sections 01–02.

## How to study

1. Classify by risk sentence: all-at-once + instant rollback → blue-green;
   gradual real traffic → canary; zero user exposure → shadow. Then check
   whether a traffic split can implement it (shadow: no).
2. Remember where canary judgment comes from: the endpoint's **inference
   table** monitored by section 02's machinery — the sections chain.
3. Then drill this section on [TestHub](https://testlogichub.com) — 12% ≈ 7
   of 60 questions. That's the full track — timed mock next.
