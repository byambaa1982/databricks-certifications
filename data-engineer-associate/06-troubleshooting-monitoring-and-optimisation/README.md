# 06 — Troubleshooting, Monitoring & Optimization (10%)

Symptom → place-to-look → lever. Notebook 01 classifies failures (analysis
vs runtime, driver vs executor — both demonstrated live) and maps every
symptom to the page that diagnoses it; notebook 02 manufactures a small-file
problem, `OPTIMIZE`s it away, and covers liquid clustering plus the
on-by-default machinery (AQE, disk cache, Photon) the exam expects you to
*not* reinvent.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_troubleshooting_and_monitoring.py`](01_troubleshooting_and_monitoring.py) | Analysis-time vs runtime failures triggered live, `Caused by:` chains, the symptom table (cluster won't start, driver vs executor OOM, straggler tasks, slowing jobs), where each monitoring surface lives |
| [`02_optimization_basics.py`](02_optimization_basics.py) | Small-file problem measured and fixed live, liquid clustering vs partitioning, OPTIMIZE vs VACUUM as different jobs, predictive optimization, AQE/Photon/disk cache, broadcast joins |

## Setup

Self-contained — notebook 02 builds its own scratch table in
`dev_catalog.de_assoc`.

## How to study

1. Classify first, always: analysis-time (instant, fix the query) vs runtime
   (executor logs, `Caused by:`); driver OOM (`collect`, big broadcast) vs
   executor OOM (skew, shuffle). Most distractors sit in the wrong quadrant.
2. Recognize the standing wrong answers: partition by high-cardinality key,
   `df.cache()` as a cure-all, hand-salting skew that AQE already handles,
   VACUUM to "speed up queries".
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 10% weight ≈ 4–5 of 45 questions.
