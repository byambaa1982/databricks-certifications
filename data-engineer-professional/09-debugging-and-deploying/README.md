# 09 — Debugging and Deploying (10%)

Two skills the exam wants to see paired: diagnosing a running system
(**which failure happens where, which UI page answers which symptom**) and
shipping changes so failures get rarer (**tests on pure functions, bundles for
promotion, service principals in prod**). Notebook 01 breaks things on
purpose; notebook 02's tests actually run; notebook 03 maps to how this repo's
own lakehouse-starter deploys.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_debugging_spark.py`](01_debugging_spark.py) | Analysis-time vs runtime failures triggered live, reading `Caused by:` chains, the symptom → Spark-UI-page table, driver vs executor OOM causes, the streaming failure-mode table |
| [`02_testing_pipelines.py`](02_testing_pipelines.py) | Transformations as pure functions, `assertDataFrameEqual` passing **and** failing live (read the diff), unit vs integration boundaries, the repo layout that keeps notebooks thin |
| [`03_bundles_and_cicd.py`](03_bundles_and_cicd.py) | `databricks.yml` anatomy, `mode: development` vs `production` (prefixing, paused schedules, `run_as` service principal), validate/deploy/run/destroy, the PR → staging → prod flow, DLT dev/prod as the confusion pair |

## Setup

Run section 01's `00_setup.py` first — notebook 01 queries the `de_pro`
clones. Notebooks 02 and 03 are self-contained. Widgets default to
`dev_catalog` / `de_pro` as usual.

## How to study

1. Classify before you answer: *analysis-time* (instant, driver, fix the
   query) vs *runtime* (executor, read `Caused by:`), and *driver* OOM
   (`collect`, big broadcast) vs *executor* OOM (skew, shuffle). Half the
   section is these two axes.
2. For deployment, anchor on the asymmetry: everything-in-the-repo can be
   rolled back and reproduced; UI-edited resources can't. Then `mode:
   development` prefixing, paused schedules, and `run_as` fall out naturally.
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 10% weight ≈ 6 of 60 questions.
