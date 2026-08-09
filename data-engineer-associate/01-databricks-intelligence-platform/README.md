# 01 — Databricks Intelligence Platform (6%)

The orientation section: the control/compute plane split, Delta Lake as
Parquet-plus-transaction-log, and which compute type fits which workload. At
6% it's the smallest section on the exam, but its vocabulary (workspace, Unity
Catalog, Lakeflow, volumes, medallion) is assumed by every other section — so
it goes first even though you should spend the least time here.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_platform_architecture.py`](01_platform_architecture.py) | The two-planes diagram (and where serverless bends it), Delta Lake's properties derived from the transaction log, live platform introspection (`current_metastore`, runtime, `SHOW CATALOGS`), the compute-selection money table, platform vocabulary |

## Setup

None — the notebook is self-contained and its introspection cells run on any
Unity Catalog-enabled compute.

## How to study

1. Know the three tables cold: what lives in which plane (data never in the
   control plane; serverless moves *compute*, not *data*), what the
   transaction log buys you, and which compute for which workload.
2. Don't over-invest — 6% ≈ 3 of 45 questions. Read once, run the notebook,
   move to section 02 where the real weight is.
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure.
