# 03 — Data Transformation and Modeling (22%)

The heaviest section. It divides cleanly in three: **cleaning** (casts that
NULL instead of erroring, dedup done deterministically, every join type proved
by row counts), **aggregating** (the NULL rules, WHERE vs HAVING, pivot), and
**choosing the gold object** (table vs view vs materialized view vs streaming
table — the decision table is the exam content). Runnable on synthetic bronze
tables whose defects are placed exactly where the lessons need them.

## Run order

| Notebook | What you practice |
|---|---|
| [`00_setup.py`](00_setup.py) | Builds `bronze_orders_assoc` (string types, mixed-case statuses, a bad amount, a bad timestamp, a NULL key, two exact duplicates) and `bronze_customers_assoc` (with a missing and an orderless customer for the join demos) |
| [`01_cleaning_and_dataframe_ops.py`](01_cleaning_and_dataframe_ops.py) | Cast-to-NULL semantics, `when/otherwise`, `distinct` vs `dropDuplicates` vs `row_number` dedup, all five join types with predicted counts, `union` vs `unionByName` |
| [`02_aggregations.py`](02_aggregations.py) | The NULL rules cell (memorize its output), `groupBy/agg`, `GROUP BY ALL`, WHERE vs HAVING, `pivot` (and why you pass the value list), one window as a teaser |
| [`03_gold_layer_objects.py`](03_gold_layer_objects.py) | Medallion in one line per layer, all four gold objects created (MV/ST degrade gracefully off serverless), the decision table, constraints vs expectations |

## Setup

Run `00_setup.py`, then the notebooks in order — 02 and 03 use the
`silver_orders_assoc` table that 01 writes. Widgets default to
`dev_catalog` / `de_assoc`. Notebook 03's cleanup removes everything;
re-run `00_setup` to start over.

## How to study

1. Predict outputs before running — especially the join-count loop and the
   NULL-rules aggregation. Both appear on the exam nearly verbatim.
2. Own the gold decision table: view = free but recomputes per query;
   MV = precomputed + incrementally refreshed; streaming table = each row
   once. Most gold questions are one row of that table in a scenario costume.
3. Deep-dive pointers: Professional
   [`01-developing-code-for-data-processing`](../../data-engineer-professional/01-developing-code-for-data-processing/)
   (windows, UDFs, MERGE) and
   [`03-data-transformation-cleansing-and-quality`](../../data-engineer-professional/03-data-transformation-cleansing-and-quality/)
   (constraints, expectations, late data).
4. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 22% weight ≈ 10 of 45 questions.
