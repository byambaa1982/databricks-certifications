# 04 — Executing Queries with Databricks SQL and SQL Warehouses (20%)

The heaviest section. Half is SQL you can predict (joins proved on
deliberately blemished data — an orphan order and an orderless customer make
every join type's count unambiguous; aggregation NULL rules; CTEs; QUALIFY;
time travel), half is the machinery around the SQL (warehouse size vs
scaling, saved queries, parameters, schedules, alerts).

## Run order

| Notebook | What you practice |
|---|---|
| [`00_setup.py`](00_setup.py) | Builds `dev_catalog.da_assoc`: customers / products / orders with placed blemishes (NULL email, NULL quantity, orphan order 5009, orderless customer 107) |
| [`01_sql_essentials.py`](01_sql_essentials.py) | The five join counts (11/12/12/13/1 — predict before running), NULL rules per aggregate, CTE pipeline, `QUALIFY` + window, the object zoo (table/view/temp view/MV), time travel read vs RESTORE |
| [`02_warehouses_and_query_tools.py`](02_warehouses_and_query_tools.py) | Size vs scaling vs auto-stop symptoms, queueing, saved queries, `:param` parameters, schedules and alerts, `system.query.history` probe, the ask → tool table |

## Setup

Run `00_setup.py`. Widgets default to `dev_catalog` / `da_assoc` — a
separate schema from the engineering tracks, so everything coexists.

## How to study

1. Predict the join counts and the NULL-rules output before running — both
   appear on the exam nearly verbatim, and this data makes the answers
   checkable.
2. For the machinery, read the symptom first: one slow query → size; many
   users → scaling; recurring report → schedule; threshold → alert.
3. Then drill this section on [TestHub](https://testlogichub.com) — 20% ≈ 9
   of 45 questions, the most of any section.
