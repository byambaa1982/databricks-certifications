# 01 — Developing Code for Data Processing using Python and SQL (22%)

The heaviest section of the exam. Everything here runs against the **real
tables** deployed by [lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter),
so you can paste any snippet into a notebook or the SQL editor and see actual results.

## The tables

```sql
USE CATALOG dev_catalog;
USE SCHEMA dev_byambaa1982_lakehouse_starter;  -- your dev-mode prefix may differ: check `databricks bundle summary -t dev`
SHOW TABLES;
```

| Table | Grain | Interesting columns |
|---|---|---|
| `bronze_orders` | raw order events (strings!) | `order_id`, `customer_id`, `amount DOUBLE`, `order_ts STRING`, `status`, `_rescued_data`, `_ingested_at`, `_source_file` |
| `bronze_customers_cdc` | raw CDC events | `customer_id`, `op` (INSERT/UPDATE/DELETE), `event_ts STRING`, `name`, `email`, `tier` |
| `silver_orders` | validated orders (CDF on) | `amount DECIMAL`, `order_ts TIMESTAMP`, `order_date DATE`, `status` |
| `silver_customers` | SCD Type 2 dimension | `customer_id`, `tier`, `__START_AT`, `__END_AT` |
| `gold_daily_revenue` | 1 row / day | `order_date`, `revenue`, `order_count`, `cancelled_count` |
| `gold_customer_revenue` | 1 row / customer | `lifetime_value`, `completed_orders`, `last_order_ts` |

Bronze keeps ingestion problems on purpose (bad rows, string timestamps) —
which makes it perfect practice material for this section.

## Study files

| File | Exam objectives covered |
|---|---|
| [`01-dataframe-transformations.md`](01-dataframe-transformations.md) | select/cast/when, joins, aggregations, narrow vs wide, `selectExpr` |
| [`02-window-functions.md`](02-window-functions.md) | ranking, running totals, lag/lead, latest-row-per-key dedup |
| [`03-udfs-and-higher-order-functions.md`](03-udfs-and-higher-order-functions.md) | Python vs pandas UDFs, SQL UDFs, `transform`/`filter`/`aggregate`, why built-ins win |
| [`04-delta-dml-and-merge.md`](04-delta-dml-and-merge.md) | MERGE upserts, CDC dedup-then-merge, UPDATE/DELETE, schema evolution |
| [`05-structured-streaming.md`](05-structured-streaming.md) | Auto Loader, Change Data Feed, triggers, output modes, watermarks |

## How to study this section

1. Run each snippet, then **predict the output before executing** — the exam
   shows you code and asks what happens.
2. Re-run the seed job (`databricks bundle run lakehouse_job -t dev`) between
   streaming exercises so new files actually arrive.
3. Drill the matching section on [TestHub](https://testlogichub.com) under time
   pressure — 22% weight means roughly 13 of 60 questions.
