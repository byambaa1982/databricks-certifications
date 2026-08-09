# 05 — Monitoring and Alerting (10%)

Every question in this section is a matching exercise: **a symptom on one side,
the one place its numbers live on the other.** Streaming rates and state size
live in query progress; per-commit write counts in `DESCRIBE HISTORY`;
expectation metrics in the DLT event log; account-wide runs and spend in
`system.` tables; and "data went stale" belongs to SQL alerts. Uses the
`de_pro` clones from section 01's setup — no setup notebook of its own.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_streaming_metrics.py`](01_streaming_metrics.py) | A `StreamingQueryListener` capturing events live, `lastProgress` / `recentProgress` field-by-field (rates, `durationMs`, state operators), Auto Loader backlog metrics and what rising backlog means |
| [`02_table_history_and_event_logs.py`](02_table_history_and_event_logs.py) | `DESCRIBE HISTORY` with `operationMetrics` queried per commit, log vs deleted-file retention (visible history you can't time-travel to), the `event_log()` TVF and the expectations-metrics query |
| [`03_jobs_system_tables_and_alerts.py`](03_jobs_system_tables_and_alerts.py) | Repair runs, duration-warning notifications, `system.lakeflow` / `system.billing` probes (run live, degrade without access), SQL alert anatomy + the freshness pattern, Lakehouse Monitoring profile types and drift |

## Setup

Run section 01's `00_setup.py` first if you haven't — notebook 01 streams from
the `bronze_orders` clone and notebook 02 mutates a scratch copy of
`silver_orders`. Widgets default to `dev_catalog` / `de_pro` as usual.

## How to study

1. Drill the symptom → tool table at the end of notebook 03 until it's
   reflexive — most of this section's questions are that table reworded.
2. Know the two retentions apart (`delta.logRetentionDuration` vs
   `delta.deletedFileRetentionDuration`) and the two latencies apart (job
   notification = immediate, SQL alert = next scheduled evaluation). Both
   contrasts are recurring questions.
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 10% weight ≈ 6 of 60 questions.
