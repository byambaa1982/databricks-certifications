# 05 — Analyzing Queries (15%)

The "why was it slow / why did the number change" section. The workflow is
fixed: query history finds the run, the query profile says where time went
(queue vs scan vs join vs spill — each with its own fix), and `DESCRIBE
HISTORY` answers the other suspicion — the query didn't change, the table
did. Around that sit the features you get for free (Photon, result cache,
disk cache) and the one you apply (liquid clustering).

## Run order

| Notebook | What you practice |
|---|---|
| [`01_query_history_and_profile.py`](01_query_history_and_profile.py) | `system.query.history` probes, profile anatomy (queue/compile/execute), the profile-pattern → diagnosis table, `EXPLAIN` read bottom-up, profile-vs-table-history disambiguation |
| [`02_caches_clustering_and_history.py`](02_caches_clustering_and_history.py) | Photon + the two caches (and the "instant second run" trap), `CLUSTER BY` applied live, `DESCRIBE HISTORY` with per-commit metrics, time-travel diffing, the four-step slow/wrong checklist |

## Setup

Run section 04's `00_setup.py` first — both notebooks query and mutate the
`da_assoc` tables.

## How to study

1. Learn the two questions apart: "why *slow*" → history + profile; "why
   *different*" → `DESCRIBE HISTORY` + time travel. Questions deliberately
   swap them.
2. Memorize the profile patterns table — scan-without-pruning, join fan-out,
   spill, and queue-dominated each map to exactly one fix.
3. Then drill this section on [TestHub](https://testlogichub.com) — 15% ≈ 7
   of 45 questions.
