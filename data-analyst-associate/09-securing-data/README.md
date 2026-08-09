# 09 — Securing Data (8%)

The analyst's share of security: the three-grant path (on every Databricks
exam), ownership of the tables you create, and what actually protects PII
when your work reaches other people — masks and filters (run live), tags as
findability-not-protection, and the section-06 interaction where embedded
dashboard credentials bypass per-viewer filtering.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_securing_data.py`](01_securing_data.py) | The three-grant path and its "has SELECT, can't query" trap, ownership and group grants, a column mask created and observed firing, dynamic views as legacy, the credentials interaction with dashboards, the four-point checklist |

## Setup

Run section 04's `00_setup.py` first (the mask demo copies its `customers`
table).

## How to study

1. Two reflexes: "can't query despite SELECT" → the path grants; "will
   viewers see the masked or unmasked data?" → whose credentials run the
   query.
2. Sort the three P's: tags = findability, masks/filters = protection,
   grants = reach.
3. Then drill this section on [TestHub](https://testlogichub.com) — 8% ≈ 4
   of 45 questions. That's all nine sections — run a full timed mock next.
