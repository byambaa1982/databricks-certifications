# 08 — Data Modeling with Databricks SQL (5%)

The smallest section: recognize star, snowflake, data vault, and one-big-table
on sight, place them in the medallion (stars and OBTs are gold), and respect
the grain. The star gets demonstrated live — section 04's schema already is
one — and the OBT gets built from it.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_modeling_for_analytics.py`](01_modeling_for_analytics.py) | The four-shapes table with the analyst-level verdicts, fact-vs-dimension roles said out loud on real tables, grain (and the 2×-revenue mixing bug), a gold OBT built live, informational PK/FK |

## Setup

Run section 04's `00_setup.py` first.

## How to study

1. Two reflexes cover the section: name the shape from a description (one
   join hop + denormalized dims = star; hubs/links/satellites = vault), and
   check the grain before trusting any aggregate.
2. Deep dive if curious: engineering
   [`10-data-modelling`](../../data-engineer-professional/10-data-modelling/)
   (surrogate keys, SCD2 mechanics).
3. Then drill this section on [TestHub](https://testlogichub.com) — 5% ≈ 2–3
   of 45 questions. Don't over-invest.
