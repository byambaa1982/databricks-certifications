# 01 — Understanding the Data Intelligence Platform (11%)

Orientation from the consumption side: where analysts work (SQL editor on a
SQL warehouse), how they find and trust data (Catalog Explorer, Marketplace,
certification, lineage), and the one table worth memorizing — serverless vs
pro vs classic warehouses, and size vs scale-out.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_platform_for_analysts.py`](01_platform_for_analysts.py) | The analyst's map of the platform, the warehouse comparison table with the size-vs-scaling trap, UC as metadata (comments, tags, certified badge, lineage, owner), Marketplace = Delta Sharing, live introspection |

## Setup

None — self-contained.

## How to study

1. The warehouse questions are the most mechanical on the exam: one slow
   query → size up; many concurrent users → scale out; idle cost →
   auto-stop/serverless. Drill that triple.
2. Know the trust signals apart: certified = canonical (a signal), grants =
   access (a control), lineage = origin (a fact). Questions mix them up on
   purpose.
3. Then drill this section on [TestHub](https://testlogichub.com) — 11% ≈ 5
   of 45 questions.
