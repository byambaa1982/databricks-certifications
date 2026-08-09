# 07 — Governance and Security (15%)

The last section and the fourth-heaviest. Two layers, one notebook each:
**whether you reach an object** (the UC hierarchy, managed vs external DROP
semantics, the three-grant path) and **what you see when you do** (column
masks and row filters run live on a scratch table, then ABAC — the
tag-driven version that scales the same idea).

## Run order

| Notebook | What you practice |
|---|---|
| [`01_unity_catalog_and_grants.py`](01_unity_catalog_and_grants.py) | The hierarchy, managed vs external DROP/UNDROP (checked live with `DESCRIBE EXTENDED`), the three-grant path and the "has SELECT but can't query" trap, inheritance, groups + service principals, GRANT/REVOKE vs legacy DENY |
| [`02_fine_grained_access.py`](02_fine_grained_access.py) | A column mask and a row filter created, bound, and observed firing; dynamic views as the legacy contrast; ABAC (tag columns once, policy applies everywhere) and the PII sweep query |

## Setup

Self-contained — both notebooks build their own scratch objects in
`dev_catalog.de_assoc`.

## How to study

1. The two reflex answers: "granted SELECT but can't query" → missing `USE
   CATALOG`/`USE SCHEMA`; "dropped an external table" → data still in the
   files. Between them they cover a surprising share of this section.
2. Scope the tools: grants = reach, masks/filters = content, ABAC = the same
   content rules applied **by tag at scale**. When a question says "across
   all tables containing PII", it's asking for ABAC.
3. Deep dives: Professional
   [`07-data-security-and-compliance`](../../data-engineer-professional/07-data-security-and-compliance/)
   and [`08-data-governance`](../../data-engineer-professional/08-data-governance/).
4. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 15% weight ≈ 7 of 45 questions. That's all seven sections —
   time for a full timed mock.
