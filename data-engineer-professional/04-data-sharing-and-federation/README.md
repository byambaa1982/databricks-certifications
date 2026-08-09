# 04 — Data Sharing and Federation (5%)

The smallest section, and the most conceptual: both topics are about **access
without copies**. Delta Sharing exports access to your data; Lakehouse
Federation imports access to someone else's. Creating shares and connections
needs admin privileges and external counterparties, so these notebooks are
reference-first — the DDL is complete and annotated, and the introspection
cells (`SHOW SHARES`, `SHOW CONNECTIONS`, …) run safely anywhere and degrade
gracefully without permissions. No setup notebook needed.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_delta_sharing.py`](01_delta_sharing.py) | The provider → share → recipient chain, full DDL (`CREATE SHARE` / `RECIPIENT` / `GRANT SELECT ON SHARE`, CDF sharing, views), D2D vs open sharing contrast table, "question says → answer" drill table |
| [`02_lakehouse_federation.py`](02_lakehouse_federation.py) | `CREATE CONNECTION` → `CREATE FOREIGN CATALOG`, pushdown behavior, read-only semantics, when to materialize instead, and the sharing-vs-federation-vs-ingestion decision table |

## How to study

1. One sentence carries most of this section's questions: **sharing exports
   access, federation imports access, ingestion imports data.** Classify the
   scenario first, then pick syntax.
2. For Delta Sharing, know *who is identified by what*: D2D recipients by
   metastore sharing identifier, open-sharing recipients by activation link +
   credential file. That distinction alone is a recurring question.
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 5% weight ≈ 3 of 60 questions.
