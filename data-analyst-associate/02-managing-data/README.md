# 02 — Managing Data (8%)

Finding tables you didn't build, labeling the ones you curate, and trusting
columns via metadata: Catalog Explorer and `information_schema` for
discovery, comments vs tags vs the certified badge (three different tools —
the exam mixes them deliberately), automatic two-direction lineage, and the
analyst's SQL-cleaning short list.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_discovery_tags_and_lineage.py`](01_discovery_tags_and_lineage.py) | `information_schema` discovery queries, comments and tags set + queried live (the PII sweep), certification as signal-not-control, lineage in both directions, the cleaning short list |

## Setup

Self-contained — uses widgets defaulting to `dev_catalog.de_assoc`.

## How to study

1. Keep the three labels straight: comment = documentation, tag = queryable
   classification, certified = canonical signal. And none of them grant or
   deny access.
2. Lineage questions are direction questions: audit a number → upstream;
   assess a change → downstream.
3. Then drill this section on [TestHub](https://testlogichub.com) — 8% ≈ 4
   of 45 questions.
