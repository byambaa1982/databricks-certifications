# 07 — Ensuring Data Security and Compliance (10%)

Three layers, one notebook each: **who can reach an object** (the UC privilege
model and its two rules), **what they see when they do** (row filters, column
masks, dynamic views — run live so you watch your own email get redacted), and
**what the platform owes the regulator** (secrets hygiene, the
DELETE → PURGE → VACUUM deletion sequence, audit tables).

## Run order

| Notebook | What you practice |
|---|---|
| [`01_grants_and_privileges.py`](01_grants_and_privileges.py) | The hierarchy + path rule (`USE CATALOG` / `USE SCHEMA` gating), inheritance, ownership vs MANAGE, `SHOW GRANTS` + `information_schema.table_privileges` live, the privilege cheat sheet |
| [`02_row_filters_and_column_masks.py`](02_row_filters_and_column_masks.py) | A column mask and a row filter created, bound, and **observed firing** on a scratch table; `is_account_group_member` patterns; dynamic views as the legacy contrast; sharing/MERGE limitations |
| [`03_secrets_pii_and_compliance.py`](03_secrets_pii_and_compliance.py) | Secret scopes and the redaction-is-not-encryption trap, the four-step right-to-be-forgotten sequence (with deletion vectors), designing for deletability, `system.access.audit` probed live |

## Setup

Run section 01's `00_setup.py` first — notebook 02 clones `silver_customers`.
Widgets default to `dev_catalog` / `de_pro` as usual.

## How to study

1. The two access questions the exam always asks: "granted SELECT but can't
   query" → missing `USE CATALOG`/`USE SCHEMA`; "ran DELETE, compliant?" → not
   until PURGE + VACUUM. Own both cold.
2. Keep the tool-to-layer mapping straight: privileges control *reach*,
   filters/masks control *content*, audit proves *history*. Distractors cross
   the layers (e.g. offering a column mask when the requirement is denial of
   access).
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 10% weight ≈ 6 of 60 questions.
