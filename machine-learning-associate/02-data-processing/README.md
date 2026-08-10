# 02 — Data Processing (19%)

Classical prep with the exam's specific angles: `summary()` vs `describe()`
vs `dbutils.data.summarize`, the stddev-vs-IQR outlier rules (IQR wins on
skew — proved on planted outliers), the missing-value decision table
executed on planted NULLs, one-hot's failure modes (high cardinality, tree
models), log transforms, and the leakage rule that everything fits on train
only.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_eda_and_cleaning.py`](01_eda_and_cleaning.py) | The three stats tools and their differences, both outlier filters run against planted 99999s (and why IQR is robust), drop-vs-impute executed, the missingness decision table, the leakage framing of `Imputer` |
| [`02_feature_prep.py`](02_feature_prep.py) | `StringIndexer` → `OneHotEncoder` with `handleInvalid="keep"`, when OHE is the wrong answer, `log1p`/`expm1`, `VectorAssembler` + `StandardScaler` fit on train only, seeded `randomSplit` |

## Setup

Run section 01's `00_setup.py` first — the planted NULLs and outliers in
`customers_churn` are what make these demos checkable.

## How to study

1. The leakage rule is the section's through-line: anything *learned from
   data* (fill values, scale factors, encodings) is learned from **train**.
   If an option fits on the full dataset, it's wrong.
2. Encoding questions decide by model family and cardinality: linear + low
   cardinality → OHE; trees → indexed is fine; high cardinality → OHE is the
   trap.
3. Then drill this section on [TestHub](https://testlogichub.com) — 19% ≈ 9
   of 45 questions.
