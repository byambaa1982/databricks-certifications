# 03 — Developing DataFrame API Applications (30%)

The heaviest section, pure API precision: the five column-selection
spellings and where strings vs expressions are legal, `withColumn`
replace-semantics, dedupe/na/sort with their argument quirks
(type-matched `na.fill`, NULL sort placement), joins by string vs
expression, aggregation NULL rules, and read/write with the
default-mode-errors trap — all as predict-then-run drills.

| Notebook | What you practice |
|---|---|
| [`01_dataframe_api_drill.py`](01_dataframe_api_drill.py) | Selection spellings, row operations, groupBy/join/union count prediction, parquet round trip with `partitionBy`, the write-mode table, reader options |

**Study:** this section is memorization by fingers — run every cell after
predicting it, then rewrite three of them from memory. 30% ≈ 13 of 45
questions on [TestHub](https://testlogichub.com).
