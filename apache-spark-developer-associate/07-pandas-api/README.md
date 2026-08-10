# 07 — Pandas API on Spark (5%)

`pyspark.pandas`: pandas syntax, distributed execution. The tested bits are
the four conversions (`to_spark` / `pandas_api` / `to_pandas` /
`from_pandas`), the driver-OOM trap on `to_pandas()`, order-dependent
operations being expensive without a total order, and the default-index
performance gotcha — plus not confusing it with `pandas_udf`.

| Notebook | What you practice |
|---|---|
| [`01_pandas_api_on_spark.py`](01_pandas_api_on_spark.py) | A live pandas-on-Spark groupby, the three-DataFrames table, the conversion round trip, the traps, the when-to-choose-what table |

**Study:** the conversions and their memory consequences are the whole
section. 5% ≈ 2 of 45 questions on [TestHub](https://testlogichub.com).
That's the last section of the last track — the repo now covers every exam.
