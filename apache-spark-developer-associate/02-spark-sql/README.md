# 02 — Using Spark SQL (20%)

One engine, two syntaxes: temp vs global-temp vs catalog views (with the
`global_temp.` prefix trap), the same query proven identical both ways, the
function families the exam samples from (with the NULL and
collect_list/collect_set gotchas), and `spark.udf.register` as the bridge
that makes Python UDFs SQL-callable.

| Notebook | What you practice |
|---|---|
| [`01_spark_sql.py`](01_spark_sql.py) | View scopes, SQL ↔ DataFrame equivalence check run live, the function-family table, UDF registration with return types |

**Study:** when two snippets differ only in syntax, they're usually
equivalent — the planted differences are NULL handling, UNION vs UNION ALL,
and unqualified global temp views. 20% ≈ 9 of 45 questions on
[TestHub](https://testlogichub.com).
