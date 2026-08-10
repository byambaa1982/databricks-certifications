# Databricks Associate Developer for Apache Spark

Study notes organized by the official exam sections (the current
**Spark 3.5-era guide**, 7 sections), one folder per section. The weight
next to each section is its share of the exam. 45 scored questions, 90
minutes, Python variant.

| # | Section | Weight |
|---|---------|--------|
| 01 | [Apache Spark Architecture and Components](01-architecture/) | 20% |
| 02 | [Using Spark SQL](02-spark-sql/) | 20% |
| 03 | [Developing DataFrame/DataSet API Applications](03-dataframe-api/) | 30% |
| 04 | [Troubleshooting and Tuning](04-troubleshooting-and-tuning/) | 10% |
| 05 | [Structured Streaming](05-structured-streaming/) | 10% |
| 06 | [Using Spark Connect](06-spark-connect/) | 5% |
| 07 | [Pandas API on Spark](07-pandas-api/) | 5% |

This is the most code-literal Databricks exam: questions show a snippet and
ask what it returns, or show four snippets and ask which works. **Predict
before running** is the entire study method — every notebook here is
runnable on any Spark cluster (no Databricks-specific services needed).

## Overlap warning (in your favor)

Sections 01/04/05 are the engineering tracks' Spark internals wearing an
OSS hat — the notebooks cross-link instead of repeating. What's *new* here:
pure-API precision (section 03), Spark SQL ↔ DataFrame equivalence (02),
Spark Connect (06), and Pandas API on Spark (07).

## Practice

- 🎯 [Timed practice exams on TestHub](https://testlogichub.com)
- 📖 [Official exam page](https://www.databricks.com/learn/certification/apache-spark-developer-associate)
