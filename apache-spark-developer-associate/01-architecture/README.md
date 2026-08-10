# 01 — Apache Spark Architecture and Components (20%)

Naming parts and predicting execution: driver/executors/cluster manager,
job → stage → task with tasks-equal-partitions, lazy evaluation proved live,
narrow vs wide read off `explain()`, repartition vs coalesce, and cache
semantics (lazy, `MEMORY_AND_DISK` default).

| Notebook | What you practice |
|---|---|
| [`01_spark_architecture.py`](01_spark_architecture.py) | The parts diagram + glossary, transformations-vs-actions table (and the printSchema/explain trap), shuffle boundaries live, partition and caching knobs |

**Study:** two facts settle most questions — tasks per stage = partitions,
and nothing executes until an action. 20% ≈ 9 of 45 questions on
[TestHub](https://testlogichub.com).
