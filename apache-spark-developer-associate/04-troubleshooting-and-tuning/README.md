# 04 — Troubleshooting and Tuning (10%)

The engineering tracks' tuning content with OSS nouns: the named configs
(`shuffle.partitions`, `autoBroadcastJoinThreshold`, `adaptive.*`),
broadcast vs sort-merge read off a real plan, the symptom → cause table
(including "Task not serializable"), and Spark UI navigation.

| Notebook | What you practice |
|---|---|
| [`01_tuning_essentials.py`](01_tuning_essentials.py) | The config table probed live, a forced broadcast join explained, the OSS symptom table, UI page-by-page |

**Study:** know the two OOMs apart (driver = collect/broadcast; executor =
skew/shuffle) and what AQE fixes without you. 10% ≈ 4–5 of 45 questions on
[TestHub](https://testlogichub.com).
