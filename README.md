# 🎓 databricks-certifications

Study notes, practice material, and exam-prep resources for the **Databricks certification** tracks — organized one folder per exam.

Sister repo to [**lakehouse-starter**](https://github.com/byambaa1982/lakehouse-starter), a deployable Asset Bundle where the concepts from these exams (medallion architecture, DLT/Lakeflow, CDC → SCD Type 2, Unity Catalog, bundles CI/CD) run for real. Read here, practice there.

## Certification tracks

| Folder | Certification | Status |
|---|---|---|
| [`data-engineer-associate/`](data-engineer-associate/) | [Data Engineer Associate](https://www.databricks.com/learn/certification/data-engineer-associate) | ✅ Earned — [credential](https://credentials.databricks.com/056070f5-1581-4e24-8996-82b1f1e23b20#acc.3C6pjHkb) |
| [`data-engineer-professional/`](data-engineer-professional/) | [Data Engineer Professional](https://www.databricks.com/learn/certification/data-engineer-professional) | ✅ Earned — [credential](https://credentials.databricks.com/d9114e66-f556-4da1-a383-fc65cb512027#acc.8lQgipFn) |
| [`data-analyst-associate/`](data-analyst-associate/) | [Data Analyst Associate](https://www.databricks.com/learn/certification/data-analyst-associate) | 📋 Planned |
| [`machine-learning-associate/`](machine-learning-associate/) | [Machine Learning Associate](https://www.databricks.com/learn/certification/machine-learning-associate) | 📋 Planned |
| [`machine-learning-professional/`](machine-learning-professional/) | [Machine Learning Professional](https://www.databricks.com/learn/certification/machine-learning-professional) | 📋 Planned |
| [`generative-ai-engineer-associate/`](generative-ai-engineer-associate/) | [Generative AI Engineer Associate](https://www.databricks.com/learn/certification/genai-engineer-associate) | 📋 Planned |
| [`apache-spark-developer-associate/`](apache-spark-developer-associate/) | [Associate Developer for Apache Spark](https://www.databricks.com/learn/certification/apache-spark-developer-associate) | 📋 Planned |
| [`resources/`](resources/) | Cross-exam material: cheat sheets, links, exam-day notes | — |

Each certification folder will follow the same shape as content lands:

```
<certification>/
├── notes/          # topic-by-topic study notes mapped to the official exam guide
├── practice/       # worked questions with explanations
└── labs/           # hands-on exercises (most run against lakehouse-starter)
```

## How I study

1. **Read the official exam guide first** — every exam page above links one. The notes folders mirror its section headings, so coverage gaps are visible.
2. **Do it, don't just read it** — each concept gets a hands-on lab, usually against a [lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter) dev deployment (serverless — a run costs cents).
3. **Drill with timed practice tests** — I built [TestHub](https://testlogichub.com) for exactly this: timed practice exams with explanations, starting with Data Engineer Associate.

## Related

- 🏗️ [lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter) — production-grade Asset Bundle template (the lab environment for this repo)
- 🎯 [TestHub](https://testlogichub.com) — timed practice exams with explanations
- 📚 [Official Databricks certification overview](https://www.databricks.com/learn/certification)

## About

Maintained by **[Byamba Enkhbat](https://github.com/byambaa1982)** — Databricks Certified Data Engineer (Associate · Professional), building Lakehouse pipelines in production since 2018.

Need help with Databricks, data pipelines, or AI automation? **[Work with me →](https://datalogichub.com/work-with-me)**

---

⭐ If these notes help you pass, a star helps others find them.
