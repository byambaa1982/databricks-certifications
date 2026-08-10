# Databricks Generative AI Engineer Associate

Study notes organized by the official exam sections (the **March 2026
guide**), one folder per section. The weight next to each section is its
share of the exam. 45 scored questions, 90 minutes.

| # | Section | Weight |
|---|---------|--------|
| 01 | [Design Applications](01-design-applications/) | 14% |
| 02 | [Data Preparation](02-data-preparation/) | 14% |
| 03 | [Application Development](03-application-development/) | 30% |
| 04 | [Assembling and Deploying Applications](04-assembling-and-deploying/) | 22% |
| 05 | [Governance](05-governance/) | 8% |
| 06 | [Evaluation and Monitoring](06-evaluation-and-monitoring/) | 12% |

Sections 03 + 04 are **52% of the exam** — building the RAG/agent
application and shipping it (Vector Search, chain logging, serving, MCP).
Most content here is reference-first: LLM apps need endpoints to run, so the
notebooks teach the **API shapes and decision tables** the exam quotes, with
runnable cells where a workspace with Foundation Model APIs makes them so.

## The mental model the whole exam hangs on

```
raw docs -> parse -> chunk -> embed -> VECTOR SEARCH INDEX      (sections 02, 04)
user query -> retrieve -> augment prompt -> LLM -> guardrails    (sections 01, 03)
        logged as a chain/agent -> UC -> Model Serving endpoint  (section 04)
        traced, evaluated (LLM-as-judge), monitored, governed    (sections 05, 06)
```

## Practice

- 🎯 [Timed practice exams on TestHub](https://testlogichub.com)
- 📖 [Official exam page](https://www.databricks.com/learn/certification/genai-engineer-associate)
