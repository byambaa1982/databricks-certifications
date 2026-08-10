# 06 — Evaluation and Monitoring (12%)

Free text can't be unit-tested, so: scorers (groundedness = the
anti-hallucination RAG metric; LLM-as-judge for everything subjective, with
custom `Guidelines`), MLflow Tracing to see which stage produced a bad
answer, inference tables for production quality, SME feedback loops, and
the cost levers (Gateway rate limits first).

| Notebook | What you practice |
|---|---|
| [`01_evaluation_and_monitoring.py`](01_evaluation_and_monitoring.py) | The `mlflow.genai.evaluate` shape, the scorer table (what needs ground truth), tracing and the which-stage-failed question, inference tables + Review App feedback, the cost-lever table |

**Study:** match metric to available evidence — expected facts → correctness;
retrieved context → groundedness; neither → LLM-judged guidelines. And keep
quality metrics separate from endpoint health. 12% ≈ 5 of 45 questions on
[TestHub](https://testlogichub.com). That's the full track — timed mock next.
