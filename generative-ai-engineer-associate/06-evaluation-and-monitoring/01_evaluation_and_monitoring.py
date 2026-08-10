# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Evaluating and monitoring GenAI apps
# MAGIC
# MAGIC You can't unit-test free text, so this section replaces assertions with
# MAGIC **scorers** (including LLM-as-judge), **traces** for debugging, and
# MAGIC **inference logs** for production watching — plus the cost levers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow evaluation — the shape
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC from mlflow.genai.scorers import Correctness, RelevanceToQuery, Safety, Guidelines
# MAGIC
# MAGIC eval_data = [  # questions + (optionally) expected facts
# MAGIC     {"inputs": {"question": "How do I configure auth?"},
# MAGIC      "expectations": {"expected_facts": ["auth is configured in setup"]}},
# MAGIC ]
# MAGIC
# MAGIC results = mlflow.genai.evaluate(
# MAGIC     data=eval_data,
# MAGIC     predict_fn=my_app,                       # the chain/agent under test
# MAGIC     scorers=[
# MAGIC         Correctness(),                       # vs expected facts
# MAGIC         RelevanceToQuery(),                  # answers the actual question?
# MAGIC         Safety(),
# MAGIC         Guidelines(name="tone", guidelines="Professional, no speculation"),
# MAGIC     ],
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC | Scorer family | Judges | Needs ground truth? |
# MAGIC |---|---|---|
# MAGIC | Correctness | matches expected facts | ✅ |
# MAGIC | RelevanceToQuery | on-topic for the question | ❌ |
# MAGIC | **groundedness / faithfulness** | claims supported by retrieved context | ❌ (needs the context) |
# MAGIC | Safety | harmful content | ❌ |
# MAGIC | Guidelines / custom | your rubric, judged by an LLM | ❌ |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - **LLM-as-judge** is how subjective qualities scale; validate the judge
# MAGIC >   against a small human-labeled set before trusting it.
# MAGIC > - **Groundedness** is the anti-hallucination metric for RAG — it checks
# MAGIC >   the answer against the *retrieved chunks*, no ground truth needed.
# MAGIC > - Custom scorer = a function (or `Guidelines` rubric) returning a score
# MAGIC >   — the named path for business-specific criteria.
# MAGIC > - Classic metrics (BLEU/ROUGE/exact match) only fit when reference
# MAGIC >   answers exist — offering them for open-ended chat is the distractor.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tracing — see inside the chain
# MAGIC **MLflow Tracing** records every span (retrieval, each LLM call, tools)
# MAGIC with inputs/outputs and latency: `mlflow.langchain.autolog()` (or
# MAGIC `@mlflow.trace` on your functions) and the trace UI shows where a bad
# MAGIC answer came from — retrieval returned junk vs the LLM ignored good
# MAGIC context. That distinction (which stage failed) is the debugging question.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production: inference tables + monitoring + people
# MAGIC - **Inference tables** log endpoint requests/responses (and traces) to
# MAGIC   Delta — query with SQL, attach Lakehouse-Monitoring-style checks,
# MAGIC   re-run scorers over logged traffic for continuous quality.
# MAGIC - **SME feedback**: the Review App + labeling sessions turn expert
# MAGIC   judgments into eval datasets — the loop that keeps the judge honest.
# MAGIC - Endpoint health (latency, errors, tokens/sec) is the serving-metrics
# MAGIC   layer — separate from answer quality.
# MAGIC
# MAGIC ## Cost control
# MAGIC
# MAGIC | Lever | Where |
# MAGIC |---|---|
# MAGIC | rate limits per user/endpoint | **AI Gateway** |
# MAGIC | usage tracking (tokens, requests, by whom) | Gateway usage logs / inference tables + `system.serving` billing |
# MAGIC | smaller model / shorter prompts / fewer chunks | the app itself (biggest lever) |
# MAGIC | scale-to-zero, provisioned-throughput sizing | endpoint config |
# MAGIC
# MAGIC The whole loop in one line: **trace it, score it, log it, watch it, feed
# MAGIC expert labels back** — every exam scenario in this section is one stop on
# MAGIC that loop.
