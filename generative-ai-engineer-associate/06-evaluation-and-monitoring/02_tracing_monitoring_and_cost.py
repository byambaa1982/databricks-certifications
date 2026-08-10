# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Tracing, production monitoring, and cost
# MAGIC
# MAGIC Evaluation (notebook 01) is pre-ship; this is post-ship: **traces** to
# MAGIC see inside a bad answer, **inference tables** as the production record,
# MAGIC monitors and feedback loops on top, and the cost levers with real token
# MAGIC arithmetic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tracing — spans for chains
# MAGIC MLflow Tracing records every step (retrieval, each LLM call, each tool)
# MAGIC as a **span** with inputs, outputs, and latency:
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC mlflow.langchain.autolog()          # frameworks: one line, everything traced
# MAGIC
# MAGIC @mlflow.trace                        # custom code: decorate the steps
# MAGIC def retrieve(query): ...
# MAGIC
# MAGIC @mlflow.trace
# MAGIC def rag_answer(query): ...
# MAGIC ```
# MAGIC
# MAGIC The debugging question a trace answers in one glance — *which stage
# MAGIC failed*:
# MAGIC
# MAGIC | Trace shows | Diagnosis |
# MAGIC |---|---|
# MAGIC | retrieval span returned junk chunks | retrieval problem (section 02's knobs) |
# MAGIC | good chunks in, answer ignores them | generation problem (grounding prompt, model) |
# MAGIC | tool span errored, model improvised | tool/error-handling problem (section 03) |
# MAGIC | one span holds 90% of latency | *that's* the latency knob, stop guessing |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference tables — production's memory
# MAGIC Enabled on the serving endpoint (AI Gateway payload logging /
# MAGIC `agents.deploy` default): every request, response, and trace lands in a
# MAGIC **Delta table** — queryable with SQL like anything else.
# MAGIC
# MAGIC What teams run on it:
# MAGIC - **continuous quality**: re-run scorers (groundedness, guidelines) over
# MAGIC   sampled logged traffic on a schedule — production evaluation without
# MAGIC   waiting for complaints;
# MAGIC - drift-style watching: request volume, topic mix, refusal rate,
# MAGIC   latency percentiles per model version;
# MAGIC - **eval-set mining**: thumbs-down + weird questions become next week's
# MAGIC   eval rows (the loop from notebook 01 closes here);
# MAGIC - audit: who asked what, answered from which prompt/model version.
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Monitor answer quality in production" → scorers over **inference
# MAGIC >   tables** on a schedule — not "check the dashboard" and not manual
# MAGIC >   review of everything.
# MAGIC > - Endpoint health (latency, error rate, tokens/sec) is the **serving
# MAGIC >   metrics** layer — different from answer quality; symptoms route to
# MAGIC >   different layers.
# MAGIC > - **SME feedback** scales through the Review App + labeling sessions —
# MAGIC >   experts label a *sample*, the validated judge covers the rest.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost — do the arithmetic once, remember the shape

# COMMAND ----------

def monthly_cost(queries_per_day, in_tokens, out_tokens,
                 price_in_per_m, price_out_per_m):
    daily = queries_per_day * (in_tokens * price_in_per_m
                               + out_tokens * price_out_per_m) / 1_000_000
    return 30 * daily

# illustrative prices per 1M tokens — plug in current ones
base = monthly_cost(10_000, in_tokens=3_000, out_tokens=400,
                    price_in_per_m=1.0, price_out_per_m=3.0)
fewer_chunks = monthly_cost(10_000, in_tokens=1_600, out_tokens=400,
                            price_in_per_m=1.0, price_out_per_m=3.0)
small_model = monthly_cost(10_000, in_tokens=3_000, out_tokens=400,
                           price_in_per_m=0.2, price_out_per_m=0.6)

print(f"baseline:              ${base:,.0f}/mo")
print(f"5 chunks -> 2 chunks:  ${fewer_chunks:,.0f}/mo  (input tokens dominate RAG)")
print(f"smaller model:         ${small_model:,.0f}/mo")

# COMMAND ----------

# MAGIC %md
# MAGIC The lesson the arithmetic teaches: **RAG cost is mostly input tokens**
# MAGIC (the chunks), so retrieval precision (fewer, better chunks — section
# MAGIC 02's re-ranker) is a cost lever, not just a quality one.
# MAGIC
# MAGIC | Lever | Where | Note |
# MAGIC |---|---|---|
# MAGIC | rate limits per user/endpoint | AI Gateway | stops abuse-driven spend |
# MAGIC | usage tracking by user/app | Gateway usage logs + `system.serving.*` | the chargeback answer |
# MAGIC | fewer/shorter chunks, shorter prompts | the app | biggest lever, free quality-check via eval set |
# MAGIC | smaller model where eval allows | the app | measure, then downsize |
# MAGIC | pay-per-token ↔ provisioned throughput | endpoint | crossover at steady volume (section 01) |
# MAGIC | scale-to-zero | endpoint | idle cost vs cold start |
# MAGIC
# MAGIC The full loop, one line: **trace it → score it → log it → watch it →
# MAGIC feed labels back → re-evaluate before promoting.** Every scenario in
# MAGIC this section is one arrow of that loop.
