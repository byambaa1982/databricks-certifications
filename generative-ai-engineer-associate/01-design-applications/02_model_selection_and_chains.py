# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Model selection and chain design
# MAGIC
# MAGIC Two design decisions the exam scenarios force: **which model, accessed
# MAGIC how** (with real trade-off reasoning, not vibes) and **how many LLM
# MAGIC calls the task actually needs** (chain decomposition, worked end to end).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model access on Databricks — three doors, one gateway
# MAGIC
# MAGIC | Door | What it is | Billing | Reach for it when |
# MAGIC |---|---|---|---|
# MAGIC | **Foundation Model APIs — pay-per-token** | hosted open models (Llama family, GTE/BGE embeddings) behind ready endpoints | per token | default: prototypes, spiky/low volume |
# MAGIC | **Provisioned throughput** | dedicated serving capacity for a foundation or **fine-tuned** model | per hour of capacity | production SLAs, high steady volume, fine-tuned weights |
# MAGIC | **External models** | OpenAI/Anthropic/etc. proxied through a serving endpoint | provider's price | need a frontier model — but keep governance |
# MAGIC
# MAGIC All three sit behind **Mosaic AI Model Serving + AI Gateway**, so
# MAGIC logging, rate limits, guardrails, and permissions are uniform — that
# MAGIC uniformity is *why* external models go through an endpoint instead of
# MAGIC being called directly from code (a tested justification).
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "High, steady production volume; per-token bill exploding" →
# MAGIC >   **provisioned throughput** (pay-per-token wins only when idle).
# MAGIC > - "Serve our fine-tuned Llama" → provisioned throughput (pay-per-token
# MAGIC >   endpoints serve *stock* models).
# MAGIC > - "Team calls OpenAI with keys in notebook code" → wrap it as an
# MAGIC >   **external model endpoint**: central key storage, logging, rate
# MAGIC >   limits, swappable later.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choosing the model itself — reason across four axes
# MAGIC
# MAGIC | Axis | The reasoning move |
# MAGIC |---|---|
# MAGIC | **quality vs cost** | try the small model first with good prompting + RAG; escalate size only on measured failure (section 06 supplies the measurement) |
# MAGIC | **latency** | interactive UX budgets ~1–3s: smaller model, shorter prompts, fewer chunks; batch jobs don't care |
# MAGIC | **context window** | must fit: system + examples + retrieved chunks + history + output. Long-doc summarization → long-context model or map-reduce chain |
# MAGIC | **task fit** | embeddings → embedding model (GTE/BGE); code → code-tuned; multilingual → check the model card — a "best overall" model can lose to a fit one |
# MAGIC
# MAGIC > **Exam trap:** the context window covers **input + output together**.
# MAGIC > "8k model, 7k of chunks, 2k answer requested" fails — trim chunks or
# MAGIC > pick a longer-context model.

# COMMAND ----------

# a context-budget check — the arithmetic the exam expects you to do
def fits(context_window, system_toks, examples_toks, chunks, toks_per_chunk,
         history_toks, max_output_toks):
    used = system_toks + examples_toks + chunks * toks_per_chunk + history_toks
    budget_left = context_window - used
    verdict = "OK" if budget_left >= max_output_toks else "OVERFLOW"
    print(f"input {used} + output {max_output_toks} vs window {context_window}: "
          f"{verdict} ({budget_left} left for output)")

fits(8192, 300, 400, chunks=5, toks_per_chunk=1200, history_toks=500, max_output_toks=1000)
fits(8192, 300, 400, chunks=3, toks_per_chunk=1200, history_toks=500, max_output_toks=1000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain decomposition — a worked example
# MAGIC **Requirement:** "Answer customer emails: look up the customer's
# MAGIC contract, answer from policy docs, escalate legal questions to a human,
# MAGIC and log a one-line summary."
# MAGIC
# MAGIC One mega-prompt fails that reliably. The decomposition:
# MAGIC
# MAGIC ```
# MAGIC 1. ROUTER      small model, structured output: {"category": "legal" | "support"}
# MAGIC                └─ "legal" -> human queue, STOP (guardrail by design)
# MAGIC 2. RETRIEVE    contract lookup (a TOOL: UC function by customer id)
# MAGIC                + policy chunks (vector search)
# MAGIC 3. ANSWER      main model, RAG prompt with both contexts
# MAGIC 4. SUMMARIZE   small cheap model: one-line log summary
# MAGIC ```
# MAGIC
# MAGIC Why stages win (each is a tested justification):
# MAGIC
# MAGIC | Property | One prompt | Chain |
# MAGIC |---|---|---|
# MAGIC | right-size models per step | one size fits all (expensive) | small router + small summarizer around one big call |
# MAGIC | testability | one opaque blob | each stage evaluated alone (section 06) |
# MAGIC | control flow | "please escalate legal" (hope) | router **enforces** escalation in code |
# MAGIC | failure isolation | whole thing reruns | retry just the failed stage |
# MAGIC
# MAGIC > **Exam trap:** deterministic logic (routing thresholds, escalation
# MAGIC > rules, formatting) belongs in **code between stages**, not inside
# MAGIC > prompts — "put the business rule in the prompt" is the fragile option.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent vs fixed chain — the last design fork
# MAGIC
# MAGIC | | Fixed chain | Agent |
# MAGIC |---|---|---|
# MAGIC | Control flow | you write it (router, stages) | the **LLM decides** which tool/step next |
# MAGIC | Predictability / cost | high / bounded | lower / variable (loops) |
# MAGIC | Use when | steps are knowable in advance | steps depend on what earlier calls discover |
# MAGIC
# MAGIC Known workflow → chain. Open-ended ("investigate this account and fix
# MAGIC the billing issue") → agent with tools (section 03). **Agent Bricks** is
# MAGIC the managed path for common agent shapes — information extraction,
# MAGIC knowledge assistants, custom LLM tasks — where you declare the task and
# MAGIC quality bar and it handles model choice, tuning, and eval plumbing on
# MAGIC the same primitives (UC, serving, MLflow) you'd use by hand.
