# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Designing GenAI applications
# MAGIC
# MAGIC Design questions are matching exercises: business ask → prompt
# MAGIC technique, task → model choice, requirement → chain component. The
# MAGIC tables here are those matches.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt engineering — the technique ladder
# MAGIC
# MAGIC | Technique | What it is | Reach for it when |
# MAGIC |---|---|---|
# MAGIC | zero-shot | just the instruction | simple, well-known tasks |
# MAGIC | **few-shot** | instruction + worked examples | format/style must be copied; edge cases teachable by example |
# MAGIC | chain-of-thought | "think step by step" / show reasoning examples | multi-step logic, math, tricky judgment |
# MAGIC | **structured output** | demand JSON matching a schema (`response_format` / tool schema) | output feeds code — parsing free-text is the anti-pattern |
# MAGIC | system prompt | persistent role/rules above user turns | tone, scope, refusals that must survive every request |
# MAGIC
# MAGIC ```python
# MAGIC # structured output via the OpenAI-compatible FM API client
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC client = WorkspaceClient().serving_endpoints.get_open_ai_client()
# MAGIC resp = client.chat.completions.create(
# MAGIC     model="databricks-meta-llama-3-3-70b-instruct",
# MAGIC     messages=[{"role": "system", "content": "Extract fields as JSON."},
# MAGIC               {"role": "user", "content": ticket_text}],
# MAGIC     response_format={"type": "json_schema", "json_schema": {...}},
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Model returns chatty prose around the JSON" → structured output /
# MAGIC >   `response_format`, not "ask more nicely".
# MAGIC > - Few-shot examples belong in the **prompt**; fine-tuning is for when
# MAGIC >   prompting has been exhausted (cost/latency/consistency at scale).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model selection — the four axes
# MAGIC
# MAGIC | Axis | The trade |
# MAGIC |---|---|
# MAGIC | quality vs cost | frontier proprietary (best, priciest) vs open (Llama-family — cheaper, tunable, self-hostable) |
# MAGIC | latency | smaller models for interactive UX; big models for offline/batch |
# MAGIC | context window | long documents / many retrieved chunks need long context |
# MAGIC | data control | sensitive data → models served **inside** Databricks (FM API / provisioned throughput), not external APIs |
# MAGIC
# MAGIC Task guidance the exam uses: extraction/classification → small model +
# MAGIC structured output; open-ended synthesis → larger model; embeddings → an
# MAGIC **embedding** model (e.g. `databricks-gte-large-en`), never a chat model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain components — name the pieces
# MAGIC
# MAGIC | Requirement in the question | Component |
# MAGIC |---|---|
# MAGIC | "answers must use OUR documents" | **retriever** (Vector Search) + prompt augmentation → RAG |
# MAGIC | "remember earlier turns" | memory / chat history in the prompt |
# MAGIC | "look up live data / act" | **tools** (function calling), agent loop |
# MAGIC | "decompose: plan, then execute, then summarize" | **multi-stage chain** — separate LLM calls per stage |
# MAGIC | "route simple vs complex queries differently" | router stage in front of two chains |
# MAGIC
# MAGIC Multi-stage beats one mega-prompt when stages need different models,
# MAGIC different context, or independent evaluation — the "why split the chain"
# MAGIC question.
# MAGIC
# MAGIC ## Agent Bricks — the managed shortcut
# MAGIC **Agent Bricks** builds tuned, task-specific agents (extraction, custom
# MAGIC LLM tasks, knowledge assistants) from your data declaratively — you
# MAGIC specify the task and quality bar; it handles model choice, tuning, and
# MAGIC evaluation. The exam-level fact: it's the low-code path on the same
# MAGIC platform primitives (UC, serving, evaluation) as hand-built agents.
