# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Building the application: RAG assembly, tools, guardrails
# MAGIC
# MAGIC The heaviest section: putting retriever, prompt, model, and tools
# MAGIC together — and knowing which knob fixes which quality problem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The RAG prompt assembly — augmentation spelled out
# MAGIC
# MAGIC ```python
# MAGIC def build_prompt(question: str, chunks: list[str]) -> list[dict]:
# MAGIC     context = "\n\n---\n\n".join(chunks)
# MAGIC     return [
# MAGIC         {"role": "system", "content":
# MAGIC          "Answer ONLY from the provided context. "
# MAGIC          "If the context is insufficient, say you don't know. "
# MAGIC          "Cite the source of each claim."},
# MAGIC         {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {question}"},
# MAGIC     ]
# MAGIC ```
# MAGIC
# MAGIC The three instructions in that system prompt — ground only in context,
# MAGIC admit ignorance, cite — are each the answer to a named failure
# MAGIC (hallucination, fabricated confidence, unverifiable claims).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Symptom → knob (the section's core table)
# MAGIC
# MAGIC | Quality problem | First knob |
# MAGIC |---|---|
# MAGIC | answers ignore the docs / hallucinate | grounding instruction + check retrieval actually returned relevant chunks |
# MAGIC | right doc exists but isn't retrieved | chunking/embedding model (section 02), more k, hybrid search |
# MAGIC | retrieved but ranked below junk | re-ranker |
# MAGIC | answers correct but wrong TONE/format | system prompt / few-shot examples |
# MAGIC | needs live data or actions | tools (function calling) — not a bigger model |
# MAGIC | good on short questions, lost on long chats | conversation memory management / summarization |
# MAGIC | latency too high | smaller model, fewer/shorter chunks, cache |
# MAGIC
# MAGIC > **Exam trap:** "increase the model size" is almost never the first fix —
# MAGIC > retrieval and prompting knobs come first and cost nothing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choosing models inside the app
# MAGIC - **Foundation Model APIs (pay-per-token)** — serverless access to hosted
# MAGIC   models (Llama family, embeddings like GTE) — the default for
# MAGIC   experiments and moderate load.
# MAGIC - **Provisioned throughput** — dedicated capacity for production
# MAGIC   latency/volume guarantees (and fine-tuned model serving).
# MAGIC - **External models** (OpenAI, Anthropic, …) — reached through the same
# MAGIC   serving/AI Gateway layer so governance and logging stay uniform.
# MAGIC - The **embedding model must match the index** — query and documents
# MAGIC   embedded by the *same* model, or retrieval silently breaks (a top trap).
# MAGIC
# MAGIC ## Frameworks
# MAGIC LangChain / LlamaIndex give you chain/retriever/agent plumbing;
# MAGIC plain-Python with the OpenAI-compatible client is fine for simple chains.
# MAGIC The exam cares that whatever you build is **loggable as an MLflow model**
# MAGIC (flavors exist for LangChain and PyFunc — section 04), not which
# MAGIC framework you prefer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tools and agents — function calling
# MAGIC
# MAGIC ```python
# MAGIC tools = [{
# MAGIC   "type": "function",
# MAGIC   "function": {
# MAGIC     "name": "lookup_order",
# MAGIC     "description": "Fetch an order's status by id",
# MAGIC     "parameters": {"type": "object",
# MAGIC                    "properties": {"order_id": {"type": "integer"}},
# MAGIC                    "required": ["order_id"]},
# MAGIC   }}]
# MAGIC resp = client.chat.completions.create(model=..., messages=msgs, tools=tools)
# MAGIC # if resp asks for a tool call: run it, append the result as a 'tool' message, loop
# MAGIC ```
# MAGIC
# MAGIC The agent loop = LLM decides → tool runs → result returns → LLM continues,
# MAGIC until it answers. **UC functions can be registered as tools** — governed,
# MAGIC shared tool definitions.
# MAGIC
# MAGIC **Multi-agent**: a supervisor/router agent delegating to specialist agents
# MAGIC (each with its own tools/context) — choose it when one agent's tool list
# MAGIC and instructions get too broad to stay reliable; the Mosaic AI Agent
# MAGIC Framework and MCP (section 04) are the platform's plumbing for it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardrails — layered, not optional
# MAGIC
# MAGIC | Layer | Mechanism |
# MAGIC |---|---|
# MAGIC | input | filter/deny harmful or off-topic requests (safety filter / Llama Guard-style classifier) |
# MAGIC | prompt | system-prompt scope + refusal rules |
# MAGIC | output | groundedness/safety check before returning; regex/PII scan |
# MAGIC | platform | **AI Gateway** on the endpoint: safety filters, PII detection, rate limits, logging (sections 05–06) |
# MAGIC
# MAGIC > **Exam trap:** guardrails in the *prompt alone* are bypassable (prompt
# MAGIC > injection) — platform-level enforcement (AI Gateway) is the robust
# MAGIC > answer for "must never leak PII / must block abuse".
