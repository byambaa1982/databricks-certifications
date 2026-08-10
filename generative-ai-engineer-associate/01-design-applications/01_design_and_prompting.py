# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Prompt engineering that survives contact with production
# MAGIC
# MAGIC Design questions give you a business requirement and four prompts (or
# MAGIC four techniques) — you pick the one that satisfies it *reliably*, not
# MAGIC just on the happy path. The prompt-building cells run anywhere; the
# MAGIC model calls need a workspace with Foundation Model APIs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anatomy of a production prompt
# MAGIC Every serious prompt has the same five slots — exam questions often ask
# MAGIC which slot a requirement belongs in:
# MAGIC
# MAGIC | Slot | Carries | Example |
# MAGIC |---|---|---|
# MAGIC | **system message** | role, scope, rules that survive every turn | "You are a support assistant for Acme. Never discuss competitors." |
# MAGIC | **instructions** | the task, output format, constraints | "Classify the ticket. Respond with JSON only." |
# MAGIC | **few-shot examples** | input→output pairs teaching format/edge cases | 2–5 worked tickets |
# MAGIC | **context** | retrieved chunks / tool results (RAG's slot) | "Context:\n..." |
# MAGIC | **user input** | the actual request — ALWAYS last, clearly delimited | "Ticket: …" |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Rules that must hold on **every** turn go in the **system message** —
# MAGIC >   burying them in the first user message loses them as the chat grows.
# MAGIC > - Delimiters (```/XML tags) around user input and context are a
# MAGIC >   **defense**: they mark "this is data, not instructions" (section 05's
# MAGIC >   injection story starts here).
# MAGIC > - Order matters: instructions *before* long context get ignored less;
# MAGIC >   the question goes **after** the context it refers to.

# COMMAND ----------

# a reusable prompt builder — the slots as code (runs anywhere)
def build_messages(system, instructions, examples, context, user_input):
    msgs = [{"role": "system", "content": system}]
    for ex_in, ex_out in examples:
        msgs.append({"role": "user", "content": ex_in})
        msgs.append({"role": "assistant", "content": ex_out})
    msgs.append({"role": "user", "content":
                 f"{instructions}\n\n<context>\n{context}\n</context>\n\n"
                 f"<input>\n{user_input}\n</input>"})
    return msgs


messages = build_messages(
    system="You are a support triage assistant. Output JSON only.",
    instructions='Classify severity as "low", "medium", or "high".',
    examples=[('<input>App crashes on login</input>', '{"severity": "high"}')],
    context="Crashes and data loss are high. Cosmetic issues are low.",
    user_input="The dashboard font looks slightly off on mobile.",
)
for m in messages:
    print(f"[{m['role']:9s}] {m['content'][:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technique selection — requirement → technique
# MAGIC
# MAGIC | Requirement says | Technique | Why not the others |
# MAGIC |---|---|---|
# MAGIC | "output must parse / feeds an app" | **structured output** (`response_format` json_schema) | prompt-begging for JSON still drifts |
# MAGIC | "must match our exact style/format" | **few-shot** examples | instructions describe; examples *demonstrate* |
# MAGIC | "multi-step reasoning keeps failing" | **chain-of-thought** ("think step by step" / reasoned examples) | bigger model costs more and may still skip steps |
# MAGIC | "same behavior across thousands of calls, prompt is huge" | consider **fine-tuning** | prompting has a per-call token cost; FT bakes it in |
# MAGIC | "answers must use our documents" | **RAG** (context slot) | fine-tuning ≠ knowledge injection — it teaches *style*, hallucinates *facts* |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - **RAG vs fine-tuning** is a recurring pair: knowledge that changes →
# MAGIC >   RAG (update the index, not the model); consistent style/format at
# MAGIC >   scale → fine-tuning. "Fine-tune the model on our docs so it knows
# MAGIC >   them" is the classic wrong answer for freshness-sensitive knowledge.
# MAGIC > - Temperature: **0 for extraction/classification** (deterministic-ish),
# MAGIC >   higher only for creative generation. "Raise temperature to fix wrong
# MAGIC >   answers" is a distractor — temperature changes *variance*, not
# MAGIC >   correctness.
# MAGIC > - `max_tokens` truncates the **output** — a truncated JSON that fails to
# MAGIC >   parse is a `max_tokens` symptom, not a model-quality one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Structured output — the exact API shape

# COMMAND ----------

schema = {
    "type": "json_schema",
    "json_schema": {
        "name": "ticket_triage",
        "schema": {
            "type": "object",
            "properties": {
                "severity": {"type": "string", "enum": ["low", "medium", "high"]},
                "product_area": {"type": "string"},
                "needs_human": {"type": "boolean"},
            },
            "required": ["severity", "needs_human"],
        },
        "strict": True,
    },
}

try:
    from databricks.sdk import WorkspaceClient
    client = WorkspaceClient().serving_endpoints.get_open_ai_client()
    resp = client.chat.completions.create(
        model="databricks-meta-llama-3-3-70b-instruct",
        messages=messages,
        response_format=schema,
        temperature=0,
        max_tokens=200,
    )
    print(resp.choices[0].message.content)   # guaranteed to match the schema
except Exception as e:
    print(f"needs Foundation Model APIs in this workspace ({type(e).__name__}) — "
          "the schema shape above is the exam content")

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - `response_format={"type": "json_schema", ...}` **constrains decoding**
# MAGIC >   — output is guaranteed valid against the schema. `{"type":
# MAGIC >   "json_object"}` only guarantees *some* JSON. Prompt-only "reply in
# MAGIC >   JSON" guarantees nothing.
# MAGIC > - The same schema idea appears as **tool/function parameters** (section
# MAGIC >   03) — structured output and function calling are the same trick
# MAGIC >   pointed at different targets.
# MAGIC > - `enum` in the schema is how you force a **closed label set** —
# MAGIC >   the classification-app answer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Iterating on prompts like an engineer
# MAGIC The workflow the exam rewards: change one thing, evaluate against a
# MAGIC fixed question set (section 06), keep prompts **versioned** (MLflow
# MAGIC Prompt Registry, section 04) — never "tweak in prod and eyeball it."
# MAGIC A/B-testing two prompt versions = two logged variants scored on the same
# MAGIC eval set, not two vibes.
