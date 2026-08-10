# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Governance: PII, injection, licensing, audit
# MAGIC
# MAGIC The smallest section, but its facts are absolute ("must never…"), which
# MAGIC makes the questions easy points if you know *where each control must
# MAGIC sit*. The PII scrub runs live — at the only stage where it fully works.

# COMMAND ----------

# MAGIC %md
# MAGIC ## PII — the stage decides everything
# MAGIC Embeddings and chunks are **copies**: PII that reaches the index will
# MAGIC eventually surface in an answer, and vectors can't be "un-embedded".
# MAGIC
# MAGIC | Stage | Control | If you rely on it alone |
# MAGIC |---|---|---|
# MAGIC | **ingestion (the right one)** | scrub/pseudonymize during chunking — masked text is what gets embedded | ✅ PII never enters the system |
# MAGIC | storage | UC grants on chunk table + index; masks on *source* tables | index copies aren't masked by source-table masks |
# MAGIC | serving | AI Gateway PII detection on requests/responses | raw PII already sits in the index |
# MAGIC | logging | inference tables are Delta — governed, but PII logged is PII stored | too late for prevention, still required hygiene |

# COMMAND ----------

# a runnable ingestion-stage scrub — regex for the deterministic layer
import re  # noqa: E402

PII_PATTERNS = [
    (re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.]+\b"), "<EMAIL>"),
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "<SSN>"),
    (re.compile(r"\b(?:\+?1[-. ]?)?\(?\d{3}\)?[-. ]?\d{3}[-. ]?\d{4}\b"), "<PHONE>"),
]

def scrub(text: str) -> str:
    for pattern, token in PII_PATTERNS:
        text = pattern.sub(token, text)
    return text

raw = ("Customer Bat (bat@example.com, 555-867-5309, SSN 123-45-6789) "
       "reported the login bug on the checkout page.")
print(scrub(raw))

# COMMAND ----------

# MAGIC %md
# MAGIC Regex catches formatted PII; **names and free-form identifiers need an
# MAGIC NER/LLM pass** — `ai_query` with an extraction prompt (or a PII model)
# MAGIC over the corpus is the scalable second layer. Pseudonymize (`<CUST_017>`)
# MAGIC rather than delete when downstream joins still need identity.
# MAGIC
# MAGIC > **Exam trap:** "add PII masking at query time" for a corpus already
# MAGIC > embedded raw = wrong stage. The remediation is **re-ingest scrubbed +
# MAGIC > rebuild the index** (and vacuum the old chunks) — expensive, which is
# MAGIC > why the design answer is always ingestion-stage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Malicious input — the attack/defense table
# MAGIC
# MAGIC | Attack | Vector | Defense |
# MAGIC |---|---|---|
# MAGIC | direct prompt injection | "ignore your instructions…" in user input | input classifier + hardened system prompt + Gateway safety filter |
# MAGIC | **indirect injection** | instructions hidden in **retrieved docs** / tool outputs | delimit context as data; sanitize corpus at ingestion; output-check before returning |
# MAGIC | prompt extraction | "print your system prompt" | refusal rules + output filters; assume prompts are *not* secrets |
# MAGIC | tool abuse / exfiltration | agent talked into misusing a tool | **least-privilege UC grants on tools**; no admin agents; cap loops |
# MAGIC | DoS / cost abuse | flooding the endpoint | AI Gateway **rate limits** per user/endpoint |
# MAGIC
# MAGIC Layering is the exam's answer pattern — each row's defense fails
# MAGIC somewhere the next layer catches.

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Gateway — governance's enforcement point
# MAGIC Configured **on the serving endpoint**, applies to every caller:
# MAGIC
# MAGIC | Feature | Governs |
# MAGIC |---|---|
# MAGIC | guardrails: safety filters, **PII detection**, topic/keyword denylists | content in and out |
# MAGIC | **rate limits** (per user / per endpoint) | abuse and cost |
# MAGIC | usage tracking + **payload logging** (inference tables) | audit and monitoring |
# MAGIC | fallbacks / traffic routing across endpoints | resilience |
# MAGIC
# MAGIC The recurring justification: app-level checks can be forgotten by one
# MAGIC team; **Gateway policies are central and unskippable**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Licensing and audit — the checklist
# MAGIC - **Model licenses**: open weights ship with licenses (Llama's has use
# MAGIC   conditions) — commercial use must be permitted. "Open source = no
# MAGIC   restrictions" is a planted wrong answer.
# MAGIC - **Corpus licenses**: scraped/third-party content carries terms the app
# MAGIC   inherits; "we can technically ingest it" ≠ "we may".
# MAGIC - **Data residency**: sensitive data staying in-platform (FM APIs /
# MAGIC   provisioned throughput) vs leaving to external APIs is a compliance
# MAGIC   choice — the governed external-model endpoint at least keeps logging
# MAGIC   and control.
# MAGIC - **Auditability**: UC lineage (source → chunks → index → app) +
# MAGIC   inference tables answer "who asked what, and what did we answer" —
# MAGIC   regulated deployments must be able to.
