# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Governance: PII, injection, and licensing
# MAGIC
# MAGIC The smallest section, three obligations: keep sensitive data out of the
# MAGIC system (or masked inside it), survive malicious input, and respect
# MAGIC licenses. Each maps to specific platform features.

# COMMAND ----------

# MAGIC %md
# MAGIC ## PII — mask BEFORE it reaches the index
# MAGIC The retrieval corpus is a copy of your documents — anything sensitive in
# MAGIC the chunks will eventually surface in an answer. Defense in depth:
# MAGIC
# MAGIC | Stage | Control |
# MAGIC |---|---|
# MAGIC | ingestion (best) | scrub/pseudonymize during chunking — regex + `ai_query`-based PII detection; masked text is what gets embedded |
# MAGIC | storage | UC grants on the chunk table + index; column masks on source tables (the engineering tracks' machinery) |
# MAGIC | serving | **AI Gateway PII detection** on requests/responses |
# MAGIC | logging | inference tables inherit UC governance — PII in logs is still PII |
# MAGIC
# MAGIC > **Exam trap:** masking at *query time only* is too late if raw PII was
# MAGIC > embedded — vectors and chunks retain it. The right stage is
# MAGIC > **before embedding**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Malicious input — prompt injection and friends
# MAGIC
# MAGIC | Attack | Defense |
# MAGIC |---|---|
# MAGIC | prompt injection ("ignore your instructions…") | system-prompt hardening + **input classification/guardrails** (Llama Guard-style, AI Gateway safety filters) |
# MAGIC | indirect injection (instructions hidden in *retrieved docs*) | treat retrieved text as **data, not instructions** in the prompt; sanitize corpus at ingestion |
# MAGIC | data exfiltration via tools | least-privilege tools — agents run with governed UC permissions, not admin |
# MAGIC | abuse / DoS | AI Gateway **rate limits** per user/endpoint |
# MAGIC
# MAGIC Layered is the answer pattern: prompt rules alone are bypassable;
# MAGIC platform-level (Gateway) + scoped permissions is what the exam rewards.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Licensing and compliance — the checklist
# MAGIC - **Model licenses**: open weights ≠ do-anything — e.g. Llama's license
# MAGIC   has use conditions; commercial use of a model must be permitted by its
# MAGIC   license.
# MAGIC - **Data licenses**: the corpus you embed must be licensed for that use —
# MAGIC   scraped or third-party content can carry restrictions the app inherits.
# MAGIC - **Data residency/processing**: sensitive data staying in-platform (FM
# MAGIC   API / provisioned throughput inside Databricks) vs leaving to external
# MAGIC   APIs is a compliance decision, not just a latency one.
# MAGIC - **Auditability**: UC lineage + inference tables give the who-asked-what
# MAGIC   trail; regulated deployments must be able to answer it.
# MAGIC
# MAGIC > **Exam trap:** "the model is open source, so no restrictions" — open
# MAGIC > *weights* still ship with licenses; checking them is a listed exam
# MAGIC > objective.
