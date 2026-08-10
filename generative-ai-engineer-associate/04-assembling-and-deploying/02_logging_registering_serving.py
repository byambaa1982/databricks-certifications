# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Logging, registering, and serving the application
# MAGIC
# MAGIC The path from "chain in a notebook" to "endpoint stakeholders use":
# MAGIC models-from-code → UC registration → `agents.deploy` — plus the
# MAGIC supporting cast (prompt registry, `ai_query` for batch, MCP, CI/CD).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Models-from-code — the current logging doctrine
# MAGIC The chain lives in a **`.py` file** ending with `set_model`; logging
# MAGIC references the *file*, not a pickled object:
# MAGIC
# MAGIC ```python
# MAGIC # ---- chain.py (the model AS code) ----
# MAGIC import mlflow
# MAGIC from databricks_langchain import ChatDatabricks, DatabricksVectorSearch
# MAGIC # ... build `chain` exactly as in section 03 ...
# MAGIC mlflow.models.set_model(chain)
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC # ---- driver notebook ----
# MAGIC from mlflow.models.resources import (
# MAGIC     DatabricksServingEndpoint, DatabricksVectorSearchIndex,
# MAGIC )
# MAGIC with mlflow.start_run():
# MAGIC     info = mlflow.langchain.log_model(          # or mlflow.pyfunc.log_model
# MAGIC         lc_model="chain.py",                    # <- file path, not an object
# MAGIC         artifact_path="chain",
# MAGIC         registered_model_name="dev_catalog.genai_assoc.rag_chain",
# MAGIC         resources=[
# MAGIC             DatabricksServingEndpoint(endpoint_name="databricks-meta-llama-3-3-70b-instruct"),
# MAGIC             DatabricksVectorSearchIndex(index_name="dev_catalog.genai_assoc.doc_chunks_index"),
# MAGIC         ],
# MAGIC         input_example={"messages": [{"role": "user", "content": "How do I configure auth?"}]},
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Why code-not-pickle: reviewable diffs, no serialization fragility,
# MAGIC >   the file *is* the deployable — the stated best practice.
# MAGIC > - **`resources` is the auth answer**: it declares which endpoints/
# MAGIC >   indexes the chain calls so Model Serving can mint credentials for
# MAGIC >   them on the served model's behalf. Missing resources = works in the
# MAGIC >   notebook (your identity), 403s on the endpoint.
# MAGIC > - `input_example` → a logged **signature**; serving validates requests
# MAGIC >   against it.
# MAGIC > - Custom agents implement MLflow's chat interfaces (**`ChatAgent`** /
# MAGIC >   pyfunc with the chat schema) so any framework serves uniformly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register → alias → deploy
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC mlflow.set_registry_uri("databricks-uc")
# MAGIC client = mlflow.MlflowClient()
# MAGIC client.set_registered_model_alias("dev_catalog.genai_assoc.rag_chain",
# MAGIC                                   "champion", info.registered_model_version)
# MAGIC
# MAGIC from databricks import agents
# MAGIC deployment = agents.deploy("dev_catalog.genai_assoc.rag_chain",
# MAGIC                            model_version=info.registered_model_version,
# MAGIC                            scale_to_zero=True)
# MAGIC ```
# MAGIC
# MAGIC `agents.deploy` = a Model Serving endpoint **plus** the agent extras:
# MAGIC
# MAGIC | Extra | What it gives you |
# MAGIC |---|---|
# MAGIC | **Review App** | a ready chat UI where SMEs try the app and leave 👍/👎 + comments — "stakeholders test it without us building a frontend" |
# MAGIC | inference tables + traces | every request/response/trace logged to Delta (section 06 consumes this) |
# MAGIC | feedback capture | Review App feedback lands as data for eval sets |
# MAGIC
# MAGIC The ML rollout machinery applies unchanged: aliases for promotion,
# MAGIC endpoint traffic splitting for canary, per-env catalogs + bundles for
# MAGIC CI/CD ([ML Pro 02](../../machine-learning-professional/02-mlops/)).
# MAGIC Custom UIs beyond the Review App → **Databricks Apps**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt Registry — version prompts like models
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC mlflow.genai.register_prompt(
# MAGIC     name="dev_catalog.genai_assoc.rag_system_prompt",
# MAGIC     template="Answer ONLY from context... {{context}} ... {{question}}",
# MAGIC     commit_message="tighten grounding language",
# MAGIC )
# MAGIC prompt = mlflow.genai.load_prompt(
# MAGIC     "prompts:/dev_catalog.genai_assoc.rag_system_prompt/2")
# MAGIC ```
# MAGIC
# MAGIC Prompts become UC-governed, versioned assets with aliases — change the
# MAGIC prompt, evaluate, promote, **without redeploying the chain**. "We need
# MAGIC to know which prompt version produced last week's answers" → this.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch inference — `ai_query` (don't build a loop)
# MAGIC
# MAGIC ```sql
# MAGIC SELECT ticket_id,
# MAGIC        ai_query('databricks-meta-llama-3-3-70b-instruct',
# MAGIC                 CONCAT('Classify severity (low/medium/high): ', body))
# MAGIC          AS severity
# MAGIC FROM support_tickets;
# MAGIC ```
# MAGIC
# MAGIC `ai_query` runs an LLM over a **table**, parallelized by the engine —
# MAGIC the answer for "summarize/classify a million rows", not a Python loop
# MAGIC over an endpoint. (Task-specific cousins: `ai_classify`,
# MAGIC `ai_extract`, `ai_summarize`, `ai_translate`.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MCP — tools over a standard protocol
# MAGIC **Model Context Protocol** standardizes agent ↔ tool wiring.
# MAGIC Databricks ships **managed MCP servers** (Vector Search, UC functions,
# MAGIC Genie spaces) and custom MCP servers can run on Databricks Apps —
# MAGIC so any MCP-speaking agent, including external ones, consumes governed
# MAGIC Databricks tools with no bespoke integration. Exam-level: MCP = the
# MAGIC open plug between agents and governed tools; the managed servers are
# MAGIC the fastest path.
