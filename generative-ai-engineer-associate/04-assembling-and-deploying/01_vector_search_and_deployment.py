# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Vector Search, chain logging, and deployment
# MAGIC
# MAGIC The shipping section: index the chunks, log the chain as a model,
# MAGIC register it in UC, serve it — plus MCP and prompt versioning, the two
# MAGIC newest exam additions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vector Search — one endpoint, two index types
# MAGIC
# MAGIC | | **Delta Sync index** | Direct Vector Access index |
# MAGIC |---|---|---|
# MAGIC | Source | a Delta table (**CDF enabled**) | you push vectors via API |
# MAGIC | Stays current | **automatically** (continuous or triggered sync) | you manage updates |
# MAGIC | Embeddings | computed **for you** (managed, from a text column) or self-provided | self-provided |
# MAGIC | Exam default | ✅ this one | niche/custom pipelines |
# MAGIC
# MAGIC ```python
# MAGIC from databricks.vector_search.client import VectorSearchClient
# MAGIC vsc = VectorSearchClient()
# MAGIC vsc.create_endpoint(name="rag-endpoint", endpoint_type="STANDARD")
# MAGIC index = vsc.create_delta_sync_index(
# MAGIC     endpoint_name="rag-endpoint",
# MAGIC     index_name="dev_catalog.genai_assoc.doc_chunks_index",
# MAGIC     source_table_name="dev_catalog.genai_assoc.doc_chunks",
# MAGIC     primary_key="chunk_id",
# MAGIC     embedding_source_column="chunk_text",                  # managed embeddings
# MAGIC     embedding_model_endpoint_name="databricks-gte-large-en",
# MAGIC     pipeline_type="TRIGGERED",
# MAGIC )
# MAGIC hits = index.similarity_search(query_text="how do I configure auth?",
# MAGIC                                columns=["chunk_id", "chunk_text", "source"],
# MAGIC                                num_results=3)
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Delta Sync **requires Change Data Feed** on the source table.
# MAGIC > - `TRIGGERED` sync = on demand/schedule (cheaper); `CONTINUOUS` = lowest
# MAGIC >   staleness (always-on cost).
# MAGIC > - **Hybrid search** (vector + keyword) is the toggle for exact-term
# MAGIC >   queries (IDs, SKUs) that pure semantic search fumbles.
# MAGIC > - Filters on metadata columns happen **in** `similarity_search` —
# MAGIC >   that's why section 02 kept metadata per chunk.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log the chain as a model — models-from-code
# MAGIC
# MAGIC ```python
# MAGIC # chain.py defines the chain and ends with:  mlflow.models.set_model(chain)
# MAGIC with mlflow.start_run():
# MAGIC     mlflow.langchain.log_model(          # or mlflow.pyfunc for custom classes
# MAGIC         lc_model="chain.py",             # <- code file, not a pickle
# MAGIC         artifact_path="chain",
# MAGIC         registered_model_name="dev_catalog.genai_assoc.rag_chain",
# MAGIC         resources=[...],                 # endpoints/indexes the chain calls
# MAGIC         input_example={"messages": [{"role": "user", "content": "hi"}]},
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC Models-from-code (a `.py` file, not serialized objects) is the current
# MAGIC recommendation — reviewable, no pickle fragility. `resources` declares
# MAGIC the endpoints/indexes so serving can authenticate to them on the model's
# MAGIC behalf (automatic auth passthrough) — the answer to "how does the served
# MAGIC chain reach Vector Search?".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy — agents.deploy or endpoint config
# MAGIC
# MAGIC ```python
# MAGIC from databricks import agents
# MAGIC agents.deploy("dev_catalog.genai_assoc.rag_chain", model_version, scale_to_zero=True)
# MAGIC ```
# MAGIC
# MAGIC `agents.deploy` = Model Serving endpoint **plus** the agent extras: the
# MAGIC **Review App** (SMEs chat with the app and give feedback), inference
# MAGIC tables on, trace logging wired. A plain chat UI for stakeholders without
# MAGIC building a frontend — that phrase in a question means Review App.
# MAGIC
# MAGIC ## MCP servers — tools over a protocol
# MAGIC **Model Context Protocol** standardizes how agents reach tools/data:
# MAGIC Databricks provides **managed MCP servers** (Vector Search, UC functions,
# MAGIC Genie) and you can host custom ones (Databricks Apps). Exam-level: MCP =
# MAGIC the open protocol that lets agents (including external ones) consume
# MAGIC governed Databricks tools without bespoke integration code.
# MAGIC
# MAGIC ## Prompt versioning & CI/CD
# MAGIC - **MLflow Prompt Registry**: prompts as versioned UC assets —
# MAGIC   `mlflow.genai.register_prompt(...)`, load by version/alias in the
# MAGIC   chain; change the prompt without redeploying code.
# MAGIC - CI/CD is the ML story (bundles, per-env catalogs, `@champion` aliases,
# MAGIC   validation gates) applied to chains — see ML Professional
# MAGIC   [`02-mlops`](../../machine-learning-professional/02-mlops/).
