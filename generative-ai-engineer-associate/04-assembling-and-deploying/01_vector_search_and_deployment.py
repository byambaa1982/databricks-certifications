# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Vector Search, in working detail
# MAGIC
# MAGIC The index is infrastructure with real decisions: endpoint vs index,
# MAGIC which index type, who computes embeddings, when it syncs, and how
# MAGIC queries filter. Each decision is a question format.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Object model — endpoint ≠ index
# MAGIC
# MAGIC ```
# MAGIC VECTOR SEARCH ENDPOINT            (compute that serves queries; hosts many indexes)
# MAGIC └── INDEX                         (a UC object: catalog.schema.index_name)
# MAGIC     ├── Delta Sync index          <- from a Delta table, stays in sync
# MAGIC     └── Direct Vector Access      <- you push vectors via API
# MAGIC ```
# MAGIC
# MAGIC | Decision | Options | Pick |
# MAGIC |---|---|---|
# MAGIC | index type | **Delta Sync** vs Direct Access | Delta Sync unless vectors are produced outside Databricks |
# MAGIC | embeddings | **managed** (from a text column, model you name) vs self-managed (a vector column you computed) | managed unless you need a custom embedding pipeline |
# MAGIC | sync mode | **`TRIGGERED`** (on demand / by your pipeline) vs `CONTINUOUS` (always-on, lowest staleness) | triggered for batch-updated corpora; continuous when freshness is the requirement |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Delta Sync **requires Change Data Feed** on the source table — the
# MAGIC >   error "CDF not enabled" question.
# MAGIC > - `CONTINUOUS` costs always-on compute; "docs update nightly" →
# MAGIC >   `TRIGGERED` + `index.sync()` after the pipeline writes.
# MAGIC > - Managed embeddings pin the **embedding endpoint on the index** —
# MAGIC >   queries are embedded by the *same model automatically*. Self-managed
# MAGIC >   embeddings move that consistency burden to you (`query_vector`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## The full API surface to recognize
# MAGIC
# MAGIC ```python
# MAGIC from databricks.vector_search.client import VectorSearchClient
# MAGIC vsc = VectorSearchClient()
# MAGIC
# MAGIC vsc.create_endpoint(name="rag-endpoint", endpoint_type="STANDARD")
# MAGIC
# MAGIC index = vsc.create_delta_sync_index(
# MAGIC     endpoint_name="rag-endpoint",
# MAGIC     index_name="dev_catalog.genai_assoc.doc_chunks_index",
# MAGIC     source_table_name="dev_catalog.genai_assoc.doc_chunks",
# MAGIC     primary_key="chunk_id",
# MAGIC     embedding_source_column="chunk_text",                    # managed embeddings
# MAGIC     embedding_model_endpoint_name="databricks-gte-large-en",
# MAGIC     pipeline_type="TRIGGERED",
# MAGIC )
# MAGIC
# MAGIC # querying — the four variants
# MAGIC index.similarity_search(query_text="configure auth",          # 1. semantic
# MAGIC                         columns=["chunk_id", "chunk_text", "source"],
# MAGIC                         num_results=3)
# MAGIC index.similarity_search(query_text="error QX-1042",           # 2. hybrid
# MAGIC                         columns=[...], num_results=3,
# MAGIC                         query_type="HYBRID")                  #    vector + keyword
# MAGIC index.similarity_search(query_text="configure auth",          # 3. filtered
# MAGIC                         columns=[...], num_results=3,
# MAGIC                         filters={"source": "agent-guide.md"})
# MAGIC index.similarity_search(query_vector=[0.12, ...],             # 4. self-managed
# MAGIC                         columns=[...], num_results=3)
# MAGIC
# MAGIC index.sync()                          # triggered refresh after new chunks land
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - **Hybrid** (`query_type="HYBRID"`) is the fix for exact-term queries
# MAGIC >   (IDs, SKUs, error codes) that pure semantic search fumbles — a query
# MAGIC >   option, not a re-index.
# MAGIC > - **Filters** run on the metadata columns section 02 preserved —
# MAGIC >   "restrict answers to 2024 policies" is a filter, not a second index.
# MAGIC > - The index is a **UC object**: grants (`SELECT` on the index),
# MAGIC >   lineage to the source table, three-level name. Access control on RAG
# MAGIC >   = grants on index + serving endpoint (not prompt rules).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sizing `num_results` (k)
# MAGIC k is a budget decision, not a bigger-is-better dial: each chunk spends
# MAGIC context tokens (section 01's arithmetic) and adds distraction risk;
# MAGIC too-small k gambles recall. The measured pattern: generous k → re-ranker
# MAGIC → top 3–5 into the prompt (section 02's numbers justify each step).
# MAGIC
# MAGIC ## Troubleshooting table
# MAGIC
# MAGIC | Symptom | Cause |
# MAGIC |---|---|
# MAGIC | new docs never appear in answers | `TRIGGERED` index nobody syncs — call `index.sync()` in the ingestion job |
# MAGIC | garbage similarity scores after a model swap | corpus and queries embedded by **different models** — re-index |
# MAGIC | "CDF not enabled" on index creation | `ALTER TABLE … SET TBLPROPERTIES (delta.enableChangeDataFeed = true)` |
# MAGIC | great answers in tests, permission errors when served | the **served model's** identity lacks index access — declare `resources` at logging time (notebook 02) |
