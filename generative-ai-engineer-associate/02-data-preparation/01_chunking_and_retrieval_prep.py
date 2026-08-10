# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Preparing data for RAG: parse, chunk, filter
# MAGIC
# MAGIC RAG quality is decided **before** the LLM sees anything: how documents
# MAGIC are parsed, how they're split into chunks, and what gets filtered out.
# MAGIC The chunking demo runs live on plain PySpark.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "genai_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## The ingestion path
# MAGIC
# MAGIC ```
# MAGIC PDFs/HTML/docs in a UC VOLUME
# MAGIC   -> parse (ai_parse_document / libraries)      extract text + structure
# MAGIC   -> clean & filter                              drop boilerplate, dedupe, PII scrub (section 05)
# MAGIC   -> chunk                                       the decisions below
# MAGIC   -> Delta table (id, text, metadata)  ->  Vector Search index (section 04)
# MAGIC ```
# MAGIC
# MAGIC Everything lands in **Delta with Change Data Feed enabled** — that's what
# MAGIC lets the index sync incrementally.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chunking — the sizing logic
# MAGIC
# MAGIC | Chunks too **big** | Chunks too **small** |
# MAGIC |---|---|
# MAGIC | retrieval imprecise (mixed topics per chunk) | context fragmentary — answers miss surrounding info |
# MAGIC | fewer fit in the context window | more lookups needed; joins across chunks fail |
# MAGIC
# MAGIC | Strategy | How | When |
# MAGIC |---|---|---|
# MAGIC | fixed-size + **overlap** | N tokens, ~10–20% overlap | default; overlap protects sentences cut at borders |
# MAGIC | **structure-aware** | split on headings/sections/paragraphs | docs with real structure (manuals, contracts) |
# MAGIC | semantic | split where topic shifts (embedding distance) | long unstructured prose; costs compute |
# MAGIC | parent-document / windowed | retrieve small chunk, hand the LLM its bigger parent | precision *and* context — the advanced answer |

# COMMAND ----------

# a runnable structure-aware chunker over a toy corpus
from pyspark.sql import functions as F  # noqa: E402

docs = spark.createDataFrame(  # noqa: F821
    [(1, "intro.md", "# Setup\nInstall the agent.\nConfigure auth.\n# Usage\nAsk questions.\nReview answers.\n# Limits\nNo PII.\nRate limits apply."),
     (2, "faq.md", "# Billing\nInvoices monthly.\n# Support\nEmail us.\nSLAs apply.")],
    "doc_id INT, source STRING, text STRING",
)
chunks = (
    docs.withColumn("chunk", F.explode(F.split("text", r"(?=# )")))
    .filter(F.length(F.trim("chunk")) > 0)
    .withColumn("chunk_id", F.concat_ws("-", "doc_id", F.abs(F.hash("chunk")) % 10000))
    .select("chunk_id", "doc_id", "source", F.trim("chunk").alias("chunk_text"))
)
chunks.write.mode("overwrite").option("delta.enableChangeDataFeed", "true") \
    .saveAsTable("doc_chunks")
display(spark.table("doc_chunks"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - Keep **metadata columns** (source, section, date) with each chunk —
# MAGIC >   they power filtered retrieval ("only 2024 policies") and citations.
# MAGIC > - Chunk size is measured against the **embedding model's input limit**
# MAGIC >   and the **LLM's context budget** — two different constraints.
# MAGIC > - Changing the chunking strategy means **re-embedding and re-indexing**
# MAGIC >   — it's not a config toggle on the index.
# MAGIC > - `ai_parse_document` is the SQL-native answer for PDFs/images at scale;
# MAGIC >   custom Python parsers are for formats it doesn't cover.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Did retrieval get better? Measure it
# MAGIC Build a small eval set of (question → relevant chunk ids) and score:
# MAGIC
# MAGIC | Metric | Question it answers |
# MAGIC |---|---|
# MAGIC | **recall@k** | did the relevant chunk make the top-k at all? |
# MAGIC | precision@k | how much of the top-k was relevant? |
# MAGIC | NDCG / MRR | is the relevant chunk ranked *high*? |
# MAGIC
# MAGIC Compare chunking strategies **by these numbers**, not vibes — the exam
# MAGIC phrase is "evaluate retrieval quality independently of generation".
# MAGIC
# MAGIC **Re-ranking**: retrieve a generous top-k with vector search, then let a
# MAGIC cross-encoder / reranker model reorder and cut to the best few — the
# MAGIC standard fix when recall is fine but the *right* chunk isn't first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup — keep `doc_chunks` (section 04 indexes it)
