# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Evaluating retrieval (before you blame the LLM)
# MAGIC
# MAGIC Retrieval is a component you can score **on its own** — no LLM, no
# MAGIC judges, just labeled examples and rank math. The metrics run live here
# MAGIC on a toy eval set, so you see exactly what recall@k and MRR reward.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The eval set — the artifact teams skip and regret
# MAGIC 20–100 rows of `(question, ids of chunks that truly answer it)`. Sources:
# MAGIC real user questions (best), SME-written questions, or synthetic ones
# MAGIC generated per chunk ("write a question this chunk answers") — synthetic
# MAGIC is fine to start, but validate a sample by hand.

# COMMAND ----------

# ground truth: question -> relevant chunk ids
eval_set = {
    "how do I configure auth?":          {"doc1-0"},
    "can I paste customer data in?":     {"doc1-2"},
    "what happens with huge documents?": {"doc1-2"},
    "how do users report bad answers?":  {"doc1-1"},
}
# what two retrieval configs actually returned (top-3, best first)
retrieved_A = {   # config A: fixed-size chunks
    "how do I configure auth?":          ["doc1-0", "doc1-1", "doc1-2"],
    "can I paste customer data in?":     ["doc1-1", "doc1-0", "doc1-2"],
    "what happens with huge documents?": ["doc1-1", "doc1-0", "doc1-2"],
    "how do users report bad answers?":  ["doc1-1", "doc1-2", "doc1-0"],
}
retrieved_B = {   # config B: structure-aware chunks
    "how do I configure auth?":          ["doc1-0", "doc1-2", "doc1-1"],
    "can I paste customer data in?":     ["doc1-2", "doc1-1", "doc1-0"],
    "what happens with huge documents?": ["doc1-2", "doc1-0", "doc1-1"],
    "how do users report bad answers?":  ["doc1-1", "doc1-0", "doc1-2"],
}

# COMMAND ----------

def recall_at_k(retrieved, relevant, k):
    return len(set(retrieved[:k]) & relevant) / len(relevant)

def mrr(retrieved, relevant):
    for rank, cid in enumerate(retrieved, start=1):
        if cid in relevant:
            return 1.0 / rank
    return 0.0

def score(retrieved_all, k=3):
    r = sum(recall_at_k(retrieved_all[q], rel, k) for q, rel in eval_set.items())
    m = sum(mrr(retrieved_all[q], rel) for q, rel in eval_set.items())
    n = len(eval_set)
    return r / n, m / n

for name, results in [("A: fixed-size", retrieved_A), ("B: structure-aware", retrieved_B)]:
    r3, m = score(results)
    print(f"config {name:20s} recall@3 = {r3:.2f}   MRR = {m:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC Both configs have perfect recall@3 (the right chunk is always
# MAGIC *somewhere* in the top 3) — but B's MRR is far higher because the right
# MAGIC chunk is ranked **first**. That's the whole reason rank-aware metrics
# MAGIC exist: with k=3 chunks in the prompt both "work", but B wastes less
# MAGIC context and survives k=1.
# MAGIC
# MAGIC | Metric | Rewards | Blind to |
# MAGIC |---|---|---|
# MAGIC | **recall@k** | the right chunk being present in top-k | *where* it ranks |
# MAGIC | precision@k | top-k not being padded with junk | missing relevant chunks |
# MAGIC | **MRR** | the first relevant hit being early | additional relevant chunks |
# MAGIC | NDCG | full graded ranking quality | (needs graded labels) |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Answers are wrong; retrieval or generation?" → score retrieval alone
# MAGIC >   first. High recall + bad answers = generation problem; low recall =
# MAGIC >   stop tuning prompts, fix retrieval.
# MAGIC > - Recall fine but the right chunk ranks low → **re-ranker** (a
# MAGIC >   cross-encoder reads query+chunk *together*, reorders a generous
# MAGIC >   top-k, keeps the best few). More accurate than bi-encoder similarity,
# MAGIC >   too slow to run on the whole corpus — that asymmetry is *why* it's a
# MAGIC >   second stage.
# MAGIC > - Exact terms (SKUs, error codes, names) fumbled by semantic search →
# MAGIC >   **hybrid search** (vector + keyword), a query-time option in section
# MAGIC >   04, not a re-chunking job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Embedding model choices that move these numbers
# MAGIC - **One model for corpus and queries** — mixing models makes the vector
# MAGIC   space meaningless. Changing embedding model = re-embed everything.
# MAGIC - Databricks-hosted: `databricks-gte-large-en` (the default pick),
# MAGIC   `databricks-bge-large-en`; both ~512-token inputs — tie back to chunk
# MAGIC   size (notebook 01).
# MAGIC - Multilingual corpus → multilingual embedding model; domain jargon →
# MAGIC   consider fine-tuned embeddings *after* proving baseline recall is the
# MAGIC   bottleneck.
# MAGIC
# MAGIC The loop this section wants you to know: **change one thing (chunking,
# MAGIC embedding model, k, re-ranker) → re-run the same eval set → compare
# MAGIC recall/MRR → keep or revert.** It's the same discipline as section 06,
# MAGIC applied to the retriever alone.
