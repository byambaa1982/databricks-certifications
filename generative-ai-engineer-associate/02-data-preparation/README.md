# 02 — Data Preparation (14%)

RAG quality is decided before the LLM sees anything: the parse → clean →
chunk → Delta(+CDF) → index path, the chunk-sizing logic (both failure
directions), four chunking strategies with a live structure-aware chunker,
retrieval metrics (recall@k and friends) for comparing strategies by
numbers, and re-ranking as the right-chunk-not-first fix.

| Notebook | What you practice |
|---|---|
| [`01_chunking_and_retrieval_prep.py`](01_chunking_and_retrieval_prep.py) | The ingestion path, chunk-size trade table, strategy table (overlap, structure-aware, semantic, parent-document), a runnable chunker writing a CDF-enabled `doc_chunks` table, retrieval metrics + re-ranking |

**Study:** chunking questions give a symptom — imprecise retrieval → smaller
/ structure-aware; fragmentary answers → bigger / parent-document; right
chunk ranked low → re-rank. 14% ≈ 6 of 45 questions on
[TestHub](https://testlogichub.com).
