# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Assembling the RAG chain
# MAGIC
# MAGIC The complete retrieve → augment → generate path, written out as code you
# MAGIC could log and serve (section 04 does exactly that). The assembly logic
# MAGIC runs live with a stub retriever; swap in Vector Search and an endpoint
# MAGIC and it's production-shaped.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The chain, end to end

# COMMAND ----------

# --- 1. retrieve (stub here; VectorSearch index.similarity_search in prod) ---
CORPUS = {
    "doc1-0": ("agent-guide.md#Setup", "Install the agent with pip. Configure OAuth "
               "credentials in the settings page. Restart after configuration."),
    "doc1-1": ("agent-guide.md#Usage", "Ask questions in plain language. The agent "
               "cites its sources. Flag bad answers with thumbs down."),
    "doc1-2": ("agent-guide.md#Limits", "Do not paste customer PII into prompts. "
               "Rate limits apply per user."),
}

def retrieve(query: str, k: int = 2):
    """Stub: keyword overlap. Prod: index.similarity_search(query_text=query,...)"""
    scored = sorted(
        CORPUS.items(),
        key=lambda kv: -len(set(query.lower().split()) & set(kv[1][1].lower().split())),
    )
    return [{"id": cid, "source": src, "text": txt} for cid, (src, txt) in scored[:k]]

# --- 2. augment: the prompt contract ---
SYSTEM = ("Answer ONLY from the provided context. If the context is "
          "insufficient, say \"I don't know\". Cite sources like [1].")

def augment(query: str, chunks: list[dict]) -> list[dict]:
    ctx = "\n\n".join(f"[{i+1}] ({c['source']}) {c['text']}"
                      for i, c in enumerate(chunks))
    return [{"role": "system", "content": SYSTEM},
            {"role": "user", "content": f"<context>\n{ctx}\n</context>\n\n"
                                        f"Question: {query}"}]

# --- 3. generate (prod: client.chat.completions.create) ---
def rag_answer(query: str):
    chunks = retrieve(query)
    messages = augment(query, chunks)
    return messages, chunks   # the LLM call goes here

messages, chunks = rag_answer("how do I configure auth credentials?")
print(messages[1]["content"])

# COMMAND ----------

# MAGIC %md
# MAGIC Notice what the assembled prompt encodes — each line is an exam answer:
# MAGIC
# MAGIC | Prompt feature | Failure it prevents |
# MAGIC |---|---|
# MAGIC | "answer ONLY from context" | hallucinating beyond the docs |
# MAGIC | "say I don't know if insufficient" | confident fabrication when retrieval misses |
# MAGIC | numbered chunks + "cite" | unverifiable answers; enables click-through citations |
# MAGIC | `<context>` delimiters | retrieved text being read as *instructions* (indirect injection, section 05) |
# MAGIC | question **after** context | the model answering before it has read the evidence |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The same chain in LangChain — recognize, don't memorize
# MAGIC
# MAGIC ```python
# MAGIC from databricks_langchain import ChatDatabricks, DatabricksVectorSearch
# MAGIC from langchain_core.prompts import ChatPromptTemplate
# MAGIC from langchain_core.output_parsers import StrOutputParser
# MAGIC from langchain_core.runnables import RunnablePassthrough
# MAGIC
# MAGIC retriever = DatabricksVectorSearch(
# MAGIC     index_name="dev_catalog.genai_assoc.doc_chunks_index",
# MAGIC     columns=["chunk_text", "source"],
# MAGIC ).as_retriever(search_kwargs={"k": 3})
# MAGIC
# MAGIC prompt = ChatPromptTemplate.from_messages(
# MAGIC     [("system", SYSTEM), ("user", "<context>{context}</context>\n\nQuestion: {question}")])
# MAGIC llm = ChatDatabricks(endpoint="databricks-meta-llama-3-3-70b-instruct")
# MAGIC
# MAGIC chain = ({"context": retriever, "question": RunnablePassthrough()}
# MAGIC          | prompt | llm | StrOutputParser())
# MAGIC chain.invoke("how do I configure auth credentials?")
# MAGIC ```
# MAGIC
# MAGIC The `|` pipeline (LCEL) is the shape to recognize: **retriever feeds the
# MAGIC context variable, passthrough feeds the question, prompt formats, llm
# MAGIC generates, parser extracts text.** The exam shows this and asks what each
# MAGIC stage does or where a bug is (e.g., retriever wired to the wrong
# MAGIC variable).
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Framework choice (LangChain / LlamaIndex / plain Python) is *not* the
# MAGIC >   tested decision — what matters is the result is **loggable as an
# MAGIC >   MLflow model** with declared resources (section 04). All three are.
# MAGIC > - `databricks_langchain` components (`ChatDatabricks`,
# MAGIC >   `DatabricksVectorSearch`, `DatabricksEmbeddings`) are the
# MAGIC >   Databricks-native bindings — recognize the names.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-turn: conversation history is YOUR problem
# MAGIC LLM calls are stateless — the chain replays history every turn, and the
# MAGIC context budget forces a policy:
# MAGIC
# MAGIC | Policy | How | Trade |
# MAGIC |---|---|---|
# MAGIC | sliding window | keep last N turns | forgets early constraints |
# MAGIC | **summarize-then-window** | small model compresses old turns into a running summary | cheap + keeps gist — the usual answer |
# MAGIC | rewrite-the-query | condense (history + new question) into a standalone query **before retrieval** | fixes "what about the second one?" retrieving nothing |
# MAGIC
# MAGIC > **Exam trap:** follow-up questions failing retrieval ("its limits?" —
# MAGIC > whose?) is a **query-rewriting** problem: retrieve with a
# MAGIC > history-condensed standalone question, not the raw follow-up.
