# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Evaluation: scorers, judges, and eval sets
# MAGIC
# MAGIC Free text can't be asserted, so evaluation becomes: a fixed **eval
# MAGIC set**, **scorers** (deterministic where possible, LLM-judged where not),
# MAGIC and discipline about what each score can claim. The deterministic
# MAGIC scorers run live here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The eval set — same discipline as section 02, one level up
# MAGIC Rows of `inputs` (+ optional `expectations`). Sources, best first:
# MAGIC **real logged questions** (inference tables), Review-App feedback
# MAGIC (thumbs-down = free hard cases), SME-written, synthetic. Keep it fixed
# MAGIC while iterating — change the app OR the eval set, never both at once.

# COMMAND ----------

# deterministic scorers you can always run — no LLM required
def exact_keywords(answer: str, required: list[str]) -> float:
    hits = sum(1 for kw in required if kw.lower() in answer.lower())
    return hits / len(required)

def cites_sources(answer: str) -> bool:
    import re
    return bool(re.search(r"\[\d+\]", answer))

def declines_when_unknown(answer: str) -> bool:
    return "don't know" in answer.lower() or "cannot find" in answer.lower()

candidates = {
    "grounded":  "Configure OAuth in the settings page [1], then restart [1].",
    "no_cite":   "Configure OAuth in the settings page, then restart.",
    "fabricated": "Set the AUTH_MODE environment variable to legacy.",
}
required = ["oauth", "settings", "restart"]
for name, ans in candidates.items():
    print(f"{name:10s} keywords={exact_keywords(ans, required):.2f} "
          f"cites={cites_sources(ans)}")

# COMMAND ----------

# MAGIC %md
# MAGIC Deterministic checks caught the missing citation — but scored the
# MAGIC **fabricated** answer 0 only because it used different words. Judging
# MAGIC *faithfulness to retrieved context* needs a reader: the **LLM judge**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `mlflow.genai.evaluate` — the harness
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC from mlflow.genai.scorers import (
# MAGIC     Correctness, RelevanceToQuery, RetrievalGroundedness, Safety, Guidelines,
# MAGIC )
# MAGIC from mlflow.genai import scorer
# MAGIC
# MAGIC @scorer                                   # custom deterministic scorer
# MAGIC def has_citation(outputs) -> bool:
# MAGIC     import re
# MAGIC     return bool(re.search(r"\[\d+\]", outputs))
# MAGIC
# MAGIC results = mlflow.genai.evaluate(
# MAGIC     data=eval_rows,                       # inputs (+ expectations)
# MAGIC     predict_fn=my_app,
# MAGIC     scorers=[
# MAGIC         Correctness(),                    # vs expected_facts   (needs ground truth)
# MAGIC         RelevanceToQuery(),               # on-topic?           (no ground truth)
# MAGIC         RetrievalGroundedness(),          # claims ⊆ retrieved context?  <- anti-hallucination
# MAGIC         Safety(),
# MAGIC         Guidelines(name="tone", guidelines="Professional. No speculation."),
# MAGIC         has_citation,
# MAGIC     ],
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC | Scorer | Judges | Needs |
# MAGIC |---|---|---|
# MAGIC | `Correctness` | matches expected facts | ground truth |
# MAGIC | `RelevanceToQuery` | answers the actual question | — |
# MAGIC | **`RetrievalGroundedness`** | every claim supported by retrieved context | the trace's retrieved chunks |
# MAGIC | `RetrievalRelevance` / `RetrievalSufficiency` | were the *chunks* on-topic / enough | (section 02's metrics, judged) |
# MAGIC | `Safety` | harmful content | — |
# MAGIC | `Guidelines` | your rubric, LLM-judged | a written rubric |
# MAGIC | `@scorer` custom | anything codeable | — |

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - **Groundedness is the anti-hallucination metric** — it checks the
# MAGIC >   answer against retrieved context, needing no ground truth. "How do we
# MAGIC >   detect hallucination in RAG at scale?" → this, not BLEU, not humans
# MAGIC >   reading everything.
# MAGIC > - Match metric to available evidence: expected facts → Correctness;
# MAGIC >   retrieved context → Groundedness; neither → Guidelines/judge.
# MAGIC >   BLEU/ROUGE need reference texts — offering them for open-ended chat
# MAGIC >   is the distractor.
# MAGIC > - **Validate the judge**: score a human-labeled sample, check
# MAGIC >   agreement, then trust it at scale. An unvalidated judge is just a
# MAGIC >   confident opinion.
# MAGIC > - Judges cost tokens — deterministic scorers run on everything, judges
# MAGIC >   on samples/failures (a cost-design question).
# MAGIC
# MAGIC The iteration loop, which several questions describe end-to-end:
# MAGIC **fixed eval set → change one thing → re-evaluate → compare → promote
# MAGIC via alias or revert** — with results logged to MLflow runs so the
# MAGIC comparison is queryable, not anecdotal.
