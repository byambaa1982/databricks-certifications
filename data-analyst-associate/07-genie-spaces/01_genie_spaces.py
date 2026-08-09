# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — AI/BI Genie spaces: curated natural-language analytics
# MAGIC
# MAGIC Genie lets stakeholders ask questions in English and get SQL-backed
# MAGIC answers — but only as well as **you** curate it. The exam tests the
# MAGIC curation levers, in roughly the order you'd pull them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What a Genie space is
# MAGIC A **scoped** chat interface: you pick the tables, add context, and Genie
# MAGIC writes SQL against exactly that scope (running on a SQL warehouse, under
# MAGIC UC permissions). It is not a general chatbot — the scoping is the point.
# MAGIC
# MAGIC ## The curation levers (= the exam objectives)
# MAGIC
# MAGIC | Lever | What it does | Rule of thumb |
# MAGIC |---|---|---|
# MAGIC | **Curated tables** | the space's entire world | **few, gold, well-named** — 5-ish focused tables beat 50 raw ones |
# MAGIC | **Column/table comments** | Genie reads them to understand the data | write them for a person; Genie benefits too (section 02's skill, cashed in) |
# MAGIC | **Instructions** | space-level prose: business definitions, defaults, caveats | "revenue means completed orders only; fiscal year starts July" |
# MAGIC | **Example / sample questions** | seed the UI and teach phrasing | start from real stakeholder asks |
# MAGIC | **Trusted assets** | parameterized example queries + SQL functions Genie must use for matching questions | encode the *official* metric once — answers get a "trusted" badge |
# MAGIC | **Benchmarks** | question + expected answer pairs, scored on demand | your regression test after every change |
# MAGIC | **Feedback / monitoring** | thumbs up/down + a review view of asked questions | the improvement loop — fix with instructions or trusted assets |

# COMMAND ----------

# MAGIC %md
# MAGIC ## The trusted-asset shape (SQL you actually write)
# MAGIC
# MAGIC ```sql
# MAGIC -- a UC SQL function Genie can call for the official metric
# MAGIC CREATE OR REPLACE FUNCTION dev_catalog.da_assoc.weekly_revenue(
# MAGIC   start_date DATE, end_date DATE
# MAGIC )
# MAGIC RETURNS TABLE(week DATE, revenue DECIMAL(18,2))
# MAGIC RETURN
# MAGIC   SELECT date_trunc('week', o.order_ts)::date AS week,
# MAGIC          sum(o.quantity * p.unit_price)       AS revenue
# MAGIC   FROM dev_catalog.da_assoc.orders o
# MAGIC   JOIN dev_catalog.da_assoc.products p USING (product_id)
# MAGIC   WHERE o.status = 'completed'
# MAGIC     AND o.order_ts BETWEEN start_date AND end_date
# MAGIC   GROUP BY 1;
# MAGIC ```
# MAGIC
# MAGIC Registered as a trusted asset, "what was revenue last month?" runs *this*
# MAGIC — not Genie's best guess at the revenue formula.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permissions and the credentials question (again)
# MAGIC - Sharing a space ≠ sharing the data: with default (viewer-credential)
# MAGIC   behavior, users need UC access to the underlying tables, and row
# MAGIC   filters/masks apply **per asker**.
# MAGIC - "Can run" vs "can edit" the space are separate levels — stakeholders
# MAGIC   run; analysts curate.
# MAGIC - Genie runs on a warehouse → its queries land in **query history** like
# MAGIC   any other (section 05's tools apply).
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Genie gives wrong answers about revenue" → add a **trusted asset** /
# MAGIC >   **instruction** defining revenue — not more tables.
# MAGIC > - "Genie can't answer questions about topic X" → is X's table even in
# MAGIC >   the space? Scope first.
# MAGIC > - "How do we know quality didn't regress after edits?" → **benchmarks**.
# MAGIC > - More tables ≠ better: broad scope dilutes accuracy — split into
# MAGIC >   multiple focused spaces per domain instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The setup recipe (what "good" looks like end-to-end)
# MAGIC 1. Build/pick **gold** tables with real comments (sections 02–04).
# MAGIC 2. Create the space with those few tables; write instructions with the
# MAGIC    business definitions.
# MAGIC 3. Seed sample questions from actual stakeholder asks.
# MAGIC 4. Encode official metrics as trusted assets.
# MAGIC 5. Benchmark, share with a pilot group, watch feedback, iterate.
# MAGIC
# MAGIC Pairing: a dashboard answers the *known* questions; its Genie space
# MAGIC absorbs the *follow-up* ones. That pairing sentence resolves several exam
# MAGIC scenarios by itself.
