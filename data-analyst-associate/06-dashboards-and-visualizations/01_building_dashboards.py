# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — AI/BI Dashboards: datasets, widgets, and filters
# MAGIC
# MAGIC AI/BI Dashboards are built in the UI, so this is a field guide: the
# MAGIC dataset → widget model, which visualization fits which question, and how
# MAGIC filters and parameters actually differ — the pair the exam tests most.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The model: datasets feed widgets
# MAGIC
# MAGIC ```
# MAGIC Dashboard
# MAGIC ├── Data tab: DATASETS — each one a SQL query (or a table pick)
# MAGIC │     e.g. SELECT order_date, tier, revenue FROM gold_daily_revenue_by_tier
# MAGIC ├── Canvas page(s): WIDGETS — each bound to a dataset
# MAGIC │     charts, counters, tables, pivot, text/markdown
# MAGIC └── Filters & parameters — page-level controls wired to datasets
# MAGIC ```
# MAGIC
# MAGIC - **One dataset can feed many widgets** — define revenue once, chart it
# MAGIC   three ways. That's the reuse the exam rewards.
# MAGIC - Multiple **pages** (tabs) organize a large dashboard; widgets can be
# MAGIC   cloned across pages.
# MAGIC - The assistant can draft a chart from a prompt — you stay responsible for
# MAGIC   checking the query underneath.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Picking the visualization — the matching table
# MAGIC
# MAGIC | Question shape | Widget |
# MAGIC |---|---|
# MAGIC | trend over time | line (area for stacked composition) |
# MAGIC | compare categories | bar (horizontal when labels are long) |
# MAGIC | single KPI now | **counter** (with comparison to a target/prior period) |
# MAGIC | composition of a whole | stacked bar / pie (few slices only) |
# MAGIC | two measures' relationship | scatter |
# MAGIC | value across two dimensions | heatmap / pivot |
# MAGIC | exact records | table |
# MAGIC | value along a funnel/stage | funnel |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Show revenue **and** whether we hit target" → counter with
# MAGIC >   comparison, not a bar chart.
# MAGIC > - Pie charts with many slices are the standing wrong answer — bar wins.
# MAGIC > - A table widget is correct when the ask is "the actual rows", not a
# MAGIC >   summary — resist over-charting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filters vs parameters — the tested distinction
# MAGIC
# MAGIC | | **Filter** | **Parameter** |
# MAGIC |---|---|---|
# MAGIC | Acts on | a dataset's **result** (a field's values) | the dataset's **SQL itself** (`:param` marker) |
# MAGIC | Defined by | picking a field | writing the query with a placeholder |
# MAGIC | Typical use | date range, region picker on the page | switch granularity, table name, top-N size |
# MAGIC | Widgets affected | all widgets on that dataset/field | widgets on that dataset — query re-runs |
# MAGIC
# MAGIC ```sql
# MAGIC -- a parameterized dataset: the page control binds to :top_n
# MAGIC SELECT name, revenue
# MAGIC FROM gold_customer_revenue
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT :top_n
# MAGIC ```
# MAGIC
# MAGIC Cross-filtering is the third interaction: clicking a bar filters the other
# MAGIC widgets on the page — on by default, no configuration, and distinct from
# MAGIC both of the above.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A gold dataset worth charting (runnable — the SQL side you do own)

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "da_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %sql
# MAGIC -- exactly the query you'd paste into a dashboard dataset
# MAGIC SELECT date_trunc('week', o.order_ts)      AS week,
# MAGIC        c.tier,
# MAGIC        sum(o.quantity * p.unit_price)      AS revenue,
# MAGIC        count(DISTINCT o.customer_id)       AS buyers
# MAGIC FROM orders o
# MAGIC JOIN customers c USING (customer_id)
# MAGIC JOIN products  p USING (product_id)
# MAGIC WHERE o.status = 'completed'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY week, tier

# COMMAND ----------

# MAGIC %md
# MAGIC The dashboard habit that scales: keep heavy logic in **gold tables /
# MAGIC materialized views** (sections 04–05) and keep dashboard datasets thin —
# MAGIC dashboards re-run their datasets on refresh, and a thin dataset over a
# MAGIC precomputed table refreshes in seconds.
