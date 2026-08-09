# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — SQL warehouses and the query toolchain
# MAGIC
# MAGIC The other half of the section: the compute your SQL runs on and the tools
# MAGIC wrapped around it — the SQL editor, saved queries, parameters, schedules,
# MAGIC and alerts. Warehouse mechanics are config, so this is reference-first
# MAGIC with the runnable checks live.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Warehouse mechanics — the decisions you'll be asked about
# MAGIC
# MAGIC | Knob | Fixes |
# MAGIC |---|---|
# MAGIC | **Size** (2X-Small → 4X-Large) | one big query is slow (more horsepower per cluster) |
# MAGIC | **Scaling** (min–max clusters) | many users at once — queries queue past capacity |
# MAGIC | **Auto-stop** | idle spend |
# MAGIC | **Serverless** | slow cold starts + all of the above managed for you |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Size ≠ scaling: sizing up does nothing for concurrency; scaling out
# MAGIC >   does nothing for one giant query. Read the symptom first.
# MAGIC > - A queued query isn't an error — it's the warehouse at max clusters;
# MAGIC >   raise max or spread the schedule.
# MAGIC > - Warehouses are SQL-only, Photon-always-on; notebooks needing Python
# MAGIC >   use other compute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The SQL editor toolchain
# MAGIC - **Saved queries** — named, shareable, versionable objects.
# MAGIC - **Parameters** — `:param` markers make one query serve many values;
# MAGIC   dashboards bind widgets to them (section 06).
# MAGIC - **Schedules** — a saved query can run on a cadence (its warehouse wakes
# MAGIC   up, runs, auto-stops).
# MAGIC - **Alerts** — a scheduled query + a condition on its first row + a
# MAGIC   notification destination: "tell me when revenue < X" without a
# MAGIC   dashboard open (evaluation latency = the schedule interval).
# MAGIC - **Query history & profile** — every run, who/when/how long, with a
# MAGIC   per-operator breakdown (section 05 lives here).
# MAGIC
# MAGIC ```sql
# MAGIC -- a parameterized saved query (the editor prompts for :min_value)
# MAGIC SELECT c.name, sum(o.quantity * p.unit_price) AS revenue
# MAGIC FROM orders o JOIN customers c USING (customer_id)
# MAGIC               JOIN products  p USING (product_id)
# MAGIC WHERE o.status = 'completed'
# MAGIC GROUP BY c.name
# MAGIC HAVING revenue >= :min_value
# MAGIC ORDER BY revenue DESC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live checks — history and warehouse visibility

# COMMAND ----------

probes = {
    "my queries, last 24h": """
        SELECT statement_type, count(*) AS queries
        FROM system.query.history
        WHERE start_time > current_timestamp() - INTERVAL 1 DAY
          AND executed_by = current_user()
        GROUP BY statement_type ORDER BY queries DESC
    """,
}
for label, sql in probes.items():
    try:
        rows = spark.sql(sql).collect()  # noqa: F821
        print(f"-- {label}")
        for r in rows[:10]:
            print(f"   {list(r)}")
        if not rows:
            print("   (no rows)")
    except Exception as e:
        print(f"-- {label}: not accessible here ({type(e).__name__}) — "
              "the Query History UI shows the same data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which tool answers which ask (the section in one table)
# MAGIC
# MAGIC | Ask | Tool |
# MAGIC |---|---|
# MAGIC | "same report, different region each time" | query **parameter** |
# MAGIC | "refresh the numbers every morning" | **scheduled** query / dashboard schedule |
# MAGIC | "ping me when the KPI crosses a line" | **alert** |
# MAGIC | "why was Monday's run slow" | **query history + profile** (section 05) |
# MAGIC | "let the team reuse this logic" | **saved query** — or promote it to a **view** (notebook 01) |
# MAGIC
# MAGIC Deep dive on warehouse cost levers: engineering
# MAGIC [`06-cost-and-performance-optimisation`](../../data-engineer-professional/06-cost-and-performance-optimisation/).
