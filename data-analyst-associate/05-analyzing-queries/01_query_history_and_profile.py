# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Query history and the query profile
# MAGIC
# MAGIC "Why was it slow?" has a workflow: find the run in **query history**, open
# MAGIC its **query profile**, and read where the time went. The exam tests
# MAGIC whether you know which pane answers which suspicion. Uses the `da_assoc`
# MAGIC tables from section 04's setup.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "da_assoc")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
spark.sql(f"USE {CATALOG}.{SCHEMA}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query history — every run, findable
# MAGIC The Query History UI lists every statement on every warehouse: who, when,
# MAGIC duration, status, warehouse — filterable by all of them. The same data is
# MAGIC SQL (where granted):

# COMMAND ----------

try:
    display(  # noqa: F821
        spark.sql("""
            SELECT executed_by,
                   statement_type,
                   round(total_duration_ms / 1000.0, 1) AS seconds,
                   execution_status
            FROM system.query.history
            WHERE start_time > current_timestamp() - INTERVAL 1 DAY
            ORDER BY total_duration_ms DESC
            LIMIT 10
        """)
    )
except Exception as e:
    print(f"system.query.history not accessible here ({type(e).__name__}) — "
          "the Query History UI shows the same, per statement")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The query profile — where the time went
# MAGIC Open any query in history → **Query profile**. Three numbers frame
# MAGIC everything:
# MAGIC
# MAGIC | Phase | Meaning | If it dominates… |
# MAGIC |---|---|---|
# MAGIC | **Scheduling / queue** | waiting for warehouse capacity | concurrency problem → scale out (section 04) |
# MAGIC | **Compilation / optimization** | planning | usually tiny; huge = very complex SQL |
# MAGIC | **Execution** | running operators | read the operator tree below it |
# MAGIC
# MAGIC Inside execution, per operator: **rows produced**, **time**, memory,
# MAGIC **files/partitions read vs pruned**, and **spill**. The patterns to
# MAGIC recognize:
# MAGIC
# MAGIC | Profile shows | Diagnosis → move |
# MAGIC |---|---|
# MAGIC | scan reads all files, prunes ~0 | no data skipping help → filter on clustered columns, or cluster the table (notebook 02) |
# MAGIC | join outputs **more** rows than both inputs | fan-out (duplicate keys) → fix the join/dedupe (section 04) |
# MAGIC | huge rows early, tiny result | filter late → push conditions into WHERE / pre-aggregate |
# MAGIC | **spill** > 0 | memory pressure → smaller intermediate results, bigger warehouse |
# MAGIC | queue time ≫ run time | warehouse maxed → scaling, off-peak schedules |

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXPLAIN — the plan without running

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN
# MAGIC SELECT c.tier, sum(o.quantity * p.unit_price) AS revenue
# MAGIC FROM orders o
# MAGIC JOIN customers c USING (customer_id)
# MAGIC JOIN products p USING (product_id)
# MAGIC WHERE o.status = 'completed'
# MAGIC GROUP BY c.tier

# COMMAND ----------

# MAGIC %md
# MAGIC Read it bottom-up: scans → joins → aggregate. `PhotonScan`/`Photon…`
# MAGIC operators mean the vectorized engine ran it (always on in SQL
# MAGIC warehouses). A `BroadcastHashJoin` on the small dims is what you *want*
# MAGIC to see for star joins.
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Query history is also the **audit** answer for "who ran what on this
# MAGIC >   warehouse yesterday" — not just performance.
# MAGIC > - The profile is per **statement run**; DESCRIBE HISTORY (notebook 02) is
# MAGIC >   per **table commit**. "Why slow" → profile; "what changed the data" →
# MAGIC >   table history. Questions swap them to see if you notice.
# MAGIC > - Two identical queries with wildly different durations → check the
# MAGIC >   second ran from **cache** (notebook 02) or the first paid warehouse
# MAGIC >   startup/queue.
