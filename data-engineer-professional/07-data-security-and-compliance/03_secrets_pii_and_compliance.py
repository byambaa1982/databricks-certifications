# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Secrets, PII deletion, and audit
# MAGIC
# MAGIC The compliance triangle: keep credentials out of code (**secrets**), be able
# MAGIC to actually forget people (**GDPR/CCPA deletes** — harder on Delta than it
# MAGIC looks), and prove who touched what (**audit logs**).

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")  # noqa: F821
dbutils.widgets.text("schema", "de_pro")  # noqa: F821
CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secrets — reference by scope/key, never paste

# COMMAND ----------

try:
    scopes = dbutils.secrets.listScopes()  # noqa: F821
    print(f"scopes visible here: {[s.name for s in scopes] or '(none)'}")
except Exception as e:
    print(f"secret scopes not listable ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC token = dbutils.secrets.get(scope="prod-scope", key="api-token")
# MAGIC print(token)          # prints [REDACTED] — values are masked in notebook output
# MAGIC ```
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - Notebook output **redacts** secret values, but redaction is a
# MAGIC >   display-level courtesy, not encryption — anyone with `READ` on the scope
# MAGIC >   can still exfiltrate a secret in code. Govern the **scope ACLs**.
# MAGIC > - In SQL (connections, `CREATE CONNECTION` options):
# MAGIC >   `secret('scope','key')` — same idea, credentials never in DDL text.
# MAGIC > - Two scope backends: Databricks-backed and Azure Key Vault-backed
# MAGIC >   (read-only from Databricks' side).

# COMMAND ----------

# MAGIC %md
# MAGIC ## The "right to be forgotten" — DELETE is only step one
# MAGIC On Delta, `DELETE FROM users WHERE user_id = X` removes the row from the
# MAGIC **current version**. The data still exists in: old file versions (time
# MAGIC travel), the Change Data Feed, and any downstream tables you copied it to.
# MAGIC The compliant sequence:
# MAGIC
# MAGIC 1. `DELETE FROM` every table holding the subject's rows (propagate
# MAGIC    downstream — CDF is the clean way to *find* what changed where).
# MAGIC 2. `VACUUM` each table past its retention so deleted files are physically
# MAGIC    removed (section 06's mechanics — retention window applies).
# MAGIC 3. If deletion vectors are enabled, `REORG TABLE … APPLY (PURGE)` first so
# MAGIC    soft-deleted rows are rewritten out of the files, *then* VACUUM.
# MAGIC 4. Remember CDF retention: change records with the PII age out with the log.
# MAGIC
# MAGIC > **Exam trap:** "we ran DELETE, are we GDPR-compliant?" — **No**, until
# MAGIC > VACUUM (and PURGE, with DVs) has removed the physical files, time travel
# MAGIC > can still read the person's data. This is *the* recurring compliance
# MAGIC > question.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Designing for deletability
# MAGIC - Keep PII in **few, known tables** keyed by user id; reference elsewhere by
# MAGIC   surrogate key (section 10's modelling advice, motivated by compliance).
# MAGIC - Or **pseudonymize at ingestion** (hash/tokenize) so raw PII never lands in
# MAGIC   the lake at all — deleting the token-map entry orphans the rest.
# MAGIC - Column masks (notebook 02) reduce *exposure*; they don't reduce
# MAGIC   *retention* — masked data still needs the delete pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit — who did what, account-wide

# COMMAND ----------

try:
    display(  # noqa: F821
        spark.sql("""
            SELECT event_time, user_identity.email AS actor, service_name, action_name
            FROM system.access.audit
            WHERE event_date >= current_date() - INTERVAL 7 DAYS
            ORDER BY event_time DESC
            LIMIT 20
        """)
    )
except Exception as e:
    print(f"system.access.audit not accessible here ({type(e).__name__})")

# COMMAND ----------

# MAGIC %md
# MAGIC | Question | Table |
# MAGIC |---|---|
# MAGIC | who ran what / granted what / logged in | `system.access.audit` |
# MAGIC | who **read** which table (lineage-grade) | `system.access.table_lineage` (section 08) |
# MAGIC | which queries hit a warehouse | `system.query.history` |
# MAGIC
# MAGIC Encryption posture, one line each (know they exist): storage is encrypted
# MAGIC at rest by the cloud provider by default; **customer-managed keys** are the
# MAGIC answer when the question says "we must control/rotate the keys"; traffic is
# MAGIC TLS in transit.
