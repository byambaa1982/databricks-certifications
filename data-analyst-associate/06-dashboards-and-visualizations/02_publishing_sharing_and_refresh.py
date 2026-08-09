# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Publishing, sharing, refresh, and alerts
# MAGIC
# MAGIC Building the dashboard is half the section; the other half is its
# MAGIC lifecycle: draft vs published, **whose credentials the viewers use** (the
# MAGIC most-tested fact), schedules, subscriptions, embedding, and alerts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Draft vs published
# MAGIC You edit the **draft**; viewers see the **published** version. Publishing
# MAGIC is an explicit act — half-finished edits never leak; re-publish to ship
# MAGIC changes. (The "my stakeholders don't see my fix" answer is almost always
# MAGIC "you didn't re-publish".)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The credentials decision — learn this cold
# MAGIC Publishing asks how viewer queries authenticate:
# MAGIC
# MAGIC | | **Embedded credentials** (default) | **Viewer's credentials** |
# MAGIC |---|---|---|
# MAGIC | Queries run as | the publisher | each viewer |
# MAGIC | Viewers need table access? | **no** — they see whatever the publisher can see | yes — UC grants per viewer |
# MAGIC | Row filters/masks apply to | the publisher's identity | each viewer's identity |
# MAGIC | Use when | broad audiences, viewers without data access | per-viewer security must hold (masked/filtered data) |
# MAGIC
# MAGIC > **Exam trap:** embedded credentials + a row-filtered table = every
# MAGIC > viewer sees the *publisher's* rows. If per-viewer filtering matters,
# MAGIC > publish with **viewer credentials** (and grant the viewers).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sharing, schedules, subscriptions, embedding
# MAGIC - **Sharing** uses standard permission levels (Can view / edit / manage);
# MAGIC   share with users, groups, or "anyone in the workspace" (or the
# MAGIC   account, for wider reach).
# MAGIC - A **schedule** re-runs the datasets on a cadence (its warehouse wakes,
# MAGIC   runs, stops); viewers then open a warm, current dashboard.
# MAGIC - **Subscriptions** email a snapshot on each scheduled refresh — the
# MAGIC   "send it to leadership every Monday" answer.
# MAGIC - **Embedding** puts a published dashboard in an external site/app
# MAGIC   (workspace embedding settings + allowed domains govern it).
# MAGIC
# MAGIC ## Alerts — the numbers watch themselves
# MAGIC An alert = a query + a condition + a schedule + notification
# MAGIC destinations. "Tell me when weekly revenue drops below X" is an alert on
# MAGIC the gold query — not a human re-opening the dashboard. Evaluation
# MAGIC latency = the alert's schedule interval.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The scenario table (the section, compressed)
# MAGIC
# MAGIC | Scenario | Answer |
# MAGIC |---|---|
# MAGIC | stakeholders see stale layout after your fix | re-**publish** the draft |
# MAGIC | viewers without table grants must see the dashboard | embedded credentials |
# MAGIC | masked/filtered per-viewer data must stay per-viewer | **viewer** credentials |
# MAGIC | "email the deck to execs every Monday 8am" | schedule + **subscription** |
# MAGIC | numbers must be fresh when the team opens it at 9 | schedule the refresh before 9 |
# MAGIC | "notify us when the KPI crosses the line" | **alert** |
# MAGIC | dashboard inside the company portal | embedding (allowed domains) |
# MAGIC | "can they explore beyond my charts?" | pair the dashboard with a **Genie space** (section 07) |
