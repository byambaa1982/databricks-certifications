# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Git folders and the Databricks CLI
# MAGIC
# MAGIC CI/CD starts with two pieces of plumbing: **Git folders** (the workspace
# MAGIC end of version control) and the **CLI** (the automation end). The exam
# MAGIC checks you know what each does — and what each *doesn't*.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Git folders (Repos) — the development side
# MAGIC A Git folder is a clone of a remote repo (GitHub, GitLab, Azure DevOps, …)
# MAGIC living in the workspace. In it you can: **clone, create/switch branches,
# MAGIC commit & push, pull, and open PRs** (the PR itself happens on the Git
# MAGIC provider).
# MAGIC
# MAGIC | Belongs in git | Does NOT belong |
# MAGIC |---|---|
# MAGIC | notebooks (stored as source files), Python modules, SQL, bundle YAML, tests | data, secrets, cluster state, job **run** history |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - The standard flow is **feature branch in your own Git folder → PR →
# MAGIC >   merge**; production runs from a folder (or bundle deploy) pinned to
# MAGIC >   `main` — nobody edits production notebooks in place.
# MAGIC > - Notebooks in Git folders are stored as **source** (`.py` with
# MAGIC >   `# Databricks notebook source` headers — exactly like this repo), so
# MAGIC >   diffs and reviews work.
# MAGIC > - A Git folder is a *workspace* copy: `git pull` is how it learns about
# MAGIC >   merges. Jobs that reference a Git folder path run **whatever is checked
# MAGIC >   out**; jobs configured with a **git source** instead fetch a fresh
# MAGIC >   commit per run — that distinction is a question.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Databricks CLI — the automation side
# MAGIC The CLI (v0.2xx, a single binary) is what CI runners use. Auth comes from
# MAGIC **profiles** (`~/.databrickscfg`) or environment variables
# MAGIC (`DATABRICKS_HOST`, `DATABRICKS_TOKEN` / OAuth) — CI uses env vars with a
# MAGIC **service principal**, never a person's PAT.
# MAGIC
# MAGIC ```bash
# MAGIC databricks auth login --host https://<workspace>   # interactive OAuth
# MAGIC databricks clusters list                            # any REST API, as commands
# MAGIC databricks workspace export-dir /Shared/proj ./out
# MAGIC databricks bundle validate                          # the CI/CD workhorses
# MAGIC databricks bundle deploy -t test
# MAGIC databricks bundle run nightly_orders -t test
# MAGIC ```
# MAGIC
# MAGIC > **Exam trap:** UI actions are manual and unrepeatable; the CLI/bundles
# MAGIC > make the same actions **scriptable** — any answer that has a human
# MAGIC > clicking through the UI inside a CI pipeline is wrong.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where this notebook itself fits
# MAGIC This repo *is* the pattern: notebooks as `.py` source files, one folder per
# MAGIC concern, deployable assets (the sister repo lakehouse-starter) in a bundle.
# MAGIC Clone it into a Git folder and you've done the first half of this section
# MAGIC by hand.

# COMMAND ----------

import subprocess  # noqa: E402

try:
    out = subprocess.run(["databricks", "--version"], capture_output=True, text=True, timeout=30)
    print(out.stdout.strip() or out.stderr.strip())
except FileNotFoundError:
    print("databricks CLI not on this compute's PATH — normal; CI runners and "
          "laptops are where the CLI lives")
