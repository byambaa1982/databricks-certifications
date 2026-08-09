# 05 — Implementing CI/CD (10%)

New as a standalone section in the May 2026 guide. Two halves: **Git folders**
(the development side — branch, PR, pull; notebooks stored as diffable source)
and **bundles + CLI** (the automation side — one YAML definition deployed to
dev/test/prod with per-target variables). Reference-first by nature, anchored
to a real working example: the sister repo lakehouse-starter is a deployable
bundle.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_git_folders_and_cli.py`](01_git_folders_and_cli.py) | What Git folders can and can't do, what belongs in git, checked-out-folder vs git-source jobs, CLI auth (profiles vs env vars, service principals in CI), the UI-clicks-in-CI anti-pattern |
| [`02_asset_bundles.py`](02_asset_bundles.py) | Reading `databricks.yml` (variables, targets, `run_as`), `mode: development` vs `production` table, declarative-deploy drift semantics, the four commands placed in a PR → test → prod flow |

## Setup

None. For the hands-on version: clone
[lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter) into a
Git folder and run `databricks bundle validate` / `deploy -t dev`.

## How to study

1. The section compresses to three facts: notebooks in Git folders are
   **source files** (reviewable); bundles make environments **reproducible**
   (and UI drift gets overwritten); prod runs as a **service principal**.
   Most questions test one of the three.
2. Know which command goes where in the flow: `validate` on PR, `deploy -t
   test` on merge, `run` for smoke tests, `deploy -t prod` on release.
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 10% weight ≈ 4–5 of 45 questions.
