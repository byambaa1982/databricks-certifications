# 02 — Data Ingestion & Acquisition (7%)

Small section, but its questions are nearly free points once you can answer
one thing instantly: **which loader remembers which files it already loaded,
and where does it keep that memory?** These are **runnable Databricks
notebooks** against a self-contained landing zone of raw JSON/CSV files — no
dependency on lakehouse-starter.

## Run order

| Notebook | What you practice |
|---|---|
| [`00_setup.py`](00_setup.py) | Creates the `landing` volume and seeds batch 1. **Run first; re-run with `mode = more`** to drop batch 2 (new column + one bad value) when a notebook asks for new files. |
| [`01_batch_reads_and_read_options.py`](01_batch_reads_and_read_options.py) | `spark.read` options, PERMISSIVE / DROPMALFORMED / FAILFAST on a genuinely malformed CSV, `_corrupt_record`, multiLine JSON, `read_files()`, CTAS from `json.\`/path\`` — and why none of this is incremental |
| [`02_copy_into.py`](02_copy_into.py) | Schemaless target + `mergeSchema`, idempotent re-runs proved live (second run = 0 rows), `PATTERN` / `FORCE`, load-history gotchas, COPY INTO vs Auto Loader decision table |
| [`03_auto_loader.py`](03_auto_loader.py) | `cloudFiles` with `schemaLocation` + `schemaHints`, the designed `addNewColumns` failure-and-restart demonstrated live, `_rescued_data` catching a bad value, directory listing vs file notification, Streaming Table form |

## Setup

Import this folder into your workspace, open `00_setup.py`, run it (default
`mode = reset`). It creates `dev_catalog.de_pro` if section 01's setup hasn't
already, plus `landing` and `checkpoints` volumes. Every notebook has
`catalog` / `schema` widgets defaulting to `dev_catalog` / `de_pro`.

## How to study

1. **Predict each cell's output before running it** — especially the second
   run of the `COPY INTO` cell and the post-`mode=more` run of the Auto Loader
   cell (it *fails*, and knowing why is the exam point).
2. The batch-2 flow is the heart of the section: run `00_setup` with
   `mode = more` between passes and watch the same new files hit `spark.read`
   (re-reads everything), `COPY INTO` (loads only them, nulls the bad value),
   and Auto Loader (fails once by design, then rescues the bad value).
3. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 7% weight ≈ 4 of 60 questions.
