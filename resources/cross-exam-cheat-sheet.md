# The Cross-Exam Cheat Sheet

Facts that appear on **multiple** Databricks exams — sometimes near-verbatim.
Learn these once, collect points everywhere. Each item links the folder that
proves it hands-on.

## Unity Catalog (on every single exam)

- **The three-grant path**: reaching a table needs `USE CATALOG` + `USE
  SCHEMA` + the object privilege. "Has SELECT but can't query" → the path.
  ([DE Pro 07](../data-engineer-professional/07-data-security-and-compliance/))
- **Managed vs external DROP**: managed = metadata *and* data deleted
  (`UNDROP` within ~30 days); external = metadata only, files stay.
  ([DE Pro 08](../data-engineer-professional/08-data-governance/))
- Grants inherit **downward**; grant to **groups**; production identities
  are **service principals**.
- **PK/FK are informational** — never enforced. Uniqueness is your job
  (dedupe before MERGE). ([DE Pro 03](../data-engineer-professional/03-data-transformation-cleansing-and-quality/))
- Three label types, three jobs: **comment** = docs, **tag** = queryable
  classification (drives ABAC), **certified** = canonical signal. None grant
  access. ([DA 02](../data-analyst-associate/02-managing-data/))
- Row filters / column masks bind to the **table**, apply to **everyone
  including the owner**; exemptions live inside the UDF.
  ([DE Pro 07](../data-engineer-professional/07-data-security-and-compliance/02_row_filters_and_column_masks.py))

## Delta Lake

- Delta = **Parquet + transaction log**; every property (ACID, time travel,
  schema enforcement, streaming+batch) derives from the log.
  ([DE Assoc 01](../data-engineer-associate/01-databricks-intelligence-platform/))
- **Two retentions**: `delta.logRetentionDuration` (history you can *see*,
  default 30d) vs `delta.deletedFileRetentionDuration` (files you can *time
  travel to*, default 7d). Visible ≠ restorable.
- **OPTIMIZE compacts, VACUUM reclaims** — different jobs. OPTIMIZE leaves
  old files for time travel; VACUUM deletes them and breaks time travel/CDF
  past the window. ([DE Pro 06](../data-engineer-professional/06-cost-and-performance-optimisation/))
- `VERSION AS OF` **reads** the past; `RESTORE` **rewrites** the present
  (and is itself a new commit).
- GDPR delete = DELETE + (PURGE if deletion vectors) + **VACUUM** — DELETE
  alone is not compliant.

## Ingestion (DE both, DA, and the loading halves of ML/GenAI)

- The memory question: **`spark.read` remembers nothing; `COPY INTO`
  remembers in the target's Delta log; Auto Loader remembers in the
  checkpoint.** Delete the memory → reload/re-ingest.
  ([DE Assoc 02](../data-engineer-associate/02-data-ingestion-and-loading/))
- Auto Loader schema evolution (`addNewColumns`) **fails once on purpose**,
  writes the new schema, ingests on restart. `_rescued_data` catches
  type-mismatched values; `_corrupt_record` (PERMISSIVE) catches unparseable
  rows — different mechanisms.
- "Incremental" / "only new files" in a question = COPY INTO or Auto Loader,
  never a plain read.

## Streaming (DE both, Spark Dev, ML deployment)

- **The checkpoint IS the stream's identity**: offsets + state, per query.
  Delete it → full reprocess; change the aggregation → incompatible.
- Aggregation + `append` mode needs a **watermark**; stateful ops without a
  watermark = unbounded state (the slowly-dying job).
- `trigger(availableNow=True)` = drain and stop — the scheduled-job form of
  streaming. ([DE Pro 01/05](../data-engineer-professional/01-developing-code-for-data-processing/05_structured_streaming.py))
- Exactly-once = replayable source + checkpoint + idempotent/transactional
  sink. All three.

## Performance (DE both, DA 05, Spark Dev 04)

- **Size vs scale-out**: one slow query → bigger (size); many users →
  more clusters (scaling). Never interchangeable.
  ([DA 01](../data-analyst-associate/01-data-intelligence-platform/))
- Broadcast the **small** side; sort-merge + double Exchange = the shuffle
  join. AQE fixes stragglers/skew at runtime — before you hand-salt.
- Partition by **low-cardinality only** — better: liquid clustering
  (`CLUSTER BY`, keys changeable in place).
- The standing wrong answers: `df.cache()` as a cure-all, high-cardinality
  partitioning, VACUUM "to speed up queries", all-purpose clusters for
  scheduled jobs, "enable Photon" for a logic problem.
- **Driver OOM** = collect/toPandas/giant broadcast; **executor OOM** =
  skew/shuffle. Fixes don't transfer.

## SQL semantics (every exam with a SELECT in it)

- NULL rules: `count(*)` counts NULLs; `count(col)`/`sum`/`avg` skip them;
  `sum` of only-NULLs is NULL → `coalesce(sum(x), 0)`.
- `WHERE` filters rows → `HAVING` filters groups → `QUALIFY` filters window
  results.
- `dropDuplicates([key])` keeps an **arbitrary** row — "latest per key"
  needs `row_number()` over a descending sort.
- Joining a fact to an SCD2 dim without the current-row/as-of filter
  **fans out**. Current = `__END_AT IS NULL`; as-of = range join.
- SQL `UNION` dedupes, `UNION ALL` doesn't; DataFrame `union()` = UNION ALL
  and is **positional** (`unionByName` aligns).

## MLflow & models (ML both, GenAI)

- Run → registered **version** (`catalog.schema.model`) → **alias**
  (`@champion`/`@challenger`). Aliases replaced stages — stage answers are
  legacy. Consumers load by alias; promotion/rollback = moving the alias.
  ([ML Assoc 01](../machine-learning-associate/01-databricks-machine-learning/03_mlflow_and_model_registry.py))
- **Deploy code, not models**: pipelines promote through envs; models
  retrain per env; validation gates run *before* the alias flips.
  ([ML Pro 02](../machine-learning-professional/02-mlops/))
- Leakage rule: anything learned from data (imputers, scalers, encoders,
  fill values) fits on **train only** — pipelines make it automatic.
- Point-in-time lookups (`timestamp_lookup_key`) prevent **future leakage**;
  batch reads offline tables, real-time needs **online** tables.

## Deployment & rollout (ML both, GenAI, DE 09)

- Freshness question: nobody waiting → batch (`spark_udf`); pipeline →
  streaming; user waiting → serving endpoint. REST-for-nightly-scores is the
  canonical wrong answer.
- Blue-green = full switch, instant rollback; canary = % of real traffic;
  **shadow = copies, zero user exposure**.
- Bundles: `mode: development` prefixes + pauses; prod runs as a service
  principal; UI edits get overwritten — the repo is the source of truth.
  ([DE Pro 09](../data-engineer-professional/09-debugging-and-deploying/))

## The meta-skill

Half of every exam is recognizing the **standing distractor**: a real
feature offered at the wrong layer (a mask when access denial is needed, a
bigger model when retrieval is broken, more workers for a driver OOM).
Name the layer first; then read the options.
