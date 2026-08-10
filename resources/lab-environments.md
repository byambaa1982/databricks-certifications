# Lab Environments Map

Every track's runnable notebooks build their practice data in a dedicated
schema under `dev_catalog` (override via the `catalog`/`schema` widgets on
every notebook). This map shows what exists where, what creates it, and what
depends on what — so you can reset or clean up any track without touching
the others.

| Schema | Created by | Contains | Used by |
|---|---|---|---|
| `de_pro` | [DE Pro 01/00_setup](../data-engineer-professional/01-developing-code-for-data-processing/00_setup.py) — **requires a deployed [lakehouse-starter](https://github.com/byambaa1982/lakehouse-starter)** | deep clones of the six medallion tables + `checkpoints` volume | DE Pro sections 01, 03, 05–10 |
| `de_pro` (additions) | [DE Pro 03/00_setup](../data-engineer-professional/03-data-transformation-cleansing-and-quality/00_setup.py) | `bronze_orders_dirty` (planted duplicates/NULLs/outliers) | DE Pro section 03 |
| `de_assoc` | [DE Assoc 02/00_setup](../data-engineer-associate/02-data-ingestion-and-loading/00_setup.py) | `landing` + `checkpoints` volumes with raw JSON/CSV (incl. nested JSON); `mode=more` adds batch 2 | DE Assoc 02; DA 03's read_files demo |
| `de_assoc` (additions) | [DE Assoc 03/00_setup](../data-engineer-associate/03-data-transformation-and-modeling/00_setup.py) | `bronze_orders_assoc`, `bronze_customers_assoc` (synthetic, blemished) | DE Assoc 03, 06, 07 |
| `da_assoc` | [DA 04/00_setup](../data-analyst-associate/04-executing-queries/00_setup.py) | `customers` / `products` / `orders` retail star (orphan order 5009, orderless customer 107) | DA sections 04, 05, 06, 08, 09 |
| `ml_assoc` | [ML Assoc 01/00_setup](../machine-learning-associate/01-databricks-machine-learning/00_setup.py) | `customers_churn` (1,000 hash-deterministic rows, planted NULLs/outliers, ~2:1 imbalance) | all ML Assoc sections |
| `ml_pro` | [ML Pro 01/00_setup](../machine-learning-professional/01-model-development/00_setup.py) | `customers`, `customer_features_history` (two timestamped snapshots), `churn_labels` (observed between them — the leakage trap) | all ML Pro sections |
| `genai_assoc` | [GenAI 02/01_chunking](../generative-ai-engineer-associate/02-data-preparation/01_chunking_and_retrieval_prep.py) | `doc_chunks` (CDF enabled, ready to index) | GenAI 02, 04 |
| — (none) | Spark Dev notebooks | build tiny DataFrames inline / temp dirs | self-contained |

## Design rules the setups share

Everything is **deterministic** — values derive from `hash(id)` or literal
rows, so re-running a setup rebuilds the identical dataset, and every
"predict this cell" answer stays stable. Blemishes (NULLs, outliers,
duplicates, orphans, future-dated snapshots) are **planted on purpose**,
each one placed where a specific lesson needs it to be visible.

## Resetting and removing

Each `00_setup` resets its own schema when re-run; section-final cleanup
cells drop their scratch objects. To remove a whole track's footprint:

```sql
DROP SCHEMA IF EXISTS dev_catalog.<schema> CASCADE;  -- one track, gone
```

The only external dependency in the repo is `de_pro`'s clone source
(lakehouse-starter). Every other schema builds from nothing.
