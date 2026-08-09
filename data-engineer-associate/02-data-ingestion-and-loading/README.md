# 02 — Data Ingestion and Loading (21%)

The second-heaviest section. Almost every question is the same decision in
different clothes: **which loader remembers what it already loaded, and
where?** `spark.read` remembers nothing, `COPY INTO` remembers in the target's
Delta log, Auto Loader remembers in its checkpoint — and once that's
reflexive, the rest is options syntax and knowing the named connectors.
Runnable against a self-contained landing zone in `dev_catalog.de_assoc`
(separate from the Professional folders' `de_pro`).

## Run order

| Notebook | What you practice |
|---|---|
| [`00_setup.py`](00_setup.py) | Seeds the `landing` volume: JSON orders, **nested** JSON customers, a malformed CSV. `mode = more` adds batch 2 (new column + one bad value) for the incremental demos. |
| [`01_batch_and_incremental_patterns.py`](01_batch_and_incremental_patterns.py) | The three loading patterns, full-reload traps, `COPY INTO` idempotency proved live (second run = 0 rows), `mergeSchema`, `force`, and the source → tool table (Lakeflow Connect, Federation, JDBC, REST) |
| [`02_auto_loader.py`](02_auto_loader.py) | `cloudFiles` with `schemaLocation`, schema enforcement via `_rescued_data`, the designed fail-once-then-evolve behavior on new columns, evolution modes, `STREAMING TABLE` + `STREAM read_files` recognition |
| [`03_semi_structured_data.py`](03_semi_structured_data.py) | Nested JSON → structs/arrays, dot paths, `explode` vs `explode_outer`, the `:` operator vs `from_json` (+ `schema_of_json`), VARIANT in one line, volumes + `binaryFile` for unstructured data |

## Setup

Run `00_setup.py` (default `mode = reset`). Widgets default to
`dev_catalog` / `de_assoc` everywhere in this track.

## How to study

1. Drill the memory question: *where does each loader keep its state, and
   what happens when that state is deleted?* (Table dropped → COPY INTO
   reloads; checkpoint deleted → Auto Loader re-ingests.) That single axis
   decides most of this section.
2. Run the batch-2 flow (`mode = more`) end to end and watch the same files
   hit `COPY INTO` (loads only them) and Auto Loader (fails once by design,
   then rescues the bad value).
3. Deep-dive pointer: the Professional folder's
   [`02-data-ingestion-and-acquisition`](../../data-engineer-professional/02-data-ingestion-and-acquisition/)
   covers the same ground with more edge cases.
4. Then drill this section on [TestHub](https://testlogichub.com) under time
   pressure — 21% weight ≈ 9–10 of 45 questions.
