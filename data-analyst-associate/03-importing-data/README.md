# 03 — Importing Data (5%)

The smallest section: five doors for getting data in, and the boundary
between the analyst's doors (UI upload, `read_files`/CTAS, `COPY INTO`,
Delta Sharing, Marketplace) and engineering's (Auto Loader, Lakeflow Connect
for continuous feeds and operational systems). Recognizing that boundary is
most of the section.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_getting_data_in.py`](01_getting_data_in.py) | The five-doors table, UI upload facts (≤ ~2GB, creates a managed UC table), `read_files` live peek + CTAS vs `COPY INTO` (the "incremental" keyword), receiving a Delta share / Marketplace listing as a read-only catalog |

## Setup

Self-contained; the `read_files` demo reuses the
`data-engineer-associate` landing volume if you've built it, and degrades
gracefully if not.

## How to study

1. Match keywords to doors: "on my laptop" → upload; "already in storage,
   once" → CTAS; "keeps arriving" → `COPY INTO`; "vendor shares" → Delta
   Sharing; "continuous/API/database" → engineering.
2. Remember what shared data is: **live and read-only** — no refresh jobs to
   schedule, and transformations happen on *your* copy.
3. Then drill this section on [TestHub](https://testlogichub.com) — 5% ≈ 2–3
   of 45 questions. Don't over-invest.
