# 06 — Working with Dashboards and Visualizations (16%)

The third-heaviest section, UI-built and so a field guide: the
dataset → widget model (one dataset, many widgets), the
visualization-matching table, filters vs parameters vs cross-filtering, and
then the lifecycle — draft vs published, the embedded-vs-viewer credentials
decision (the section's most-tested fact), schedules, subscriptions,
embedding, and alerts.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_building_dashboards.py`](01_building_dashboards.py) | Datasets feeding widgets, the question-shape → widget table, filters vs parameters (result vs SQL), cross-filtering, a runnable gold dataset query, thin-dataset-over-gold-table hygiene |
| [`02_publishing_sharing_and_refresh.py`](02_publishing_sharing_and_refresh.py) | Draft vs published, embedded vs viewer credentials (and the row-filter interaction trap), sharing levels, schedules + subscriptions, embedding, alerts, the scenario table |

## Setup

Section 04's `00_setup.py` supplies the tables the example dataset queries;
the dashboard itself you build in the UI — take the dataset query from
notebook 01 and reproduce the flow for real.

## How to study

1. The credentials question is nearly guaranteed: embedded = publisher's
   access for everyone (convenient, ignores per-viewer filters); viewer =
   per-person enforcement (requires grants). Decide from whether per-viewer
   security matters.
2. Filters act on results, parameters act on SQL, cross-filtering is free —
   three different mechanisms, one per question.
3. Then drill this section on [TestHub](https://testlogichub.com) — 16% ≈ 7
   of 45 questions.
