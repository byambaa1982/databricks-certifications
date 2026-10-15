# Video companion notebooks

The notebooks behind the **Emma on Data** YouTube videos, one file per video, exported from the exact cells the
video shows. Import a file into any Databricks workspace (Workspace > Import, or clone this repo as a Git folder)
and run it top to bottom; serverless compute is enough and Free Edition works.

Each notebook maps to a section of a certification track in this repo, so after the video you can go deeper in the
matching folder.

| # | Video | Notebook | Track / section |
|---|---|---|---|
| 01 | Delta time travel (coming) | [`data-engineer-associate/01_delta_time_travel.py`](data-engineer-associate/01_delta_time_travel.py) | [DE Associate 03 — Data Transformation and Modeling](../data-engineer-associate/03-data-transformation-and-modeling/) |

Conventions:

- Folders follow the track names used in this repo (`data-engineer-associate/`, `data-engineer-professional/`, ...).
- Files are numbered in release order and stored as Databricks notebook source (`# Databricks notebook source`).
- Every cell in a video notebook was run before publishing; the outputs you see in the video are real.
- Found a mistake? Open an issue with the video number and the cell.
