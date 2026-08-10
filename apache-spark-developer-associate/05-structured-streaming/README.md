# 05 — Structured Streaming (10%)

The four levers (output mode, trigger, watermark, checkpoint), a live
rate-source stream with `availableNow`, the output-mode legality grid (the
most-tested fact: aggregation + append needs a watermark), watermark
semantics, the three ingredients of exactly-once, and `foreachBatch`.

| Notebook | What you practice |
|---|---|
| [`01_streaming_essentials.py`](01_streaming_essentials.py) | The levers table, a runnable rate-source stream, the legality grid, window+watermark shape, checkpoint/exactly-once rules, `foreachBatch` |

**Study:** memorize the legality grid and the exactly-once triple —
most questions are one cell of the grid or one missing ingredient.
10% ≈ 4–5 of 45 questions on [TestHub](https://testlogichub.com).
