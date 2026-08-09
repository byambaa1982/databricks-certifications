# 07 — Developing, Sharing, and Maintaining AI/BI Genie Spaces (12%)

The newest topic on the exam. Genie is only as good as its curation, and the
seven levers — curated tables (few and gold), comments, instructions, sample
questions, trusted assets, benchmarks, feedback — are the section's
objectives almost verbatim. One field-guide notebook, including the SQL you
actually write (a trusted-asset table function).

## Run order

| Notebook | What you practice |
|---|---|
| [`01_genie_spaces.py`](01_genie_spaces.py) | The curation-levers table, the trusted-asset SQL function shape, permissions and per-asker filtering, symptom → lever traps ("wrong revenue" → trusted asset, "can't answer X" → scope, "did we regress" → benchmarks), the end-to-end setup recipe |

## Setup

Conceptual + reference; the trusted-asset function targets the `da_assoc`
tables from section 04 if you want to run it and build a real space on top.

## How to study

1. Map symptom → lever until it's reflexive — that mapping *is* most of the
   questions: wrong metric → trusted asset/instruction; missing topic →
   table scope; regression fear → benchmarks; steady improvement → feedback
   loop.
2. Remember the scoping philosophy: a few gold tables per space, more spaces
   per domain — never one space with everything.
3. Then drill this section on [TestHub](https://testlogichub.com) — 12% ≈ 5
   of 45 questions.
