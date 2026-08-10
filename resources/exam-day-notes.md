# Exam-Day Notes

Logistics and tactics that apply to every Databricks exam. Verify current
logistics on the official exam page ~2 weeks out — formats drift.

## The shape of every exam

45–60 multiple-choice/multi-select questions, 90–120 minutes, ~70% to pass,
online proctored, $200, credential valid 2 years. That works out to **~2
minutes per question** — comfortable if you don't stall.

## Time tactics

Answer every question on first pass — there's no penalty for guessing, so
never leave blanks. Flag anything that takes more than 90 seconds and move
on; a first pass through all questions banks the easy points and often a
later question jogs the flagged one loose. Budget the last 15 minutes for
flagged reviews only, and resist rewriting confident answers — first
instincts on recognition questions are usually right.

## Reading tactics

Read the **last sentence first** — it holds the actual ask ("which is
CORRECT", "which is NOT", "the MOST cost-effective", "the FIRST step").
Most wrong answers in these exams are real features at the wrong layer, so
name the layer before reading options (see the
[cheat sheet](cross-exam-cheat-sheet.md)'s meta-skill note). For code
questions, execute the snippet mentally line by line before looking at the
choices — the options are engineered to match plausible misreadings.

Words that decide answers: *incremental* (→ COPY INTO / Auto Loader),
*without affecting users* (→ shadow), *only new files*, *idempotent*,
*low-latency* (→ serving/online), *across all tables* (→ ABAC/policy),
*without writing code* (→ AutoML / managed feature), *at scale* (→ the
distributed option).

## The week before

Run one **full timed mock** per exam on [TestHub](https://testlogichub.com)
under real conditions — the time pressure, not the questions, is what mocks
train. Re-run the highest-weight section's notebooks and predict every cell
before executing; wrong predictions are your remaining study list. Skim
each section README's "How to study" closer — they hold the section's most
recurrent traps.

## Proctoring practicalities

Clean desk, government ID, stable connection, no second monitor, webcam on
throughout. You cannot pause. Bathroom first; water in a clear container is
typically fine. If the proctor software fails mid-exam, note the time and
contact support — sessions are recoverable.
