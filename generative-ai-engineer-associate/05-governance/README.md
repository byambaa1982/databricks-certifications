# 05 — Governance (8%)

Three obligations, each with a platform mechanism: PII masked **before
embedding** (query-time masking is too late), prompt injection defended in
layers (Gateway filters + data-not-instructions + least-privilege tools),
and licensing checked for both models and corpus (open weights still carry
licenses).

| Notebook | What you practice |
|---|---|
| [`01_genai_governance.py`](01_genai_governance.py) | The four-stage PII table with the before-embedding trap, the attack → defense table incl. indirect injection, the licensing checklist |

**Study:** governance answers are stage answers — name *where* the control
must sit (ingestion vs serving vs logging) and the option picks itself.
8% ≈ 4 of 45 questions on [TestHub](https://testlogichub.com).
