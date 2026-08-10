# 05 — Governance (8%)

Easy points if you know where each control must sit. A runnable PII scrub
at the only stage where scrubbing fully works (ingestion — vectors can't be
un-embedded, and the remediation for a raw-embedded corpus is re-ingest +
rebuild), the attack/defense table including indirect injection via
retrieved documents, AI Gateway as the central unskippable enforcement
point, and the licensing/audit checklist.

| Notebook | What you practice |
|---|---|
| [`01_genai_governance.py`](01_genai_governance.py) | The four-stage PII table with a live regex scrub (+ the NER/`ai_query` second layer), the attack table, the Gateway feature table, licensing and audit |

**Study:** every governance answer is a *stage* answer — name where the
control sits (ingestion / storage / serving / logging) before reading
options, and remember Gateway's justification: central and unskippable.
8% ≈ 4 of 45 questions on [TestHub](https://testlogichub.com).
