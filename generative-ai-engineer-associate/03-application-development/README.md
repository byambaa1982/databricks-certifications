# 03 — Application Development (30%)

The heaviest section: assembling retriever + prompt + model + tools into an
application, and fixing quality with the cheapest effective knob. The
symptom → knob table is the section's core; around it sit the RAG prompt
contract (ground, admit ignorance, cite), model access tiers (FM API vs
provisioned throughput vs external), the embedding-model-must-match-index
trap, function calling and the agent loop, multi-agent supervision, and
layered guardrails.

| Notebook | What you practice |
|---|---|
| [`01_building_rag_and_agents.py`](01_building_rag_and_agents.py) | Prompt assembly spelled out, the symptom → knob table, model tiers, framework stance (loggable beats fashionable), tool schemas + the agent loop, UC functions as tools, multi-agent, the four guardrail layers |

**Study:** for every quality complaint, name the cheapest knob before
reading options — "bigger model" is the standing distractor. 30% ≈ 13 of 45
questions on [TestHub](https://testlogichub.com).
