# 01 — Design Applications (14%)

Two notebooks, two design layers. First, prompts as engineered artifacts:
the five-slot anatomy (and which slot each requirement belongs in), a
runnable prompt builder, technique selection with the RAG-vs-fine-tuning
pair, and structured output with the exact `json_schema` shape — including
what it *guarantees* vs `json_object` vs prompt-begging. Second, model and
chain design: the three access doors (pay-per-token vs provisioned
throughput vs external, and why external models still go through an
endpoint), the four selection axes with a runnable context-budget check,
and a worked chain decomposition showing why stages beat mega-prompts.

## Run order

| Notebook | What you practice |
|---|---|
| [`01_design_and_prompting.py`](01_design_and_prompting.py) | Prompt anatomy + builder, requirement → technique table, RAG vs fine-tuning, temperature and `max_tokens` traps, structured output end to end |
| [`02_model_selection_and_chains.py`](02_model_selection_and_chains.py) | The three access doors, four selection axes, context-window arithmetic run live, the email-triage chain decomposition, agent vs fixed chain, Agent Bricks |

## How to study

1. For every scenario, name the slot/door/stage before reading options:
   per-turn rule → system slot; changing knowledge → RAG not fine-tuning;
   steady volume → provisioned throughput; enforceable rule → code between
   stages.
2. Do the context-window arithmetic by hand once — input *and* output share
   the window, and one question usually hinges on exactly that.
3. Then drill this section on [TestHub](https://testlogichub.com) — 14% ≈ 6
   of 45 questions.
