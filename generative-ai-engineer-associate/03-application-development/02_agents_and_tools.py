# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Agents and tools: the loop, run live
# MAGIC
# MAGIC Function calling is a **protocol**, and the agent loop is mechanical —
# MAGIC so this notebook runs it with a stubbed LLM, deterministically. Every
# MAGIC message role and every branch is visible; swap the stub for a real
# MAGIC endpoint and nothing else changes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The protocol
# MAGIC 1. You send messages **plus tool schemas** (name, description, JSON
# MAGIC    params).
# MAGIC 2. The model either answers, or returns **`tool_calls`** — a request
# MAGIC    that *you* execute the tool.
# MAGIC 3. You run it, append the result as a **`role: "tool"`** message, and
# MAGIC    call the model again.
# MAGIC 4. Repeat until the model answers in prose. **The LLM never executes
# MAGIC    anything — your code does.** (That sentence is an exam answer.)

# COMMAND ----------

import json  # noqa: E402

TOOLS = [{
    "type": "function",
    "function": {
        "name": "lookup_order",
        "description": "Get an order's status and ETA by order id",
        "parameters": {"type": "object",
                       "properties": {"order_id": {"type": "integer"}},
                       "required": ["order_id"]},
    },
}]

def lookup_order(order_id: int) -> dict:          # the actual tool
    return {"order_id": order_id, "status": "shipped", "eta_days": 2}

def stub_llm(messages, tools):
    """Deterministic stand-in for client.chat.completions.create."""
    last = messages[-1]
    if last["role"] == "user" and "order" in last["content"].lower():
        return {"tool_calls": [{"id": "call_1", "function": {
            "name": "lookup_order", "arguments": '{"order_id": 5001}'}}]}
    if last["role"] == "tool":
        data = json.loads(last["content"])
        return {"content": f"Order {data['order_id']} is {data['status']}; "
                           f"ETA {data['eta_days']} days."}
    return {"content": "How can I help?"}

# COMMAND ----------

# the agent loop — this exact structure, real or stubbed
messages = [{"role": "system", "content": "You are a support agent."},
            {"role": "user", "content": "Where is order 5001?"}]

for step in range(5):                              # ALWAYS cap iterations
    resp = stub_llm(messages, TOOLS)
    if "tool_calls" in resp:
        for tc in resp["tool_calls"]:
            args = json.loads(tc["function"]["arguments"])
            print(f"step {step}: model requested {tc['function']['name']}({args})")
            result = {"lookup_order": lookup_order}[tc["function"]["name"]](**args)
            messages.append({"role": "tool", "tool_call_id": tc["id"],
                             "content": json.dumps(result)})
    else:
        print(f"step {step}: final answer -> {resp['content']}")
        break

# COMMAND ----------

# MAGIC %md
# MAGIC > **Exam traps:**
# MAGIC > - The tool **description** is the model's only basis for choosing it —
# MAGIC >   "agent never uses the right tool" → fix descriptions/parameter docs
# MAGIC >   before anything else.
# MAGIC > - Tool results go back as **`role: "tool"`** messages tied to the
# MAGIC >   `tool_call_id` — feeding them in as user text breaks the protocol.
# MAGIC > - **Cap the loop** (max iterations, timeouts) — agents can cycle;
# MAGIC >   unbounded loops are the cost/latency incident.
# MAGIC > - Tool errors: return the error **as the tool message** so the model
# MAGIC >   can recover/rephrase — swallowing it stalls the loop.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governed tools: Unity Catalog functions
# MAGIC A SQL/Python UC function is a tool with governance built in — grants
# MAGIC (`EXECUTE`), lineage, one definition shared by every agent:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION dev_catalog.genai_assoc.lookup_order(order_id INT)
# MAGIC RETURNS STRING
# MAGIC RETURN (SELECT to_json(struct(status, eta_days))
# MAGIC         FROM dev_catalog.genai_assoc.orders WHERE id = order_id);
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC from unitycatalog.ai.langchain.toolkit import UCFunctionToolkit
# MAGIC toolkit = UCFunctionToolkit(function_names=["dev_catalog.genai_assoc.lookup_order"])
# MAGIC agent_tools = toolkit.tools          # ready for any agent framework
# MAGIC ```
# MAGIC
# MAGIC > **Exam trap:** the agent runs tools with **governed permissions** —
# MAGIC > least privilege on the function is the defense against "the agent was
# MAGIC > talked into reading the salaries table" (sections 05's story).
# MAGIC > Retrievers are tools too: a Vector Search index is just a governed
# MAGIC > lookup function from the agent's point of view.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-agent — when one agent's job gets too big
# MAGIC One agent + 15 tools + 3 pages of instructions = wrong tool choices and
# MAGIC drifting behavior. The fix is decomposition, same logic as chains:
# MAGIC
# MAGIC | Pattern | Shape | Use |
# MAGIC |---|---|---|
# MAGIC | **supervisor** | router agent delegates to specialist agents (billing / tech / returns), each with few tools + tight instructions | the default multi-agent answer |
# MAGIC | pipeline of agents | researcher → writer → reviewer | stages need genuinely different behavior |
# MAGIC | agent-as-tool | a whole agent exposed as one callable tool | compose without re-architecting |
# MAGIC
# MAGIC Signals to split: unrelated tool clusters, instructions with many "but
# MAGIC if…" branches, evaluation showing failures concentrated in one duty.
# MAGIC **MCP** (section 04) standardizes how any of these reach governed
# MAGIC Databricks tools.
