# 04 — Assembling and Deploying Applications (22%)

The shipping section: Delta Sync vs Direct Access indexes (CDF required,
managed embeddings, triggered vs continuous sync, hybrid search),
models-from-code chain logging with `resources` for auth passthrough,
`agents.deploy` and what it adds (Review App, inference tables, tracing),
managed MCP servers, and the Prompt Registry.

| Notebook | What you practice |
|---|---|
| [`01_vector_search_and_deployment.py`](01_vector_search_and_deployment.py) | The index-type table and full `create_delta_sync_index` shape, `similarity_search` with filters, `log_model` from code with `resources`, `agents.deploy` + Review App, MCP in one paragraph, prompt versioning |

**Study:** the four facts that keep recurring — CDF before Delta Sync,
same embedding model for docs and queries, `resources` = how serving
authenticates to the index, Review App = stakeholder chat without a
frontend. 22% ≈ 10 of 45 questions on [TestHub](https://testlogichub.com).
