# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Deployment strategies and serving APIs
# MAGIC
# MAGIC The smallest section: three rollout strategies, how Model Serving's
# MAGIC traffic config implements two of them, and the two ways to query an
# MAGIC endpoint. Builds directly on the Associate's
# MAGIC [deployment notebook](../../machine-learning-associate/04-model-deployment/01_inference_and_serving.py).

# COMMAND ----------

# MAGIC %md
# MAGIC ## The three strategies — know them as risk postures
# MAGIC
# MAGIC | Strategy | How | Risk / cost profile |
# MAGIC |---|---|---|
# MAGIC | **Blue-green** | run old (blue) and new (green) side by side; **switch all traffic at once**, keep blue warm for instant rollback | binary cutover; simple; brief 2× capacity |
# MAGIC | **Canary** | route a **small %** to the new version, watch, ramp up | gradual exposure; needs live metrics to judge |
# MAGIC | **Shadow** | new version receives **copies** of traffic, responses discarded | zero user risk; costs full duplicate compute; validates behavior, not user impact |
# MAGIC
# MAGIC > **Exam traps:**
# MAGIC > - "Test v2 against production traffic **without affecting users**" →
# MAGIC >   **shadow** (not canary — canary users get v2's answers).
# MAGIC > - "Gradually shift real traffic while monitoring" → **canary**.
# MAGIC > - "Instant rollback with a full switch" → **blue-green** (rollback =
# MAGIC >   flip back; with aliases, move `@champion` back one version).
# MAGIC > - Traffic split on a serving endpoint implements blue-green (100/0 →
# MAGIC >   0/100) and canary (90/10 → …); shadow needs the copy-and-discard
# MAGIC >   plumbing (e.g., inference-table replay), not a traffic split.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The endpoint config that does it
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC w.serving_endpoints.update_config(
# MAGIC     name="churn-endpoint",
# MAGIC     served_entities=[
# MAGIC         {"name": "champion",  "entity_name": "prod.ml.churn_model",
# MAGIC          "entity_version": "3", "workload_size": "Small",
# MAGIC          "scale_to_zero_enabled": True},
# MAGIC         {"name": "challenger", "entity_name": "prod.ml.churn_model",
# MAGIC          "entity_version": "4", "workload_size": "Small",
# MAGIC          "scale_to_zero_enabled": True},
# MAGIC     ],
# MAGIC     traffic_config={"routes": [
# MAGIC         {"served_model_name": "champion",   "traffic_percentage": 90},
# MAGIC         {"served_model_name": "challenger", "traffic_percentage": 10},   # canary
# MAGIC     ]},
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Enable the endpoint's **inference table** (auto-logged requests +
# MAGIC responses to a Delta table) and section 02's monitoring attaches straight
# MAGIC to it — that's how canary judgment gets its data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying the endpoint — the two shapes
# MAGIC
# MAGIC ```python
# MAGIC # 1) MLflow Deployments SDK
# MAGIC from mlflow.deployments import get_deploy_client
# MAGIC client = get_deploy_client("databricks")
# MAGIC preds = client.predict(endpoint="churn-endpoint",
# MAGIC                        inputs={"dataframe_split": df.to_dict(orient="split")})
# MAGIC
# MAGIC # 2) plain REST
# MAGIC # POST https://<workspace>/serving-endpoints/churn-endpoint/invocations
# MAGIC # Authorization: Bearer <token>
# MAGIC # {"dataframe_split": {"columns": [...], "data": [[...], ...]}}
# MAGIC ```
# MAGIC
# MAGIC Accepted payload formats: `dataframe_split`, `dataframe_records`,
# MAGIC `instances`/`inputs` (tensor styles). The model's logged **signature**
# MAGIC validates them — a schema mismatch is a 4xx from the endpoint, not a
# MAGIC silent wrong answer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom PyFunc behind an endpoint — the assembly recap
# MAGIC 1. Wrap logic in `mlflow.pyfunc.PythonModel`
# MAGIC    ([section 01, nb 03](../01-model-development/03_pyfunc_and_point_in_time_features.py))
# MAGIC    with a signature/`input_example`.
# MAGIC 2. Register in UC; validate; set `@champion`.
# MAGIC 3. Serve the UC model on an endpoint; split traffic to roll out.
# MAGIC 4. Inference table → monitor → drift alert → retraining job — the whole
# MAGIC    professional loop in one sentence.
