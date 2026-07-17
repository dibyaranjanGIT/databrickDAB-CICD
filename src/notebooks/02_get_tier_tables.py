# Databricks notebook source
# MAGIC %md
# MAGIC ## get_tier_tables
# MAGIC Reads the rows for a given `tier` from `control.pipeline_control` and publishes
# MAGIC them as task values so the downstream `for_each_task` can fan out over them.
# MAGIC
# MAGIC Called once per tier by the Workflow (see 03_databricks_workflow_job.json):
# MAGIC `get_tier1_tables`, `get_tier2_tables`, `get_tier3_tables` all point at this
# MAGIC same notebook, just with a different `tier` parameter.

# COMMAND ----------
dbutils.widgets.text("tier", "1")
tier = int(dbutils.widgets.get("tier"))

# COMMAND ----------
rows = spark.sql(f"""
    SELECT source_table, target_table, watermark_col, primary_key,
           CAST(last_loaded_ts AS STRING) AS last_loaded_ts
    FROM control.pipeline_control
    WHERE tier = {tier} AND is_active = true
""").collect()

if len(rows) == 0:
    raise ValueError(f"No active tables found for tier {tier} -- check pipeline_control")

# COMMAND ----------
import json

control_rows = [json.dumps(r.asDict()) for r in rows]
print(f"Tier {tier}: {len(control_rows)} tables -> {[r['source_table'] for r in [json.loads(x) for x in control_rows]]}")

# this is what load_tier_N's for_each_task.inputs reads via
# {{tasks.get_tierN_tables.values.control_rows}}
dbutils.jobs.taskValues.set(key="control_rows", value=control_rows)
