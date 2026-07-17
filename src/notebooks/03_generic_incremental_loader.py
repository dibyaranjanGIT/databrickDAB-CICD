# Databricks notebook source
# MAGIC %md
# MAGIC ## generic_incremental_loader
# MAGIC One generic notebook handles all 10 tables. The `for_each_task` in the Workflow
# MAGIC calls this once per table, passing that table's row from `pipeline_control` as
# MAGIC a JSON string via the `control_row` widget.
# MAGIC
# MAGIC Steps: pull incremental rows (JDBC watermark pushdown, or the demo's Delta
# MAGIC stand-in) -> MERGE into bronze -> advance the watermark in the control table.

# COMMAND ----------
dbutils.widgets.text("control_row", "")

import json
from pyspark.sql import functions as F

row = json.loads(dbutils.widgets.get("control_row"))

source_table   = row["source_table"]
target_table   = row["target_table"]
watermark_col  = row["watermark_col"]
primary_key    = row["primary_key"]
last_loaded_ts = row["last_loaded_ts"]

print(f"Loading {source_table} -> {target_table}  (watermark > {last_loaded_ts})")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Pull incremental data
# MAGIC `USE_REAL_JDBC = True` is the code you'd actually run against your production
# MAGIC database. The `else` branch is only here so this demo runs end-to-end using
# MAGIC the `source_sim` Delta tables created in step 01, with no external DB needed.

# COMMAND ----------
USE_REAL_JDBC = False  # flip to True once pointed at your real JDBC source

if USE_REAL_JDBC:
    jdbc_url  = dbutils.secrets.get("de_grp_scope", "jdbc_url")
    jdbc_user = dbutils.secrets.get("de_grp_scope", "jdbc_user")
    jdbc_pwd  = dbutils.secrets.get("de_grp_scope", "jdbc_password")

    push_down_query = (
        f"(SELECT * FROM {source_table} "
        f"WHERE {watermark_col} > '{last_loaded_ts}') AS src"
    )

    incremental_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", push_down_query)
        .option("user", jdbc_user)
        .option("password", jdbc_pwd)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("fetchsize", "10000")
        .option("numPartitions", "4")
        .option("partitionColumn", primary_key)
        .load()
    )
else:
    incremental_df = (
        spark.table(source_table)
        .filter(F.col(watermark_col) > F.lit(last_loaded_ts))
    )

row_count = incremental_df.count()
print(f"Pulled {row_count} changed rows")

# COMMAND ----------
# MAGIC %md ### Merge into bronze (idempotent upsert on primary key)

# COMMAND ----------
if row_count > 0:
    if not spark.catalog.tableExists(target_table):
        incremental_df.write.format("delta").saveAsTable(target_table)
    else:
        from delta.tables import DeltaTable
        target = DeltaTable.forName(spark, target_table)
        (target.alias("t")
            .merge(incremental_df.alias("s"), f"t.{primary_key} = s.{primary_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())

    new_watermark = incremental_df.agg(F.max(watermark_col)).collect()[0][0]

    spark.sql(f"""
        UPDATE control.pipeline_control
        SET last_loaded_ts = '{new_watermark}'
        WHERE source_table = '{source_table}'
    """)
    print(f"Watermark advanced to {new_watermark}")
else:
    print("No new rows -- watermark left unchanged")

# COMMAND ----------
dbutils.notebook.exit(json.dumps({"source_table": source_table, "rows_loaded": row_count}))
