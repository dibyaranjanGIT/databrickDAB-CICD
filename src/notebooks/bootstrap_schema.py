# Databricks notebook source
# MAGIC %md
# MAGIC ## bootstrap_schema
# MAGIC Runs schema/table DDL as part of the bundle deploy, instead of requiring
# MAGIC someone to remember to paste SQL into the editor by hand. Safe to re-run --
# MAGIC uses CREATE IF NOT EXISTS, and only seeds control.pipeline_control if it's
# MAGIC genuinely empty, so this never wipes existing watermark state.

# COMMAND ----------
import sys, os

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
if not notebook_path.startswith("/Workspace"):
    notebook_path = "/Workspace" + notebook_path

# This notebook lives at .../files/src/notebooks/bootstrap_schema. sql/ is a
# SIBLING of src/ (repo root: databricks.yml, resources/, src/, sql/, tests/),
# not inside it -- so this needs three levels up from the notebook path:
#   dirname #1: .../files/src/notebooks   (strips "bootstrap_schema")
#   dirname #2: .../files/src             (strips "notebooks")
#   dirname #3: .../files                 (strips "src" -- this is the one that was missing)
sql_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(notebook_path))), "sql")
print(f"Reading DDL from: {sql_dir}")

# COMMAND ----------
# MAGIC %md ### Run the idempotent DDL file

# COMMAND ----------
ddl_file = os.path.join(sql_dir, "09_bootstrap_schema_idempotent.sql")

with open(ddl_file) as f:
    script = f.read()

# Strip full-line SQL comments BEFORE splitting on ";" -- splitting the raw
# text directly is what caused the earlier bug: a semicolon inside an
# English sentence in a comment ("...runs; this comment...") got treated as
# a statement boundary, since split(";") has no idea what a SQL comment is.
# Removing "--" lines first means a stray semicolon in a comment can never
# fool the splitter again.
lines = [ln for ln in script.split("\n") if not ln.strip().startswith("--")]
script_no_comments = "\n".join(lines)

for statement in script_no_comments.split(";"):
    statement = statement.strip()
    if statement:
        spark.sql(statement)

print("Schema/table DDL applied.")

# COMMAND ----------
# MAGIC %md ### Seed control.pipeline_control ONLY if it's empty
# MAGIC This is the guard that makes seeding idempotent -- checked in Python,
# MAGIC not embedded as a conditional in the SQL file itself.

# COMMAND ----------
row_count = spark.table("cdc_demo.control.pipeline_control").count()

if row_count == 0:
    print("Control table is empty -- seeding initial tier rows.")
    spark.sql("""
        INSERT INTO cdc_demo.control.pipeline_control VALUES
         ('cdc_demo.source_sim.customers',     'cdc_demo.bronze.customers',     1, 'updated_at', 'customer_id',   TIMESTAMP('1900-01-01'), true),
         ('cdc_demo.source_sim.products',      'cdc_demo.bronze.products',      1, 'updated_at', 'product_id',    TIMESTAMP('1900-01-01'), true),
         ('cdc_demo.source_sim.categories',    'cdc_demo.bronze.categories',    1, 'updated_at', 'category_id',   TIMESTAMP('1900-01-01'), true),
         ('cdc_demo.source_sim.orders',        'cdc_demo.bronze.orders',        2, 'updated_at', 'order_id',      TIMESTAMP('1900-01-01'), true),
         ('cdc_demo.source_sim.order_items',   'cdc_demo.bronze.order_items',   2, 'updated_at', 'order_item_id', TIMESTAMP('1900-01-01'), true),
         ('cdc_demo.source_sim.payments',      'cdc_demo.bronze.payments',      2, 'updated_at', 'payment_id',    TIMESTAMP('1900-01-01'), true),
         ('cdc_demo.source_sim.shipments',     'cdc_demo.bronze.shipments',     3, 'updated_at', 'shipment_id',   TIMESTAMP('1900-01-01'), true),
         ('cdc_demo.source_sim.returns',       'cdc_demo.bronze.returns',       3, 'updated_at', 'return_id',     TIMESTAMP('1900-01-01'), true),
         ('cdc_demo.source_sim.refunds',       'cdc_demo.bronze.refunds',       3, 'updated_at', 'refund_id',     TIMESTAMP('1900-01-01'), true),
         ('cdc_demo.source_sim.inventory_log', 'cdc_demo.bronze.inventory_log', 3, 'updated_at', 'log_id',        TIMESTAMP('1900-01-01'), true)
    """)
    print("Seeded 10 rows.")
else:
    print(f"Control table already has {row_count} rows -- skipping seed, nothing changed.")