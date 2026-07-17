# Databricks notebook source
# MAGIC %md
# MAGIC ## orchestrate_silver
# MAGIC Orchestration only. All business logic lives in the `transformations`
# MAGIC package and is imported below -- this notebook just wires
# MAGIC read -> transform -> write and logs row counts.

# COMMAND ----------
import sys, os

# Resolve the `src` folder dynamically from this notebook's own deployed path,
# instead of hardcoding a workspace path. This matters once deployment happens
# via a bundle (CI/CD): the deployed path differs per target (dev vs prod) and
# per deploying user, e.g.
#   /Workspace/Users/<you>/.bundle/cdc_pipeline/dev/files/src/notebooks/orchestrate_silver
# A hardcoded path would only work for one specific target/user.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
src_root = os.path.dirname(os.path.dirname(notebook_path))  # .../files/src
sys.path.append(src_root)

from transformations.customers import clean_customers
from transformations.products import clean_products
from transformations.orders import clean_orders, clean_order_items
from transformations.payments import clean_payments

# COMMAND ----------
# MAGIC %md ### Read bronze

# COMMAND ----------
bronze_customers   = spark.table("cdc_demo.bronze.customers")
bronze_products    = spark.table("cdc_demo.bronze.products")
bronze_categories  = spark.table("cdc_demo.bronze.categories")
bronze_orders      = spark.table("cdc_demo.bronze.orders")
bronze_order_items = spark.table("cdc_demo.bronze.order_items")
bronze_payments    = spark.table("cdc_demo.bronze.payments")

# COMMAND ----------
# MAGIC %md ### Transform (delegates entirely to transformations/*.py)

# COMMAND ----------
silver_customers   = clean_customers(bronze_customers)
silver_products    = clean_products(bronze_products, bronze_categories)
silver_orders      = clean_orders(bronze_orders)
silver_order_items = clean_order_items(bronze_order_items)
silver_payments     = clean_payments(bronze_payments)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Write silver
# MAGIC Using `overwrite` for this demo since bronze itself is small and fully
# MAGIC re-cleanable each run. In a real production silver layer, once bronze
# MAGIC grows large, you'd switch this to a MERGE keyed on the natural key
# MAGIC (same pattern as the bronze loader's MERGE) so silver only touches
# MAGIC changed rows instead of rewriting the whole table every run.

# COMMAND ----------
silver_customers.write.format("delta").mode("overwrite").saveAsTable("cdc_demo.silver.customers")
silver_products.write.format("delta").mode("overwrite").saveAsTable("cdc_demo.silver.products")
silver_orders.write.format("delta").mode("overwrite").saveAsTable("cdc_demo.silver.orders")
silver_order_items.write.format("delta").mode("overwrite").saveAsTable("cdc_demo.silver.order_items")
silver_payments.write.format("delta").mode("overwrite").saveAsTable("cdc_demo.silver.payments")

print("Silver layer refreshed:")
for t in ["customers", "products", "orders", "order_items", "payments"]:
    cnt = spark.table(f"cdc_demo.silver.{t}").count()
    print(f"  silver.{t}: {cnt} rows")
