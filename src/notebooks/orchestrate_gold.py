# Databricks notebook source
# MAGIC %md
# MAGIC ## orchestrate_gold
# MAGIC Orchestration only. Builds the star schema (dim_customer, dim_product,
# MAGIC dim_date, fact_orders) by calling functions from `transformations/`.

# COMMAND ----------
import sys, os

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

if not notebook_path.startswith("/Workspace"):
    notebook_path = "/Workspace" + notebook_path

src_root = os.path.dirname(os.path.dirname(notebook_path))  # .../files/src
sys.path.append(src_root)
print(f"Added to sys.path: {src_root}")

from transformations.dimensions import build_dim_customer, build_dim_product, build_dim_date
from transformations.facts import build_fact_orders

# COMMAND ----------
# MAGIC %md ### Read silver

# COMMAND ----------
silver_customers   = spark.table("cdc_demo.silver.customers")
silver_products    = spark.table("cdc_demo.silver.products")
silver_orders      = spark.table("cdc_demo.silver.orders")
silver_order_items = spark.table("cdc_demo.silver.order_items")
silver_payments    = spark.table("cdc_demo.silver.payments")

# COMMAND ----------
# MAGIC %md ### Build dimensions and fact

# COMMAND ----------
dim_customer = build_dim_customer(silver_customers)
dim_product  = build_dim_product(silver_products)
dim_date     = build_dim_date(spark, "2026-01-01", "2026-12-31")

fact_orders = build_fact_orders(
    silver_orders, silver_order_items, silver_payments, dim_customer, dim_product
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Write gold
# MAGIC Dimensions and the fact are all overwritten here since this demo
# MAGIC recomputes gold fully from silver each run. At production data volumes,
# MAGIC dim_customer/dim_product would typically switch to MERGE (Type-1) or
# MAGIC a proper SCD Type 2 pattern if history needs to be preserved, and
# MAGIC fact_orders would be partitioned by order_date_sk and merged
# MAGIC incrementally rather than rewritten in full each run.

# COMMAND ----------
dim_customer.write.format("delta").mode("overwrite").saveAsTable("cdc_demo.gold.dim_customer")
dim_product.write.format("delta").mode("overwrite").saveAsTable("cdc_demo.gold.dim_product")
dim_date.write.format("delta").mode("overwrite").saveAsTable("cdc_demo.gold.dim_date")
fact_orders.write.format("delta").mode("overwrite").saveAsTable("cdc_demo.gold.fact_orders")

print("Gold layer refreshed:")
for t in ["dim_customer", "dim_product", "dim_date", "fact_orders"]:
    cnt = spark.table(f"cdc_demo.gold.{t}").count()
    print(f"  gold.{t}: {cnt} rows")