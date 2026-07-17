"""Gold-layer fact table builder: fact_orders.

Grain: one row per order line item (order_id + product_id combination).
"""
from pyspark.sql import DataFrame, functions as F


def build_fact_orders(
    silver_orders: DataFrame,
    silver_order_items: DataFrame,
    silver_payments: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
) -> DataFrame:
    """Join orders + order_items + payments down to dimension surrogate keys.

    Payments are aggregated to order level first, since an order can have
    multiple payment rows (e.g. partial/split payments) but the fact grain
    here is per line item, not per payment -- aggregating avoids fanning
    out line items by the number of payments on the order.
    """
    payments_by_order = silver_payments.groupBy("order_id").agg(
        F.sum("amount").alias("total_paid")
    )

    orders_enriched = (
        silver_orders.alias("o")
        .join(dim_customer.select("customer_id", "customer_sk"), "customer_id", "left")
        .join(payments_by_order, "order_id", "left")
    )

    return (
        silver_order_items.alias("oi")
        .join(orders_enriched, "order_id", "inner")
        .join(dim_product.select("product_id", "product_sk"), "product_id", "left")
        .withColumn("order_date_sk", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .select(
            "order_id",
            "order_item_id",
            "customer_sk",
            "product_sk",
            "order_date_sk",
            "status",
            "qty",
            "price",
            "line_total",
            F.coalesce("total_paid", F.lit(0.0)).alias("total_paid"),
        )
    )
