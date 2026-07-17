"""Silver-layer transformations for orders and order line items."""
from pyspark.sql import DataFrame, functions as F

VALID_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]


def clean_orders(bronze_orders: DataFrame) -> DataFrame:
    """Standardize status values; drop orders with no customer reference.

    An order with a null customer_id can't be attached to dim_customer in
    gold -- it's filtered out here, in silver, rather than silently
    producing an orphaned (unjoinable) fact row later in gold.
    """
    return (
        bronze_orders
        .filter(F.col("customer_id").isNotNull())
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn(
            "status",
            F.when(F.col("status").isin(VALID_STATUSES), F.col("status"))
             .otherwise(F.lit("UNKNOWN")),
        )
    )


def clean_order_items(bronze_order_items: DataFrame) -> DataFrame:
    """Compute line_total; filter out non-positive quantities/prices.

    A qty <= 0 or price <= 0 almost always indicates a source data error
    or a cancellation recorded as a negative row -- surfaced here as a
    filter rather than silently propagated into a negative-revenue fact row.
    """
    return (
        bronze_order_items
        .filter((F.col("qty") > 0) & (F.col("price") > 0))
        .withColumn("line_total", F.round(F.col("qty") * F.col("price"), 2))
    )
