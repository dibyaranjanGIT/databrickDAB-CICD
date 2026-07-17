"""Gold-layer dimension builders.

Each function returns a Type-1 (current-state, overwrite-safe) dimension
table with a stable surrogate key. Surrogate keys are a deterministic hash
of the natural key -- NOT monotonically_increasing_id() -- so re-running
the build on the same customer_id/product_id always produces the same
surrogate key. That stability is what lets fact_orders join against these
dimensions correctly across repeated/incremental runs.
"""
from pyspark.sql import DataFrame, functions as F


def build_dim_customer(silver_customers: DataFrame) -> DataFrame:
    """Type-1 customer dimension: one current row per customer_id."""
    return (
        silver_customers
        .withColumn("customer_sk", F.sha2(F.col("customer_id").cast("string"), 256))
        .select("customer_sk", "customer_id", "name", "city", "is_city_missing")
    )


def build_dim_product(silver_products: DataFrame) -> DataFrame:
    """Type-1 product dimension, including denormalized category_name."""
    return (
        silver_products
        .withColumn("product_sk", F.sha2(F.col("product_id").cast("string"), 256))
        .select("product_sk", "product_id", "product_name", "category_id", "category_name")
    )


def build_dim_date(spark, start_date: str, end_date: str) -> DataFrame:
    """Standard date dimension spanning [start_date, end_date] inclusive."""
    return (
        spark.sql(
            f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), "
            f"interval 1 day)) AS full_date"
        )
        .withColumn("date_sk", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
        .select("date_sk", "full_date", "year", "month", "day", "day_of_week", "is_weekend")
    )
