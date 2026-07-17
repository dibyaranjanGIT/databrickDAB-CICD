"""Silver-layer transformation for products.

Denormalizes category_name onto each product row so the gold-layer
dim_product build doesn't need to join categories a second time.
"""
from pyspark.sql import DataFrame, functions as F


def clean_products(bronze_products: DataFrame, bronze_categories: DataFrame) -> DataFrame:
    """Join products to their category name; standardize text fields.

    Uses a left join and coalesces to "Uncategorized" rather than an inner
    join, so a product referencing a not-yet-loaded or deleted category
    doesn't silently disappear from silver.
    """
    categories = bronze_categories.select(
        "category_id",
        F.trim(F.col("category_name")).alias("category_name"),
    )

    return (
        bronze_products
        .withColumn("product_name", F.trim(F.col("product_name")))
        .join(categories, "category_id", "left")
        .withColumn("category_name", F.coalesce(F.col("category_name"), F.lit("Uncategorized")))
    )
