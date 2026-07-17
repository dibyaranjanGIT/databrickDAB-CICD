"""Silver-layer transformation for payments."""
from pyspark.sql import DataFrame, functions as F


def clean_payments(bronze_payments: DataFrame) -> DataFrame:
    """Standardize payment_method casing; drop non-positive amounts."""
    return (
        bronze_payments
        .filter(F.col("amount") > 0)
        .withColumn("payment_method", F.upper(F.trim(F.col("payment_method"))))
    )
