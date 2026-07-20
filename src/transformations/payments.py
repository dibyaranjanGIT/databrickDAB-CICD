"""Silver-layer transformation for payments."""
from pyspark.sql import DataFrame, functions as F


def clean_payments(bronze_payments: DataFrame) -> DataFrame:
    """Standardize payment_method casing; drop non-positive amounts and
    rows with no payment_method -- a payment with an unknown method can't
    be trusted for reconciliation against the source system, so it's
    filtered here rather than passed downstream as "UNKNOWN"."""
    return (
        bronze_payments
        .filter(F.col("amount") > 0)
        .filter(F.col("payment_method").isNotNull())
        .withColumn("payment_method", F.upper(F.trim(F.col("payment_method"))))
    )