"""Silver-layer transformation for the customer entity.

Pure functions -- input DataFrame(s) in, transformed DataFrame out.
No reads, no writes, no catalog references. This is what makes it unit
testable without a cluster (see tests/test_customers.py) and reusable
across any notebook or job that needs the same cleaning logic.
"""
from pyspark.sql import DataFrame, functions as F, Window


def clean_customers(bronze_customers: DataFrame) -> DataFrame:
    """Dedupe, standardize, and flag customers coming out of bronze.

    - Keeps only the latest row per customer_id (by updated_at), collapsing
      any CDC updates that landed as multiple physical rows in bronze.
    - Trims whitespace and title-cases name/city so joins and grouping in
      gold aren't broken by inconsistent casing ("bengaluru" vs "Bengaluru").
    - Flags rows with a null city rather than dropping them -- a customer
      with an unknown city is still a valid customer for order attribution,
      just incomplete for geographic reporting.
    """
    dedup_window = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

    return (
        bronze_customers
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("name", F.initcap(F.trim(F.col("name"))))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("is_city_missing", F.col("city").isNull())
    )
