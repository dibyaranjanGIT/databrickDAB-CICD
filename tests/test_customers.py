"""Unit test for transformations/customers.py -- runs locally with pytest,
no Databricks cluster required. This is exactly what you can't do with
logic embedded directly in a notebook cell.
"""
import pytest
from datetime import datetime
from pyspark.sql import SparkSession

from transformations.customers import clean_customers


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_customers").getOrCreate()


def test_dedup_keeps_latest_row_by_updated_at(spark):
    data = [
        (1, "asha rao", "bengaluru", datetime(2026, 1, 1)),
        (1, "Asha Rao", "Bengaluru", datetime(2026, 1, 5)),  # newer -- should win
    ]
    df = spark.createDataFrame(data, ["customer_id", "name", "city", "updated_at"])

    result = clean_customers(df).collect()

    assert len(result) == 1
    assert result[0]["updated_at"] == datetime(2026, 1, 5)


def test_name_and_city_are_standardized(spark):
    data = [(1, "  asha rao  ", "  bengaluru  ", datetime(2026, 1, 1))]
    df = spark.createDataFrame(data, ["customer_id", "name", "city", "updated_at"])

    result = clean_customers(df).collect()[0]

    assert result["name"] == "Asha Rao"
    assert result["city"] == "Bengaluru"


def test_missing_city_is_flagged_not_dropped(spark):
    data = [(2, "Ravi Nair", None, datetime(2026, 1, 1))]
    df = spark.createDataFrame(data, ["customer_id", "name", "city", "updated_at"])

    result = clean_customers(df).collect()

    assert len(result) == 1  # row is kept
    assert result[0]["is_city_missing"] is True
