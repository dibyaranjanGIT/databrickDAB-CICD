"""Unit tests for transformations/payments.py."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from decimal import Decimal

from transformations.payments import clean_payments

PAYMENT_SCHEMA = StructType([
    StructField("payment_id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("payment_method", StringType()),
])


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_payments").getOrCreate()


def test_non_positive_amount_is_dropped(spark):
    data = [(1, 100, Decimal("0.00"), "UPI")]
    df = spark.createDataFrame(data, PAYMENT_SCHEMA)

    result = clean_payments(df).collect()

    assert len(result) == 0


def test_null_payment_method_is_dropped(spark):
    data = [
        (1, 100, Decimal("500.00"), None),
        (2, 101, Decimal("300.00"), "upi"),
    ]
    df = spark.createDataFrame(data, PAYMENT_SCHEMA)

    result = clean_payments(df).collect()

    assert len(result) == 1
    assert result[0]["payment_id"] == 2


def test_payment_method_is_upper_and_trimmed(spark):
    data = [(1, 100, Decimal("500.00"), "  upi  ")]
    df = spark.createDataFrame(data, PAYMENT_SCHEMA)

    result = clean_payments(df).collect()[0]

    assert result["payment_method"] == "UPI"