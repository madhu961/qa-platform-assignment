import pytest
from pyspark.sql import SparkSession

from src.quality_checks import (
    DataQualityException,
    run_source1_checks,
    run_source2_checks,
)


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("pytest-spark").getOrCreate()
    yield spark
    spark.stop()


def test_source1_null_name_fails(spark):
    df = spark.createDataFrame(
        [
            ("", "123 Main St", "NY|CA", "Globex|Initech"),
            ("Acme Corp", "124 Main St", "NY", "Globex"),
        ],
        ["corporate_name_s1", "address", "activity_places", "top_suppliers"],
    )

    with pytest.raises(DataQualityException):
        run_source1_checks(df, min_rows=1)


def test_source2_negative_revenue_fails(spark):
    df = spark.createDataFrame(
        [
            ("Acme Corp", "RetailCo", -10.0, 5.0),
            ("Globex Ltd", "MegaMart", 100.0, 10.0),
        ],
        ["corporate_name_s2", "main_customers", "revenue", "profit"],
    )

    with pytest.raises(DataQualityException):
        run_source2_checks(df, min_rows=1)
