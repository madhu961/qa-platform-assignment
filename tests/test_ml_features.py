
import pytest
from pyspark.sql import SparkSession

from src.feature_engineering import add_ml_features


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("pytest-ml").getOrCreate()
    yield spark
    spark.stop()


def test_add_ml_features(spark):
    df = spark.createDataFrame(
        [
            ("c1", "Acme", "NY", "Globex|Initech", "RetailCo|BuildIt", 1000000.0, 200000.0),
            ("c2", "Globex", "CA", "Initech", "MegaMart", 500000.0, 20000.0),
        ],
        ["corporate_id", "canonical_name", "activity_places", "top_suppliers", "main_customers", "revenue", "profit"],
    )

    result = add_ml_features(df, profit_threshold=100000)
    rows = result.select("num_top_suppliers", "num_main_customers", "num_activity_places", "label").collect()

    assert rows[0]["num_top_suppliers"] == 2
    assert rows[0]["num_main_customers"] == 2
    assert rows[0]["label"] == 1
    assert rows[1]["label"] == 0
