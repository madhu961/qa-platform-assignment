from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityException(Exception):
    pass


def assert_min_row_count(df: DataFrame, min_rows: int, source_name: str) -> None:
    count = df.count()
    if count < min_rows:
        raise DataQualityException(f"{source_name} row count too low: {count} < {min_rows}")


def assert_non_null_column(df: DataFrame, column_name: str, source_name: str) -> None:
    bad = df.filter(F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")).count()
    if bad > 0:
        raise DataQualityException(f"{source_name} has {bad} null/blank values in {column_name}")


def assert_non_negative_revenue(df: DataFrame, source_name: str) -> None:
    if "revenue" not in df.columns:
        return
    bad = df.filter(F.col("revenue") < 0).count()
    if bad > 0:
        raise DataQualityException(f"{source_name} has {bad} negative revenue rows")


def assert_reasonable_revenue_distribution(df: DataFrame, source_name: str) -> None:
    if "revenue" not in df.columns:
        return

    stats = df.select(
        F.min("revenue").alias("min_revenue"),
        F.max("revenue").alias("max_revenue"),
        F.avg("revenue").alias("avg_revenue")
    ).collect()[0]

    if stats["max_revenue"] is not None and stats["avg_revenue"] is not None:
        if stats["max_revenue"] > stats["avg_revenue"] * 1000:
            raise DataQualityException(
                f"{source_name} failed suspicious revenue distribution check"
            )


def run_source1_checks(df: DataFrame, min_rows: int) -> None:
    assert_min_row_count(df, min_rows, "source1")
    assert_non_null_column(df, "corporate_name_s1", "source1")


def run_source2_checks(df: DataFrame, min_rows: int) -> None:
    assert_min_row_count(df, min_rows, "source2")
    assert_non_null_column(df, "corporate_name_s2", "source2")
    assert_non_negative_revenue(df, "source2")
    assert_reasonable_revenue_distribution(df, "source2")
