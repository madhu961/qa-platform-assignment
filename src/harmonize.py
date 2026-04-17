from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def harmonize_records(resolved_df: DataFrame) -> DataFrame:
    return (
        resolved_df
        .withColumn(
            "canonical_address",
            F.coalesce(F.col("address"), F.lit(""))
        )
        .withColumn(
            "activity_places_final",
            F.coalesce(F.col("activity_places"), F.lit(""))
        )
        .withColumn(
            "top_suppliers_final",
            F.coalesce(F.col("top_suppliers"), F.lit(""))
        )
        .withColumn(
            "main_customers_final",
            F.coalesce(F.col("main_customers"), F.lit(""))
        )
        .withColumn(
            "revenue_final",
            F.coalesce(F.col("revenue").cast("double"), F.lit(0.0))
        )
        .withColumn(
            "profit_final",
            F.coalesce(F.col("profit").cast("double"), F.lit(0.0))
        )
        .select(
            "corporate_id",
            F.col("canonical_name"),
            F.col("canonical_address"),
            F.col("activity_places_final").alias("activity_places"),
            F.col("top_suppliers_final").alias("top_suppliers"),
            F.col("main_customers_final").alias("main_customers"),
            F.col("revenue_final").alias("revenue"),
            F.col("profit_final").alias("profit"),
            "match_confidence",
            F.col("corporate_name_s1").isNotNull().alias("source1_present"),
            F.col("corporate_name_s2").isNotNull().alias("source2_present"),
            F.current_timestamp().alias("last_updated_ts"),
            F.coalesce(F.col("batch_id"), F.lit("unknown")).alias("batch_id"),
            F.col("match_type")
        )
    )
