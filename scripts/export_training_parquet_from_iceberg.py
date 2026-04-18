import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.feature_engineering import add_ml_features


REQUIRED_COLUMNS = [
    "top_suppliers",
    "main_customers",
    "activity_places",
    "revenue",
    "profit",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--glue-database", required=True)
    parser.add_argument("--glue-table", required=True)
    parser.add_argument("--output-parquet", required=True)
    parser.add_argument("--profit-threshold", type=float, default=1000000.0)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("export-training-parquet-from-iceberg")
        .getOrCreate()
    )

    table_ref = f"glue_catalog.{args.glue_database}.{args.glue_table}"
    df = spark.read.format("iceberg").load(table_ref)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in Iceberg table: {missing}")

    # Ensure string list columns are non-null
    for c in ["top_suppliers", "main_customers", "activity_places"]:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit("")))

    # Ensure numeric columns are non-null
    df = (
        df.withColumn("revenue", F.coalesce(F.col("revenue").cast("double"), F.lit(0.0)))
          .withColumn("profit", F.coalesce(F.col("profit").cast("double"), F.lit(0.0)))
    )

    featured = add_ml_features(df, profit_threshold=args.profit_threshold)

    selected_cols = [
        c for c in [
            "entity_id",
            "company_key",
            "corporate_name",
            "address",
            "revenue",
            "profit",
            "top_suppliers",
            "main_customers",
            "activity_places",
            "num_top_suppliers",
            "num_main_customers",
            "num_activity_places",
            "label",
        ] if c in featured.columns
    ]

    training_df = featured.select(*selected_cols)

    os.makedirs(os.path.dirname(args.output_parquet), exist_ok=True)
    training_df.write.mode("overwrite").parquet(args.output_parquet)

    print(f"Read Iceberg table: {table_ref}")
    print(f"Exported parquet: {args.output_parquet}")
    print(f"Row count: {training_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
